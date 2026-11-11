import { expect, test, type Page } from '@playwright/test';
import { psql, unlock, waitForLive } from './support';

/**
 * Operator flows against the real stack, in order: a rejected and a cancelled start, then a seeded synthetic run
 * with one event type rejected by a fault scenario (producing dead letters), its reconciliation, and replay and
 * discard of those dead letters.
 */
test.describe.serial('operator flows', () => {
  test.beforeAll(() => {
    psql('DELETE FROM fault_scenarios');
  });
  test.afterAll(() => {
    psql('DELETE FROM fault_scenarios');
  });

  test('a start without the key changes nothing; a wrong key is rejected by the server', async ({
    page,
  }) => {
    await page.goto('/simulations');
    await waitForLive(page);
    const before = await (await page.request.get('/api/v1/simulations?limit=100')).json();

    await page.getByRole('button', { name: 'Start synthetic run' }).click();
    const dialog = page.getByRole('dialog', { name: 'Unlock operator actions' });
    await dialog.getByRole('button', { name: 'Cancel' }).click();
    await expect(dialog).toBeHidden();

    await page.getByRole('button', { name: 'Start synthetic run' }).click();
    await dialog.getByLabel('Operator key').fill('not-the-key');
    await dialog.getByRole('button', { name: 'Unlock' }).click();
    await expect(page.getByRole('alert')).toContainText('The operator key was rejected');
    await expect(page.getByRole('button', { name: 'Read-only · Unlock' })).toBeVisible();

    const after = await (await page.request.get('/api/v1/simulations?limit=100')).json();
    expect(after.length).toBe(before.length);
  });

  test('a synthetic run publishes, processes and reconciles with dead letters accounted for', async ({
    page,
  }) => {
    psql(
      "INSERT INTO fault_scenarios (kind, event_type, failures_per_record, expires_at) VALUES ('REJECT_EVENT_TYPE', 'APPLICATION_ERROR', 1, now() + interval '9 minutes')",
    );
    await page.goto('/simulations');
    await waitForLive(page);

    await page.getByLabel('Events', { exact: true }).fill('3000');
    await page.getByLabel('Rate (events/s)').fill('1000');
    await page.getByLabel('Seed', { exact: true }).fill('20261111');
    await page.getByRole('button', { name: 'Start synthetic run' }).click();
    await unlock(page);
    await expect(page.getByText(/started: 3,000 events at 1,000\/s, seed 20261111/)).toBeVisible();
    await expect(page.getByRole('button', { name: 'A run is in progress' })).toBeDisabled();

    const card = page.getByRole('region', { name: /^Run [0-9a-f]{8}/ });
    await expect(card.getByText('Finished')).toBeVisible({ timeout: 90_000 });
    await expect(card.getByText('Reconciliation', { exact: true }).locator('..')).toContainText(
      'PASS',
    );
    psql('DELETE FROM fault_scenarios');

    const published = card.getByRole('meter', { name: 'Published (acknowledged by Kafka)' });
    await expect(published).toHaveAttribute('aria-valuenow', '3000');
    const deadLetters = Number(
      (
        await card
          .getByText('Dead letters', { exact: true })
          .locator('..')
          .locator('dd')
          .innerText()
      ).replace(/,/g, ''),
    );
    expect(deadLetters).toBeGreaterThan(0);
    const processed = Number(
      (
        await card.getByText('Processed', { exact: true }).locator('..').locator('dd').innerText()
      ).replace(/,/g, ''),
    );
    expect(processed + deadLetters).toBe(3000);
  });

  test('dead letters can be replayed and discarded, each behind a confirmation', async ({
    page,
  }) => {
    await page.goto('/dead-letters');
    await waitForLive(page);
    const boxes = page.getByRole('checkbox', { name: /Select dead letter/ });
    await expect(boxes.first()).toBeEnabled();
    const firstName = await boxes.first().getAttribute('aria-label');
    const replayId = firstName?.match(/\d+/)?.[0];

    await boxes.first().check();
    await page.getByRole('button', { name: 'Replay selected (1)' }).click();
    const confirm = page.getByRole('dialog', { name: 'Replay 1 dead letter(s)?' });
    await confirm.getByRole('button', { name: 'Replay' }).click();
    await unlock(page);
    await expect(page.getByText('Replay finished: 1 of 1 republished.')).toBeVisible();
    await expect(page.getByText(`#${replayId}: Replayed`)).toBeVisible();

    // Now unlocked: discarding asks for confirmation and a reason, but not for the key again.
    const discardButton = page.getByRole('button', { name: /Discard dead letter/ }).first();
    const discardId = (await discardButton.getAttribute('aria-label'))?.match(/\d+/)?.[0];
    await discardButton.click();
    const dialog = page.getByRole('dialog', { name: `Discard dead letter #${discardId}?` });
    await expect(dialog.getByRole('button', { name: 'Discard permanently' })).toBeDisabled();
    await dialog.getByLabel('Reason (required)').fill('e2e: synthetic fault, not worth replaying');
    await dialog.getByRole('button', { name: 'Discard permanently' }).click();
    await expect(page.getByText(`Dead letter #${discardId} was discarded.`)).toBeVisible();

    const detail = await (await page.request.get(`/api/v1/dead-letters/${discardId}`)).json();
    expect(detail.summary.status).toBe('DISCARDED');
    await expectReplayed(page, Number(replayId));
  });
});

async function expectReplayed(page: Page, id: number) {
  await expect
    .poll(
      async () =>
        (await (await page.request.get(`/api/v1/dead-letters/${id}`)).json()).summary.status,
    )
    .toBe('REPLAYED');
}
