import { expect, test } from '@playwright/test';
import { expectAccessible, expectNoHorizontalScroll, waitForLive } from './support';

const SECTIONS = [
  { path: '/', heading: 'Overview' },
  { path: '/live', heading: 'Live event stream' },
  { path: '/kafka', heading: 'Kafka & processing' },
  { path: '/analytics', heading: 'Analytics' },
  { path: '/dead-letters', heading: 'Dead letters' },
  { path: '/simulations', heading: 'Simulations' },
  { path: '/system', heading: 'System status' },
];

test.describe('every section', () => {
  for (const section of SECTIONS) {
    test(`${section.heading} renders live backend data and passes axe`, async ({ page }) => {
      await page.goto(section.path);
      await expect(page.getByRole('heading', { level: 1, name: section.heading })).toBeVisible();
      await waitForLive(page);
      await expect(page.getByText('Could not load this data')).toHaveCount(0);
      await expectAccessible(page);
    });
  }
});

test.describe('responsive layout', () => {
  for (const viewport of [
    { name: 'phone', width: 375, height: 812 },
    { name: 'tablet', width: 768, height: 1024 },
    { name: 'desktop', width: 1440, height: 900 },
  ]) {
    test(`no horizontal page scroll on a ${viewport.name}`, async ({ page }) => {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      for (const section of SECTIONS) {
        await page.goto(section.path);
        await expect(page.getByRole('heading', { level: 1, name: section.heading })).toBeVisible();
        await expectNoHorizontalScroll(page);
      }
    });
  }
});

test('keyboard: skip link, primary navigation and the unlock dialog', async ({ page }) => {
  await page.goto('/');
  await page.keyboard.press('Tab');
  const skip = page.getByRole('link', { name: 'Skip to content' });
  await expect(skip).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(page.locator('#main')).toBeFocused();

  // Reach "Dead letters" in the primary navigation by keyboard alone.
  await page.getByRole('link', { name: 'Overview' }).focus();
  for (let i = 0; i < 4; i++) {
    await page.keyboard.press('Tab');
  }
  await expect(page.getByRole('link', { name: 'Dead letters' })).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(page.getByRole('heading', { level: 1, name: 'Dead letters' })).toBeVisible();

  // The unlock dialog traps focus in its field and closes on Escape without unlocking.
  await page.getByRole('button', { name: 'Read-only · Unlock' }).focus();
  await page.keyboard.press('Enter');
  const dialog = page.getByRole('dialog', { name: 'Unlock operator actions' });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByLabel('Operator key')).toBeFocused();
  await page.keyboard.press('Escape');
  await expect(dialog).toBeHidden();
  await expect(page.getByRole('button', { name: 'Read-only · Unlock' })).toBeVisible();
});

test('reduced motion stops the live indicator animation', async ({ page }) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.goto('/');
  await waitForLive(page);
  const animation = await page
    .locator('.live-dot')
    .first()
    .evaluate((el) => getComputedStyle(el).animationName);
  expect(animation).toBe('none');
});

test('the live feed pushes about once per second and every headline value comes from the API', async ({
  page,
}) => {
  const frames: number[] = [];
  page.on('websocket', (socket) => {
    socket.on('framereceived', (frame) => {
      if (typeof frame.payload === 'string' && frame.payload.includes('"snapshot"')) {
        frames.push(Date.now());
      }
    });
  });
  await page.goto('/kafka');
  await waitForLive(page);
  await page.waitForTimeout(5_000);
  expect(frames.length).toBeGreaterThanOrEqual(3);
  expect(frames.length).toBeLessThanOrEqual(8);

  // With the pipeline idle, the page shows exactly what the API reports.
  const live = await (await page.request.get('/api/v1/live')).json();
  const records = page.getByText('Records in topic', { exact: true }).locator('..');
  await expect(records).toContainText(
    new Intl.NumberFormat('en-US').format(live.kafka.logEndOffset),
  );
  const members = page.getByText('Members', { exact: true }).locator('..');
  await expect(members).toContainText(String(live.kafka.members));
});

test('the dashboard recovers from a dropped live connection with a fresh REST snapshot', async ({
  page,
}) => {
  // Record the page's sockets so the test can drop them as a network failure would.
  await page.addInitScript(() => {
    const Native = window.WebSocket;
    const sockets: WebSocket[] = [];
    (window as unknown as { __sockets: WebSocket[] }).__sockets = sockets;
    window.WebSocket = class extends Native {
      constructor(url: string | URL, protocols?: string | string[]) {
        super(url, protocols);
        sockets.push(this);
      }
    };
  });
  await page.goto('/');
  await waitForLive(page);

  const resync = page.waitForResponse((r) => r.url().endsWith('/api/v1/live') && r.ok());
  const reconnected = page.waitForEvent('websocket');
  await page.evaluate(() =>
    (window as unknown as { __sockets: WebSocket[] }).__sockets.forEach((s) =>
      s.close(4001, 'test'),
    ),
  );
  await expect(page.getByRole('status').filter({ hasText: /^Reconnecting/ })).toBeVisible();
  await reconnected;
  await resync;
  await waitForLive(page);
});
