import AxeBuilder from '@axe-core/playwright';
import { expect, type Page } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import path from 'node:path';

/** The throwaway key the e2e stack was started with (compose.e2e.yaml). */
export function operatorKey(): string {
  const key = process.env.STREAMFLOW_E2E_OPERATOR_KEY;
  if (!key) {
    throw new Error('Set STREAMFLOW_E2E_OPERATOR_KEY to the key the e2e stack was started with');
  }
  return key;
}

const repoRoot = path.resolve(import.meta.dirname, '..', '..');

/** Runs SQL in the stack's PostgreSQL (test set-up only: fault scenarios). */
export function psql(sql: string): string {
  return execFileSync(
    'docker',
    [
      'compose',
      '-f',
      'compose.yaml',
      '-f',
      'compose.e2e.yaml',
      'exec',
      '-T',
      'postgres',
      'psql',
      '-U',
      'streamflow',
      '-d',
      'streamflow',
      '-tAc',
      sql,
    ],
    { cwd: repoRoot, encoding: 'utf8' },
  ).trim();
}

/** No serious or critical axe violations (WCAG 2.2 A/AA rules). */
export async function expectAccessible(page: Page) {
  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa'])
    .analyze();
  const blocking = results.violations.filter(
    (v) => v.impact === 'serious' || v.impact === 'critical',
  );
  expect(
    blocking.map(
      (v) => `${v.id}: ${v.help} (${v.nodes.map((n) => n.target.join(' ')).join(', ')})`,
    ),
  ).toEqual([]);
}

export async function expectNoHorizontalScroll(page: Page) {
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
  );
  expect(overflow).toBeLessThanOrEqual(1);
}

export async function waitForLive(page: Page) {
  await expect(page.getByRole('status').filter({ hasText: /^Live/ })).toBeVisible({
    timeout: 20_000,
  });
}

export async function unlock(page: Page) {
  const dialog = page.getByRole('dialog', { name: 'Unlock operator actions' });
  await expect(dialog).toBeVisible();
  await dialog.getByLabel('Operator key').fill(operatorKey());
  await dialog.getByRole('button', { name: 'Unlock' }).click();
}
