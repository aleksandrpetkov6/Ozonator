import { test, expect, type Locator, type Page } from '@playwright/test';

const TIMEOUT_SHORT = 1500;
async function firstVisible(list: readonly Locator[], index = 0): Promise<Locator | null> {
  if (index >= list.length) return null;

  try {
    if (await list[index].isVisible({ timeout: TIMEOUT_SHORT })) {
      return list[index];
    }
  } catch {
    // ignore and continue
  }

  return firstVisible(list, index + 1);
}

async function clickFirstVisible(candidates: readonly Locator[]): Promise<boolean> {
  const target = await firstVisible(candidates);
  if (!target) return false;

  try {
    await target.click({ timeout: TIMEOUT_SHORT });
    return true;
  } catch {
    return false;
  }
}

async function fillFirstVisible(candidates: readonly Locator[], value: string): Promise<boolean> {
  const target = await firstVisible(candidates);
  if (!target) return false;

  try {
    await target.fill(value, { timeout: TIMEOUT_SHORT });
    return true;
  } catch {
    return false;
  }
}

async function pressIfVisible(page: Page, key: string): Promise<void> {
  try {
    await page.keyboard.press(key);
  } catch {
    // ignore
  }
}

async function stepSafe(page: Page, action: () => Promise<void>): Promise<void> {
  try {
    await action();
    await page.waitForLoadState('domcontentloaded', { timeout: TIMEOUT_SHORT });
  } catch {
    // keep smoke test resilient
  }
}

test.describe('Human smoke UI interactions', () => {
  test('click / scroll / filter / sort / columns', async ({ page }) => {
    const pageErrors: string[] = [];

    page.on('pageerror', (error) => {
      pageErrors.push(String(error));
    });

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        pageErrors.push(msg.text());
      }
    });

    await page.goto('/', { waitUntil: 'domcontentloaded', timeout: 30000 });
    await expect(page.locator('body')).toBeVisible({ timeout: 10000 });

    await stepSafe(page, async () => {
      await clickFirstVisible([
        page.getByRole('button', { name: /закрыть|close|ok|понятно/i }),
        page.getByRole('button', { name: /принять|accept/i }),
      ]);
    });

    await stepSafe(page, async () => {
      await clickFirstVisible([
        page.getByRole('tab').first(),
        page.locator('[role="tab"]').first(),
      ]);
    });

    await stepSafe(page, async () => {
      await fillFirstVisible(
        [
          page.getByPlaceholder(/поиск|search/i),
          page.getByRole('searchbox'),
          page.locator('input[type="search"]'),
          page.locator('input').first(),
        ],
        'test',
      );
    });

    await stepSafe(page, async () => {
      await pressIfVisible(page, 'Control+A');
      await pressIfVisible(page, 'Backspace');
    });

    await stepSafe(page, async () => {
      await clickFirstVisible([
        page.getByRole('button', { name: /фильтр|filter/i }),
        page.getByRole('button', { name: /сорт|sort/i }),
        page.getByRole('button', { name: /колон|столб/i }),
      ]);
    });

    await stepSafe(page, async () => {
      await clickFirstVisible([
        page.locator('th').first(),
        page.locator('[role="columnheader"]').first(),
      ]);
    });

    await stepSafe(page, async () => {
      const grid = await firstVisible([
        page.locator('[role="grid"]'),
        page.locator('[role="table"]'),
        page.locator('table'),
        page.locator('.ag-body-viewport'),
        page.locator('.ReactVirtualized__Grid'),
      ]);

      if (grid) {
        await grid.hover();
        await page.mouse.wheel(0, 900);
        await page.mouse.wheel(0, -500);
        await page.mouse.wheel(900, 0);
        await page.mouse.wheel(-500, 0);
      } else {
        await page.mouse.wheel(0, 900);
        await page.mouse.wheel(0, -500);
      }
    });

    await stepSafe(page, async () => {
      await clickFirstVisible([
        page.getByRole('button', { name: /добавить столбец|add column/i }),
        page.getByRole('button', { name: /колонки|columns/i }),
      ]);
    });

    await stepSafe(page, async () => {
      await clickFirstVisible([
        page.getByRole('checkbox').first(),
        page.locator('input[type="checkbox"]').first(),
      ]);
    });

    await stepSafe(page, async () => {
      await clickFirstVisible([
        page.getByRole('button', { name: /применить|apply|сохранить|save/i }),
        page.getByRole('button', { name: /закрыть|close/i }),
      ]);
    });

    await expect(page.locator('body')).toBeVisible({ timeout: 5000 });
    await expect(pageErrors, `UI errors:\n${pageErrors.join('\n')}`).toEqual([]);
  });
});
