import { test, expect, type Locator, type Page } from '@playwright/test';

const TIMEOUT_SHORT = 1500;
const STEP_WAIT_MS = 120;

type GridMetrics = {
  scrollTop: number;
  scrollLeft: number;
  maxTop: number;
  maxLeft: number;
  clientHeight: number;
  clientWidth: number;
};

type GridSnapshot = {
  visibleTextCount: number;
  rowLikeCount: number;
  cellLikeCount: number;
  isBusy: boolean;
};

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

async function waitFrame(page: Page, frames = 1): Promise<void> {
  await page.evaluate(async (count: number) => {
    const waitOne = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
    for (let i = 0; i < count; i += 1) {
      await waitOne();
    }
  }, frames);
}

async function locateMainScrollableArea(page: Page): Promise<Locator | null> {
  const direct = await firstVisible([
    page.locator('[role="grid"]'),
    page.locator('[role="table"]'),
    page.locator('table'),
    page.locator('.ag-body-viewport'),
    page.locator('.ReactVirtualized__Grid'),
    page.locator('[data-testid*="grid"]'),
  ]);
  if (direct) return direct;

  const selector = await page.evaluate(() => {
    const MARK = 'data-e2e-scroll-probe';
    document.querySelectorAll(`[${MARK}]`).forEach((el) => el.removeAttribute(MARK));

    const all = Array.from(document.querySelectorAll<HTMLElement>('body *'));
    let best: HTMLElement | null = null;
    let bestScore = -1;

    for (const el of all) {
      const rect = el.getBoundingClientRect();
      if (rect.width < 300 || rect.height < 140) continue;

      const style = window.getComputedStyle(el);
      const canY = el.scrollHeight > el.clientHeight + 40 || /(auto|scroll|overlay)/i.test(style.overflowY);
      const canX = el.scrollWidth > el.clientWidth + 40 || /(auto|scroll|overlay)/i.test(style.overflowX);
      if (!canX && !canY) continue;

      const identity = `${el.id} ${String(el.className)} ${el.getAttribute('role') ?? ''}`;
      const hint = /(grid|table|viewport|virtual|body|content)/i.test(identity) ? 15000 : 0;
      const rowLike = el.querySelectorAll('tr,[role="row"],.ag-row,.rt-tr').length;
      const cellLike = el.querySelectorAll('td,[role="gridcell"],[role="cell"],.ag-cell').length;
      const score =
        hint +
        (rect.width * rect.height) +
        (Math.max(0, el.scrollHeight - el.clientHeight) * 10) +
        (Math.max(0, el.scrollWidth - el.clientWidth) * 10) +
        (rowLike * 30) +
        (cellLike * 10);

      if (score > bestScore) {
        best = el;
        bestScore = score;
      }
    }

    if (!best) return null;
    best.setAttribute(MARK, '1');
    return `[${MARK}="1"]`;
  });

  return selector ? page.locator(selector) : null;
}

async function readGridMetrics(grid: Locator): Promise<GridMetrics> {
  return grid.evaluate((node) => {
    const el = node as HTMLElement;
    return {
      scrollTop: el.scrollTop,
      scrollLeft: el.scrollLeft,
      maxTop: Math.max(0, el.scrollHeight - el.clientHeight),
      maxLeft: Math.max(0, el.scrollWidth - el.clientWidth),
      clientHeight: el.clientHeight,
      clientWidth: el.clientWidth,
    };
  });
}

async function setGridScroll(grid: Locator, top: number, left?: number): Promise<void> {
  await grid.evaluate(
    (node, payload) => {
      const el = node as HTMLElement;
      el.scrollTop = payload.top;
      if (typeof payload.left === 'number') {
        el.scrollLeft = payload.left;
      }
    },
    { top, left },
  );
}

async function readGridSnapshot(grid: Locator): Promise<GridSnapshot> {
  return grid.evaluate((node) => {
    const root = node as HTMLElement;
    const rootRect = root.getBoundingClientRect();

    const isVisible = (el: Element): boolean => {
      const htmlEl = el as HTMLElement;
      const style = window.getComputedStyle(htmlEl);
      if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) return false;
      const rect = htmlEl.getBoundingClientRect();
      if (rect.width < 1 || rect.height < 1) return false;
      const intersects =
        rect.bottom > rootRect.top &&
        rect.top < rootRect.bottom &&
        rect.right > rootRect.left &&
        rect.left < rootRect.right;
      return intersects;
    };

    const rowLikeCount = root.querySelectorAll('tr,[role="row"],.ag-row,.rt-tr,[data-row-index]').length;
    const cellLikeCount = root.querySelectorAll('td,[role="gridcell"],[role="cell"],.ag-cell').length;

    const preferredTextNodes = Array.from(
      root.querySelectorAll<HTMLElement>(
        'td,[role="gridcell"],[role="cell"],.ag-cell,th,[role="columnheader"],.ag-header-cell',
      ),
    );

    let visibleTextCount = 0;
    for (const el of preferredTextNodes) {
      if (!isVisible(el)) continue;
      const text = (el.innerText || el.textContent || '').trim();
      if (!text) continue;
      visibleTextCount += 1;
      if (visibleTextCount >= 200) break;
    }

    if (visibleTextCount === 0) {
      const genericTextNodes = Array.from(root.querySelectorAll<HTMLElement>('div,span'));
      for (const el of genericTextNodes) {
        if (!isVisible(el)) continue;
        const text = (el.innerText || el.textContent || '').trim();
        if (!text) continue;
        visibleTextCount += 1;
        if (visibleTextCount >= 200) break;
      }
    }

    const isBusy = Boolean(
      root.querySelector(
        '[aria-busy="true"], .loading, .spinner, .ant-spin-spinning, [data-loading="true"], [data-testid*="loading"]',
      ),
    );

    return {
      visibleTextCount,
      rowLikeCount,
      cellLikeCount,
      isBusy,
    };
  });
}

async function ensureMainUiLoaded(page: Page): Promise<void> {
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
}

async function runBasicInteractions(page: Page): Promise<void> {
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
}

async function assertVerticalScrollNoBlank(page: Page, grid: Locator): Promise<void> {
  const metrics = await readGridMetrics(grid);
  expect(metrics.maxTop, 'Нет вертикального диапазона прокрутки для проверки').toBeGreaterThan(80);

  const baseline = await readGridSnapshot(grid);
  expect(
    baseline.visibleTextCount + baseline.rowLikeCount + baseline.cellLikeCount,
    'До прокрутки в таблице не видно данных (нечего проверять)',
  ).toBeGreaterThan(0);

  const points = [0, 0.08, 0.2, 0.45, 0.7, 0.95, 0.55, 0.9, 0.15, 1];
  const blankHits: string[] = [];

  for (const ratio of points) {
    const targetTop = Math.round(metrics.maxTop * ratio);
    await setGridScroll(grid, targetTop);

    for (let i = 0; i < 5; i += 1) {
      await waitFrame(page, 1);
      await page.waitForTimeout(STEP_WAIT_MS);

      const snap = await readGridSnapshot(grid);
      const score = snap.visibleTextCount + snap.rowLikeCount + snap.cellLikeCount;
      if (score === 0 && !snap.isBusy) {
        blankHits.push(`top=${targetTop}, step=${i}`);
      }
    }
  }

  expect(blankHits, `При резкой вертикальной прокрутке данные исчезали: ${blankHits.join(' | ')}`).toEqual([]);
}

async function assertHorizontalScrollAvailable(grid: Locator): Promise<void> {
  const before = await readGridMetrics(grid);
  if (before.maxLeft <= 40) {
    return; // горизонтального переполнения нет — проверка не применима
  }

  await setGridScroll(grid, before.scrollTop, before.maxLeft);
  const afterRight = await readGridMetrics(grid);
  expect(afterRight.scrollLeft, 'Горизонтальная прокрутка не работает (не двигается вправо)').toBeGreaterThan(10);

  await setGridScroll(grid, afterRight.scrollTop, 0);
  const afterLeft = await readGridMetrics(grid);
  expect(afterLeft.scrollLeft, 'Горизонтальная прокрутка не возвращается влево').toBeLessThanOrEqual(2);
}

async function assertColumnsDragAttempt(page: Page): Promise<void> {
  const headers = page.locator('th,[role="columnheader"],.ag-header-cell');
  const count = await headers.count();
  if (count < 2) {
    return; // в текущем экране нет заголовков — не ломаем smoke
  }

  const first = headers.nth(0);
  const second = headers.nth(1);

  const firstVisibleOk = await first.isVisible().catch(() => false);
  const secondVisibleOk = await second.isVisible().catch(() => false);
  if (!firstVisibleOk || !secondVisibleOk) return;

  const beforeA = await first.boundingBox();
  const beforeB = await second.boundingBox();
  if (!beforeA || !beforeB) return;

  try {
    await first.dragTo(second, { timeout: 3000 });
  } catch {
    // fallback: manual drag gesture
    await first.hover({ timeout: 1500 });
    await page.mouse.down();
    await page.mouse.move(beforeB.x + beforeB.width / 2, beforeB.y + beforeB.height / 2, { steps: 10 });
    await page.mouse.up();
  }

  await page.waitForTimeout(250);

  const afterA = await first.boundingBox();
  const afterB = await second.boundingBox();
  if (!afterA || !afterB) return;

  const moved = Math.abs(afterA.x - beforeA.x) > 4 || Math.abs(afterB.x - beforeB.x) > 4;
  expect(moved, 'Перетаскивание столбцов не отрабатывает (позиции заголовков не меняются)').toBeTruthy();
}

async function assertCategoryAndLogsVisible(page: Page): Promise<void> {
  await stepSafe(page, async () => {
    await clickFirstVisible([
      page.getByRole('tab', { name: /лог/i }),
      page.getByRole('button', { name: /лог/i }),
    ]);
  });

  const bodyText = (await page.locator('body').innerText()).toLowerCase();
  const hasCategory = bodyText.includes('категор') || bodyText.includes('category');
  const hasLogs = bodyText.includes('лог') || bodyText.includes('log');

  expect(hasCategory, 'Не видно отображения категории в интерфейсе').toBeTruthy();
  expect(hasLogs, 'Не видно отображения логов в интерфейсе').toBeTruthy();
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

    await ensureMainUiLoaded(page);
    await runBasicInteractions(page);

    await stepSafe(page, async () => {
      const grid = await locateMainScrollableArea(page);

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

  test('user-like scrollbar and table checks (no app fixes, only detection)', async ({ page }) => {
    await ensureMainUiLoaded(page);
    await runBasicInteractions(page);

    const grid = await locateMainScrollableArea(page);
    expect(grid, 'Не найден основной скроллируемый блок таблицы').not.toBeNull();
    if (!grid) return;

    await grid.hover();
    await assertVerticalScrollNoBlank(page, grid);
    await assertHorizontalScrollAvailable(grid);
    await assertColumnsDragAttempt(page);
    await assertCategoryAndLogsVisible(page);
  });
});
