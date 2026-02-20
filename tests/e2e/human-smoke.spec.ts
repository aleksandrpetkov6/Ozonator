import { test, expect, Page, Locator } from '@playwright/test';
import { mkdir, writeFile } from 'node:fs/promises';

const NON_DESTRUCTIVE_BUTTON_BLACKLIST = /(удал|delete|remove|reset|сброс|drop|clear all|очист|logout|выйти|exit|close app|quit)/i;

test.use({
  screenshot: 'on',
  trace: 'on',
  video: 'on',
});

async function firstVisible(locator: Locator, max = 10): Promise<Locator | null> {
  const count = await locator.count();
  const limit = Math.min(count, max);
  for (let i = 0; i < limit; i += 1) {
    const item = locator.nth(i);
    if (await item.isVisible().catch(() => false)) return item;
  }
  return null;
}

async function safeClick(locator: Locator): Promise<boolean> {
  try {
    if (!(await locator.isVisible())) return false;
    if (!(await locator.isEnabled())) return false;
    await locator.click({ timeout: 2000 });
    return true;
  } catch {
    return false;
  }
}

async function resolveBaseUrl(): Promise<string> {
  const fromEnv = [
    process.env.E2E_BASE_URL,
    process.env.PLAYWRIGHT_BASE_URL,
    process.env.BASE_URL,
  ].filter(Boolean) as string[];

  const defaults = [
    'http://127.0.0.1:4173',
    'http://localhost:4173',
    'http://127.0.0.1:5173',
    'http://localhost:5173',
    'http://127.0.0.1:3000',
    'http://localhost:3000',
  ];

  const candidates = [...new Set([...fromEnv, ...defaults])];

  for (const url of candidates) {
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 1500);
      const res = await fetch(url, { method: 'GET', signal: ctrl.signal });
      clearTimeout(t);
      if (res.ok || (res.status >= 300 && res.status < 500)) return url;
    } catch {
      // try next
    }
  }

  throw new Error(`Не найден доступный URL для UI. Укажи E2E_BASE_URL (проверены: ${candidates.join(', ')})`);
}

async function clickByTexts(page: Page, patterns: RegExp[], maxClicks = 2): Promise<number> {
  let clicks = 0;
  for (const pattern of patterns) {
    if (clicks >= maxClicks) return clicks;

    const byRoleButton = page.getByRole('button', { name: pattern }).first();
    if (await safeClick(byRoleButton)) {
      clicks += 1;
      await page.waitForTimeout(200);
      continue;
    }

    const byRoleTab = page.getByRole('tab', { name: pattern }).first();
    if (await safeClick(byRoleTab)) {
      clicks += 1;
      await page.waitForTimeout(200);
      continue;
    }

    const generic = page.getByText(pattern).first();
    if (await safeClick(generic)) {
      clicks += 1;
      await page.waitForTimeout(200);
    }
  }
  return clicks;
}

type ScrollProbe = {
  hasTarget: boolean;
  scrollTop: number;
  scrollLeft: number;
  maxTop: number;
  maxLeft: number;
  rowLike: number;
  cellLike: number;
  textLen: number;
  busy: boolean;
};

type ScrollMeta = ScrollProbe & {
  rect: { x: number; y: number; width: number; height: number } | null;
};

async function probePrimaryScrollable(page: Page, move?: { top?: number; left?: number }): Promise<ScrollProbe> {
  return page.evaluate((moveArg) => {
    const candidates = Array.from(document.querySelectorAll<HTMLElement>('*'))
      .filter((el) => {
        const cs = window.getComputedStyle(el);
        const y = /(auto|scroll)/.test(cs.overflowY);
        const x = /(auto|scroll)/.test(cs.overflowX);
        return (y || x) && (el.scrollHeight > el.clientHeight || el.scrollWidth > el.clientWidth);
      })
      .sort((a, b) => (b.clientWidth * b.clientHeight) - (a.clientWidth * a.clientHeight));

    const target = candidates[0];
    if (!target) {
      return {
        hasTarget: false,
        scrollTop: 0,
        scrollLeft: 0,
        maxTop: 0,
        maxLeft: 0,
        rowLike: 0,
        cellLike: 0,
        textLen: 0,
        busy: false,
      };
    }

    if (typeof moveArg?.top === 'number') target.scrollTop = moveArg.top;
    if (typeof moveArg?.left === 'number') target.scrollLeft = moveArg.left;

    const rowLike = target.querySelectorAll('tr, [role="row"], .ag-row').length;
    const cellLike = target.querySelectorAll('td, [role="gridcell"], .ag-cell').length;
    const textLen = (target.innerText || '').trim().length;
    const busy =
      !!target.querySelector('[aria-busy="true"], .loading, .loader, .spinner') ||
      /загрузка|loading|подожд/i.test((target.innerText || '').slice(0, 300));

    return {
      hasTarget: true,
      scrollTop: target.scrollTop,
      scrollLeft: target.scrollLeft,
      maxTop: Math.max(0, target.scrollHeight - target.clientHeight),
      maxLeft: Math.max(0, target.scrollWidth - target.clientWidth),
      rowLike,
      cellLike,
      textLen,
      busy,
    };
  }, move ?? {});
}

async function getPrimaryScrollableMeta(page: Page): Promise<ScrollMeta> {
  return page.evaluate(() => {
    const candidates = Array.from(document.querySelectorAll<HTMLElement>('*'))
      .filter((el) => {
        const cs = window.getComputedStyle(el);
        const y = /(auto|scroll)/.test(cs.overflowY);
        const x = /(auto|scroll)/.test(cs.overflowX);
        return (y || x) && (el.scrollHeight > el.clientHeight || el.scrollWidth > el.clientWidth);
      })
      .sort((a, b) => (b.clientWidth * b.clientHeight) - (a.clientWidth * a.clientHeight));

    const target = candidates[0];
    if (!target) {
      return {
        hasTarget: false,
        scrollTop: 0,
        scrollLeft: 0,
        maxTop: 0,
        maxLeft: 0,
        rowLike: 0,
        cellLike: 0,
        textLen: 0,
        busy: false,
        rect: null,
      };
    }

    const r = target.getBoundingClientRect();
    const rowLike = target.querySelectorAll('tr, [role="row"], .ag-row').length;
    const cellLike = target.querySelectorAll('td, [role="gridcell"], .ag-cell').length;
    const textLen = (target.innerText || '').trim().length;
    const busy =
      !!target.querySelector('[aria-busy="true"], .loading, .loader, .spinner') ||
      /загрузка|loading|подожд/i.test((target.innerText || '').slice(0, 300));

    return {
      hasTarget: true,
      scrollTop: target.scrollTop,
      scrollLeft: target.scrollLeft,
      maxTop: Math.max(0, target.scrollHeight - target.clientHeight),
      maxLeft: Math.max(0, target.scrollWidth - target.clientWidth),
      rowLike,
      cellLike,
      textLen,
      busy,
      rect: { x: r.x, y: r.y, width: r.width, height: r.height },
    };
  });
}

type VerticalAggressiveDebug = {
  dragSupported: boolean;
  dragAttempts: number;
  dragMovedCount: number;
  wheelAttempts: number;
  wheelMovedCount: number;
  jsJumpAttempts: number;
  maxBlankStreakMs: number;
  blankEvents: Array<{ mode: string; at: number; topBefore: number; topAfter: number; streakMs: number }>;
};

async function measureBlankWhileSettling(
  page: Page,
  mode: string,
  topBefore: number,
  topAfter: number,
  dbg: VerticalAggressiveDebug,
): Promise<void> {
  const started = Date.now();
  let blankStreak = 0;
  while (Date.now() - started < 1800) {
    const snap = await probePrimaryScrollable(page);
    const hasData = (snap.rowLike > 0 || snap.cellLike > 0 || snap.textLen > 40) && !snap.busy;
    if (hasData) {
      blankStreak = 0;
    } else if (!snap.busy) {
      blankStreak += 60;
      if (blankStreak > dbg.maxBlankStreakMs) dbg.maxBlankStreakMs = blankStreak;
      if (blankStreak >= 120 && dbg.blankEvents.length < 20) {
        dbg.blankEvents.push({
          mode,
          at: Date.now() - started,
          topBefore,
          topAfter,
          streakMs: blankStreak,
        });
      }
    }
    await page.waitForTimeout(60);
  }
}

async function assertAggressiveVerticalScrollNoBlank(page: Page): Promise<VerticalAggressiveDebug> {
  const dbg: VerticalAggressiveDebug = {
    dragSupported: false,
    dragAttempts: 0,
    dragMovedCount: 0,
    wheelAttempts: 0,
    wheelMovedCount: 0,
    jsJumpAttempts: 0,
    maxBlankStreakMs: 0,
    blankEvents: [],
  };

  const base = await getPrimaryScrollableMeta(page);
  if (!base.hasTarget || base.maxTop <= 8 || !base.rect) return dbg;

  const rect = base.rect;
  const vp = page.viewportSize() ?? { width: 1280, height: 720 };

  // Пытаемся воспроизвести именно сценарий пользователя: резкий drag за правый край (область полосы прокрутки).
  const dragX = Math.max(2, Math.min(Math.round(rect.x + rect.width - 3), vp.width - 2));
  const topY = Math.max(4, Math.round(rect.y + 12));
  const bottomY = Math.min(vp.height - 4, Math.round(rect.y + rect.height - 12));
  const midY = Math.round((topY + bottomY) / 2);

  const dragPairs: Array<[number, number]> = [
    [midY, bottomY],
    [bottomY - 8, topY],
    [topY + 8, bottomY],
    [bottomY - 8, topY + 4],
  ];

  for (const [fromY, toY] of dragPairs) {
    dbg.dragAttempts += 1;
    const before = await probePrimaryScrollable(page);
    await page.mouse.move(dragX, fromY);
    await page.mouse.down();
    await page.mouse.move(dragX, toY, { steps: 2 });
    await page.mouse.up();
    await page.waitForTimeout(50);
    const after = await probePrimaryScrollable(page);
    if (Math.abs(after.scrollTop - before.scrollTop) > 12) {
      dbg.dragSupported = true;
      dbg.dragMovedCount += 1;
    }
    await measureBlankWhileSettling(page, 'drag', before.scrollTop, after.scrollTop, dbg);
  }

  // Дополнительно — агрессивные колесом (быстро вниз/вверх) в зоне таблицы.
  const wheelX = Math.max(4, Math.min(Math.round(rect.x + rect.width / 2), vp.width - 4));
  const wheelY = Math.max(4, Math.min(Math.round(rect.y + rect.height / 2), vp.height - 4));
  await page.mouse.move(wheelX, wheelY);
  const wheelDeltas = [2200, -2200, 1800, -1800, 2600, -2600];
  for (const delta of wheelDeltas) {
    dbg.wheelAttempts += 1;
    const before = await probePrimaryScrollable(page);
    await page.mouse.wheel(0, delta);
    await page.waitForTimeout(30);
    const after = await probePrimaryScrollable(page);
    if (Math.abs(after.scrollTop - before.scrollTop) > 12) dbg.wheelMovedCount += 1;
    await measureBlankWhileSettling(page, 'wheel', before.scrollTop, after.scrollTop, dbg);
  }

  // Fallback для сред, где drag нативного скроллбара не перехватывается Playwright.
  const jumps = [
    Math.floor(base.maxTop * 0.98),
    Math.floor(base.maxTop * 0.03),
    Math.floor(base.maxTop * 0.9),
    Math.floor(base.maxTop * 0.1),
    base.maxTop,
    0,
  ];
  for (const top of jumps) {
    dbg.jsJumpAttempts += 1;
    const before = await probePrimaryScrollable(page);
    await probePrimaryScrollable(page, { top });
    const after = await probePrimaryScrollable(page);
    await measureBlankWhileSettling(page, 'js-jump', before.scrollTop, after.scrollTop, dbg);
  }

  // Жёсткая проверка: данные не должны пропадать заметно (более 150 мс подряд) при резких действиях.
  expect(
    dbg.maxBlankStreakMs,
    `Данные пропадали при агрессивной вертикальной прокрутке. maxBlank=${dbg.maxBlankStreakMs}ms; dragMoved=${dbg.dragMovedCount}/${dbg.dragAttempts}; wheelMoved=${dbg.wheelMovedCount}/${dbg.wheelAttempts}`,
  ).toBeLessThanOrEqual(150);

  return dbg;
}

async function assertHorizontalScrollAlwaysReachable(page: Page): Promise<void> {
  const base = await probePrimaryScrollable(page);
  if (!base.hasTarget || base.maxLeft <= 8) return;

  const left1 = Math.floor(base.maxLeft * 0.95);
  await probePrimaryScrollable(page, { left: left1 });
  const s1 = await probePrimaryScrollable(page);

  await probePrimaryScrollable(page, { left: 0 });
  const s2 = await probePrimaryScrollable(page);

  expect(s1.scrollLeft, 'Горизонтальная прокрутка не сдвигается вправо').toBeGreaterThan(0);
  expect(s2.scrollLeft, 'Горизонтальная прокрутка не возвращается влево').toBeLessThanOrEqual(2);
}

async function saveDebugJson(data: unknown): Promise<void> {
  await mkdir('test-results', { recursive: true });
  await writeFile('test-results/human-scroll-debug.json', JSON.stringify(data, null, 2), 'utf8');
}

test('human smoke: UI usage (aggressive scrollbar drag/wheel, columns, logs, category)', async ({ page }) => {
  const baseUrl = await resolveBaseUrl();

  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];

  page.on('pageerror', (err) => {
    pageErrors.push(String(err?.message || err));
  });

  page.on('console', (msg) => {
    if (msg.type() === 'error') {
      const t = msg.text();
      if (!/favicon|download the react devtools|source map/i.test(t)) {
        consoleErrors.push(t);
      }
    }
  });

  await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 30_000 });
  await page.waitForLoadState('networkidle', { timeout: 10_000 }).catch(() => {});
  await expect(page.locator('body')).toBeVisible();

  const productsOpen1 = await clickByTexts(page, [/товар/i, /products?/i, /каталог/i], 2);
  expect(productsOpen1, 'Не удалось открыть раздел товаров/каталога').toBeGreaterThan(0);

  const logsOpen = await clickByTexts(page, [/лог/i, /logs?/i, /истори/i], 1);
  expect(logsOpen, 'Не удалось открыть раздел логов').toBeGreaterThan(0);
  const logsBody = (await page.locator('body').innerText()).toLowerCase();
  expect(/лог|logs?|истори|history|журнал/.test(logsBody), 'Экран логов не отобразился').toBeTruthy();

  const settingsOpen = await clickByTexts(page, [/настро/i, /settings?/i], 1);
  expect(settingsOpen, 'Не удалось открыть раздел настроек').toBeGreaterThan(0);

  const productsOpen2 = await clickByTexts(page, [/товар/i, /products?/i], 1);
  expect(productsOpen2, 'Не удалось вернуться в раздел товаров').toBeGreaterThan(0);

  const categoryVisible = await page.getByText(/категор|category/i).first().isVisible().catch(() => false);
  expect(categoryVisible, 'Не найдено отображение категории (заголовок/метка)').toBeTruthy();

  const searchInput = await firstVisible(
    page.locator(
      [
        'input[type="search"]',
        'input[placeholder*="Поиск"]',
        'input[placeholder*="поиск"]',
        'input[placeholder*="Search"]',
        'input[placeholder*="search"]',
        'input',
      ].join(','),
    ),
    20,
  );
  if (searchInput) {
    await searchInput.fill('test');
    await page.keyboard.press('Enter').catch(() => {});
    await page.waitForTimeout(300);
    await searchInput.fill('');
    await page.waitForTimeout(200);
  }

  await clickByTexts(page, [/фильтр/i, /filters?/i], 2);

  const selectEl = await firstVisible(page.locator('select'), 10);
  if (selectEl) {
    const values = await selectEl.locator('option').evaluateAll((opts) =>
      opts.map((o) => (o as HTMLOptionElement).value).filter(Boolean),
    );
    if (values.length > 1) {
      await selectEl.selectOption(values[1]).catch(() => {});
      await page.waitForTimeout(200);
    }
  }

  await clickByTexts(page, [/колонк/i, /columns?/i, /вид/i, /display/i], 2);

  const header = await firstVisible(page.locator('th, [role="columnheader"], .ag-header-cell, .rt-th'), 20);
  if (header) {
    await safeClick(header);
    await page.waitForTimeout(250);
    await safeClick(header);
    await page.waitForTimeout(250);
  }

  const buttons = page.locator('button');
  const btnCount = Math.min(await buttons.count(), 20);
  let clicked = 0;
  for (let i = 0; i < btnCount && clicked < 4; i += 1) {
    const b = buttons.nth(i);
    const text = ((await b.innerText().catch(() => '')) || '').trim();
    if (!text) continue;
    if (NON_DESTRUCTIVE_BUTTON_BLACKLIST.test(text)) continue;
    if (await safeClick(b)) {
      clicked += 1;
      await page.waitForTimeout(200);
      await page.keyboard.press('Escape').catch(() => {});
    }
  }

  await page.evaluate(() => {
    window.scrollTo({ top: 0 });
    window.scrollTo({ top: document.body.scrollHeight });
    window.scrollTo({ top: 0 });
  });

  const verticalDebug = await assertAggressiveVerticalScrollNoBlank(page);
  await assertHorizontalScrollAlwaysReachable(page);

  const visibleRows = await page.locator('table tr, [role="row"], .ag-row').count().catch(() => 0);
  const visibleText = (await page.locator('body').innerText()).trim();
  expect(visibleText.length).toBeGreaterThan(0);

  const debugPayload = {
    baseUrl,
    timestamp: new Date().toISOString(),
    verticalScroll: verticalDebug,
    visibleRows,
    pageErrors,
    consoleErrors: consoleErrors.slice(0, 20),
  };

  await saveDebugJson(debugPayload);
  await test.info().attach('human-scroll-debug', {
    body: JSON.stringify(debugPayload, null, 2),
    contentType: 'application/json',
  });

  await page.screenshot({ path: 'test-results/human-smoke-success.png', fullPage: true }).catch(() => {});

  expect(pageErrors, `Uncaught page errors:\n${pageErrors.join('\n')}`).toEqual([]);

  if (visibleRows === 0 && !/товар|products|лог|settings|настро/i.test(visibleText)) {
    throw new Error('UI открылся, но не найдено ожидаемых элементов (таблица/экраны). Проверь селекторы/маршрут.');
  }

  test.info().annotations.push({
    type: 'console-errors',
    description: consoleErrors.slice(0, 10).join(' | ') || 'none',
  });
});
