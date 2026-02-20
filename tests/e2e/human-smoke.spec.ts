import { test, expect, Page, Locator } from '@playwright/test';

const NON_DESTRUCTIVE_BUTTON_BLACKLIST = /(удал|delete|remove|reset|сброс|drop|clear all|очист|logout|выйти|exit|close app|quit)/i;

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

  throw new Error(
    `Не найден доступный URL для UI. Укажи E2E_BASE_URL (проверены: ${candidates.join(', ')})`,
  );
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

    const generic = page.locator(`text=${pattern.source}`).first();
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

async function assertAggressiveVerticalScrollNoBlank(page: Page): Promise<void> {
  const base = await probePrimaryScrollable(page);
  if (!base.hasTarget || base.maxTop <= 8) return;

  const jumps = [
    Math.floor(base.maxTop * 0.98),
    Math.floor(base.maxTop * 0.03),
    Math.floor(base.maxTop * 0.90),
    Math.floor(base.maxTop * 0.12),
    Math.floor(base.maxTop * 0.80),
    Math.floor(base.maxTop * 0.22),
    Math.floor(base.maxTop * 0.70),
    Math.floor(base.maxTop * 0.05),
    base.maxTop,
    0,
  ];

  let maxBlankStreakMs = 0;
  const examples: string[] = [];

  for (const top of jumps) {
    await probePrimaryScrollable(page, { top });

    const started = Date.now();
    let blankStreak = 0;

    while (Date.now() - started < 1800) {
      const snap = await probePrimaryScrollable(page);
      const hasData = (snap.rowLike > 0 || snap.cellLike > 0 || snap.textLen > 40) && !snap.busy;

      if (hasData) {
        blankStreak = 0;
      } else if (!snap.busy) {
        blankStreak += 60;
        maxBlankStreakMs = Math.max(maxBlankStreakMs, blankStreak);
        if (examples.length < 10) {
          examples.push(`top=${top}, t=${Date.now() - started}ms`);
        }
      }

      await page.waitForTimeout(60);
    }
  }

  expect(
    maxBlankStreakMs,
    `Данные пропадали при агрессивной вертикальной прокрутке (рывки вниз/вверх). Макс. пустой интервал: ${maxBlankStreakMs}мс. Примеры: ${examples.join(' | ')}`,
  ).toBe(0);
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


test('human smoke: UI usage (aggressive scroll, columns, logs, category)', async ({ page }) => {
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

  // Переходы по типичным вкладкам/экранам + проверки, что экраны реально открылись
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

  // Ввод в поиск / фильтр
  const searchInput = await firstVisible(
    page.locator([
      'input[type="search"]',
      'input[placeholder*="Поиск"]',
      'input[placeholder*="поиск"]',
      'input[placeholder*="Search"]',
      'input[placeholder*="search"]',
      'input',
    ].join(',')),
    20,
  );

  if (searchInput) {
    await searchInput.fill('test');
    await page.keyboard.press('Enter').catch(() => {});
    await page.waitForTimeout(300);
    await searchInput.fill('');
    await page.waitForTimeout(200);
  }

  // Кнопка фильтров / панель фильтра
  await clickByTexts(page, [/фильтр/i, /filters?/i], 2);

  // Работа с селектами (если есть)
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

  // Колонки / меню таблицы
  await clickByTexts(page, [/колонк/i, /columns?/i, /вид/i, /display/i], 2);

  // Сортировка по заголовкам таблицы (1-2 клика)
  const header = await firstVisible(
    page.locator('th, [role="columnheader"], .ag-header-cell, .rt-th'),
    20,
  );
  if (header) {
    await safeClick(header);
    await page.waitForTimeout(250);
    await safeClick(header);
    await page.waitForTimeout(250);
  }

  // Несколько безопасных кнопок (не удаление)
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
      // Закрываем модалку, если появилась
      await page.keyboard.press('Escape').catch(() => {});
    }
  }

  // Базовый скролл страницы + агрессивная проверка таблицы/списка (рывки вниз/вверх)
  await page.evaluate(() => {
    window.scrollTo({ top: 0 });
    window.scrollTo({ top: document.body.scrollHeight });
    window.scrollTo({ top: 0 });
  });

  await assertAggressiveVerticalScrollNoBlank(page);
  await assertHorizontalScrollAlwaysReachable(page);

  // Проверка, что UI живой и не пустой после действий/скролла
  const visibleRows = await page.locator('table tr, [role="row"], .ag-row').count().catch(() => 0);
  const visibleText = (await page.locator('body').innerText()).trim();
  expect(visibleText.length).toBeGreaterThan(0);

  // Скрин на успех (для артефактов)
  await page.screenshot({ path: 'test-results/human-smoke-success.png', fullPage: true }).catch(() => {});

  // Не валим по единичным шумным консольным предупреждениям, но валим по pageerror
  expect(pageErrors, `Uncaught page errors:\n${pageErrors.join('\n')}`).toEqual([]);

  // Если совсем ничего не нашли в таблице и нет типичных экранов — тоже сигнализируем
  if (visibleRows === 0 && !/товар|products|лог|settings|настро/i.test(visibleText)) {
    throw new Error('UI открылся, но не найдено ожидаемых элементов (таблица/экраны). Проверь селекторы/маршрут.');
  }

  // Сохраняем как мягкую диагностику (не ломаем, если есть одиночные console.error от внешних библиотек)
  test.info().annotations.push({
    type: 'console-errors',
    description: consoleErrors.slice(0, 10).join(' | ') || 'none',
  });
});
