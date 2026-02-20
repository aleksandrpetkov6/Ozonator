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

async function clickByTexts(page: Page, patterns: RegExp[], maxClicks = 2) {
  let clicks = 0;
  for (const pattern of patterns) {
    if (clicks >= maxClicks) return;
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
}

test('human smoke: open app, click around, type, filter, sort, scroll', async ({ page }) => {
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

  // Переходы по типичным вкладкам/экранам
  await clickByTexts(page, [/товар/i, /products?/i, /каталог/i], 2);
  await clickByTexts(page, [/лог/i, /logs?/i, /истори/i], 1);
  await clickByTexts(page, [/настро/i, /settings?/i], 1);
  await clickByTexts(page, [/товар/i, /products?/i], 1);

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

  // Вертикальный и горизонтальный скролл (главная боль)
  await page.evaluate(() => {
    window.scrollTo({ top: 0 });
    window.scrollTo({ top: document.body.scrollHeight });
    window.scrollTo({ top: 0 });

    const candidates = Array.from(document.querySelectorAll<HTMLElement>('*'))
      .filter((el) => {
        const cs = window.getComputedStyle(el);
        const y = /(auto|scroll)/.test(cs.overflowY);
        const x = /(auto|scroll)/.test(cs.overflowX);
        return (y || x) && (el.scrollHeight > el.clientHeight || el.scrollWidth > el.clientWidth);
      })
      .sort((a, b) => (b.clientWidth * b.clientHeight) - (a.clientWidth * a.clientHeight));

    const target = candidates[0];
    if (target) {
      target.scrollTop = Math.max(0, target.scrollHeight);
      target.scrollTop = 0;
      target.scrollLeft = Math.max(0, target.scrollWidth);
      target.scrollLeft = 0;
    }
  });

  await page.waitForTimeout(500);

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
