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

type AggressiveScrollProbe = {
  foundTarget: boolean;
  targetTag?: string;
  targetClass?: string;
  targetClientHeight?: number;
  targetClientWidth?: number;
  targetScrollHeight?: number;
  targetScrollWidth?: number;
  verticalHadBlankFrames: boolean;
  verticalMaxBlankMs: number;
  verticalWorstObservedTextLen: number;
  horizontalChanged: boolean;
  samples: Array<{
    step: number;
    axis: 'y' | 'x';
    blankMs: number;
    textLen: number;
    rows: number;
    cells: number;
    top: number;
    left: number;
  }>;
};

async function aggressiveScrollProbe(page: Page): Promise<AggressiveScrollProbe> {
  return page.evaluate(async () => {
    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
    const raf = () => new Promise((r) => requestAnimationFrame(() => r(null)));

    const styleOverflow = (el: HTMLElement) => {
      const cs = window.getComputedStyle(el);
      return {
        x: /(auto|scroll)/.test(cs.overflowX),
        y: /(auto|scroll)/.test(cs.overflowY),
      };
    };

    const all = Array.from(document.querySelectorAll<HTMLElement>('body *'));
    const candidates = all
      .filter((el) => {
        if (!el.isConnected) return false;
        const rect = el.getBoundingClientRect();
        if (rect.width < 120 || rect.height < 80) return false;
        const ov = styleOverflow(el);
        const canY = ov.y && el.scrollHeight > el.clientHeight + 40;
        const canX = ov.x && el.scrollWidth > el.clientWidth + 40;
        return canY || canX;
      })
      .sort((a, b) => {
        const as = a.clientWidth * a.clientHeight;
        const bs = b.clientWidth * b.clientHeight;
        return bs - as;
      });

    const target = candidates[0];
    if (!target) {
      return {
        foundTarget: false,
        verticalHadBlankFrames: false,
        verticalMaxBlankMs: 0,
        verticalWorstObservedTextLen: 0,
        horizontalChanged: false,
        samples: [],
      } satisfies AggressiveScrollProbe;
    }

    const snap = () => {
      const rows = target.querySelectorAll('tr, [role="row"], .ag-row').length;
      const cells = target.querySelectorAll('td, [role="gridcell"], .ag-cell, .cell, .row').length;
      const txt = (target.innerText || '').replace(/\s+/g, ' ').trim();
      return { rows, cells, textLen: txt.length };
    };

    // Прогрев
    target.scrollTop = 0;
    target.scrollLeft = 0;
    await raf();
    await sleep(30);

    const samples: AggressiveScrollProbe['samples'] = [];
    let verticalMaxBlankMs = 0;
    let verticalWorstObservedTextLen = Number.MAX_SAFE_INTEGER;

    const yMax = Math.max(0, target.scrollHeight - target.clientHeight);
    const xMax = Math.max(0, target.scrollWidth - target.clientWidth);

    // Агрессивные рывки по вертикали: как пользователь резко тянет ползунок туда-сюда
    if (yMax > 0) {
      const yPositions = [yMax, 0, yMax, 0, yMax, 0, Math.floor(yMax * 0.6), Math.floor(yMax * 0.2), yMax, 0];

      for (let i = 0; i < yPositions.length; i += 1) {
        target.scrollTop = yPositions[i];

        const started = performance.now();
        let blankMs = 0;
        let bestTextLen = 0;
        let ok = false;

        // Проверяем сразу и потом быстро опрашиваем ~350мс, чтобы поймать "пустой экран"
        for (let t = 0; t < 22; t += 1) {
          await raf();
          const s = snap();
          bestTextLen = Math.max(bestTextLen, s.textLen);

          const hasContent = s.rows > 0 || s.cells > 10 || s.textLen > 20;
          if (hasContent) {
            blankMs = Math.round(performance.now() - started);
            verticalWorstObservedTextLen = Math.min(verticalWorstObservedTextLen, s.textLen);
            samples.push({
              step: i,
              axis: 'y',
              blankMs,
              textLen: s.textLen,
              rows: s.rows,
              cells: s.cells,
              top: target.scrollTop,
              left: target.scrollLeft,
            });
            verticalMaxBlankMs = Math.max(verticalMaxBlankMs, blankMs);
            ok = true;
            break;
          }
          await sleep(16);
        }

        if (!ok) {
          // Контент так и не появился за окно наблюдения
          blankMs = Math.round(performance.now() - started);
          verticalWorstObservedTextLen = Math.min(verticalWorstObservedTextLen, bestTextLen);
          samples.push({
            step: i,
            axis: 'y',
            blankMs,
            textLen: bestTextLen,
            rows: 0,
            cells: 0,
            top: target.scrollTop,
            left: target.scrollLeft,
          });
          verticalMaxBlankMs = Math.max(verticalMaxBlankMs, blankMs);
        }
      }
    }

    // Горизонталь: проверяем, что реально скроллится туда-сюда
    let horizontalChanged = false;
    if (xMax > 0) {
      const before = target.scrollLeft;
      target.scrollLeft = xMax;
      await raf();
      await sleep(20);
      const atMax = target.scrollLeft;
      target.scrollLeft = 0;
      await raf();
      await sleep(20);
      const back = target.scrollLeft;
      horizontalChanged = atMax > before || back < atMax;

      const s = snap();
      samples.push({
        step: 0,
        axis: 'x',
        blankMs: 0,
        textLen: s.textLen,
        rows: s.rows,
        cells: s.cells,
        top: target.scrollTop,
        left: target.scrollLeft,
      });
    }

    return {
      foundTarget: true,
      targetTag: target.tagName,
      targetClass: target.className || '',
      targetClientHeight: target.clientHeight,
      targetClientWidth: target.clientWidth,
      targetScrollHeight: target.scrollHeight,
      targetScrollWidth: target.scrollWidth,
      verticalHadBlankFrames: verticalMaxBlankMs > 150,
      verticalMaxBlankMs,
      verticalWorstObservedTextLen: Number.isFinite(verticalWorstObservedTextLen) ? verticalWorstObservedTextLen : 0,
      horizontalChanged,
      samples,
    } satisfies AggressiveScrollProbe;
  });
}

test('human smoke: open app, click around, type, filter, sort, aggressive scroll', async ({ page }) => {
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
      await page.keyboard.press('Escape').catch(() => {});
    }
  }

  // Агрессивная проверка скролла (рывки вниз/вверх) + горизонталь
  const scrollProbe = await aggressiveScrollProbe(page);
  test.info().annotations.push({
    type: 'scroll-probe',
    description: JSON.stringify(scrollProbe).slice(0, 1500),
  });

  // Проверка, что UI живой и не пустой после действий/скролла
  const visibleRows = await page.locator('table tr, [role="row"], .ag-row').count().catch(() => 0);
  const visibleText = (await page.locator('body').innerText()).trim();
  expect(visibleText.length).toBeGreaterThan(0);

  // Скрин на успех/состояние (для артефактов)
  await page.screenshot({ path: 'test-results/human-smoke-success.png', fullPage: true }).catch(() => {});

  // Не валим по единичным шумным консольным предупреждениям, но валим по pageerror
  expect(pageErrors, `Uncaught page errors:\n${pageErrors.join('\n')}`).toEqual([]);

  if (visibleRows === 0 && !/товар|products|лог|settings|настро/i.test(visibleText)) {
    throw new Error('UI открылся, но не найдено ожидаемых элементов (таблица/экраны). Проверь селекторы/маршрут.');
  }

  if (!scrollProbe.foundTarget) {
    throw new Error('Не найден прокручиваемый контейнер для smoke-проверки скролла.');
  }

  // Ключевая проверка: после резких рывков контент должен появляться практически мгновенно.
  // 150мс — мягкий предел для UI; 1-2 секунды гарантированно поймается как fail.
  expect(
    scrollProbe.verticalMaxBlankMs,
    `Слишком долгое исчезновение данных при агрессивной вертикальной прокрутке: ${scrollProbe.verticalMaxBlankMs}мс`,
  ).toBeLessThanOrEqual(150);

  // Горизонтальный скролл не обязателен на любом экране, но если контент шире контейнера — он должен двигаться.
  const horizontalNeeded = (scrollProbe.targetScrollWidth || 0) > (scrollProbe.targetClientWidth || 0) + 20;
  if (horizontalNeeded) {
    expect(scrollProbe.horizontalChanged, 'Горизонтальная прокрутка есть, но не двигается').toBeTruthy();
  }

  test.info().annotations.push({
    type: 'console-errors',
    description: consoleErrors.slice(0, 10).join(' | ') || 'none',
  });
});
