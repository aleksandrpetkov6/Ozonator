import { existsSync, rmSync, statSync } from 'fs'
import { createHash } from 'crypto'
import Database from 'better-sqlite3'
import type { ProductPlacementRow, ProductRow, StockViewRow } from '../types'
import { ensurePersistentStorageReady, getLifecycleMarkerPath, getPersistentDbPath } from './paths'

let db: Database.Database | null = null

const DEFAULT_LOG_RETENTION_DAYS = 30
const MAX_JSON_LEN = 20000
const MAX_API_JSON_LEN = 750000
const REINSTALL_UNINSTALL_SUPPRESS_WINDOW_MS = 10 * 60 * 1000

type AppLogType =
  | 'check_auth'
  | 'sync_products'
  | 'app_install'
  | 'app_update'
  | 'app_reinstall'
  | 'app_uninstall'
  | 'admin_settings'

type GridColsDataset = 'products' | 'sales' | 'returns' | 'stocks'

type GridColLayoutItem = {
  id: string
  w: number
  visible: boolean
}

const GRID_COLS_DATASETS = new Set<GridColsDataset>(['products', 'sales', 'returns', 'stocks'])
const GRID_COLS_KEY_PREFIX = 'grid_cols_layout:'

function dbPath() {
  return getPersistentDbPath()
}

function mustDb(): Database.Database {
  if (!db) throw new Error('DB not initialized (call ensureDb() first)')
  return db
}

function getSettingValue(key: string): string | null {
  const row = mustDb().prepare('SELECT value FROM app_settings WHERE key = ?').get(key) as { value?: string } | undefined
  return typeof row?.value === 'string' ? row.value : null
}

function setSettingValue(key: string, value: string) {
  mustDb().prepare(`
    INSERT INTO app_settings (key, value, updated_at)
    VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET
      value = excluded.value,
      updated_at = excluded.updated_at
  `).run(key, value, new Date().toISOString())
}

function parsePositiveInt(value: unknown, fallback: number): number {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  const i = Math.trunc(n)
  if (i <= 0) return fallback
  return i
}

function normalizeRetentionDays(value: unknown): number {
  const n = parsePositiveInt(value, DEFAULT_LOG_RETENTION_DAYS)
  return Math.min(3650, Math.max(1, n))
}

function safeJson(value: any): string | null {
  if (value == null) return null
  try {
    return JSON.stringify(value).slice(0, MAX_JSON_LEN)
  } catch {
    return JSON.stringify({ unserializable: true }).slice(0, MAX_JSON_LEN)
  }
}

function safeJsonWithLimit(value: any, limit: number): { text: string | null; truncated: boolean } {
  if (value == null) return { text: null, truncated: false }
  try {
    const raw = JSON.stringify(value)
    if (raw.length <= limit) return { text: raw, truncated: false }
    return { text: raw.slice(0, limit), truncated: true }
  } catch {
    const raw = JSON.stringify({ unserializable: true })
    return { text: raw.slice(0, limit), truncated: raw.length > limit }
  }
}

function sha256Hex(input: string): string {
  return createHash('sha256').update(input).digest('hex')
}

function makeApiRegistryKey(method: string, endpoint: string): string {
  return `${String(method ?? '').toUpperCase()} ${String(endpoint ?? '').trim()}`.trim()
}

function mergeJsonStringArray(existingJson: string | null | undefined, incoming: string[], maxLen = 32): string | null {
  const acc = new Set<string>()
  if (typeof existingJson === 'string' && existingJson.trim()) {
    try {
      const arr = JSON.parse(existingJson)
      if (Array.isArray(arr)) {
        for (const v of arr) {
          const s = String(v ?? '').trim()
          if (s) acc.add(s)
        }
      }
    } catch {}
  }
  for (const v of incoming) {
    const s = String(v ?? '').trim()
    if (s) acc.add(s)
    if (acc.size >= maxLen) break
  }
  return JSON.stringify(Array.from(acc).slice(0, maxLen))
}

function inferEntityHintFromEndpoint(endpoint: string): string | null {
  const parts = String(endpoint ?? '')
    .split('/')
    .map((x) => x.trim())
    .filter(Boolean)
    .filter((x) => !/^v\d+$/i.test(x))
  if (parts.length === 0) return null
  return parts.slice(-2).join('_')
}

function collectObservedArrayPaths(root: any): string[] {
  const out = new Set<string>()
  const queue: Array<{ value: any; path: string; depth: number }> = [{ value: root, path: '$', depth: 0 }]
  const seen = new Set<any>()

  while (queue.length) {
    const cur = queue.shift()!
    const value = cur.value
    if (value == null || cur.depth > 6) continue
    if (typeof value !== 'object') continue
    if (seen.has(value)) continue
    seen.add(value)

    if (Array.isArray(value)) {
      if (value.some((x) => x && typeof x === 'object' && !Array.isArray(x))) {
        out.add(cur.path)
      }
      for (const item of value.slice(0, 20)) {
        queue.push({ value: item, path: `${cur.path}[]`, depth: cur.depth + 1 })
      }
      continue
    }

    for (const [k, v] of Object.entries(value)) {
      queue.push({ value: v, path: `${cur.path}.${k}`, depth: cur.depth + 1 })
    }
  }

  return Array.from(out).slice(0, 24)
}

function inferKeyCandidates(root: any): string[] {
  const counters = new Map<string, number>()
  const queue: Array<{ value: any; depth: number }> = [{ value: root, depth: 0 }]
  const seen = new Set<any>()

  while (queue.length) {
    const cur = queue.shift()!
    const value = cur.value
    if (value == null || cur.depth > 6) continue
    if (typeof value !== 'object') continue
    if (seen.has(value)) continue
    seen.add(value)

    if (Array.isArray(value)) {
      for (const item of value.slice(0, 50)) queue.push({ value: item, depth: cur.depth + 1 })
      continue
    }

    for (const [k, v] of Object.entries(value)) {
      const key = String(k)
      const scalar = v == null || ['string', 'number', 'boolean'].includes(typeof v)
      if (scalar) counters.set(key, (counters.get(key) ?? 0) + 1)
      if (v && typeof v === 'object') queue.push({ value: v, depth: cur.depth + 1 })
    }
  }

  const priority = new Map<string, number>([
    ['offer_id', 1000],
    ['product_id', 990],
    ['sku', 980],
    ['warehouse_id', 970],
    ['id', 960],
  ])

  const ranked = Array.from(counters.entries())
    .filter(([k]) => {
      const low = k.toLowerCase()
      return low === 'id' || low.endsWith('_id') || low.endsWith('id') || ['sku', 'offer_id', 'product_id', 'warehouse_id'].includes(low)
    })
    .sort((a, b) => {
      const aKey = a[0].toLowerCase()
      const bKey = b[0].toLowerCase()
      const aScore = (priority.get(aKey) ?? 0) + a[1]
      const bScore = (priority.get(bKey) ?? 0) + b[1]
      if (bScore !== aScore) return bScore - aScore
      return aKey.localeCompare(bKey)
    })
    .map(([k]) => k)

  return ranked.slice(0, 16)
}

export function ensureDb() {
  if (db) return

  ensurePersistentStorageReady()

  db = new Database(dbPath())
  db.pragma('journal_mode = WAL')

  db.exec(`
    CREATE TABLE IF NOT EXISTS products (
      offer_id TEXT PRIMARY KEY,
      product_id INTEGER NULL,
      sku TEXT NULL,
      archived INTEGER NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sync_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      status TEXT NOT NULL,
      started_at TEXT NOT NULL,
      finished_at TEXT NULL,
      items_count INTEGER NULL,
      error_message TEXT NULL,
      error_details TEXT NULL,
      meta TEXT NULL,
      store_client_id TEXT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_sync_log_started_at ON sync_log(started_at);

    CREATE TABLE IF NOT EXISTS api_endpoint_registry (
      registry_key TEXT PRIMARY KEY,
      method TEXT NOT NULL,
      endpoint TEXT NOT NULL,
      entity_hint TEXT NULL,
      key_candidates TEXT NULL,
      observed_paths TEXT NULL,
      sample_count INTEGER NOT NULL DEFAULT 0,
      first_seen_at TEXT NOT NULL,
      last_seen_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS api_raw_cache (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      store_client_id TEXT NULL,
      method TEXT NOT NULL,
      endpoint TEXT NOT NULL,
      registry_key TEXT NOT NULL,
      entity_hint TEXT NULL,
      request_body TEXT NULL,
      request_truncated INTEGER NOT NULL DEFAULT 0,
      response_body TEXT NULL,
      response_truncated INTEGER NOT NULL DEFAULT 0,
      response_sha256 TEXT NULL,
      http_status INTEGER NULL,
      is_success INTEGER NOT NULL DEFAULT 1,
      error_message TEXT NULL,
      fetched_at TEXT NOT NULL,
      FOREIGN KEY (registry_key) REFERENCES api_endpoint_registry(registry_key)
    );

    CREATE INDEX IF NOT EXISTS idx_api_raw_cache_store_time ON api_raw_cache(store_client_id, fetched_at DESC);
    CREATE INDEX IF NOT EXISTS idx_api_raw_cache_endpoint_time ON api_raw_cache(endpoint, fetched_at DESC);
    CREATE INDEX IF NOT EXISTS idx_api_raw_cache_registry_time ON api_raw_cache(registry_key, fetched_at DESC);

    CREATE TABLE IF NOT EXISTS app_settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS product_placements (
      store_client_id TEXT NOT NULL,
      warehouse_id INTEGER NOT NULL,
      warehouse_name TEXT NULL,
      sku TEXT NOT NULL,
      ozon_sku TEXT NULL,
      seller_sku TEXT NULL,
      placement_zone TEXT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (store_client_id, warehouse_id, sku)
    );

    CREATE INDEX IF NOT EXISTS idx_product_placements_store_sku ON product_placements(store_client_id, sku);
  `)

  const cols = new Set(
    (db.prepare('PRAGMA table_info(products)').all() as any[]).map((r) => String(r.name))
  )

  const add = (name: string, decl: string) => {
    if (cols.has(name)) return
    db!.exec(`ALTER TABLE products ADD COLUMN ${name} ${decl};`)
    cols.add(name)
  }

  add('barcode', 'TEXT NULL')
  add('brand', 'TEXT NULL')
  add('category', 'TEXT NULL')
  add('type', 'TEXT NULL')
  add('name', 'TEXT NULL')
  add('photo_url', 'TEXT NULL')
  add('is_visible', 'INTEGER NULL')
  add('hidden_reasons', 'TEXT NULL')
  add('created_at', 'TEXT NULL')
  add('store_client_id', 'TEXT NULL')

  const logCols = new Set(
    (db.prepare('PRAGMA table_info(sync_log)').all() as any[]).map((r) => String(r.name))
  )
  if (!logCols.has('store_client_id')) {
    db.exec(`ALTER TABLE sync_log ADD COLUMN store_client_id TEXT NULL;`)
  }

  const placementCols = new Set(
    (db.prepare('PRAGMA table_info(product_placements)').all() as any[]).map((r) => String(r.name))
  )
  if (!placementCols.has('ozon_sku')) {
    db.exec(`ALTER TABLE product_placements ADD COLUMN ozon_sku TEXT NULL;`)
  }
  if (!placementCols.has('seller_sku')) {
    db.exec(`ALTER TABLE product_placements ADD COLUMN seller_sku TEXT NULL;`)
  }
  db.exec(`CREATE INDEX IF NOT EXISTS idx_product_placements_store_ozon_sku ON product_placements(store_client_id, ozon_sku);`)
  db.exec(`CREATE INDEX IF NOT EXISTS idx_product_placements_store_seller_sku ON product_placements(store_client_id, seller_sku);`)

  if (getSettingValue('log_retention_days') == null) {
    setSettingValue('log_retention_days', String(DEFAULT_LOG_RETENTION_DAYS))
  }

  dbPruneLogsByRetention()
}

export function dbRecordApiRawResponse(args: {
  storeClientId?: string | null
  method: string
  endpoint: string
  requestBody?: any
  responseBody?: any
  httpStatus?: number | null
  isSuccess?: boolean
  errorMessage?: string | null
  fetchedAt?: string
}) {
  const method = String(args.method ?? '').toUpperCase().trim() || 'GET'
  const endpoint = String(args.endpoint ?? '').trim()
  const registryKey = makeApiRegistryKey(method, endpoint)
  const now = args.fetchedAt ?? new Date().toISOString()
  const entityHint = inferEntityHintFromEndpoint(endpoint)

  const req = safeJsonWithLimit(args.requestBody ?? null, MAX_API_JSON_LEN)
  const res = safeJsonWithLimit(args.responseBody ?? null, MAX_API_JSON_LEN)
  const responseSha = sha256Hex(res.text ?? '')

  const keyCandidates = inferKeyCandidates(args.responseBody)
  const observedPaths = collectObservedArrayPaths(args.responseBody)

  const tx = mustDb().transaction(() => {
    const row: any = mustDb().prepare(`
      SELECT registry_key, entity_hint, key_candidates, observed_paths, sample_count
      FROM api_endpoint_registry
      WHERE registry_key = ?
    `).get(registryKey)

    if (!row) {
      mustDb().prepare(`
        INSERT INTO api_endpoint_registry (
          registry_key, method, endpoint, entity_hint, key_candidates, observed_paths, sample_count, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        registryKey,
        method,
        endpoint,
        entityHint,
        keyCandidates.length ? JSON.stringify(keyCandidates) : null,
        observedPaths.length ? JSON.stringify(observedPaths) : null,
        1,
        now,
        now,
      )
    } else {
      const mergedEntityHint = String(row.entity_hint ?? '').trim() || entityHint
      const mergedKeys = mergeJsonStringArray(row.key_candidates, keyCandidates)
      const mergedPaths = mergeJsonStringArray(row.observed_paths, observedPaths)
      mustDb().prepare(`
        UPDATE api_endpoint_registry
        SET entity_hint = ?,
            key_candidates = ?,
            observed_paths = ?,
            sample_count = COALESCE(sample_count, 0) + 1,
            last_seen_at = ?
        WHERE registry_key = ?
      `).run(mergedEntityHint ?? null, mergedKeys, mergedPaths, now, registryKey)
    }

    mustDb().prepare(`
      INSERT INTO api_raw_cache (
        store_client_id, method, endpoint, registry_key, entity_hint,
        request_body, request_truncated,
        response_body, response_truncated, response_sha256,
        http_status, is_success, error_message, fetched_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      args.storeClientId ?? null,
      method,
      endpoint,
      registryKey,
      entityHint,
      req.text,
      req.truncated ? 1 : 0,
      res.text,
      res.truncated ? 1 : 0,
      responseSha,
      (typeof args.httpStatus === 'number' && Number.isFinite(args.httpStatus)) ? Math.trunc(args.httpStatus) : null,
      args.isSuccess === false ? 0 : 1,
      args.errorMessage ?? null,
      now,
    )
  })

  tx()
}

export function dbGetAdminSettings() {
  const raw = getSettingValue('log_retention_days')
  return {
    logRetentionDays: normalizeRetentionDays(raw ?? DEFAULT_LOG_RETENTION_DAYS),
  }
}

export function dbSaveAdminSettings(input: { logRetentionDays: number }) {
  const logRetentionDays = normalizeRetentionDays(input.logRetentionDays)
  setSettingValue('log_retention_days', String(logRetentionDays))
  dbPruneLogsByRetention()

  dbLogEvent('admin_settings', {
    status: 'success',
    meta: { logRetentionDays },
  })

  return { logRetentionDays }
}


function normalizeGridColsDataset(value: unknown): GridColsDataset | null {
  const v = String(value ?? '').trim() as GridColsDataset
  return GRID_COLS_DATASETS.has(v) ? v : null
}

function normalizeGridColLayoutItems(value: unknown): GridColLayoutItem[] {
  if (!Array.isArray(value)) return []
  const out: GridColLayoutItem[] = []
  const seen = new Set<string>()

  for (const raw of value) {
    if (!raw || typeof raw !== 'object') continue
    const id = String((raw as any).id ?? '').trim()
    if (!id || seen.has(id)) continue

    const wRaw = Number((raw as any).w)
    const w = Number.isFinite(wRaw) ? Math.max(60, Math.min(2000, Math.round(wRaw))) : 120
    const visible = typeof (raw as any).visible === 'boolean' ? (raw as any).visible : true

    out.push({ id, w, visible })
    seen.add(id)
    if (out.length >= 200) break
  }

  return out
}

export function dbGetGridColumns(datasetRaw: unknown): { dataset: GridColsDataset; cols: GridColLayoutItem[] | null } {
  const dataset = normalizeGridColsDataset(datasetRaw)
  if (!dataset) throw new Error('Некорректный набор данных для раскладки колонок')

  const raw = getSettingValue(`${GRID_COLS_KEY_PREFIX}${dataset}`)
  if (!raw) return { dataset, cols: null }

  try {
    const parsed = JSON.parse(raw)
    const cols = normalizeGridColLayoutItems(parsed)
    return { dataset, cols: cols.length ? cols : null }
  } catch {
    return { dataset, cols: null }
  }
}

export function dbSaveGridColumns(datasetRaw: unknown, colsRaw: unknown) {
  const dataset = normalizeGridColsDataset(datasetRaw)
  if (!dataset) throw new Error('Некорректный набор данных для раскладки колонок')

  const cols = normalizeGridColLayoutItems(colsRaw)
  setSettingValue(`${GRID_COLS_KEY_PREFIX}${dataset}`, JSON.stringify(cols))

  return { dataset, savedCount: cols.length }
}

export function dbPruneLogsByRetention() {
  const days = dbGetAdminSettings().logRetentionDays
  const cutoffMs = Date.now() - (days * 24 * 60 * 60 * 1000)
  const cutoffIso = new Date(cutoffMs).toISOString()

  mustDb().prepare(`
    DELETE FROM sync_log
    WHERE COALESCE(finished_at, started_at) < ?
  `).run(cutoffIso)
}

export function dbIngestLifecycleMarkers(args: { appVersion: string }) {
  const uninstallMarker = getLifecycleMarkerPath('uninstall')
  const installMarker = getLifecycleMarkerPath('installer')

  const uninstallExists = existsSync(uninstallMarker)
  const installExists = existsSync(installMarker)

  const uninstallAt = uninstallExists ? statSync(uninstallMarker).mtime : null
  const installAt = installExists ? statSync(installMarker).mtime : null

  const suppressUninstallAsPartOfReinstall = (() => {
    if (!uninstallAt || !installAt) return false
    const diff = installAt.getTime() - uninstallAt.getTime()
    return diff >= 0 && diff <= REINSTALL_UNINSTALL_SUPPRESS_WINDOW_MS
  })()

  if (uninstallAt && !suppressUninstallAsPartOfReinstall) {
    const at = uninstallAt.toISOString()
    dbLogEvent('app_uninstall', {
      status: 'success',
      startedAt: at,
      finishedAt: at,
      meta: { source: 'nsis-marker' },
    })
  }

  if (installAt) {
    const at = installAt.toISOString()
    const prevVersion = getSettingValue('app_version_last_seen')

    const type: AppLogType = !prevVersion
      ? 'app_install'
      : (prevVersion === args.appVersion ? 'app_reinstall' : 'app_update')

    dbLogEvent(type, {
      status: 'success',
      startedAt: at,
      finishedAt: at,
      meta: {
        source: 'nsis-marker',
        appVersion: args.appVersion,
        previousVersion: prevVersion,
      },
    })
  }

  if (uninstallExists) {
    try { rmSync(uninstallMarker, { force: true }) } catch {}
  }
  if (installExists) {
    try { rmSync(installMarker, { force: true }) } catch {}
  }

  // Если приложение уже существовало до внедрения маркеров — просто зафиксируем текущую версию,
  // чтобы следующие события корректно определялись как обновление/переустановка.
  if (!getSettingValue('app_version_last_seen')) {
    setSettingValue('app_version_last_seen', args.appVersion)
  } else {
    setSettingValue('app_version_last_seen', args.appVersion)
  }
}

export function dbUpsertProducts(items: Array<{
  offer_id: string
  product_id?: number
  sku?: string | null
  barcode?: string | null
  brand?: string | null
  category?: string | null
  type?: string | null
  name?: string | null
  photo_url?: string | null
  is_visible?: number | boolean | null
  hidden_reasons?: string | null
  created_at?: string | null
  archived?: boolean
  store_client_id?: string | null
}>) {
  const now = new Date().toISOString()

  const stmt = mustDb().prepare(`
    INSERT INTO products (
      offer_id, product_id, sku,
      barcode, brand, category, type, name, photo_url, is_visible, hidden_reasons, created_at,
      store_client_id,
      archived, updated_at
    )
    VALUES (
      @offer_id, @product_id, @sku,
      @barcode, @brand, @category, @type, @name, @photo_url, @is_visible, @hidden_reasons, @created_at,
      @store_client_id,
      @archived, @updated_at
    )
    ON CONFLICT(offer_id) DO UPDATE SET
      product_id=excluded.product_id,
      sku=excluded.sku,
      barcode=excluded.barcode,
      brand=excluded.brand,
      category=excluded.category,
      type=excluded.type,
      name=excluded.name,
      photo_url=excluded.photo_url,
      is_visible=excluded.is_visible,
      hidden_reasons=excluded.hidden_reasons,
      created_at=excluded.created_at,
      store_client_id=excluded.store_client_id,
      archived=excluded.archived,
      updated_at=excluded.updated_at
  `)

  const tx = mustDb().transaction((rows: any[]) => {
    for (const r of rows) {
      const vis =
        r.is_visible === true ? 1 :
        r.is_visible === false ? 0 :
        (typeof r.is_visible === 'number' ? r.is_visible : null)

      stmt.run({
        offer_id: r.offer_id,
        product_id: r.product_id ?? null,
        sku: (() => {
          const sku = r?.sku == null ? '' : String(r.sku).trim()
          return sku || null
        })(),
        barcode: r.barcode ?? null,
        brand: r.brand ?? null,
        category: r.category ?? null,
        type: r.type ?? null,
        name: r.name ?? null,
        photo_url: r.photo_url ?? null,
        is_visible: vis,
        hidden_reasons: r.hidden_reasons ?? null,
        created_at: r.created_at ?? null,
        store_client_id: (r.store_client_id ?? null),
        archived: typeof r.archived === 'boolean' ? (r.archived ? 1 : 0) : null,
        updated_at: now,
      })
    }
  })

  tx(items as any[])
}

export function dbReplaceProductPlacementsForStore(storeClientId: string, items: Array<{
  warehouse_id: number
  warehouse_name?: string | null
  sku?: string | null
  ozon_sku?: string | null
  seller_sku?: string | null
  placement_zone?: string | null
}>): number {
  const cleanStore = String(storeClientId ?? '').trim()
  if (!cleanStore) return 0

  const now = new Date().toISOString()
  const delStmt = mustDb().prepare('DELETE FROM product_placements WHERE store_client_id = ?')
  const insStmt = mustDb().prepare(`
    INSERT INTO product_placements (
      store_client_id, warehouse_id, warehouse_name, sku, ozon_sku, seller_sku, placement_zone, updated_at
    ) VALUES (
      @store_client_id, @warehouse_id, @warehouse_name, @sku, @ozon_sku, @seller_sku, @placement_zone, @updated_at
    )
    ON CONFLICT(store_client_id, warehouse_id, sku) DO UPDATE SET
      warehouse_name = excluded.warehouse_name,
      ozon_sku = excluded.ozon_sku,
      seller_sku = excluded.seller_sku,
      placement_zone = excluded.placement_zone,
      updated_at = excluded.updated_at
  `)

  const tx = mustDb().transaction((rows: any[]) => {
    delStmt.run(cleanStore)
    for (const r of rows) {
      const ozonSku = String(r?.ozon_sku ?? '').trim() || null
      const sellerSku = String(r?.seller_sku ?? '').trim() || null
      const legacySku = String(r?.sku ?? '').trim() || null
      const sku = ozonSku || sellerSku || legacySku
      const wid = Number(r?.warehouse_id)
      if (!sku || !Number.isFinite(wid)) continue
      insStmt.run({
        store_client_id: cleanStore,
        warehouse_id: Math.trunc(wid),
        warehouse_name: r?.warehouse_name == null ? null : String(r.warehouse_name),
        sku,
        ozon_sku: ozonSku,
        seller_sku: sellerSku,
        placement_zone: r?.placement_zone == null ? null : String(r.placement_zone),
        updated_at: now,
      })
    }
  })

  tx(items as any[])

  const row = mustDb().prepare('SELECT COUNT(*) as cnt FROM product_placements WHERE store_client_id = ?').get(cleanStore) as { cnt?: number } | undefined
  return Number(row?.cnt ?? 0)
}

export function dbGetProductPlacements(storeClientId?: string | null): ProductPlacementRow[] {
  if (storeClientId) {
    return mustDb().prepare(`
      SELECT store_client_id, warehouse_id, warehouse_name, sku, ozon_sku, seller_sku, placement_zone, updated_at
      FROM product_placements
      WHERE store_client_id = ?
      ORDER BY sku COLLATE NOCASE ASC, warehouse_id ASC
    `).all(storeClientId) as any
  }

  return mustDb().prepare(`
    SELECT store_client_id, warehouse_id, warehouse_name, sku, ozon_sku, seller_sku, placement_zone, updated_at
    FROM product_placements
    ORDER BY store_client_id ASC, sku COLLATE NOCASE ASC, warehouse_id ASC
  `).all() as any
}

export function dbGetStockViewRows(storeClientId?: string | null): StockViewRow[] {
  const productsSql = storeClientId
    ? `
      SELECT
        offer_id,
        product_id,
        sku,
        barcode,
        brand,
        category,
        type,
        name,
        photo_url,
        is_visible,
        hidden_reasons,
        created_at,
        store_client_id,
        archived,
        updated_at
      FROM products
      WHERE store_client_id = ?
      ORDER BY offer_id COLLATE NOCASE ASC
    `
    : `
      SELECT
        offer_id,
        product_id,
        sku,
        barcode,
        brand,
        category,
        type,
        name,
        photo_url,
        is_visible,
        hidden_reasons,
        created_at,
        store_client_id,
        archived,
        updated_at
      FROM products
      ORDER BY COALESCE(store_client_id, '') ASC, offer_id COLLATE NOCASE ASC
    `

  const placementsSql = storeClientId
    ? `
      SELECT store_client_id, warehouse_id, warehouse_name, sku, ozon_sku, seller_sku, placement_zone
      FROM product_placements
      WHERE store_client_id = ?
      ORDER BY sku COLLATE NOCASE ASC, COALESCE(warehouse_name, '') COLLATE NOCASE ASC, warehouse_id ASC
    `
    : `
      SELECT store_client_id, warehouse_id, warehouse_name, sku, ozon_sku, seller_sku, placement_zone
      FROM product_placements
      ORDER BY COALESCE(store_client_id, '') ASC, sku COLLATE NOCASE ASC, COALESCE(warehouse_name, '') COLLATE NOCASE ASC, warehouse_id ASC
    `

  const products = (storeClientId
    ? mustDb().prepare(productsSql).all(storeClientId)
    : mustDb().prepare(productsSql).all()) as ProductRow[]

  const placementRows = (storeClientId
    ? mustDb().prepare(placementsSql).all(storeClientId)
    : mustDb().prepare(placementsSql).all()) as ProductPlacementRow[]

  const placementsByOzonSku = new Map<string, ProductPlacementRow[]>()
  const placementsBySellerSku = new Map<string, ProductPlacementRow[]>()
  for (const row of placementRows) {
    const storeKey = String(row?.store_client_id ?? '').trim()
    const legacySku = String(row?.sku ?? '').trim()
    const explicitOzonSku = String((row as any)?.ozon_sku ?? '').trim()
    const explicitSellerSku = String((row as any)?.seller_sku ?? '').trim()
    const ozonSku = explicitOzonSku || (/^\d+$/.test(legacySku) ? legacySku : '')
    const sellerSku = explicitSellerSku || (legacySku && legacySku !== ozonSku ? legacySku : '')

    if (ozonSku) {
      const mapKey = `${storeKey}::${ozonSku}`
      const list = placementsByOzonSku.get(mapKey)
      if (list) list.push(row)
      else placementsByOzonSku.set(mapKey, [row])
    }

    if (sellerSku) {
      const mapKey = `${storeKey}::${sellerSku}`
      const list = placementsBySellerSku.get(mapKey)
      if (list) list.push(row)
      else placementsBySellerSku.set(mapKey, [row])
    }
  }

  const out: StockViewRow[] = []
  for (const product of products) {
    const ozonSku = String(product?.sku ?? '').trim()
    const sellerSku = String(product?.offer_id ?? '').trim()
    const storeKey = String(product?.store_client_id ?? '').trim()

    const matched: ProductPlacementRow[] = []
    const seenPlacement = new Set<string>()
    const appendUnique = (rows: ProductPlacementRow[]) => {
      for (const row of rows) {
        const k = [
          String(row?.store_client_id ?? ''),
          String(row?.warehouse_id ?? ''),
          String((row as any)?.ozon_sku ?? row?.sku ?? ''),
          String((row as any)?.seller_sku ?? ''),
          String(row?.placement_zone ?? ''),
        ].join('::')
        if (seenPlacement.has(k)) continue
        seenPlacement.add(k)
        matched.push(row)
      }
    }

    if (ozonSku) appendUnique(placementsByOzonSku.get(`${storeKey}::${ozonSku}`) ?? [])
    if (sellerSku) appendUnique(placementsBySellerSku.get(`${storeKey}::${sellerSku}`) ?? [])
    const placements = matched

    if (placements.length === 0) {
      out.push({
        ...product,
        warehouse_id: null,
        warehouse_name: null,
        placement_zone: null,
      })
      continue
    }

    const zoneBuckets = new Map<string, ProductPlacementRow[]>()
    for (const placement of placements) {
      const zone = String(placement?.placement_zone ?? '').trim()
      const bucketKey = zone
      const bucket = zoneBuckets.get(bucketKey)
      if (bucket) bucket.push(placement)
      else zoneBuckets.set(bucketKey, [placement])
    }

    const bucketLists = Array.from(zoneBuckets.values())
    const rowsToShow = bucketLists.length <= 1 ? [placements[0]] : bucketLists.map((bucket) => bucket[0]).filter(Boolean)

    for (const placement of rowsToShow) {
      out.push({
        ...product,
        warehouse_id: placement?.warehouse_id ?? null,
        warehouse_name: placement?.warehouse_name ?? null,
        placement_zone: placement?.placement_zone ?? null,
      })
    }
  }

  return out
}

export function dbGetProducts(storeClientId?: string | null): ProductRow[] {
  if (storeClientId) {
    return mustDb().prepare(`
      SELECT
        offer_id,
        product_id,
        sku,
        barcode,
        brand,
        category,
        type,
        name,
        photo_url,
        is_visible,
        hidden_reasons,
        created_at,
        store_client_id,
        archived,
        updated_at
      FROM products
      WHERE store_client_id = ?
      ORDER BY offer_id COLLATE NOCASE ASC
    `).all(storeClientId) as any
  }

  return mustDb().prepare(`
    SELECT
      offer_id,
      product_id,
      sku,
      barcode,
      brand,
      category,
      type,
      name,
      photo_url,
      is_visible,
      hidden_reasons,
      created_at,
      store_client_id,
      archived,
      updated_at
    FROM products
    ORDER BY offer_id COLLATE NOCASE ASC
  `).all() as any
}

export function dbCountProducts(storeClientId?: string | null): number {
  if (storeClientId) {
    const row = mustDb().prepare(`SELECT COUNT(*) AS cnt FROM products WHERE store_client_id = ?`).get(storeClientId) as { cnt: number }
    return Number(row?.cnt ?? 0)
  }

  const row = mustDb().prepare(`SELECT COUNT(*) AS cnt FROM products`).get() as { cnt: number }
  return Number(row?.cnt ?? 0)
}

export function dbDeleteProductsMissingForStore(storeClientId: string, keepOfferIds: string[]) {
  const uniq = Array.from(new Set((keepOfferIds ?? []).map((v) => String(v)).filter(Boolean)))

  if (uniq.length === 0) {
    const info = mustDb().prepare(`DELETE FROM products WHERE store_client_id = ?`).run(storeClientId)
    return Number(info.changes ?? 0)
  }

  const placeholders = uniq.map(() => '?').join(', ')
  const sql = `DELETE FROM products WHERE store_client_id = ? AND offer_id NOT IN (${placeholders})`
  const info = mustDb().prepare(sql).run(storeClientId, ...uniq)
  return Number(info.changes ?? 0)
}

export function dbLogStart(type: 'check_auth' | 'sync_products', storeClientId?: string | null): number {
  const startedAt = new Date().toISOString()
  const info = mustDb().prepare(`
    INSERT INTO sync_log (type, status, started_at, store_client_id)
    VALUES (?, 'pending', ?, ?)
  `).run(type, startedAt, storeClientId ?? null)

  return Number(info.lastInsertRowid)
}

export function dbLogFinish(id: number, args: {
  status: 'success' | 'error'
  itemsCount?: number
  errorMessage?: string
  errorDetails?: any
  meta?: any
  storeClientId?: string | null
}) {
  const finishedAt = new Date().toISOString()
  mustDb().prepare(`
    UPDATE sync_log
    SET status=?, finished_at=?, items_count=?, error_message=?, error_details=?, meta=?, store_client_id=COALESCE(?, store_client_id)
    WHERE id=?
  `).run(
    args.status,
    finishedAt,
    args.itemsCount ?? null,
    args.errorMessage ?? null,
    safeJson(args.errorDetails),
    safeJson(args.meta),
    args.storeClientId ?? null,
    id
  )

  dbPruneLogsByRetention()
}

export function dbLogEvent(type: AppLogType, args?: {
  status?: 'success' | 'error'
  startedAt?: string
  finishedAt?: string | null
  itemsCount?: number | null
  errorMessage?: string | null
  errorDetails?: any
  meta?: any
  storeClientId?: string | null
}) {
  const now = new Date().toISOString()
  const startedAt = args?.startedAt ?? now
  const finishedAt = args?.finishedAt ?? startedAt

  mustDb().prepare(`
    INSERT INTO sync_log (
      type, status, started_at, finished_at, items_count, error_message, error_details, meta, store_client_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    type,
    args?.status ?? 'success',
    startedAt,
    finishedAt,
    args?.itemsCount ?? null,
    args?.errorMessage ?? null,
    safeJson(args?.errorDetails),
    safeJson(args?.meta),
    args?.storeClientId ?? null,
  )

  dbPruneLogsByRetention()
}

export function dbGetSyncLog(storeClientId?: string | null) {
  if (storeClientId) {
    return mustDb().prepare(`
      SELECT id, type, status, started_at, finished_at, items_count, error_message, error_details, meta, store_client_id
      FROM sync_log
      WHERE store_client_id = ? OR store_client_id IS NULL
      ORDER BY id DESC
    `).all(storeClientId)
  }

  return mustDb().prepare(`
    SELECT id, type, status, started_at, finished_at, items_count, error_message, error_details, meta, store_client_id
    FROM sync_log
    ORDER BY id DESC
  `).all()
}

export function dbClearLogs() {
  mustDb().prepare('DELETE FROM sync_log').run()
}
