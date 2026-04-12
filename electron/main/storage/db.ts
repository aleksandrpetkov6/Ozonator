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
  | 'sales_fbo_shipment_trace'

type GridColsDataset = string

type DatasetSnapshotMergeStrategy = 'replace' | 'incremental_upsert_backfill' | 'authoritative_upsert_prune_backfill'

type GridColHiddenBucket = 'main' | 'add'

type GridColLayoutItem = {
  id: string
  w: number
  visible: boolean
  hiddenBucket: GridColHiddenBucket
}

export type ApiRawCacheResponseRow = {
  endpoint: string
  response_body: string | null
  fetched_at: string
  store_client_id?: string | null
}

export type ApiRawCacheEntryRow = {
  endpoint: string
  request_body: string | null
  response_body: string | null
  fetched_at: string
  store_client_id?: string | null
}

export type ApiRawCacheStoredRow = ApiRawCacheEntryRow & {
  request_truncated: number
  response_truncated: number
  response_body_len: number | null
}

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


export function dbGetAppSetting(key: string): string | null {
  return getSettingValue(key)
}

export function dbSetAppSetting(key: string, value: string) {
  setSettingValue(key, value)
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


function isPlainObject(value: unknown): value is Record<string, any> {
  return !!value && typeof value === 'object' && !Array.isArray(value)
}

function normalizeSnapshotKeyPart(value: unknown): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  return ''
}

function getSnapshotPathValue(source: Record<string, any>, path: string): unknown {
  const parts = String(path ?? '').split('.').map((x) => x.trim()).filter(Boolean)
  let cur: any = source
  for (const part of parts) {
    if (!cur || typeof cur !== 'object' || !(part in cur)) return undefined
    cur = cur[part]
  }
  return cur
}

function hasMeaningfulSnapshotValue(value: unknown): boolean {
  if (value == null) return false
  if (typeof value === 'string') return value.trim().length > 0
  if (typeof value === 'number') return Number.isFinite(value)
  if (typeof value === 'boolean') return true
  if (Array.isArray(value)) return value.length > 0
  if (isPlainObject(value)) return Object.keys(value).length > 0
  return false
}

function cloneSnapshotValue<T>(value: T): T {
  if (value == null) return value
  try {
    return JSON.parse(JSON.stringify(value))
  } catch {
    return value
  }
}

function collectSnapshotFieldCatalog(rows: any[]): string[] {
  const out = new Set<string>()
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!isPlainObject(row)) continue
    for (const key of Object.keys(row)) {
      const normalized = String(key ?? '').trim()
      if (!normalized) continue
      out.add(normalized)
      if (out.size >= 512) return Array.from(out).sort((a, b) => a.localeCompare(b, 'ru'))
    }
  }
  return Array.from(out).sort((a, b) => a.localeCompare(b, 'ru'))
}

function parseSnapshotFieldCatalog(raw: unknown): string[] {
  if (typeof raw !== 'string' || !raw.trim()) return []
  try {
    const parsed = JSON.parse(raw)
    if (!Array.isArray(parsed)) return []
    return Array.from(new Set(parsed.map((value) => String(value ?? '').trim()).filter(Boolean)))
  } catch {
    return []
  }
}

function buildSnapshotFieldCatalogJson(existingRaw: unknown, rows: any[]): string | null {
  const merged = new Set<string>(parseSnapshotFieldCatalog(existingRaw))
  for (const key of collectSnapshotFieldCatalog(rows)) merged.add(key)
  return merged.size ? JSON.stringify(Array.from(merged).sort((a, b) => a.localeCompare(b, 'ru'))) : null
}

function inferDatasetSnapshotMergeStrategy(dataset: string, sourceKind: string): DatasetSnapshotMergeStrategy {
  const normalizedDataset = String(dataset ?? '').trim().toLowerCase()
  const normalizedSourceKind = String(sourceKind ?? '').trim().toLowerCase()
  if (normalizedDataset === 'sales') return 'incremental_upsert_backfill'
  if (normalizedSourceKind === 'db-table' || normalizedSourceKind === 'db-view' || normalizedSourceKind === 'derived-products') {
    return 'authoritative_upsert_prune_backfill'
  }
  return 'incremental_upsert_backfill'
}

function inferDatasetSnapshotRowKey(dataset: string, row: any): string | null {
  if (!isPlainObject(row)) return null
  const normalizedDataset = String(dataset ?? '').trim().toLowerCase()
  const pathGroups: string[][] = []
  if (normalizedDataset === 'sales') {
    pathGroups.push(['delivery_model', 'posting_number', 'sku', 'offer_id', 'name'])
  } else if (normalizedDataset === 'stocks') {
    pathGroups.push(['warehouse_id', 'sku'])
    pathGroups.push(['warehouse_id', 'offer_id'])
    pathGroups.push(['warehouse_id', 'ozon_sku'])
    pathGroups.push(['warehouse_id', 'seller_sku'])
  } else if (normalizedDataset === 'products' || normalizedDataset === 'returns' || normalizedDataset === 'forecast-demand') {
    pathGroups.push(['offer_id'])
    pathGroups.push(['product_id'])
    pathGroups.push(['sku'])
    pathGroups.push(['ozon_sku'])
    pathGroups.push(['seller_sku'])
  }
  pathGroups.push(['id'])
  pathGroups.push(['posting_number', 'sku'])
  pathGroups.push(['posting_number', 'offer_id'])
  pathGroups.push(['warehouse_id', 'sku'])
  pathGroups.push(['warehouse_id', 'offer_id'])
  pathGroups.push(['offer_id'])
  pathGroups.push(['product_id'])
  pathGroups.push(['sku'])
  pathGroups.push(['ozon_sku'])
  pathGroups.push(['seller_sku'])
  pathGroups.push(['fbo_sku'])
  pathGroups.push(['fbs_sku'])
  for (const group of pathGroups) {
    const parts = group.map((path) => normalizeSnapshotKeyPart(getSnapshotPathValue(row, path)))
    if (parts.every(Boolean)) return `${normalizedDataset || 'dataset'}::${group.join('+')}::${parts.join('::')}`
  }
  try {
    return `${normalizedDataset || 'dataset'}::row_sha256::${sha256Hex(JSON.stringify(row))}`
  } catch {
    return null
  }
}

function mergeSnapshotRows(existingRow: any, incomingRow: any): any {
  if (!isPlainObject(existingRow) || !isPlainObject(incomingRow)) {
    return hasMeaningfulSnapshotValue(incomingRow) ? cloneSnapshotValue(incomingRow) : cloneSnapshotValue(existingRow)
  }
  const out: Record<string, any> = {}
  const keys = new Set<string>([...Object.keys(existingRow), ...Object.keys(incomingRow)])
  for (const key of keys) {
    const existingValue = (existingRow as any)[key]
    const incomingValue = (incomingRow as any)[key]
    if (isPlainObject(existingValue) && isPlainObject(incomingValue)) {
      out[key] = mergeSnapshotRows(existingValue, incomingValue)
      continue
    }
    if (Array.isArray(existingValue) && Array.isArray(incomingValue)) {
      out[key] = incomingValue.length > 0 ? cloneSnapshotValue(incomingValue) : cloneSnapshotValue(existingValue)
      continue
    }
    out[key] = hasMeaningfulSnapshotValue(incomingValue)
      ? cloneSnapshotValue(incomingValue)
      : cloneSnapshotValue(existingValue)
  }
  return out
}

function normalizeDatasetRowsForStorage(args: { dataset: string; rows: any[]; maxRows: number }): { rows: any[]; rowsCount: number } {
  const sourceRows = Array.isArray(args.rows) ? args.rows : []
  const maxRows = Math.max(1, Math.trunc(Number(args.maxRows) || 0))
  const rows = sourceRows.length > maxRows ? sourceRows.slice(0, maxRows) : sourceRows
  return {
    rows: rows.map((row) => cloneSnapshotValue(row)),
    rowsCount: rows.length,
  }
}

function mergeDatasetSnapshotRows(args: {
  dataset: string
  strategy: DatasetSnapshotMergeStrategy
  existingRows: any[]
  incomingRows: any[]
  maxRows: number
}): { rows: any[]; rowsCount: number; mergeMeta: Record<string, any> } {
  const normalizedExisting = normalizeDatasetRowsForStorage({ dataset: args.dataset, rows: args.existingRows, maxRows: args.maxRows }).rows
  const normalizedIncoming = normalizeDatasetRowsForStorage({ dataset: args.dataset, rows: args.incomingRows, maxRows: args.maxRows }).rows

  if (args.strategy === 'replace') {
    return {
      rows: normalizedIncoming,
      rowsCount: normalizedIncoming.length,
      mergeMeta: {
        strategy: args.strategy,
        existingRowsCount: normalizedExisting.length,
        incomingRowsCount: normalizedIncoming.length,
        updatedRowsCount: 0,
        insertedRowsCount: normalizedIncoming.length,
        preservedRowsCount: 0,
        prunedRowsCount: Math.max(0, normalizedExisting.length - normalizedIncoming.length),
      },
    }
  }

  const existingByKey = new Map<string, any>()
  const incomingByKey = new Map<string, any>()
  const existingWithoutKey: any[] = []
  const incomingWithoutKey: any[] = []
  const existingOrder: string[] = []
  const incomingOrder: string[] = []

  for (const row of normalizedExisting) {
    const key = inferDatasetSnapshotRowKey(args.dataset, row)
    if (!key) {
      existingWithoutKey.push(row)
      continue
    }
    if (!existingByKey.has(key)) existingOrder.push(key)
    existingByKey.set(key, row)
  }

  for (const row of normalizedIncoming) {
    const key = inferDatasetSnapshotRowKey(args.dataset, row)
    if (!key) {
      incomingWithoutKey.push(row)
      continue
    }
    if (!incomingByKey.has(key)) incomingOrder.push(key)
    incomingByKey.set(key, row)
  }

  const result: any[] = []
  let updatedRowsCount = 0
  let insertedRowsCount = 0
  let preservedRowsCount = 0
  let prunedRowsCount = 0

  if (args.strategy === 'authoritative_upsert_prune_backfill') {
    for (const key of incomingOrder) {
      const incoming = incomingByKey.get(key)
      const existing = existingByKey.get(key)
      if (existing !== undefined) {
        result.push(mergeSnapshotRows(existing, incoming))
        updatedRowsCount += 1
      } else {
        result.push(cloneSnapshotValue(incoming))
        insertedRowsCount += 1
      }
    }
    result.push(...incomingWithoutKey.map((row) => cloneSnapshotValue(row)))
    insertedRowsCount += incomingWithoutKey.length
    prunedRowsCount = Math.max(0, existingByKey.size + existingWithoutKey.length - updatedRowsCount)
  } else {
    const usedIncomingKeys = new Set<string>()
    for (const key of existingOrder) {
      const existing = existingByKey.get(key)
      if (incomingByKey.has(key)) {
        result.push(mergeSnapshotRows(existing, incomingByKey.get(key)))
        usedIncomingKeys.add(key)
        updatedRowsCount += 1
      } else {
        result.push(cloneSnapshotValue(existing))
        preservedRowsCount += 1
      }
    }
    result.push(...existingWithoutKey.map((row) => cloneSnapshotValue(row)))
    preservedRowsCount += existingWithoutKey.length
    for (const key of incomingOrder) {
      if (usedIncomingKeys.has(key)) continue
      result.push(cloneSnapshotValue(incomingByKey.get(key)))
      insertedRowsCount += 1
    }
    result.push(...incomingWithoutKey.map((row) => cloneSnapshotValue(row)))
    insertedRowsCount += incomingWithoutKey.length
  }

  const capped = result.length > args.maxRows
    ? (args.strategy === 'incremental_upsert_backfill' ? result.slice(-args.maxRows) : result.slice(0, args.maxRows))
    : result

  return {
    rows: capped,
    rowsCount: capped.length,
    mergeMeta: {
      strategy: args.strategy,
      existingRowsCount: normalizedExisting.length,
      incomingRowsCount: normalizedIncoming.length,
      updatedRowsCount,
      insertedRowsCount,
      preservedRowsCount,
      prunedRowsCount,
      cappedRowsDropped: Math.max(0, result.length - capped.length),
    },
  }
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
      ozon_sku TEXT NULL,
      seller_sku TEXT NULL,
      fbo_sku TEXT NULL,
      fbs_sku TEXT NULL,
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

    CREATE TABLE IF NOT EXISTS dataset_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      store_client_id TEXT NOT NULL DEFAULT '',
      dataset TEXT NOT NULL,
      scope_key TEXT NOT NULL DEFAULT '',
      period_from TEXT NULL,
      period_to TEXT NULL,
      schema_version INTEGER NOT NULL DEFAULT 1,
      source_kind TEXT NOT NULL DEFAULT 'projection',
      source_endpoints TEXT NULL,
      field_catalog_json TEXT NULL,
      merge_strategy TEXT NOT NULL DEFAULT 'replace',
      merge_meta_json TEXT NULL,
      rows_json TEXT NOT NULL,
      rows_count INTEGER NOT NULL DEFAULT 0,
      fetched_at TEXT NOT NULL,
      UNIQUE(store_client_id, dataset, scope_key)
    );

    CREATE INDEX IF NOT EXISTS idx_dataset_snapshots_lookup ON dataset_snapshots(store_client_id, dataset, scope_key);
    CREATE INDEX IF NOT EXISTS idx_dataset_snapshots_time ON dataset_snapshots(fetched_at DESC);

    CREATE TABLE IF NOT EXISTS cbr_rate_days (
      requested_date TEXT PRIMARY KEY,
      effective_date TEXT NULL,
      is_success INTEGER NOT NULL DEFAULT 1,
      error_message TEXT NULL,
      fetched_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS cbr_rates_daily (
      requested_date TEXT NOT NULL,
      currency_code TEXT NOT NULL,
      nominal INTEGER NOT NULL,
      value_rub REAL NOT NULL,
      rate_per_unit REAL NOT NULL,
      PRIMARY KEY (requested_date, currency_code)
    );

    CREATE INDEX IF NOT EXISTS idx_cbr_rates_daily_lookup ON cbr_rates_daily(requested_date, currency_code);
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
  add('ozon_sku', 'TEXT NULL')
  add('seller_sku', 'TEXT NULL')
  add('fbo_sku', 'TEXT NULL')
  add('fbs_sku', 'TEXT NULL')
  add('store_client_id', 'TEXT NULL')

  db.exec(`UPDATE products SET ozon_sku = sku WHERE ozon_sku IS NULL AND sku IS NOT NULL`)
  db.exec(`UPDATE products SET seller_sku = offer_id WHERE seller_sku IS NULL AND offer_id IS NOT NULL`)

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

  const datasetSnapshotCols = new Set(
    (db.prepare('PRAGMA table_info(dataset_snapshots)').all() as any[]).map((r) => String(r.name))
  )
  if (!datasetSnapshotCols.has('field_catalog_json')) {
    db.exec(`ALTER TABLE dataset_snapshots ADD COLUMN field_catalog_json TEXT NULL;`)
  }
  if (!datasetSnapshotCols.has('merge_strategy')) {
    db.exec(`ALTER TABLE dataset_snapshots ADD COLUMN merge_strategy TEXT NOT NULL DEFAULT 'replace';`)
  }
  if (!datasetSnapshotCols.has('merge_meta_json')) {
    db.exec(`ALTER TABLE dataset_snapshots ADD COLUMN merge_meta_json TEXT NULL;`)
  }

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

  const apiJsonLimit = method === 'LOCAL' && endpoint.startsWith('/__local__/sales-cache/postings')
    ? Math.max(MAX_API_JSON_LEN, 5_000_000)
    : MAX_API_JSON_LEN

  const req = safeJsonWithLimit(args.requestBody ?? null, apiJsonLimit)
  const res = safeJsonWithLimit(args.responseBody ?? null, apiJsonLimit)
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

export function dbGetLatestApiRawResponses(storeClientId: string | null | undefined, endpointsRaw: unknown): ApiRawCacheResponseRow[] {
  const endpoints = Array.from(new Set((Array.isArray(endpointsRaw) ? endpointsRaw : [])
    .map((v) => String(v ?? '').trim())
    .filter(Boolean)))

  if (endpoints.length === 0) return []

  const placeholders = endpoints.map(() => '?').join(', ')
  const params = [...endpoints]
  let sql = `
    SELECT endpoint, response_body, fetched_at, store_client_id
    FROM api_raw_cache
    WHERE is_success = 1
      AND response_body IS NOT NULL
      AND response_truncated = 0
      AND endpoint IN (${placeholders})
  `

  const scopedStoreClientId = String(storeClientId ?? '').trim()
  if (scopedStoreClientId) {
    sql += ` AND store_client_id = ?`
    params.push(scopedStoreClientId)
  }

  sql += ` ORDER BY fetched_at DESC, id DESC`

  const rows = mustDb().prepare(sql).all(...params) as any[]
  const out: ApiRawCacheResponseRow[] = []
  const seen = new Set<string>()

  for (const row of rows) {
    const endpoint = String((row as any)?.endpoint ?? '').trim()
    if (!endpoint || seen.has(endpoint)) continue

    out.push({
      endpoint,
      response_body: typeof (row as any)?.response_body === 'string' ? (row as any).response_body : null,
      fetched_at: String((row as any)?.fetched_at ?? ''),
      store_client_id: typeof (row as any)?.store_client_id === 'string' ? (row as any).store_client_id : null,
    })
    seen.add(endpoint)

    if (seen.size >= endpoints.length) break
  }

  return out
}

export function dbGetApiRawResponses(storeClientId: string | null | undefined, endpointsRaw: unknown): ApiRawCacheEntryRow[] {
  const endpoints = Array.from(new Set((Array.isArray(endpointsRaw) ? endpointsRaw : [])
    .map((v) => String(v ?? '').trim())
    .filter(Boolean)))

  if (endpoints.length === 0) return []

  const placeholders = endpoints.map(() => '?').join(', ')
  const params = [...endpoints]
  let sql = `
    SELECT endpoint, request_body, response_body, fetched_at, store_client_id
    FROM api_raw_cache
    WHERE is_success = 1
      AND request_truncated = 0
      AND response_body IS NOT NULL
      AND response_truncated = 0
      AND endpoint IN (${placeholders})
  `

  const scopedStoreClientId = String(storeClientId ?? '').trim()
  if (scopedStoreClientId) {
    sql += ` AND store_client_id = ?`
    params.push(scopedStoreClientId)
  }

  sql += ` ORDER BY fetched_at DESC, id DESC`

  return (mustDb().prepare(sql).all(...params) as any[]).map((row) => ({
    endpoint: String((row as any)?.endpoint ?? '').trim(),
    request_body: typeof (row as any)?.request_body === 'string' ? (row as any).request_body : null,
    response_body: typeof (row as any)?.response_body === 'string' ? (row as any).response_body : null,
    fetched_at: String((row as any)?.fetched_at ?? ''),
    store_client_id: typeof (row as any)?.store_client_id === 'string' ? (row as any).store_client_id : null,
  }))
}

export function dbGetLatestApiRawStoredResponses(storeClientId: string | null | undefined, endpointsRaw: unknown): ApiRawCacheStoredRow[] {
  const endpoints = Array.from(new Set((Array.isArray(endpointsRaw) ? endpointsRaw : [])
    .map((v) => String(v ?? '').trim())
    .filter(Boolean)))

  if (endpoints.length === 0) return []

  const placeholders = endpoints.map(() => '?').join(', ')
  const params = [...endpoints]
  let sql = `
    SELECT endpoint, request_body, response_body, fetched_at, store_client_id,
           request_truncated, response_truncated,
           length(response_body) AS response_body_len
    FROM api_raw_cache
    WHERE is_success = 1
      AND response_body IS NOT NULL
      AND endpoint IN (${placeholders})
  `

  const scopedStoreClientId = String(storeClientId ?? '').trim()
  if (scopedStoreClientId) {
    sql += ` AND store_client_id = ?`
    params.push(scopedStoreClientId)
  }

  sql += ` ORDER BY fetched_at DESC, id DESC`

  const rows = mustDb().prepare(sql).all(...params) as any[]
  const out: ApiRawCacheStoredRow[] = []
  const seen = new Set<string>()

  for (const row of rows) {
    const endpoint = String((row as any)?.endpoint ?? '').trim()
    if (!endpoint || seen.has(endpoint)) continue

    out.push({
      endpoint,
      request_body: typeof (row as any)?.request_body === 'string' ? (row as any).request_body : null,
      response_body: typeof (row as any)?.response_body === 'string' ? (row as any).response_body : null,
      fetched_at: String((row as any)?.fetched_at ?? ''),
      store_client_id: typeof (row as any)?.store_client_id === 'string' ? (row as any).store_client_id : null,
      request_truncated: Number((row as any)?.request_truncated ?? 0),
      response_truncated: Number((row as any)?.response_truncated ?? 0),
      response_body_len: (typeof (row as any)?.response_body_len === 'number' && Number.isFinite((row as any).response_body_len))
        ? Number((row as any).response_body_len)
        : null,
    })
    seen.add(endpoint)

    if (seen.size >= endpoints.length) break
  }

  return out
}

export function dbSaveDatasetSnapshot(args: {
  storeClientId?: string | null
  dataset: string
  scopeKey?: string | null
  periodFrom?: string | null
  periodTo?: string | null
  schemaVersion?: number
  sourceKind?: string
  sourceEndpoints?: string[]
  mergeStrategy?: DatasetSnapshotMergeStrategy | null
  rows: any[]
  fetchedAt?: string
}) {
  const storeClientId = String(args.storeClientId ?? '').trim()
  const dataset = String(args.dataset ?? '').trim()
  if (!dataset) throw new Error('Некорректный dataset для локального snapshot')

  const scopeKey = String(args.scopeKey ?? '').trim()
  const fetchedAt = String(args.fetchedAt ?? '').trim() || new Date().toISOString()
  const schemaVersion = Number.isFinite(Number(args.schemaVersion)) ? Math.max(1, Math.trunc(Number(args.schemaVersion))) : 1
  const sourceKind = String(args.sourceKind ?? '').trim() || 'projection'
  const sourceEndpoints = Array.from(new Set((Array.isArray(args.sourceEndpoints) ? args.sourceEndpoints : [])
    .map((value) => String(value ?? '').trim())
    .filter(Boolean)))
  const maxRows = dataset === 'sales' ? 5000 : 20000
  const mergeStrategy = (args.mergeStrategy && String(args.mergeStrategy).trim()
    ? args.mergeStrategy
    : inferDatasetSnapshotMergeStrategy(dataset, sourceKind)) as DatasetSnapshotMergeStrategy

  const existingRow = mustDb().prepare(`
    SELECT rows_json, field_catalog_json, merge_strategy, merge_meta_json
    FROM dataset_snapshots
    WHERE store_client_id = ? AND dataset = ? AND scope_key = ?
    LIMIT 1
  `).get(storeClientId, dataset, scopeKey) as {
    rows_json?: string | null
    field_catalog_json?: string | null
    merge_strategy?: string | null
    merge_meta_json?: string | null
  } | undefined

  let existingRows: any[] = []
  if (typeof existingRow?.rows_json === 'string' && existingRow.rows_json.trim()) {
    try {
      const parsed = JSON.parse(existingRow.rows_json)
      if (Array.isArray(parsed)) existingRows = parsed
    } catch {}
  }

  const merged = mergeDatasetSnapshotRows({
    dataset,
    strategy: mergeStrategy,
    existingRows,
    incomingRows: Array.isArray(args.rows) ? args.rows : [],
    maxRows,
  })

  let rowsJson = '[]'
  try {
    rowsJson = JSON.stringify(merged.rows)
  } catch {
    rowsJson = '[]'
  }

  const fieldCatalogJson = buildSnapshotFieldCatalogJson(existingRow?.field_catalog_json ?? null, merged.rows)
  const mergeMetaJson = JSON.stringify({
    ...(merged.mergeMeta ?? {}),
    sourceKind,
    sourceEndpointsCount: sourceEndpoints.length,
    schemaVersion,
    savedAt: fetchedAt,
  })

  mustDb().prepare(`
    INSERT INTO dataset_snapshots (
      store_client_id, dataset, scope_key, period_from, period_to,
      schema_version, source_kind, source_endpoints,
      field_catalog_json, merge_strategy, merge_meta_json,
      rows_json, rows_count, fetched_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(store_client_id, dataset, scope_key) DO UPDATE SET
      period_from = excluded.period_from,
      period_to = excluded.period_to,
      schema_version = excluded.schema_version,
      source_kind = excluded.source_kind,
      source_endpoints = excluded.source_endpoints,
      field_catalog_json = excluded.field_catalog_json,
      merge_strategy = excluded.merge_strategy,
      merge_meta_json = excluded.merge_meta_json,
      rows_json = excluded.rows_json,
      rows_count = excluded.rows_count,
      fetched_at = excluded.fetched_at
  `).run(
    storeClientId,
    dataset,
    scopeKey,
    args.periodFrom ?? null,
    args.periodTo ?? null,
    schemaVersion,
    sourceKind,
    sourceEndpoints.length ? JSON.stringify(sourceEndpoints) : null,
    fieldCatalogJson,
    mergeStrategy,
    mergeMetaJson,
    rowsJson,
    merged.rowsCount,
    fetchedAt,
  )
}

export function dbGetDatasetSnapshotRows(args: {
  storeClientId?: string | null
  dataset: string
  scopeKey?: string | null
}): any[] | null {
  const dataset = String(args.dataset ?? '').trim()
  if (!dataset) return null

  const scopeKey = String(args.scopeKey ?? '').trim()
  const storeClientId = String(args.storeClientId ?? '').trim()

  const rows = mustDb().prepare(`
    SELECT rows_json
    FROM dataset_snapshots
    WHERE dataset = ?
      AND scope_key = ?
      AND store_client_id IN (?, '')
    ORDER BY CASE WHEN store_client_id = ? THEN 0 ELSE 1 END, fetched_at DESC, id DESC
    LIMIT 1
  `).all(dataset, scopeKey, storeClientId, storeClientId) as Array<{ rows_json?: string }>

  const raw = typeof rows?.[0]?.rows_json === 'string' ? rows[0].rows_json : ''
  if (!raw) return null

  try {
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? parsed : null
  } catch {
    return null
  }
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
  const v = String(value ?? '').trim()
  if (!v) return null
  return v
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
    const hiddenBucket: GridColHiddenBucket = (raw as any).hiddenBucket === 'add' ? 'add' : 'main'

    out.push({ id, w, visible, hiddenBucket })
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
  ozon_sku?: string | null
  seller_sku?: string | null
  fbo_sku?: string | null
  fbs_sku?: string | null
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
      offer_id, product_id, sku, ozon_sku, seller_sku, fbo_sku, fbs_sku,
      barcode, brand, category, type, name, photo_url, is_visible, hidden_reasons, created_at,
      store_client_id,
      archived, updated_at
    )
    VALUES (
      @offer_id, @product_id, @sku, @ozon_sku, @seller_sku, @fbo_sku, @fbs_sku,
      @barcode, @brand, @category, @type, @name, @photo_url, @is_visible, @hidden_reasons, @created_at,
      @store_client_id,
      @archived, @updated_at
    )
    ON CONFLICT(offer_id) DO UPDATE SET
      product_id=excluded.product_id,
      sku=excluded.sku,
      ozon_sku=excluded.ozon_sku,
      seller_sku=excluded.seller_sku,
      fbo_sku=excluded.fbo_sku,
      fbs_sku=excluded.fbs_sku,
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
        ozon_sku: (() => {
          const v = r?.ozon_sku ?? r?.sku
          const s = v == null ? '' : String(v).trim()
          return s || null
        })(),
        seller_sku: (() => {
          const v = r?.seller_sku ?? r?.offer_id
          const s = v == null ? '' : String(v).trim()
          return s || null
        })(),
        fbo_sku: (() => {
          const s = r?.fbo_sku == null ? '' : String(r.fbo_sku).trim()
          return s || null
        })(),
        fbs_sku: (() => {
          const s = r?.fbs_sku == null ? '' : String(r.fbs_sku).trim()
          return s || null
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
        ozon_sku,
        seller_sku,
        fbo_sku,
        fbs_sku,
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
        ozon_sku,
        seller_sku,
        fbo_sku,
        fbs_sku,
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
        ozon_sku,
        seller_sku,
        fbo_sku,
        fbs_sku,
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
      ozon_sku,
      seller_sku,
      fbo_sku,
      fbs_sku,
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


type CbrRateDaySaveArgs = {
  requestedDate: string
  effectiveDate?: string | null
  isSuccess: boolean
  errorMessage?: string | null
  fetchedAt?: string | null
}

type CbrDailyRateSaveArgs = {
  requestedDate: string
  effectiveDate?: string | null
  rates: Array<{ currencyCode: string; nominal: number; valueRub: number; ratePerUnit: number }>
}

export function dbGetMissingCbrRateDays(requestedDates: string[]): string[] {
  const dates = Array.from(new Set((Array.isArray(requestedDates) ? requestedDates : []).map((value) => String(value ?? '').trim()).filter(Boolean)))
  if (dates.length === 0) return []

  const placeholders = dates.map(() => '?').join(', ')
  const existingRows = mustDb().prepare(`
    SELECT requested_date
    FROM cbr_rate_days
    WHERE requested_date IN (${placeholders})
  `).all(...dates) as Array<{ requested_date?: string }>

  const existing = new Set(existingRows.map((row) => String(row?.requested_date ?? '').trim()).filter(Boolean))
  return dates.filter((date) => !existing.has(date))
}

export function dbSaveCbrRateDay(args: CbrRateDaySaveArgs) {
  const requestedDate = String(args.requestedDate ?? '').trim()
  if (!requestedDate) return
  const fetchedAt = String(args.fetchedAt ?? '').trim() || new Date().toISOString()
  const effectiveDate = String(args.effectiveDate ?? '').trim() || null
  const errorMessage = String(args.errorMessage ?? '').trim() || null
  mustDb().prepare(`
    INSERT INTO cbr_rate_days (requested_date, effective_date, is_success, error_message, fetched_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(requested_date) DO UPDATE SET
      effective_date = excluded.effective_date,
      is_success = excluded.is_success,
      error_message = excluded.error_message,
      fetched_at = excluded.fetched_at
  `).run(requestedDate, effectiveDate, args.isSuccess ? 1 : 0, errorMessage, fetchedAt)
}

export function dbSaveCbrRates(args: CbrDailyRateSaveArgs) {
  const requestedDate = String(args.requestedDate ?? '').trim()
  if (!requestedDate) return
  const normalizedRates = (Array.isArray(args.rates) ? args.rates : [])
    .map((entry) => ({
      currencyCode: String(entry?.currencyCode ?? '').trim().toUpperCase(),
      nominal: Number(entry?.nominal),
      valueRub: Number(entry?.valueRub),
      ratePerUnit: Number(entry?.ratePerUnit),
    }))
    .filter((entry) => /^[A-Z]{3}$/.test(entry.currencyCode) && Number.isFinite(entry.nominal) && entry.nominal > 0 && Number.isFinite(entry.valueRub) && entry.valueRub > 0 && Number.isFinite(entry.ratePerUnit) && entry.ratePerUnit > 0)

  const txn = mustDb().transaction(() => {
    mustDb().prepare('DELETE FROM cbr_rates_daily WHERE requested_date = ?').run(requestedDate)
    if (normalizedRates.length === 0) return
    const stmt = mustDb().prepare(`
      INSERT INTO cbr_rates_daily (requested_date, currency_code, nominal, value_rub, rate_per_unit)
      VALUES (?, ?, ?, ?, ?)
    `)
    for (const entry of normalizedRates) {
      stmt.run(requestedDate, entry.currencyCode, Math.max(1, Math.trunc(entry.nominal)), entry.valueRub, entry.ratePerUnit)
    }
  })
  txn()
}

export function dbGetCbrRatesByDate(requestedDate: string): Array<{ currencyCode: string; nominal: number; valueRub: number; ratePerUnit: number }> {
  const dateKey = String(requestedDate ?? '').trim()
  if (!dateKey) return []
  const rows = mustDb().prepare(`
    SELECT currency_code, nominal, value_rub, rate_per_unit
    FROM cbr_rates_daily
    WHERE requested_date = ?
  `).all(dateKey) as Array<{ currency_code?: string; nominal?: number; value_rub?: number; rate_per_unit?: number }>

  return rows.map((row) => ({
    currencyCode: String(row?.currency_code ?? '').trim().toUpperCase(),
    nominal: Number(row?.nominal ?? 0),
    valueRub: Number(row?.value_rub ?? 0),
    ratePerUnit: Number(row?.rate_per_unit ?? 0),
  }))
}
