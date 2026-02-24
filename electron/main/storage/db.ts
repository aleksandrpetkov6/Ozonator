import { existsSync, rmSync, statSync } from 'fs'
import Database from 'better-sqlite3'
import type { ProductPlacementRow, ProductRow, StockViewRow } from '../types'
import { ensurePersistentStorageReady, getLifecycleMarkerPath, getPersistentDbPath } from './paths'

let db: Database.Database | null = null

const DEFAULT_LOG_RETENTION_DAYS = 30
const MAX_JSON_LEN = 20000
const REINSTALL_UNINSTALL_SUPPRESS_WINDOW_MS = 10 * 60 * 1000

type AppLogType =
  | 'check_auth'
  | 'sync_products'
  | 'app_install'
  | 'app_update'
  | 'app_reinstall'
  | 'app_uninstall'
  | 'admin_settings'

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

  if (getSettingValue('log_retention_days') == null) {
    setSettingValue('log_retention_days', String(DEFAULT_LOG_RETENTION_DAYS))
  }

  dbPruneLogsByRetention()
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
        sku: r.sku ?? null,
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
  sku: string
  placement_zone?: string | null
}>): number {
  const cleanStore = String(storeClientId ?? '').trim()
  if (!cleanStore) return 0

  const now = new Date().toISOString()
  const delStmt = mustDb().prepare('DELETE FROM product_placements WHERE store_client_id = ?')
  const insStmt = mustDb().prepare(`
    INSERT INTO product_placements (
      store_client_id, warehouse_id, warehouse_name, sku, placement_zone, updated_at
    ) VALUES (
      @store_client_id, @warehouse_id, @warehouse_name, @sku, @placement_zone, @updated_at
    )
    ON CONFLICT(store_client_id, warehouse_id, sku) DO UPDATE SET
      warehouse_name = excluded.warehouse_name,
      placement_zone = excluded.placement_zone,
      updated_at = excluded.updated_at
  `)

  const tx = mustDb().transaction((rows: any[]) => {
    delStmt.run(cleanStore)
    for (const r of rows) {
      const sku = String(r?.sku ?? '').trim()
      const wid = Number(r?.warehouse_id)
      if (!sku || !Number.isFinite(wid)) continue
      insStmt.run({
        store_client_id: cleanStore,
        warehouse_id: Math.trunc(wid),
        warehouse_name: r?.warehouse_name == null ? null : String(r.warehouse_name),
        sku,
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
      SELECT store_client_id, warehouse_id, warehouse_name, sku, placement_zone, updated_at
      FROM product_placements
      WHERE store_client_id = ?
      ORDER BY sku COLLATE NOCASE ASC, warehouse_id ASC
    `).all(storeClientId) as any
  }

  return mustDb().prepare(`
    SELECT store_client_id, warehouse_id, warehouse_name, sku, placement_zone, updated_at
    FROM product_placements
    ORDER BY store_client_id ASC, sku COLLATE NOCASE ASC, warehouse_id ASC
  `).all() as any
}

export function dbGetStockViewRows(storeClientId?: string | null): StockViewRow[] {
  const sqlBase = `
    SELECT
      p.offer_id,
      p.product_id,
      p.sku,
      p.barcode,
      p.brand,
      p.category,
      p.type,
      p.name,
      p.photo_url,
      p.is_visible,
      p.hidden_reasons,
      p.created_at,
      p.store_client_id,
      p.archived,
      p.updated_at,
      pp.warehouse_id AS warehouse_id,
      pp.warehouse_name AS warehouse_name,
      pp.placement_zone AS placement_zone
    FROM products p
    LEFT JOIN product_placements pp
      ON pp.store_client_id = p.store_client_id
     AND pp.sku = p.sku
  `

  if (storeClientId) {
    return mustDb().prepare(`${sqlBase}
      WHERE p.store_client_id = ?
      ORDER BY p.offer_id COLLATE NOCASE ASC, COALESCE(pp.warehouse_name, '') COLLATE NOCASE ASC, COALESCE(pp.warehouse_id, 0) ASC
    `).all(storeClientId) as any
  }

  return mustDb().prepare(`${sqlBase}
    ORDER BY COALESCE(p.store_client_id, '') ASC, p.offer_id COLLATE NOCASE ASC, COALESCE(pp.warehouse_name, '') COLLATE NOCASE ASC, COALESCE(pp.warehouse_id, 0) ASC
  `).all() as any
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
