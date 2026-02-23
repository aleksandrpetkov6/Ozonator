import { existsSync, rmSync, statSync } from 'fs'
import Database from 'better-sqlite3'
import type { ProductRow } from '../types'
import { ensurePersistentStorageReady, getLifecycleMarkerPath, getPersistentDbPath } from './paths'

let db: Database.Database | null = null

const DEFAULT_LOG_RETENTION_DAYS = 30
const MAX_JSON_LEN = 20000

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

  if (existsSync(uninstallMarker)) {
    const at = statSync(uninstallMarker).mtime.toISOString()
    dbLogEvent('app_uninstall', {
      status: 'success',
      startedAt: at,
      finishedAt: at,
      meta: { source: 'nsis-marker' },
    })
    try { rmSync(uninstallMarker, { force: true }) } catch {}
  }

  if (existsSync(installMarker)) {
    const at = statSync(installMarker).mtime.toISOString()
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
      barcode, brand, category, type, name, is_visible, hidden_reasons, created_at,
      store_client_id,
      archived, updated_at
    )
    VALUES (
      @offer_id, @product_id, @sku,
      @barcode, @brand, @category, @type, @name, @is_visible, @hidden_reasons, @created_at,
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
