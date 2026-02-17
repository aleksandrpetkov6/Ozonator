import Database from 'better-sqlite3'
import { app } from 'electron'
import { mkdirSync } from 'fs'
import { join } from 'path'
import type { ProductRow, SyncLogRow, SyncLogStatus, SyncLogType } from '../types'

let _db: Database.Database | null = null

function getDb(): Database.Database {
  if (_db) return _db

  const dir = app.getPath('userData')
  try {
    mkdirSync(dir, { recursive: true })
  } catch {}

  const dbPath = join(dir, 'app.db')
  _db = new Database(dbPath)
  _db.pragma('journal_mode = WAL')
  _db.exec(`
    CREATE TABLE IF NOT EXISTS products (
      offer_id TEXT PRIMARY KEY,
      product_id INTEGER,
      sku TEXT,
      barcode TEXT,
      brand TEXT,
      category TEXT,
      type TEXT,
      name TEXT,
      is_visible INTEGER,
      hidden_reasons TEXT,
      created_at TEXT,
      archived INTEGER NOT NULL DEFAULT 0,
      store_client_id TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_products_store ON products(store_client_id);

    CREATE TABLE IF NOT EXISTS sync_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      status TEXT NOT NULL,
      message TEXT,
      details TEXT,
      created_at TEXT NOT NULL,
      version TEXT,
      store_client_id TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_sync_log_store ON sync_log(store_client_id);

    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT
    );
  `)

  return _db
}

export function dbGetMeta(key: string): string | null {
  const row = getDb().prepare('SELECT value FROM meta WHERE key=?').get(key) as any
  return row?.value ?? null
}

export function dbSetMeta(key: string, value: string): void {
  getDb()
    .prepare('INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .run(key, value)
}

export function dbGetProducts(storeClientId: string | null): ProductRow[] {
  const stmt = storeClientId
    ? getDb().prepare('SELECT * FROM products WHERE store_client_id=? ORDER BY offer_id')
    : getDb().prepare('SELECT * FROM products ORDER BY offer_id')

  const rows = (storeClientId ? stmt.all(storeClientId) : stmt.all()) as any[]
  return rows.map((r) => ({
    offer_id: r.offer_id,
    product_id: r.product_id ?? null,
    sku: r.sku ?? null,
    barcode: r.barcode ?? null,
    brand: r.brand ?? null,
    category: r.category ?? null,
    type: r.type ?? null,
    name: r.name ?? null,
    is_visible: r.is_visible === null || r.is_visible === undefined ? null : !!r.is_visible,
    hidden_reasons: r.hidden_reasons ?? null,
    created_at: r.created_at ?? null,
    archived: !!r.archived,
    store_client_id: r.store_client_id,
  }))
}

export function dbUpsertProducts(items: ProductRow[]): void {
  if (!items.length) return
  const stmt = getDb().prepare(`
    INSERT INTO products
    (offer_id, product_id, sku, barcode, brand, category, type, name, is_visible, hidden_reasons, created_at, archived, store_client_id)
    VALUES
    (@offer_id, @product_id, @sku, @barcode, @brand, @category, @type, @name, @is_visible, @hidden_reasons, @created_at, @archived, @store_client_id)
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
      archived=excluded.archived,
      store_client_id=excluded.store_client_id
  `)

  const tx = getDb().transaction((rows: ProductRow[]) => {
    for (const r of rows) {
      stmt.run({
        ...r,
        is_visible: r.is_visible === null ? null : r.is_visible ? 1 : 0,
        archived: r.archived ? 1 : 0,
      })
    }
  })
  tx(items)
}

export function dbGetSyncLog(storeClientId: string | null): SyncLogRow[] {
  const stmt = storeClientId
    ? getDb().prepare('SELECT * FROM sync_log WHERE store_client_id=? ORDER BY id DESC')
    : getDb().prepare('SELECT * FROM sync_log ORDER BY id DESC')

  const rows = (storeClientId ? stmt.all(storeClientId) : stmt.all()) as any[]
  return rows.map((r) => ({
    id: Number(r.id),
    type: r.type,
    status: r.status,
    message: r.message ?? null,
    details: r.details ? safeJsonParse(r.details) : null,
    created_at: r.created_at,
    version: r.version ?? null,
    store_client_id: r.store_client_id ?? null,
  }))
}

export function dbClearLogs(storeClientId?: string | null): void {
  if (storeClientId) getDb().prepare('DELETE FROM sync_log WHERE store_client_id=?').run(storeClientId)
  else getDb().prepare('DELETE FROM sync_log').run()
}

export function dbLogStart(type: SyncLogType, storeClientId: string | null): number {
  const now = new Date().toISOString()
  const version = typeof app.getVersion === 'function' ? app.getVersion() : null
  const info = getDb()
    .prepare('INSERT INTO sync_log(type,status,message,details,created_at,version,store_client_id) VALUES(?,?,?,?,?,?,?)')
    .run(type, 'started', null, null, now, version, storeClientId)

  return Number(info.lastInsertRowid)
}

export function dbLogFinish(
  id: number,
  params: { status: SyncLogStatus; message?: string | null; details?: unknown; storeClientId: string | null }
): void {
  const detailsStr = params.details === undefined ? null : JSON.stringify(params.details)
  getDb()
    .prepare('UPDATE sync_log SET status=?, message=?, details=?, store_client_id=? WHERE id=?')
    .run(params.status, params.message ?? null, detailsStr, params.storeClientId, id)
}

function safeJsonParse(s: string): unknown {
  try {
    return JSON.parse(s)
  } catch {
    return s
  }
}
