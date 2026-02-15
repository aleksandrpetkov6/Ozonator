import { app } from 'electron'
import { join } from 'path'
import Database from 'better-sqlite3'
import type { ProductRow } from '../types'

let db: Database.Database | null = null

function dbPath() {
  return join(app.getPath('userData'), 'app.db')
}

/**
 * Инициализация БД + миграции.
 * Важно: эту функцию вызывает electron/main/index.ts при старте приложения.
 */
export function ensureDb() {
  if (db) return

  db = new Database(dbPath())
  db.pragma('journal_mode = WAL')

  // Базовая схема
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
      meta TEXT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_sync_log_started_at ON sync_log(started_at);
  `)

  // Миграции: добавляем недостающие колонки в products (для расширенного экрана «Товары»)
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

  // Чтобы не смешивать товары разных магазинов при смене ключей
  add('store_client_id', 'TEXT NULL')
}

function mustDb(): Database.Database {
  if (!db) throw new Error('DB not initialized (call ensureDb() first)')
  return db
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

export function dbLogStart(type: 'check_auth' | 'sync_products'): number {
  const startedAt = new Date().toISOString()
  const info = mustDb().prepare(`
    INSERT INTO sync_log (type, status, started_at)
    VALUES (?, 'pending', ?)
  `).run(type, startedAt)

  return Number(info.lastInsertRowid)
}

export function dbLogFinish(id: number, args: {
  status: 'success' | 'error'
  itemsCount?: number
  errorMessage?: string
  errorDetails?: any
  meta?: any
}) {
  const finishedAt = new Date().toISOString()
  mustDb().prepare(`
    UPDATE sync_log
    SET status=?, finished_at=?, items_count=?, error_message=?, error_details=?, meta=?
    WHERE id=?
  `).run(
    args.status,
    finishedAt,
    args.itemsCount ?? null,
    args.errorMessage ?? null,
    args.errorDetails ? JSON.stringify(args.errorDetails).slice(0, 20000) : null,
    args.meta ? JSON.stringify(args.meta).slice(0, 20000) : null,
    id
  )
}

export function dbGetSyncLog() {
  return mustDb().prepare(`
    SELECT id, type, status, started_at, finished_at, items_count, error_message, error_details, meta
    FROM sync_log
    ORDER BY id DESC
    LIMIT 500
  `).all()
}

export function dbClearLogs() {
  mustDb().prepare('DELETE FROM sync_log').run()
}
