import Database from 'better-sqlite3'
import type { SalesPayloadEnvelope } from '../sales-sync'
import { extractPostingsFromPayload, getSalesPostingDetailsKey } from '../sales-sync'
import { ensurePersistentStorageReady, getPersistentDbPath } from './paths'

let fboDb: Database.Database | null = null

function db(): Database.Database {
  if (fboDb) return fboDb
  ensurePersistentStorageReady()
  fboDb = new Database(getPersistentDbPath())
  fboDb.pragma('journal_mode = WAL')
  fboDb.exec(`
    CREATE TABLE IF NOT EXISTS fbo_postings (
      store_client_id TEXT NOT NULL,
      period_key TEXT NOT NULL DEFAULT '',
      posting_number TEXT NOT NULL,
      order_id TEXT NULL,
      related_postings TEXT NULL,
      shipment_date TEXT NULL,
      delivery_date TEXT NULL,
      delivery_cluster TEXT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (store_client_id, period_key, posting_number)
    );
    CREATE INDEX IF NOT EXISTS idx_fbo_postings_lookup ON fbo_postings(store_client_id, period_key, posting_number);

    CREATE TABLE IF NOT EXISTS fbo_posting_items (
      store_client_id TEXT NOT NULL,
      period_key TEXT NOT NULL DEFAULT '',
      posting_number TEXT NOT NULL,
      line_no INTEGER NOT NULL,
      sku TEXT NULL,
      offer_id TEXT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (store_client_id, period_key, posting_number, line_no)
    );
    CREATE INDEX IF NOT EXISTS idx_fbo_posting_items_lookup ON fbo_posting_items(store_client_id, period_key, posting_number, sku, offer_id);
  `)
  return fboDb
}

function text(value: any): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function getByPath(source: any, path: string) {
  if (!source || typeof source !== 'object') return undefined
  let cur = source
  for (const part of String(path).split('.').filter(Boolean)) {
    if (cur == null || typeof cur !== 'object' || !(part in cur)) return undefined
    cur = cur[part]
  }
  return cur
}

function pick(source: any, paths: string[]): any {
  for (const path of paths) {
    const value = getByPath(source, path)
    if (value === undefined || value === null || value === '') continue
    return value
  }
  return undefined
}

function pickFromSources(paths: string[], ...sources: any[]): any {
  for (const source of sources) {
    const value = pick(source, paths)
    if (value === undefined || value === null || value === '') continue
    return value
  }
  return undefined
}

function dateText(value: any): string {
  const raw = text(value)
  if (!raw) return ''
  const dt = new Date(raw)
  return Number.isNaN(dt.getTime()) ? raw : dt.toISOString()
}

function postingNumberOf(source: any): string {
  return text(pick(source, ['posting_number', 'postingNumber']))
}

function orderIdOf(source: any): string {
  return text(pick(source, ['order_id', 'order_number']))
}

function relatedOf(detail: any, fallback: string[]): string {
  const direct = pickFromSources(['related_postings'], detail)
  if (Array.isArray(direct)) {
    const vals = direct.map((v) => text(v)).filter(Boolean)
    if (vals.length > 0) return vals.join(', ')
  }
  const raw = text(direct)
  if (raw) return raw
  return fallback.join(', ')
}

function shipmentDateOf(detail: any, posting: any): string {
  const state = text(pickFromSources(['new_state', 'result.new_state'], detail, posting)).toLowerCase()
  const changed = dateText(pickFromSources(['changed_state_date', 'result.changed_state_date'], detail, posting))
  if (state === 'posting_transferring_to_delivery' && changed) return changed
  return dateText(pickFromSources(['shipment_date', 'shipment_date_actual', 'shipped_at', 'delivering_date'], detail, posting))
}

function deliveryDateOf(detail: any, posting: any): string {
  return dateText(pickFromSources([
    'fact_delivery_date',
    'result.fact_delivery_date',
    'delivered_date',
    'result.delivered_date',
    'delivery_date',
    'result.delivery_date',
    'analytics_data.delivery_date',
    'result.analytics_data.delivery_date',
  ], detail, posting))
}

function deliveryClusterOf(detail: any, posting: any): string {
  return text(pickFromSources(['financial_data.cluster_to', 'result.financial_data.cluster_to', 'cluster_to', 'result.cluster_to'], detail, posting))
}

export function buildAndPersistFboSalesSnapshot(args: {
  storeClientId: string
  periodKey?: string | null
  fboPayloads: SalesPayloadEnvelope[]
  postingDetailsByKey: Map<string, any>
  fetchedAt?: string
}) {
  const storeClientId = text(args.storeClientId)
  if (!storeClientId) return { postingsCount: 0, itemsCount: 0 }
  const periodKey = text(args.periodKey)
  const fetchedAt = text(args.fetchedAt) || new Date().toISOString()
  const orderPostings = new Map<string, Set<string>>()
  const postingRows = new Map<string, any>()
  const itemRows = new Map<string, any>()

  for (const envelope of args.fboPayloads) {
    for (const posting of extractPostingsFromPayload(envelope.payload)) {
      const orderId = orderIdOf(posting)
      const postingNumber = postingNumberOf(posting)
      if (!orderId || !postingNumber) continue
      let bucket = orderPostings.get(orderId)
      if (!bucket) {
        bucket = new Set<string>()
        orderPostings.set(orderId, bucket)
      }
      bucket.add(postingNumber)
    }
  }

  for (const envelope of args.fboPayloads) {
    for (const posting of extractPostingsFromPayload(envelope.payload)) {
      const basePostingNumber = postingNumberOf(posting)
      if (!basePostingNumber) continue
      const detail = args.postingDetailsByKey.get(getSalesPostingDetailsKey('FBO', basePostingNumber)) ?? null
      const postingNumber = postingNumberOf(detail) || basePostingNumber
      const orderId = orderIdOf(detail) || orderIdOf(posting)
      const fallback = orderId ? Array.from(orderPostings.get(orderId) ?? []).filter((v) => v !== postingNumber) : []

      postingRows.set(postingNumber, {
        store_client_id: storeClientId,
        period_key: periodKey,
        posting_number: postingNumber,
        order_id: orderId || null,
        related_postings: relatedOf(detail, fallback) || null,
        shipment_date: shipmentDateOf(detail, posting) || null,
        delivery_date: deliveryDateOf(detail, posting) || null,
        delivery_cluster: deliveryClusterOf(detail, posting) || null,
        updated_at: fetchedAt,
      })

      const items = Array.isArray(detail?.products)
        ? detail.products
        : (Array.isArray(posting?.products)
          ? posting.products
          : (Array.isArray(detail?.items)
            ? detail.items
            : (Array.isArray(posting?.items) ? posting.items : [])))

      let lineNo = 0
      for (const item of items) {
        const sku = text(pick(item, ['sku', 'sku_id', 'id']))
        const offerId = text(pick(item, ['offer_id', 'offerId', 'article']))
        if (!sku && !offerId) continue
        lineNo += 1
        itemRows.set(`${postingNumber}|${lineNo}|${sku}|${offerId}`, {
          store_client_id: storeClientId,
          period_key: periodKey,
          posting_number: postingNumber,
          line_no: lineNo,
          sku: sku || null,
          offer_id: offerId || null,
          updated_at: fetchedAt,
        })
      }
    }
  }

  const conn = db()
  const tx = conn.transaction(() => {
    conn.prepare('DELETE FROM fbo_posting_items WHERE store_client_id = ? AND period_key = ?').run(storeClientId, periodKey)
    conn.prepare('DELETE FROM fbo_postings WHERE store_client_id = ? AND period_key = ?').run(storeClientId, periodKey)

    const insertPosting = conn.prepare(`
      INSERT INTO fbo_postings (
        store_client_id, period_key, posting_number, order_id, related_postings,
        shipment_date, delivery_date, delivery_cluster, updated_at
      ) VALUES (
        @store_client_id, @period_key, @posting_number, @order_id, @related_postings,
        @shipment_date, @delivery_date, @delivery_cluster, @updated_at
      )
    `)
    const insertItem = conn.prepare(`
      INSERT INTO fbo_posting_items (
        store_client_id, period_key, posting_number, line_no, sku, offer_id, updated_at
      ) VALUES (
        @store_client_id, @period_key, @posting_number, @line_no, @sku, @offer_id, @updated_at
      )
    `)

    for (const row of postingRows.values()) insertPosting.run(row)
    for (const row of itemRows.values()) insertItem.run(row)
  })
  tx()

  return { postingsCount: postingRows.size, itemsCount: itemRows.size }
}

export function mergeSalesRowsWithFboLocalDb(args: {
  rows: any[]
  storeClientId?: string | null
  periodKey?: string | null
}) {
  const periodKey = text(args.periodKey)
  const rows = Array.isArray(args.rows) ? args.rows : []
  const storeClientId = text(args.storeClientId)
  let sql = `
    SELECT
      i.posting_number,
      i.sku,
      i.offer_id,
      p.related_postings,
      p.shipment_date,
      p.delivery_date,
      p.delivery_cluster
    FROM fbo_posting_items i
    INNER JOIN fbo_postings p
      ON p.store_client_id = i.store_client_id
     AND p.period_key = i.period_key
     AND p.posting_number = i.posting_number
    WHERE i.period_key = ?
  `
  const params: any[] = [periodKey]
  if (storeClientId) {
    sql += ' AND i.store_client_id = ?'
    params.push(storeClientId)
  }
  const enrich = db().prepare(sql).all(...params) as any[]
  if (enrich.length === 0) return rows

  const byKey = new Map<string, any>()
  for (const row of enrich) {
    const postingNumber = text(row?.posting_number)
    const sku = text(row?.sku)
    const offerId = text(row?.offer_id)
    if (!postingNumber || (!sku && !offerId)) continue
    const key = `${postingNumber}|${sku || offerId}`
    if (!byKey.has(key)) byKey.set(key, row)
  }

  return rows.map((row) => {
    const postingNumber = text(row?.posting_number)
    const sku = text(row?.sku)
    const offerId = text(row?.offer_id)
    const extra = byKey.get(`${postingNumber}|${sku || offerId}`)
    if (!extra) return row
    return {
      ...row,
      related_postings: text(row?.related_postings) || text(extra?.related_postings),
      shipment_date: text(row?.shipment_date) || text(extra?.shipment_date),
      delivery_date: text(row?.delivery_date) || text(extra?.delivery_date),
      delivery_cluster: text(row?.delivery_cluster) || text(extra?.delivery_cluster),
      delivery_model: text(row?.delivery_model) || 'FBO',
    }
  })
}
