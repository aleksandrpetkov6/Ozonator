import Database from 'better-sqlite3'
import type { SalesPayloadEnvelope } from '../sales-sync'
import { collectSalesStateEvents, extractPostingsFromPayload, getSalesPostingDetailsKey, resolveFboShipmentDateFromSources } from '../sales-sync'
import { ensurePersistentStorageReady, getPersistentDbPath } from './paths'

let fboDb: Database.Database | null = null

function ensurePostingStateEventsSchema(conn: Database.Database) {
  const columns = conn.prepare("PRAGMA table_info('posting_state_events')").all() as Array<{ name?: string }>|null
  const names = new Set((columns ?? []).map((row) => String(row?.name ?? '').trim()).filter(Boolean))

  if (!names.has('event_type')) {
    conn.exec("ALTER TABLE posting_state_events ADD COLUMN event_type TEXT NULL")
  }
  if (!names.has('new_state')) {
    conn.exec("ALTER TABLE posting_state_events ADD COLUMN new_state TEXT NULL")
  }

  conn.exec(`
    UPDATE posting_state_events
    SET event_type = COALESCE(NULLIF(event_type, ''), 'type_state_changed')
    WHERE event_type IS NULL OR event_type = '';

    UPDATE posting_state_events
    SET new_state = COALESCE(NULLIF(new_state, ''), state)
    WHERE new_state IS NULL OR new_state = '';
  `)
}

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

    CREATE TABLE IF NOT EXISTS posting_state_events (
      store_client_id TEXT NOT NULL,
      period_key TEXT NOT NULL DEFAULT '',
      posting_number TEXT NOT NULL,
      event_key TEXT NOT NULL,
      event_type TEXT NULL,
      new_state TEXT NULL,
      state TEXT NOT NULL,
      changed_state_date TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (store_client_id, period_key, posting_number, event_key)
    );
    CREATE INDEX IF NOT EXISTS idx_posting_state_events_lookup ON posting_state_events(store_client_id, period_key, posting_number, event_type, new_state, changed_state_date);
  `)
  ensurePostingStateEventsSchema(fboDb)
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

const FBO_STATE_CHANGED_EVENT_TYPE = 'type_state_changed'
const FBO_SHIPMENT_PUSH_PERIOD_KEY = '__push__'

const FBO_DELIVERED_STATES = new Set<string>([
  'delivered',
  'delivered_to_customer',
  'customer_received',
  'posting_delivered',
  'posting_delivered_to_customer',
])

function itemsOf(source: any): any[] {
  if (Array.isArray(source?.products)) return source.products
  if (Array.isArray(source?.items)) return source.items
  return []
}

function mergeItem(detailItem: any, postingItem: any) {
  const detailObj = detailItem && typeof detailItem === 'object' ? detailItem : {}
  const postingObj = postingItem && typeof postingItem === 'object' ? postingItem : {}
  return {
    ...postingObj,
    ...detailObj,
    sku: pickFromSources(['sku', 'sku_id', 'id'], detailObj, postingObj),
    offer_id: pickFromSources(['offer_id', 'offerId', 'article'], detailObj, postingObj),
  }
}

function mergedItems(detail: any, posting: any): any[] {
  const detailItems = itemsOf(detail)
  const postingItems = itemsOf(posting)
  if (detailItems.length === 0 && postingItems.length === 0) return []
  const maxLen = Math.max(detailItems.length, postingItems.length)
  const out: any[] = []
  for (let i = 0; i < maxLen; i += 1) {
    out.push(mergeItem(detailItems[i], postingItems[i]))
  }
  return out
}

function relatedOf(detail: any, posting: any, fallback: string[]): string {
  const direct = pickFromSources([
    'related_postings.related_posting_numbers',
    'result.related_postings.related_posting_numbers',
    'related_postings.related_postings',
    'related_postings',
  ], detail, posting)
  if (Array.isArray(direct)) {
    const vals = direct.map((v) => text(v)).filter(Boolean)
    if (vals.length > 0) return vals.join(', ')
  }
  const raw = text(direct)
  if (raw) return raw
  return fallback.join(', ')
}

function pushTraceSample(target: string[], value: string, limit = 10) {
  if (!value || target.includes(value) || target.length >= limit) return
  target.push(value)
}

function countBySql(conn: Database.Database, sql: string, ...params: any[]): number {
  const row = conn.prepare(sql).get(...params) as { cnt?: number } | undefined
  return Number(row?.cnt ?? 0)
}

function getPersistedShipmentDateForPosting(storeClientId: string, postingNumber: string, periodKey: string): string {
  const row = db().prepare(`
    SELECT MAX(changed_state_date) AS shipment_date
    FROM posting_state_events
    WHERE store_client_id = ?
      AND posting_number = ?
      AND period_key IN (?, ?)
      AND COALESCE(NULLIF(event_type, ''), 'type_state_changed') = 'type_state_changed'
      AND COALESCE(NULLIF(new_state, ''), state) = 'posting_transferring_to_delivery'
  `).get(storeClientId, postingNumber, periodKey, FBO_SHIPMENT_PUSH_PERIOD_KEY) as { shipment_date?: string | null } | undefined
  return text(row?.shipment_date)
}

function shipmentDateOf(detail: any, posting: any): string {
  return resolveFboShipmentDateFromSources(detail, posting)
}

function hasDeliveredStateOf(...sources: any[]): boolean {
  for (const source of sources) {
    const state = text(pick(source, [
      'status',
      'result.status',
      'provider_status',
      'result.provider_status',
      'new_state',
      'result.new_state',
      'state',
      'result.state',
    ])).toLowerCase()
    if (state && FBO_DELIVERED_STATES.has(state)) return true
  }
  return false
}

function deliveryDateOf(detail: any, posting: any): string {
  if (!hasDeliveredStateOf(detail, posting)) return ''

  const exact = dateText(pickFromSources([
    'fact_delivery_date',
    'result.fact_delivery_date',
  ], detail, posting))
  if (exact) return exact

  return dateText(pickFromSources([
    'result.customer_deliver_date',
    'customer_deliver_date',
    'result.delivered_at',
    'delivered_at',
    'delivered_date',
    'result.delivered_date',
  ], detail, posting))
}

function deliveryClusterOf(detail: any, posting: any): string {
  return text(pickFromSources([
    'financial_data.cluster_to',
    'result.financial_data.cluster_to',
    'cluster_to',
    'result.cluster_to',
  ], detail, posting))
}

export function buildAndPersistFboSalesSnapshot(args: {
  storeClientId: string
  periodKey?: string | null
  fboPayloads: SalesPayloadEnvelope[]
  postingDetailsByKey: Map<string, any>
  fetchedAt?: string
}) {
  const storeClientId = text(args.storeClientId)
  if (!storeClientId) return {
    postingsCount: 0,
    itemsCount: 0,
    eventsCount: 0,
    trace: {
      postingsSeen: 0,
      postingsWithDetail: 0,
      postingsWithoutDetail: 0,
      postingsWithAnyStateEvents: 0,
      postingsWithShipmentTransferEvent: 0,
      postingsWithResolvedShipmentDate: 0,
      missingDetailPostingNumbers: [],
      missingShipmentDatePostingNumbers: [],
      shipmentTransferPostingNumbers: [],
    },
    persisted: {
      postingsCount: 0,
      itemsCount: 0,
      eventsCount: 0,
      shipmentDateCount: 0,
      shipmentTransferEventCount: 0,
    },
  }
  const periodKey = text(args.periodKey)
  const fetchedAt = text(args.fetchedAt) || new Date().toISOString()
  const orderPostings = new Map<string, Set<string>>()
  const postingRows = new Map<string, any>()
  const itemRows = new Map<string, any>()
  const eventRows = new Map<string, any>()
  const postingsSeen = new Set<string>()
  const postingsWithDetail = new Set<string>()
  const postingsWithAnyStateEvents = new Set<string>()
  const postingsWithShipmentTransferEvent = new Set<string>()
  const postingsWithResolvedShipmentDate = new Set<string>()
  const missingDetailPostingNumbers: string[] = []
  const missingShipmentDatePostingNumbers: string[] = []
  const shipmentTransferPostingNumbers: string[] = []

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
      const shipmentDate = shipmentDateOf(detail, posting) || getPersistedShipmentDateForPosting(storeClientId, postingNumber, periodKey)
      const stateEvents = collectSalesStateEvents(detail, posting)
      const shipmentTransferEvents = stateEvents.filter((event) => (
        text(event?.event_type) === FBO_STATE_CHANGED_EVENT_TYPE
        && text(event?.new_state || event?.state) === 'posting_transferring_to_delivery'
      ))

      postingsSeen.add(postingNumber)
      if (detail) postingsWithDetail.add(postingNumber)
      else pushTraceSample(missingDetailPostingNumbers, postingNumber)
      if (stateEvents.length > 0) postingsWithAnyStateEvents.add(postingNumber)
      if (shipmentTransferEvents.length > 0) {
        postingsWithShipmentTransferEvent.add(postingNumber)
        pushTraceSample(shipmentTransferPostingNumbers, postingNumber)
      }
      if (shipmentDate) postingsWithResolvedShipmentDate.add(postingNumber)
      else pushTraceSample(missingShipmentDatePostingNumbers, postingNumber)

      postingRows.set(postingNumber, {
        store_client_id: storeClientId,
        period_key: periodKey,
        posting_number: postingNumber,
        order_id: orderId || null,
        related_postings: relatedOf(detail, posting, fallback) || null,
        shipment_date: shipmentDate || null,
        delivery_date: deliveryDateOf(detail, posting) || null,
        delivery_cluster: deliveryClusterOf(detail, posting) || null,
        updated_at: fetchedAt,
      })

      if (postingNumber && stateEvents.length > 0) {
        for (const event of stateEvents) {
          const eventType = text(event.event_type) || FBO_STATE_CHANGED_EVENT_TYPE
          const newState = text(event.new_state) || text(event.state)
          const eventKey = `${eventType}|${newState}|${event.changed_state_date}`
          eventRows.set(`${postingNumber}|${eventKey}`, {
            store_client_id: storeClientId,
            period_key: periodKey,
            posting_number: postingNumber,
            event_key: eventKey,
            event_type: eventType,
            new_state: newState,
            state: newState,
            changed_state_date: event.changed_state_date,
            updated_at: fetchedAt,
          })
        }
      }

      const items = mergedItems(detail, posting)
      let lineNo = 0
      for (const item of items) {
        const sku = text((item as any)?.sku)
        const offerId = text((item as any)?.offer_id)
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
    conn.prepare('DELETE FROM posting_state_events WHERE store_client_id = ? AND period_key = ?').run(storeClientId, periodKey)
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
    const insertEvent = conn.prepare(`
      INSERT INTO posting_state_events (
        store_client_id, period_key, posting_number, event_key, event_type, new_state, state, changed_state_date, updated_at
      ) VALUES (
        @store_client_id, @period_key, @posting_number, @event_key, @event_type, @new_state, @state, @changed_state_date, @updated_at
      )
    `)

    for (const row of postingRows.values()) insertPosting.run(row)
    for (const row of itemRows.values()) insertItem.run(row)
    for (const row of eventRows.values()) insertEvent.run(row)
  })
  tx()

  const connAfter = db()
  const persistedPostingsCount = countBySql(
    connAfter,
    'SELECT COUNT(*) AS cnt FROM fbo_postings WHERE store_client_id = ? AND period_key = ?',
    storeClientId,
    periodKey,
  )
  const persistedItemsCount = countBySql(
    connAfter,
    'SELECT COUNT(*) AS cnt FROM fbo_posting_items WHERE store_client_id = ? AND period_key = ?',
    storeClientId,
    periodKey,
  )
  const persistedEventsCount = countBySql(
    connAfter,
    'SELECT COUNT(*) AS cnt FROM posting_state_events WHERE store_client_id = ? AND period_key = ?',
    storeClientId,
    periodKey,
  )
  const persistedShipmentDateCount = countBySql(
    connAfter,
    `SELECT COUNT(*) AS cnt
       FROM fbo_postings
      WHERE store_client_id = ?
        AND period_key = ?
        AND shipment_date IS NOT NULL
        AND shipment_date <> ''`,
    storeClientId,
    periodKey,
  )
  const persistedShipmentTransferEventCount = countBySql(
    connAfter,
    `SELECT COUNT(*) AS cnt
       FROM posting_state_events
      WHERE store_client_id = ?
        AND period_key = ?
        AND COALESCE(NULLIF(event_type, ''), 'type_state_changed') = 'type_state_changed'
        AND COALESCE(NULLIF(new_state, ''), state) = 'posting_transferring_to_delivery'`,
    storeClientId,
    periodKey,
  )

  return {
    postingsCount: postingRows.size,
    itemsCount: itemRows.size,
    eventsCount: eventRows.size,
    trace: {
      postingsSeen: postingsSeen.size,
      postingsWithDetail: postingsWithDetail.size,
      postingsWithoutDetail: Math.max(0, postingsSeen.size - postingsWithDetail.size),
      postingsWithAnyStateEvents: postingsWithAnyStateEvents.size,
      postingsWithShipmentTransferEvent: postingsWithShipmentTransferEvent.size,
      postingsWithResolvedShipmentDate: postingsWithResolvedShipmentDate.size,
      missingDetailPostingNumbers,
      missingShipmentDatePostingNumbers,
      shipmentTransferPostingNumbers,
    },
    persisted: {
      postingsCount: persistedPostingsCount,
      itemsCount: persistedItemsCount,
      eventsCount: persistedEventsCount,
      shipmentDateCount: persistedShipmentDateCount,
      shipmentTransferEventCount: persistedShipmentTransferEventCount,
    },
  }
}


export function persistFboPushShipmentEvents(args: {
  storeClientId: string
  events: Array<{
    posting_number: string
    event_type?: string | null
    new_state?: string | null
    state?: string | null
    changed_state_date: string
  }>
  fetchedAt?: string
}) {
  const storeClientId = text(args.storeClientId)
  const fetchedAt = text(args.fetchedAt) || new Date().toISOString()
  if (!storeClientId) {
    return {
      acceptedEventsCount: 0,
      shipmentTransferEventCount: 0,
      shipmentDateCount: 0,
      samplePostingNumbers: [] as string[],
    }
  }

  const normalizedEvents = (Array.isArray(args.events) ? args.events : [])
    .map((event) => {
      const postingNumber = text((event as any)?.posting_number)
      const changedStateDate = dateText((event as any)?.changed_state_date)
      const eventType = text((event as any)?.event_type) || FBO_STATE_CHANGED_EVENT_TYPE
      const newState = text((event as any)?.new_state || (event as any)?.state) || 'posting_transferring_to_delivery'
      if (!postingNumber || !changedStateDate) return null
      return {
        store_client_id: storeClientId,
        period_key: FBO_SHIPMENT_PUSH_PERIOD_KEY,
        posting_number: postingNumber,
        event_key: `${eventType}|${newState}|${changedStateDate}`,
        event_type: eventType,
        new_state: newState,
        state: newState,
        changed_state_date: changedStateDate,
        updated_at: fetchedAt,
      }
    })
    .filter((event): event is {
      store_client_id: string
      period_key: string
      posting_number: string
      event_key: string
      event_type: string
      new_state: string
      state: string
      changed_state_date: string
      updated_at: string
    } => Boolean(event))

  if (normalizedEvents.length === 0) {
    return {
      acceptedEventsCount: 0,
      shipmentTransferEventCount: 0,
      shipmentDateCount: 0,
      samplePostingNumbers: [] as string[],
    }
  }

  const conn = db()
  const insertEvent = conn.prepare(`
    INSERT INTO posting_state_events (
      store_client_id, period_key, posting_number, event_key, event_type, new_state, state, changed_state_date, updated_at
    ) VALUES (
      @store_client_id, @period_key, @posting_number, @event_key, @event_type, @new_state, @state, @changed_state_date, @updated_at
    )
    ON CONFLICT(store_client_id, period_key, posting_number, event_key) DO UPDATE SET
      event_type = excluded.event_type,
      new_state = excluded.new_state,
      state = excluded.state,
      changed_state_date = excluded.changed_state_date,
      updated_at = excluded.updated_at
  `)
  const updatePostingShipmentDate = conn.prepare(`
    UPDATE fbo_postings
    SET shipment_date = CASE
          WHEN shipment_date IS NULL OR shipment_date = '' OR shipment_date < @changed_state_date THEN @changed_state_date
          ELSE shipment_date
        END,
        updated_at = @updated_at
    WHERE store_client_id = @store_client_id
      AND posting_number = @posting_number
  `)

  const tx = conn.transaction((rows: typeof normalizedEvents) => {
    for (const row of rows) {
      insertEvent.run(row)
      if (text(row.new_state) === 'posting_transferring_to_delivery') {
        updatePostingShipmentDate.run(row)
      }
    }
  })
  tx(normalizedEvents)

  const shipmentTransferEventCount = countBySql(
    conn,
    `SELECT COUNT(*) AS cnt
       FROM posting_state_events
      WHERE store_client_id = ?
        AND period_key = ?
        AND COALESCE(NULLIF(event_type, ''), 'type_state_changed') = 'type_state_changed'
        AND COALESCE(NULLIF(new_state, ''), state) = 'posting_transferring_to_delivery'`,
    storeClientId,
    FBO_SHIPMENT_PUSH_PERIOD_KEY,
  )
  const shipmentDateCount = countBySql(
    conn,
    `SELECT COUNT(*) AS cnt
       FROM fbo_postings
      WHERE store_client_id = ?
        AND shipment_date IS NOT NULL
        AND shipment_date <> ''`,
    storeClientId,
  )

  const samplePostingNumbers: string[] = []
  for (const row of normalizedEvents) {
    pushTraceSample(samplePostingNumbers, row.posting_number, 10)
  }

  return {
    acceptedEventsCount: normalizedEvents.length,
    shipmentTransferEventCount,
    shipmentDateCount,
    samplePostingNumbers,
  }
}

export function mergeSalesRowsWithFboLocalDb(args: {
  rows: any[]
  storeClientId?: string | null
  periodKey?: string | null
}) {
  const periodKey = text(args.periodKey)
  const rows = Array.isArray(args.rows) ? args.rows : []
  if (rows.length === 0) return rows

  const storeClientId = text(args.storeClientId)
  let sql = `
    SELECT
      p.posting_number,
      p.related_postings,
      COALESCE(
        p.shipment_date,
        (
          SELECT MAX(e.changed_state_date)
          FROM posting_state_events e
          WHERE e.store_client_id = p.store_client_id
            AND e.posting_number = p.posting_number
            AND e.period_key IN (p.period_key, '__push__')
            AND COALESCE(NULLIF(e.event_type, ''), 'type_state_changed') = 'type_state_changed'
            AND COALESCE(NULLIF(e.new_state, ''), e.state) = 'posting_transferring_to_delivery'
        )
      ) AS shipment_date,
      p.delivery_date,
      p.delivery_cluster
    FROM fbo_postings p
    WHERE p.period_key = ?
  `
  const params: any[] = [periodKey]
  if (storeClientId) {
    sql += ' AND p.store_client_id = ?'
    params.push(storeClientId)
  }

  const enrich = db().prepare(sql).all(...params) as any[]
  if (enrich.length === 0) return rows

  const byPosting = new Map<string, any>()
  for (const row of enrich) {
    const postingNumber = text(row?.posting_number)
    if (!postingNumber || byPosting.has(postingNumber)) continue
    byPosting.set(postingNumber, row)
  }

  return rows.map((row) => {
    const postingNumber = text(row?.posting_number)
    const extra = byPosting.get(postingNumber)
    if (!extra) return row

    const shipmentDateFromExtra = text(extra?.shipment_date)
    const shipmentDateFromRow = text(row?.shipment_date)
    const acceptedAt = text(row?.in_process_at)
    const normalizedExtraShipment = shipmentDateFromExtra && shipmentDateFromExtra !== acceptedAt ? shipmentDateFromExtra : ''
    const normalizedRowShipment = shipmentDateFromRow && shipmentDateFromRow !== acceptedAt ? shipmentDateFromRow : ''
    const shipmentDate = normalizedExtraShipment || normalizedRowShipment

    const deliveryDateFromRow = text(row?.delivery_date)
    const deliveryDateFromExtra = text(extra?.delivery_date)
    const rowStatus = text(row?.status).toLowerCase()
    const isDelivered = Boolean(
      rowStatus
      && (
        rowStatus.includes('доставлен')
        || rowStatus.includes('получен покупателем')
        || FBO_DELIVERED_STATES.has(rowStatus)
      )
    )
    const deliveryDate = isDelivered ? (deliveryDateFromRow || deliveryDateFromExtra) : ''

    return {
      ...row,
      related_postings: text(row?.related_postings) || text(extra?.related_postings),
      shipment_date: shipmentDate,
      delivery_date: deliveryDate,
      delivery_cluster: text(row?.delivery_cluster) || text(extra?.delivery_cluster),
      delivery_model: text(row?.delivery_model) || 'FBO',
    }
  })
}
