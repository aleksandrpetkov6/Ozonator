import { ozonPostingFboList, ozonPostingFbsList } from './ozon'
import { fetchSalesEndpointPages, fetchSalesPostingDetails, normalizeSalesRows, type SalesPeriod } from './sales-sync'
import { dbGetDatasetSnapshotRows, dbGetLatestApiRawResponses, dbGetProducts, dbGetStockViewRows, dbRecordApiRawResponse, dbSaveDatasetSnapshot } from './storage/db'
import { buildAndPersistFboSalesSnapshot, mergeSalesRowsWithFboLocalDb } from './storage/fbo-sales'
import type { Secrets } from './types'

const SALES_CACHE_SNAPSHOT_ENDPOINTS = {
  fbs: '/__local__/sales-cache/fbs',
  fbo: '/__local__/sales-cache/fbo',
  details: '/__local__/sales-cache/posting-details',
} as const

const SALES_CACHE_SNAPSHOT_KEYS = [
  SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs,
  SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo,
  SALES_CACHE_SNAPSHOT_ENDPOINTS.details,
] as const

const SALES_LEGACY_ENDPOINTS = ['/v3/posting/fbs/list', '/v2/posting/fbo/list'] as const

export type LocalDatasetName = string

function parseJsonTextSafe(text: string | null | undefined) {
  if (typeof text !== 'string' || !text.trim()) return null
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}

function normalizeSalesPeriodValue(value: any): string | null {
  const raw = typeof value === 'string' ? value.trim() : ''
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null
}

function normalizeSalesPeriod(period: SalesPeriod | null | undefined): { from: string | null; to: string | null } {
  let from = normalizeSalesPeriodValue(period?.from)
  let to = normalizeSalesPeriodValue(period?.to)
  if (!from && to) from = to
  if (from && !to) to = from
  if (from && to && from > to) [from, to] = [to, from]
  return { from, to }
}

function sameSalesPeriod(
  left: { from?: string | null; to?: string | null } | null | undefined,
  right: { from?: string | null; to?: string | null } | null | undefined,
) {
  return (left?.from ?? null) === (right?.from ?? null) && (left?.to ?? null) === (right?.to ?? null)
}

function getSalesSnapshotMap(storeClientId: string | null | undefined) {
  const scoped = dbGetLatestApiRawResponses(storeClientId ?? null, SALES_CACHE_SNAPSHOT_KEYS as unknown as string[])
  const rows = scoped.length > 0 ? scoped : dbGetLatestApiRawResponses(null, SALES_CACHE_SNAPSHOT_KEYS as unknown as string[])
  const out = new Map<string, any>()
  for (const row of rows) {
    if (out.has(row.endpoint)) continue
    const parsed = parseJsonTextSafe(row?.response_body)
    if (parsed) out.set(row.endpoint, parsed)
  }
  return out
}

function getLegacySalesPayloadMap(storeClientId: string | null | undefined) {
  const scoped = dbGetLatestApiRawResponses(storeClientId ?? null, SALES_LEGACY_ENDPOINTS as unknown as string[])
  const rows = scoped.length > 0 ? scoped : dbGetLatestApiRawResponses(null, SALES_LEGACY_ENDPOINTS as unknown as string[])
  const out = new Map<string, any>()
  for (const row of rows) {
    if (out.has(row.endpoint)) continue
    const parsed = parseJsonTextSafe(row?.response_body)
    if (parsed) out.set(row.endpoint, parsed)
  }
  return out
}

function buildSalesPayloadsFromSnapshotMap(
  cacheByEndpoint: Map<string, any>,
  requestedPeriod: SalesPeriod | null | undefined,
) {
  const normalizedRequestedPeriod = normalizeSalesPeriod(requestedPeriod)
  const out: Array<{ endpoint: string; payload: any }> = []

  const pushSnapshot = (snapshot: any, fallbackEndpoint: string) => {
    if (!snapshot || typeof snapshot !== 'object') return
    const snapshotPeriod = normalizeSalesPeriod(snapshot?.period ?? null)
    if (!sameSalesPeriod(snapshotPeriod, normalizedRequestedPeriod)) return
    const payloads = Array.isArray(snapshot?.payloads) ? snapshot.payloads : []
    const sourceEndpoint = String(snapshot?.sourceEndpoint ?? '').trim() || fallbackEndpoint
    for (const payload of payloads) out.push({ endpoint: sourceEndpoint, payload })
  }

  pushSnapshot(cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs), '/v3/posting/fbs/list')
  pushSnapshot(cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo), '/v2/posting/fbo/list')
  return out
}

function buildSalesPostingDetailsFromSnapshotMap(
  cacheByEndpoint: Map<string, any>,
  requestedPeriod: SalesPeriod | null | undefined,
): Map<string, any> {
  const detailsSnapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.details)
  if (!detailsSnapshot || typeof detailsSnapshot !== 'object') return new Map<string, any>()
  const normalizedRequestedPeriod = normalizeSalesPeriod(requestedPeriod)
  const detailsPeriod = normalizeSalesPeriod(detailsSnapshot?.period ?? null)
  if (!sameSalesPeriod(detailsPeriod, normalizedRequestedPeriod)) return new Map<string, any>()

  const out = new Map<string, any>()
  const items = Array.isArray(detailsSnapshot?.items) ? detailsSnapshot.items : []
  for (const item of items) {
    const key = String(item?.key ?? '').trim()
    if (!key) continue
    out.set(key, item?.payload ?? null)
  }
  return out
}

function buildSalesRowsFromLocalRawCache(storeClientId: string | null | undefined, requestedPeriod: SalesPeriod | null | undefined) {
  const products = dbGetProducts(storeClientId ?? null)
  const cacheByEndpoint = getSalesSnapshotMap(storeClientId)
  const payloads = buildSalesPayloadsFromSnapshotMap(cacheByEndpoint, requestedPeriod)
  const postingDetailsByKey = buildSalesPostingDetailsFromSnapshotMap(cacheByEndpoint, requestedPeriod)
  const sourceEndpoints = new Set<string>()

  for (const payload of payloads) sourceEndpoints.add(String(payload.endpoint))
  if (payloads.length === 0 && cacheByEndpoint.size === 0) {
    const legacyPayloads = getLegacySalesPayloadMap(storeClientId)
    for (const [endpoint, payload] of legacyPayloads.entries()) {
      payloads.push({ endpoint, payload })
      sourceEndpoints.add(endpoint)
    }
  }

  const rows = normalizeSalesRows(payloads, products, postingDetailsByKey)
  const mergedRows = mergeSalesRowsWithFboLocalDb({
    rows,
    storeClientId: storeClientId ?? null,
    periodKey: buildDatasetScopeKey(requestedPeriod),
  })
  return { rows: mergedRows, sourceEndpoints: Array.from(sourceEndpoints) }
}

function buildDatasetScopeKey(requestedPeriod: SalesPeriod | null | undefined): string {
  const normalized = normalizeSalesPeriod(requestedPeriod)
  if (!normalized.from && !normalized.to) return ''
  return `${normalized.from ?? ''}|${normalized.to ?? ''}`
}

function persistDatasetSnapshot(args: {
  storeClientId?: string | null
  dataset: string
  rows: any[]
  scopeKey?: string | null
  period?: SalesPeriod | null
  sourceKind?: string
  sourceEndpoints?: string[]
}) {
  const period = normalizeSalesPeriod(args.period ?? null)
  dbSaveDatasetSnapshot({
    storeClientId: args.storeClientId ?? null,
    dataset: args.dataset,
    scopeKey: args.scopeKey ?? '',
    periodFrom: period.from,
    periodTo: period.to,
    sourceKind: args.sourceKind ?? 'projection',
    sourceEndpoints: args.sourceEndpoints ?? [],
    rows: Array.isArray(args.rows) ? args.rows : [],
  })
}

export function refreshCoreLocalDatasetSnapshots(storeClientId: string | null | undefined) {
  const productsRows = dbGetProducts(storeClientId ?? null)
  const stocksRows = dbGetStockViewRows(storeClientId ?? null)

  persistDatasetSnapshot({ storeClientId, dataset: 'products', rows: productsRows, sourceKind: 'db-table' })
  persistDatasetSnapshot({ storeClientId, dataset: 'returns', rows: productsRows, sourceKind: 'derived-products' })
  persistDatasetSnapshot({ storeClientId, dataset: 'forecast-demand', rows: productsRows, sourceKind: 'derived-products' })
  persistDatasetSnapshot({ storeClientId, dataset: 'stocks', rows: stocksRows, sourceKind: 'db-view' })

  return {
    productsRowsCount: productsRows.length,
    stocksRowsCount: stocksRows.length,
  }
}

export async function refreshSalesRawSnapshotFromApi(
  secrets: Secrets,
  requestedPeriod: SalesPeriod | null | undefined,
) {
  const normalizedRequestedPeriod = normalizeSalesPeriod(requestedPeriod)
  const fbsPayloads = await fetchSalesEndpointPages(
    (body) => ozonPostingFbsList(secrets, body),
    requestedPeriod,
    '/v3/posting/fbs/list',
  )
  const fboPayloads = await fetchSalesEndpointPages(
    (body) => ozonPostingFboList(secrets, body),
    requestedPeriod,
    '/v2/posting/fbo/list',
  )
  const payloads = [...fbsPayloads, ...fboPayloads]
  const postingDetailsByKey = payloads.length > 0
    ? await fetchSalesPostingDetails(secrets, payloads)
    : new Map<string, any>()
  const fetchedAt = new Date().toISOString()

  buildAndPersistFboSalesSnapshot({
    storeClientId: secrets.clientId,
    periodKey: buildDatasetScopeKey(requestedPeriod),
    fboPayloads,
    postingDetailsByKey,
    fetchedAt,
  })

  const persistRawSnapshot = (endpoint: string, responseBody: any) => {
    dbRecordApiRawResponse({
      storeClientId: secrets.clientId,
      method: 'LOCAL',
      endpoint,
      requestBody: {
        mode: 'sales-cache-snapshot',
        period: normalizedRequestedPeriod,
      },
      responseBody,
      httpStatus: 200,
      isSuccess: true,
      fetchedAt,
    })
  }

  persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs, {
    sourceEndpoint: '/v3/posting/fbs/list',
    period: normalizedRequestedPeriod,
    payloads: fbsPayloads.map((item) => item.payload),
  })
  persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo, {
    sourceEndpoint: '/v2/posting/fbo/list',
    period: normalizedRequestedPeriod,
    payloads: fboPayloads.map((item) => item.payload),
  })
  persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.details, {
    period: normalizedRequestedPeriod,
    items: Array.from(postingDetailsByKey.entries()).map(([key, payload]) => ({ key, payload })),
  })

  const { rows, sourceEndpoints } = buildSalesRowsFromLocalRawCache(secrets.clientId, requestedPeriod)
  persistDatasetSnapshot({
    storeClientId: secrets.clientId,
    dataset: 'sales',
    scopeKey: buildDatasetScopeKey(requestedPeriod),
    period: requestedPeriod,
    rows,
    sourceKind: 'api-raw-cache',
    sourceEndpoints,
  })

  return { rowsCount: rows.length }
}

function salesRowNeedsFboBackfill(row: any): boolean {
  if (!row || typeof row !== 'object') return false
  const model = String(row?.delivery_model ?? '').trim().toUpperCase()
  if (model !== 'FBO') return false

  const status = String(row?.status ?? '').trim()
  const shipmentDate = String(row?.shipment_date ?? '').trim()
  const deliveryCluster = String(row?.delivery_cluster ?? '').trim()
  const deliveryDate = String(row?.delivery_date ?? '').trim()

  if (!shipmentDate) return true
  if (!deliveryCluster) return true
  if (status === 'Доставлен' && !deliveryDate) return true
  return false
}

function salesRowsNeedFboBackfill(rows: any[]): boolean {
  return rows.some((row) => salesRowNeedsFboBackfill(row))
}

export async function ensureLocalSalesSnapshotFromApiIfMissing(
  secrets: Secrets | null | undefined,
  requestedPeriod: SalesPeriod | null | undefined,
): Promise<{ refreshed: boolean; rowsCount: number }> {
  const storeClientId = secrets?.clientId ?? null
  const localRows = getLocalDatasetRows(storeClientId, 'sales', { period: requestedPeriod ?? null })
  const rows = Array.isArray(localRows) ? localRows : []
  const needsRefresh = rows.length === 0 || salesRowsNeedFboBackfill(rows)

  if (!secrets || !needsRefresh) {
    return { refreshed: false, rowsCount: rows.length }
  }

  try {
    await refreshSalesRawSnapshotFromApi(secrets, requestedPeriod)
    const refreshedRows = getLocalDatasetRows(storeClientId, 'sales', { period: requestedPeriod ?? null })
    return {
      refreshed: true,
      rowsCount: Array.isArray(refreshedRows) ? refreshedRows.length : 0,
    }
  } catch {
    return { refreshed: false, rowsCount: rows.length }
  }
}

export function getLocalDatasetRows(
  storeClientId: string | null | undefined,
  datasetRaw: LocalDatasetName,
  options?: { period?: SalesPeriod | null },
): any[] {
  const dataset = String(datasetRaw ?? '').trim() || 'products'
  const scopeKey = buildDatasetScopeKey(options?.period ?? null)
  const fromSnapshot = dbGetDatasetSnapshotRows({ storeClientId: storeClientId ?? null, dataset, scopeKey })
  if (Array.isArray(fromSnapshot)) return fromSnapshot

  if (dataset === 'products') {
    const rows = dbGetProducts(storeClientId ?? null)
    persistDatasetSnapshot({ storeClientId, dataset, rows, sourceKind: 'db-table' })
    return rows
  }

  if (dataset === 'stocks') {
    const rows = dbGetStockViewRows(storeClientId ?? null)
    persistDatasetSnapshot({ storeClientId, dataset, rows, sourceKind: 'db-view' })
    return rows
  }

  if (dataset === 'returns' || dataset === 'forecast-demand') {
    const rows = dbGetProducts(storeClientId ?? null)
    persistDatasetSnapshot({ storeClientId, dataset, rows, sourceKind: 'derived-products' })
    return rows
  }

  if (dataset === 'sales') {
    const { rows, sourceEndpoints } = buildSalesRowsFromLocalRawCache(storeClientId ?? null, options?.period ?? null)
    persistDatasetSnapshot({
      storeClientId,
      dataset,
      scopeKey,
      period: options?.period ?? null,
      rows,
      sourceKind: 'api-raw-cache',
      sourceEndpoints,
    })
    return rows
  }

  return []
}
