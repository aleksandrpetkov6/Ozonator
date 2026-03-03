import { ozonPostingFboList, ozonPostingFbsList } from './ozon'
import { fetchSalesEndpointPages, fetchSalesPostingDetails, getSalesPostingDetailsKey, normalizeSalesRows, resolveFboShipmentDateFromSources, type SalesPeriod } from './sales-sync'
import { dbGetDatasetSnapshotRows, dbGetLatestApiRawResponses, dbGetProducts, dbGetStockViewRows, dbRecordApiRawResponse, dbSaveDatasetSnapshot } from './storage/db'
import { buildAndPersistFboSalesSnapshot, mergeSalesRowsWithFboLocalDb } from './storage/fbo-sales'
import { fetchFboPostingDetailsCompat } from './fbo-detail-compat'
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

function normalizeTextValue(value: any): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function getByPath(source: any, path: string): any {
  if (!source || typeof source !== 'object') return undefined
  let cur = source
  for (const part of String(path ?? '').split('.').filter(Boolean)) {
    if (cur == null || typeof cur !== 'object' || !(part in cur)) return undefined
    cur = cur[part]
  }
  return cur
}

function pickFirstPresent(source: any, paths: string[]): any {
  for (const path of paths) {
    const value = getByPath(source, path)
    if (value === undefined || value === null || value === '') continue
    return value
  }
  return undefined
}

function hasFboCompatDetail(detail: any): boolean {
  const cluster = normalizeTextValue(pickFirstPresent(detail, [
    'financial_data.cluster_to',
    'result.financial_data.cluster_to',
    'cluster_to',
    'result.cluster_to',
  ]))
  if (!cluster) return false

  const shipment = normalizeTextValue(resolveFboShipmentDateFromSources(detail))
  return Boolean(shipment)
}

function collectFboPostingNumbersNeedingCompat(
  fboPayloads: Array<{ endpoint: string; payload: any }>,
  postingDetailsByKey: Map<string, any>,
): string[] {
  const out: string[] = []
  const seen = new Set<string>()

  for (const envelope of fboPayloads) {
    const postings = Array.isArray(envelope?.payload?.result?.postings)
      ? envelope.payload.result.postings
      : (Array.isArray(envelope?.payload?.postings) ? envelope.payload.postings : [])
    for (const posting of postings) {
      const postingNumber = normalizeTextValue(posting?.posting_number ?? posting?.postingNumber)
      if (!postingNumber || seen.has(postingNumber)) continue
      seen.add(postingNumber)
      const key = getSalesPostingDetailsKey('FBO', postingNumber)
      if (hasFboCompatDetail(postingDetailsByKey.get(key))) continue
      out.push(postingNumber)
    }
  }

  return out
}

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

function extractSalesRowPeriodDay(value: unknown): string {
  const raw = typeof value === 'string' ? value.trim() : ''
  if (!raw) return ''

  const isoHead = raw.slice(0, 10)
  if (/^\d{4}-\d{2}-\d{2}$/.test(isoHead)) return isoHead

  const ruMatch = raw.match(/^(\d{2})\.(\d{2})\.(\d{2}|\d{4})/)
  if (ruMatch) {
    const day = ruMatch[1]
    const month = ruMatch[2]
    const yearRaw = ruMatch[3]
    const year = yearRaw.length === 2 ? `20${yearRaw}` : yearRaw
    return `${year}-${month}-${day}`
  }

  const parsed = new Date(raw)
  if (Number.isNaN(parsed.getTime())) return ''
  return parsed.toISOString().slice(0, 10)
}

function filterSalesRowsStrictByPeriod(
  rows: any[],
  requestedPeriod: SalesPeriod | null | undefined,
): any[] {
  const normalized = normalizeSalesPeriod(requestedPeriod)
  let from = normalized.from
  let to = normalized.to

  if (!from && !to) return Array.isArray(rows) ? rows : []
  if (!from && to) from = to
  if (from && !to) to = from
  if (!from || !to) return Array.isArray(rows) ? rows : []
  if (from > to) [from, to] = [to, from]

  return (Array.isArray(rows) ? rows : []).filter((row) => {
    const day = extractSalesRowPeriodDay(row?.in_process_at)
    return Boolean(day && day >= from! && day <= to!)
  })
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

  const pushSnapshot = (snapshot: any, fallbackEndpoint: string, allowAnyPeriod = false) => {
    if (!snapshot || typeof snapshot !== 'object') return false
    const snapshotPeriod = normalizeSalesPeriod(snapshot?.period ?? null)
    if (!allowAnyPeriod && !sameSalesPeriod(snapshotPeriod, normalizedRequestedPeriod)) return false
    const payloads = Array.isArray(snapshot?.payloads) ? snapshot.payloads : []
    const sourceEndpoint = String(snapshot?.sourceEndpoint ?? '').trim() || fallbackEndpoint
    for (const payload of payloads) out.push({ endpoint: sourceEndpoint, payload })
    return payloads.length > 0
  }

  const fbsSnapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs)
  const fboSnapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo)
  const hasExact = [
    pushSnapshot(fbsSnapshot, '/v3/posting/fbs/list'),
    pushSnapshot(fboSnapshot, '/v2/posting/fbo/list'),
  ].some(Boolean)

  if (!hasExact) {
    pushSnapshot(fbsSnapshot, '/v3/posting/fbs/list', true)
    pushSnapshot(fboSnapshot, '/v2/posting/fbo/list', true)
  }

  return out
}

function getSalesRawCoverageFromSnapshotMap(cacheByEndpoint: Map<string, any>) {
  let hasPayloads = false
  let from: string | null = null
  let to: string | null = null

  for (const endpoint of [SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs, SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo]) {
    const snapshot = cacheByEndpoint.get(endpoint)
    if (!snapshot || typeof snapshot !== 'object') continue

    const payloads = Array.isArray(snapshot?.payloads) ? snapshot.payloads : []
    if (payloads.length > 0) hasPayloads = true

    const period = normalizeSalesPeriod(snapshot?.period ?? null)
    const periodFrom = period.from
    const periodTo = period.to
    if (periodFrom && (!from || periodFrom < from)) from = periodFrom
    if (periodTo && (!to || periodTo > to)) to = periodTo
  }

  return { hasPayloads, from, to }
}

function isRequestedSalesPeriodCoveredByRawCache(
  cacheByEndpoint: Map<string, any>,
  requestedPeriod: SalesPeriod | null | undefined,
) {
  const requested = normalizeSalesPeriod(requestedPeriod)
  const coverage = getSalesRawCoverageFromSnapshotMap(cacheByEndpoint)

  if (!requested.from && !requested.to) return coverage.hasPayloads
  if (!coverage.hasPayloads) return false

  let from = requested.from
  let to = requested.to
  if (!from && to) from = to
  if (from && !to) to = from
  if (!from || !to) return false
  if (from > to) [from, to] = [to, from]

  if (!coverage.from || !coverage.to) return false
  return from >= coverage.from && to <= coverage.to
}

function buildSalesPostingDetailsFromSnapshotMap(
  cacheByEndpoint: Map<string, any>,
  requestedPeriod: SalesPeriod | null | undefined,
): Map<string, any> {
  const detailsSnapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.details)
  if (!detailsSnapshot || typeof detailsSnapshot !== 'object') return new Map<string, any>()
  const normalizedRequestedPeriod = normalizeSalesPeriod(requestedPeriod)
  const detailsPeriod = normalizeSalesPeriod(detailsSnapshot?.period ?? null)
  const useSnapshot = sameSalesPeriod(detailsPeriod, normalizedRequestedPeriod) || Boolean(detailsSnapshot?.items)
  if (!useSnapshot) return new Map<string, any>()

  const out = new Map<string, any>()
  const items = Array.isArray(detailsSnapshot?.items) ? detailsSnapshot.items : []
  for (const item of items) {
    const key = String(item?.key ?? '').trim()
    if (!key) continue
    out.set(key, item?.payload ?? null)
  }
  return out
}

function buildSalesRowsFromPayloads(
  storeClientId: string | null | undefined,
  requestedPeriod: SalesPeriod | null | undefined,
  payloads: Array<{ endpoint: string; payload: any }>,
  postingDetailsByKey: Map<string, any>,
) {
  const products = dbGetProducts(storeClientId ?? null)
  const sourceEndpoints = new Set<string>()

  for (const payload of payloads) {
    const endpoint = String(payload?.endpoint ?? '').trim()
    if (endpoint) sourceEndpoints.add(endpoint)
  }

  const rows = normalizeSalesRows(payloads, products, postingDetailsByKey)
  const mergedRows = mergeSalesRowsWithFboLocalDb({
    rows,
    storeClientId: storeClientId ?? null,
    periodKey: buildDatasetScopeKey(requestedPeriod),
  })
  const strictRows = filterSalesRowsStrictByPeriod(mergedRows, requestedPeriod)
  return { rows: strictRows, sourceEndpoints: Array.from(sourceEndpoints) }
}

function buildSalesRowsFromLocalRawCache(storeClientId: string | null | undefined, requestedPeriod: SalesPeriod | null | undefined) {
  const cacheByEndpoint = getSalesSnapshotMap(storeClientId)
  const payloads = buildSalesPayloadsFromSnapshotMap(cacheByEndpoint, requestedPeriod)
  const postingDetailsByKey = buildSalesPostingDetailsFromSnapshotMap(cacheByEndpoint, requestedPeriod)

  if (payloads.length === 0 && cacheByEndpoint.size === 0) {
    const legacyPayloads = getLegacySalesPayloadMap(storeClientId)
    for (const [endpoint, payload] of legacyPayloads.entries()) {
      payloads.push({ endpoint, payload })
    }
  }

  return buildSalesRowsFromPayloads(storeClientId, requestedPeriod, payloads, postingDetailsByKey)
}

function readScopedSalesSnapshotRows(
  storeClientId: string | null | undefined,
  requestedPeriod: SalesPeriod | null | undefined,
): any[] | null {
  const rows = dbGetDatasetSnapshotRows({
    storeClientId: storeClientId ?? null,
    dataset: 'sales',
    scopeKey: buildDatasetScopeKey(requestedPeriod),
  })

  if (!Array.isArray(rows)) return null
  return filterSalesRowsStrictByPeriod(rows, requestedPeriod)
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
  const compatFboPostingNumbers = collectFboPostingNumbersNeedingCompat(fboPayloads, postingDetailsByKey)
  if (compatFboPostingNumbers.length > 0) {
    const compatDetails = await fetchFboPostingDetailsCompat(secrets, compatFboPostingNumbers)
    for (const [postingNumber, payload] of compatDetails.entries()) {
      postingDetailsByKey.set(getSalesPostingDetailsKey('FBO', postingNumber), payload)
    }
  }
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

  const { rows, sourceEndpoints } = buildSalesRowsFromPayloads(secrets.clientId, requestedPeriod, payloads, postingDetailsByKey)
  persistDatasetSnapshot({
    storeClientId: secrets.clientId,
    dataset: 'sales',
    scopeKey: buildDatasetScopeKey(requestedPeriod),
    period: requestedPeriod,
    rows,
    sourceKind: 'api-live',
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
  const cacheByEndpoint = getSalesSnapshotMap(storeClientId)
  const hasLocalCoverage = isRequestedSalesPeriodCoveredByRawCache(cacheByEndpoint, requestedPeriod)

  if (!hasLocalCoverage && secrets) {
    const refreshed = await refreshSalesRawSnapshotFromApi(secrets, requestedPeriod)
    return {
      refreshed: true,
      rowsCount: Number(refreshed?.rowsCount ?? 0),
    }
  }

  const snapshotRows = readScopedSalesSnapshotRows(storeClientId, requestedPeriod)
  if (snapshotRows) {
    return {
      refreshed: false,
      rowsCount: snapshotRows.length,
    }
  }

  if (hasLocalCoverage) {
    const { rows, sourceEndpoints } = buildSalesRowsFromLocalRawCache(storeClientId, requestedPeriod)
    persistDatasetSnapshot({
      storeClientId,
      dataset: 'sales',
      scopeKey: buildDatasetScopeKey(requestedPeriod),
      period: requestedPeriod,
      rows,
      sourceKind: 'api-raw-cache',
      sourceEndpoints,
    })
    return {
      refreshed: false,
      rowsCount: rows.length,
    }
  }

  return {
    refreshed: false,
    rowsCount: 0,
  }
}

export function getLocalDatasetRows(
  storeClientId: string | null | undefined,
  datasetRaw: LocalDatasetName,
  options?: { period?: SalesPeriod | null },
): any[] {
  const dataset = String(datasetRaw ?? '').trim() || 'products'
  const requestedPeriod = options?.period ?? null
  const scopeKey = buildDatasetScopeKey(requestedPeriod)

  if (dataset === 'sales') {
    const exactSnapshotRows = readScopedSalesSnapshotRows(storeClientId ?? null, requestedPeriod)
    if (exactSnapshotRows) return exactSnapshotRows

    if (scopeKey) {
      const broadSnapshotRows = readScopedSalesSnapshotRows(storeClientId ?? null, null)
      if (broadSnapshotRows && broadSnapshotRows.length > 0) {
        const scopedRows = filterSalesRowsStrictByPeriod(broadSnapshotRows, requestedPeriod)
        if (scopedRows.length > 0) {
          persistDatasetSnapshot({
            storeClientId,
            dataset,
            scopeKey,
            period: requestedPeriod,
            rows: scopedRows,
            sourceKind: 'dataset-snapshot-fallback',
            sourceEndpoints: [],
          })
          return scopedRows
        }
      }
    }

    const cacheByEndpoint = getSalesSnapshotMap(storeClientId ?? null)
    const hasLocalCoverage = isRequestedSalesPeriodCoveredByRawCache(cacheByEndpoint, requestedPeriod)
    if (cacheByEndpoint.size > 0 && hasLocalCoverage) {
      const { rows, sourceEndpoints } = buildSalesRowsFromLocalRawCache(storeClientId ?? null, requestedPeriod)
      persistDatasetSnapshot({
        storeClientId,
        dataset,
        scopeKey,
        period: requestedPeriod,
        rows,
        sourceKind: 'api-raw-cache',
        sourceEndpoints,
      })
      return rows
    }

    if (cacheByEndpoint.size > 0 && scopeKey) {
      const { rows: broadRows, sourceEndpoints } = buildSalesRowsFromLocalRawCache(storeClientId ?? null, null)
      const scopedRows = filterSalesRowsStrictByPeriod(broadRows, requestedPeriod)
      if (scopedRows.length > 0) {
        persistDatasetSnapshot({
          storeClientId,
          dataset,
          scopeKey,
          period: requestedPeriod,
          rows: scopedRows,
          sourceKind: 'api-raw-cache-fallback',
          sourceEndpoints,
        })
        return scopedRows
      }
    }
  }

  const fromSnapshot = dbGetDatasetSnapshotRows({ storeClientId: storeClientId ?? null, dataset, scopeKey })
  if (Array.isArray(fromSnapshot)) {
    if (dataset === 'sales') {
      return filterSalesRowsStrictByPeriod(fromSnapshot, options?.period ?? null)
    }
    return fromSnapshot
  }

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
    return []
  }

  return []
}
