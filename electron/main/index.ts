import { app, BrowserWindow, ipcMain, nativeTheme, safeStorage, net, dialog } from 'electron'
import { join } from 'path'
import { appendFileSync, mkdirSync } from 'fs'
import { ensureDb, dbGetAdminSettings, dbSaveAdminSettings, dbIngestLifecycleMarkers, dbGetLatestApiRawResponses, dbGetProducts, dbGetSyncLog, dbClearLogs, dbLogFinish, dbLogStart, dbUpsertProducts, dbDeleteProductsMissingForStore, dbCountProducts, dbGetStockViewRows, dbReplaceProductPlacementsForStore, dbRecordApiRawResponse, dbGetGridColumns, dbSaveGridColumns } from './storage/db'
import { deleteSecrets, hasSecrets, loadSecrets, saveSecrets, updateStoreName } from './storage/secrets'
import { ozonGetStoreName, ozonPlacementZoneInfo, ozonPostingFboGet, ozonPostingFboList, ozonPostingFbsGet, ozonPostingFbsList, ozonProductInfoList, ozonProductList, ozonTestAuth, ozonWarehouseList, setOzonApiCaptureHook } from './ozon'
let mainWindow: BrowserWindow | null = null
let startupShowTimer: NodeJS.Timeout | null = null
function startupLog(...args: any[]) {
try {
const dir = app?.isReady?.() ? app.getPath('userData') : app.getPath('temp')
mkdirSync(dir, { recursive: true })
const line = `[${new Date().toISOString()}] ` + args.map((a) => {
try { return typeof a === 'string' ? a : JSON.stringify(a) } catch { return String(a) }
}).join(' ') + '\n'
appendFileSync(join(dir, 'ozonator-startup.log'), line, 'utf8')
} catch {}
try { console.log('[startup]', ...args) } catch {}
}
function safeShowMainWindow(reason: string) {
try {
if (!mainWindow || mainWindow.isDestroyed()) return
startupLog('safeShowMainWindow', { reason, visible: mainWindow.isVisible() })
if (!mainWindow.isVisible()) {
try { mainWindow.show() } catch {}
}
try { mainWindow.focus() } catch {}
try { mainWindow.maximize() } catch {}
} catch (e: any) {
startupLog('safeShowMainWindow.error', e?.message ?? String(e))
}
}
function chunk<T>(arr: T[], size: number): T[][] {
const out: T[][] = []
for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
return out
}
const SALES_ENDPOINTS = ['/v3/posting/fbs/list', '/v2/posting/fbo/list'] as const
type SalesPeriod = {
from?: string | null
to?: string | null
}
type SalesRow = GridApiRow & {
in_process_at?: string | null
posting_number?: string | null
related_postings?: string | null
shipment_date?: string | null
status?: string | null
status_details?: string | null
carrier_status_details?: string | null
delivery_date?: string | null
delivery_cluster?: string | null
delivery_model?: string | null
price?: number | ''
quantity?: number | ''
paid_by_customer?: number | ''
}
type SalesPayloadEnvelope = {
endpoint: string
payload: any
}
function safeGetByPath(source: any, path: string, fallback: any = undefined) {
if (!source || typeof source !== 'object') return fallback
const parts = String(path ?? '').split('.').map((x) => x.trim()).filter(Boolean)
if (parts.length === 0) return fallback
let cur = source
for (const part of parts) {
if (cur == null || typeof cur !== 'object' || !(part in cur)) return fallback
cur = cur[part]
}
return cur == null ? fallback : cur
}
function pickFirstPresent(source: any, paths: string[]) {
for (const path of paths) {
const value = safeGetByPath(source, path, undefined)
if (value === undefined || value === null || value === '') continue
return value
}
return undefined
}
function normalizeTextValue(value: any): string {
if (typeof value === 'string') return value.trim()
if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
return ''
}

const SALES_STATUS_LABELS_RU: Record<string, string> = {
awaiting_packaging: 'Ожидает упаковки',
awaiting_deliver: 'Ожидает доставки',
awaiting_approve: 'Ожидает подтверждения',
awaiting_registration: 'Ожидает регистрации',
awaiting_customer: 'Ожидает покупателя',
acceptance_in_progress: 'Идёт приёмка',
created: 'Создан',
processing: 'В обработке',
in_process: 'В обработке',
ready_to_ship: 'Готов к отгрузке',
ready_for_pickup: 'Готов к выдаче',
shipped: 'Отгружен',
handed_over_to_delivery: 'Передан в доставку',
sent_to_delivery: 'Передан в доставку',
sent_by_seller: 'Отправлен продавцом',
driver_pickup: 'Забирает курьер',
in_transit: 'В пути',
transit: 'В пути',
on_the_way: 'В пути',
on_route: 'В пути',
delivering: 'Доставляется',
delivery_failed: 'Доставка не удалась',
delivered: 'Доставлен',
delivered_to_customer: 'Доставлен покупателю',
customer_received: 'Получен покупателем',
returned: 'Возвращён',
returning: 'Возвращается',
return_in_progress: 'Возврат в обработке',
return_preparing: 'Готовится возврат',
return_arrived_to_seller: 'Возврат прибыл продавцу',
return_ready_for_seller_pickup: 'Готов к выдаче продавцу',
return_not_possible: 'Возврат невозможен',
cancelled: 'Отменён',
not_accepted: 'Не принят',
not_in_time: 'Не доставлен вовремя',
not_found: 'Не найден',
lost: 'Утерян',
damaged: 'Повреждён',
arbitration: 'Арбитраж',
client_arbitration: 'Арбитраж с клиентом',
posting_created: 'Создан',
posting_created_from_split: 'Создан после разделения',
posting_registered: 'Зарегистрирован',
posting_accepted: 'Принят',
posting_reception_transfer: 'Передан на приёмку',
posting_ready_to_ship: 'Готов к отгрузке',
posting_sent_by_seller: 'Отправлен продавцом',
posting_transferring_to_delivery: 'Передаётся в доставку',
posting_transfered_to_courier_service: 'Передан в службу доставки',
posting_transferred_to_courier_service: 'Передан в службу доставки',
posting_driver_pick_up: 'Забирает курьер',
posting_in_carriage: 'В пути',
posting_sent_to_city: 'Отправлен в город получения',
posting_on_way_to_city: 'В пути в город получения',
posting_on_way_to_pickup_point: 'В пути в пункт выдачи',
posting_arrived_at_pickup_point: 'Прибыл в пункт выдачи',
posting_in_pickup_point: 'В пункте выдачи',
posting_on_pickup_point: 'В пункте выдачи',
posting_waiting_buyer: 'Ожидает покупателя',
posting_waiting_passport_data: 'Ожидает паспортные данные',
posting_conditionally_delivered: 'Условно доставлен',
posting_delivering: 'Доставляется',
posting_delivered: 'Доставлен',
posting_delivered_to_customer: 'Доставлен покупателю',
posting_not_in_sort_center: 'Не найден в сортировочном центре',
posting_not_in_pickup_point: 'Не найден в пункте выдачи',
posting_lost: 'Утерян',
posting_damaged: 'Повреждён',
posting_timeout: 'Истёк срок хранения',
posting_return_in_progress: 'Возврат в обработке',
posting_returning: 'Возвращается',
posting_returned: 'Возвращён',
posting_returned_to_seller: 'Возвращён продавцу',
posting_partial_return: 'Частичный возврат',
returned_to_seller: 'Возвращён продавцу',
}

const SALES_PROVIDER_STATUS_LABELS_RU: Record<string, string> = {
created: 'Создан',
accepted: 'Принят',
awaiting_registration: 'Ожидает регистрации',
ready_for_pickup: 'Готов к выдаче',
ready_to_ship: 'Готов к отгрузке',
handed_over_to_delivery: 'Передан в доставку',
sent_to_delivery: 'Передан в доставку',
in_transit: 'В пути',
transit: 'В пути',
on_the_way: 'В пути',
on_route: 'В пути',
delivering: 'Доставляется',
delivery_failed: 'Доставка не удалась',
delivered: 'Доставлен',
delivered_to_customer: 'Доставлен покупателю',
returned: 'Возвращён',
returning: 'Возвращается',
cancelled: 'Отменён',
lost: 'Утерян',
damaged: 'Повреждён',
not_found: 'Не найден',
on_point: 'В пункте выдачи',
pickup: 'В пункте выдачи',
at_pickup_point: 'В пункте выдачи',
}

function capitalizeSalesText(value: string): string {
const text = value.trim()
if (!text) return ''
return text.charAt(0).toUpperCase() + text.slice(1)
}

function humanizeSalesCode(value: string): string {
const text = normalizeTextValue(value)
if (!text) return ''
return capitalizeSalesText(text.replace(/[./]+/g, ' ').replace(/[_-]+/g, ' ').replace(/\s+/g, ' ').trim())
}

function getUnknownSalesText(mode: 'status' | 'detail' | 'provider'): string {
if (mode === 'provider') return 'Прочий статус перевозчика'
if (mode === 'detail') return 'Прочая деталь статуса'
return 'Прочий статус'
}

function normalizeSalesLookupKey(value: any): string {
let text = normalizeTextValue(value)
if (!text) return ''
const prefixed = text.match(/^(substatus|previous_substatus|provider_status|status|state)\s*[:=]\s*(.+)$/i)
if (prefixed?.[2]) text = prefixed[2].trim()
return text
.toLowerCase()
.replace(/[|]+/g, ' ')
.replace(/[./\\]+/g, ' ')
.replace(/[:=]+/g, ' ')
.replace(/[_\-\s]+/g, '_')
.replace(/^_+|_+$/g, '')
}

function translateSalesCodeValue(value: any, mode: 'status' | 'detail' | 'provider' = 'status'): string {
const raw = normalizeTextValue(value)
if (!raw) return ''
if (raw.includes('|')) {
const parts = raw
.split(/\s*\|\s*/)
.map((part) => translateSalesCodeValue(part, mode))
.filter(Boolean)
return Array.from(new Set(parts)).join(' | ')
}
const prefixedPrevious = raw.match(/^(previous|previous_substatus)\s*[:=]\s*(.+)$/i)
if (prefixedPrevious?.[2]) {
const translated = translateSalesCodeValue(prefixedPrevious[2], 'detail')
return translated ? `Предыдущий: ${translated}` : ''
}
if (/[А-Яа-яЁё]/.test(raw)) return capitalizeSalesText(raw)
const key = normalizeSalesLookupKey(raw)
if (!key) return ''
const providerMapped = SALES_PROVIDER_STATUS_LABELS_RU[key]
const statusMapped = SALES_STATUS_LABELS_RU[key]
const mapped = mode === 'provider' ? (providerMapped ?? statusMapped) : (statusMapped ?? providerMapped)
if (mapped) return capitalizeSalesText(mapped)
return getUnknownSalesText(mode)
}

function pushUniqueSalesPart(parts: string[], value: any) {
const text = normalizeTextValue(value)
if (!text) return
if (!parts.includes(text)) parts.push(text)
}

function pushLabeledSalesPart(parts: string[], label: string, value: any) {
const text = normalizeTextValue(value)
if (!text) return
const normalized = `${label}: ${text}`
if (!parts.includes(normalized)) parts.push(normalized)
}
function normalizeDateValue(value: any): string {
if (value == null || value === '') return ''
const raw = typeof value === 'string' ? value.trim() : String(value).trim()
if (!raw) return ''
const parsed = new Date(raw)
return Number.isNaN(parsed.getTime()) ? '' : raw
}
function normalizeNumberValue(value: any): number | '' {
if (value == null || value === '') return ''
const n = Number(value)
if (!Number.isFinite(n)) return ''
return n
}
function parseJsonTextSafe(text: string | null | undefined) {
if (typeof text !== 'string' || !text.trim()) return null
try {
return JSON.parse(text)
} catch {
return null
}
}
function extractPostingsFromPayload(payload: any): any[] {
const fromResult = safeGetByPath(payload, 'result.postings', null)
if (Array.isArray(fromResult)) return fromResult
const direct = safeGetByPath(payload, 'postings', null)
if (Array.isArray(direct)) return direct
return []
}
function normalizeTextList(values: any[]): string[] {
const out: string[] = []
const seen = new Set<string>()
const pushOne = (value: any) => {
if (value == null) return
if (Array.isArray(value)) {
for (const item of value) pushOne(item)
return
}
if (typeof value === 'object') {
const obj = value as Record<string, unknown>
const direct = [
obj.posting_number,
obj.postingNumber,
obj.related_posting_number,
obj.related_posting_numbers,
obj.relatedPostingNumbers,
obj.related_postings,
obj.relatedPostings,
obj.parent_posting_number,
obj.parentPostingNumber,
obj.number,
obj.value,
obj.name,
obj.id,
]
for (const candidate of direct) pushOne(candidate)
return
}
const raw = String(value).trim()
if (!raw) return
for (const part of raw.split(',')) {
const normalized = part.trim()
if (!normalized || seen.has(normalized)) continue
seen.add(normalized)
out.push(normalized)
}
}
for (const value of values) pushOne(value)
return out
}
function buildRelatedPostingsText(posting: any, fallbackPostings: string[] = []): string {
const candidates = normalizeTextList([
safeGetByPath(posting, 'related_postings.related_posting_numbers', undefined),
safeGetByPath(posting, 'result.related_postings.related_posting_numbers', undefined),
safeGetByPath(posting, 'related_postings.related_postings', undefined),
safeGetByPath(posting, 'related_postings_numbers', undefined),
safeGetByPath(posting, 'related_posting_numbers', undefined),
safeGetByPath(posting, 'related_postings', undefined),
safeGetByPath(posting, 'parent_posting_number', undefined),
fallbackPostings,
])
return candidates.join(', ')
}
function normalizeSalesEndpointName(endpoint: string): 'FBS' | 'FBO' | '' {
if (endpoint.includes('/posting/fbs/')) return 'FBS'
if (endpoint.includes('/posting/fbo/')) return 'FBO'
return ''
}
function buildDeliveryModelValue(posting: any, endpoint: string): string {
const explicit = normalizeTextValue(pickFirstPresent(posting, [
'delivery_schema',
'posting.delivery_schema',
'analytics_data.delivery_schema',
]))
if (explicit) return explicit.toUpperCase()
const normalizedEndpoint = normalizeSalesEndpointName(endpoint)
if (normalizedEndpoint) return normalizedEndpoint
return normalizeTextValue(pickFirstPresent(posting, ['delivery_method.name', 'delivery_method', 'delivery_type']))
}
function buildSalesStatusDetailsValue(posting: any, endpoint: string): string {
const parts: string[] = []
if (normalizeSalesEndpointName(endpoint) === 'FBO') {
const nextState = translateSalesCodeValue(pickFirstPresent(posting, ['new_state', 'result.new_state']), 'detail')
pushUniqueSalesPart(parts, nextState)
pushLabeledSalesPart(parts, 'Дата изменения', pickFirstPresent(posting, ['changed_state_date', 'result.changed_state_date']))
return parts.join(' | ')
}
const substatus = translateSalesCodeValue(pickFirstPresent(posting, ['substatus', 'result.substatus']), 'detail')
pushUniqueSalesPart(parts, substatus)
const previousSubstatus = translateSalesCodeValue(pickFirstPresent(posting, ['previous_substatus', 'result.previous_substatus']), 'detail')
if (previousSubstatus && previousSubstatus !== substatus) {
pushLabeledSalesPart(parts, 'Предыдущий', previousSubstatus)
}
return parts.join(' | ')
}
function buildSalesCarrierStatusDetailsValue(posting: any): string {
const parts: string[] = []
pushUniqueSalesPart(parts, translateSalesCodeValue(pickFirstPresent(posting, ['provider_status', 'result.provider_status']), 'provider'))
return parts.join(' | ')
}
function normalizeSalesPeriodDate(value: any): string {
const raw = typeof value === 'string' ? value.trim() : ''
return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : ''
}
function buildSalesRequestBody(period: SalesPeriod | null | undefined, limit = 1000, offset = 0, endpoint?: string) {
let from = normalizeSalesPeriodDate(period?.from)
let to = normalizeSalesPeriodDate(period?.to)
if (!from && to) from = to
if (from && !to) to = from
if (from && to && from > to) [from, to] = [to, from]
if (from && to) {
const [fromYear, fromMonth, fromDay] = from.split('-').map((x) => Number(x))
const [toYear, toMonth, toDay] = to.split('-').map((x) => Number(x))
const body = {
dir: 'DESC',
filter: {
since: new Date(fromYear, fromMonth - 1, fromDay, 0, 0, 0, 0).toISOString(),
to: new Date(toYear, toMonth - 1, toDay, 23, 59, 59, 999).toISOString(),
},
limit,
offset,
}
if (endpoint?.includes('/posting/fbs/')) {
;(body as any).with = {
analytics_data: true,
financial_data: true,
barcodes: true,
related_postings: true,
}
}
return body
}
const now = new Date()
const since = new Date(now.getTime() - (90 * 24 * 60 * 60 * 1000))
const body = {
dir: 'DESC',
filter: {
since: since.toISOString(),
to: now.toISOString(),
},
limit,
offset,
}
if (endpoint?.includes('/posting/fbs/')) {
;(body as any).with = {
analytics_data: true,
financial_data: true,
barcodes: true,
related_postings: true,
}
}
return body
}
async function fetchSalesEndpointPages(
loader: (body: any) => Promise<any>,
period: SalesPeriod | null | undefined,
endpoint: string,
): Promise<SalesPayloadEnvelope[]> {
const hasExplicitPeriod = Boolean(normalizeSalesPeriodDate(period?.from) || normalizeSalesPeriodDate(period?.to))
const limit = 1000
const maxPages = hasExplicitPeriod ? 100 : 1
const payloads: SalesPayloadEnvelope[] = []
for (let page = 0; page < maxPages; page++) {
const offset = page * limit
const payload = await loader(buildSalesRequestBody(period, limit, offset, endpoint))
payloads.push({ endpoint, payload })
const postings = extractPostingsFromPayload(payload)
if (postings.length < limit) break
}
return payloads
}
function shouldReplaceSalesRow(prev: SalesRow, next: SalesRow): boolean {
const prevDelivered = String(prev?.delivery_date ?? '').trim()
const nextDelivered = String(next?.delivery_date ?? '').trim()
if (!prevDelivered && nextDelivered) return true
if (prevDelivered && !nextDelivered) return false
if (prevDelivered && nextDelivered) return nextDelivered > prevDelivered
return false
}
function getSalesPostingDetailsKey(endpointKind: 'FBS' | 'FBO' | '', postingNumber: string): string {
return `${endpointKind}|${String(postingNumber ?? '').trim()}`
}
function extractSalesPostingResult(payload: any): any {
const result = safeGetByPath(payload, 'result', null)
if (result && typeof result === 'object') return result
if (payload && typeof payload === 'object') return payload
return null
}
function getFactDeliveryDateValue(source: any): string {
return normalizeDateValue(pickFirstPresent(source, ['result.fact_delivery_date', 'fact_delivery_date']))
}
const SALES_DELIVERED_STATUS_KEYS = new Set([
'delivered',
'delivered_to_customer',
'customer_received',
'posting_delivered',
'posting_delivered_to_customer',
'posting_conditionally_delivered',
])
function hasDeliveredStatusSignal(posting: any): boolean {
const statusCandidates = [
pickFirstPresent(posting, ['status', 'result.status', 'state', 'result.state']),
pickFirstPresent(posting, ['provider_status', 'result.provider_status']),
pickFirstPresent(posting, ['new_state', 'result.new_state']),
]
for (const candidate of statusCandidates) {
const key = normalizeSalesLookupKey(candidate)
if (key && SALES_DELIVERED_STATUS_KEYS.has(key)) return true
}
return false
}
function shouldFetchSalesPostingDetails(posting: any): boolean {
if (getFactDeliveryDateValue(posting)) return false
if (normalizeDateValue(pickFirstPresent(posting, ['delivery_date', 'result.delivery_date']))) return true
if (hasDeliveredStatusSignal(posting)) return true
return false
}
async function fetchSalesPostingDetails(
secrets: NonNullable<ReturnType<typeof loadSecrets>>,
payloads: SalesPayloadEnvelope[],
): Promise<Map<string, any>> {
const requests: Array<{ endpointKind: 'FBS' | 'FBO'; postingNumber: string }> = []
const seen = new Set<string>()
for (const envelope of payloads) {
const endpointKind = normalizeSalesEndpointName(envelope.endpoint)
if (endpointKind !== 'FBS' && endpointKind !== 'FBO') continue
for (const posting of extractPostingsFromPayload(envelope.payload)) {
const postingNumber = normalizeTextValue(pickFirstPresent(posting, ['posting_number', 'postingNumber']))
if (!postingNumber || !shouldFetchSalesPostingDetails(posting)) continue
const requestKey = getSalesPostingDetailsKey(endpointKind, postingNumber)
if (seen.has(requestKey)) continue
seen.add(requestKey)
requests.push({ endpointKind, postingNumber })
}
}
if (requests.length === 0) return new Map()
const out = new Map<string, any>()
for (const batch of chunk(requests, 10)) {
const settled = await Promise.allSettled(batch.map(async (request) => {
const payload = request.endpointKind === 'FBS'
? await ozonPostingFbsGet(secrets, request.postingNumber)
: await ozonPostingFboGet(secrets, request.postingNumber)
return { request, payload }
}))
for (const result of settled) {
if (result.status !== 'fulfilled') continue
const detailPosting = extractSalesPostingResult(result.value.payload)
if (!detailPosting) continue
const endpointKind = result.value.request.endpointKind
const postingNumber = normalizeTextValue(pickFirstPresent(detailPosting, ['posting_number', 'postingNumber'])) || result.value.request.postingNumber
if (!postingNumber) continue
out.set(getSalesPostingDetailsKey(endpointKind, postingNumber), detailPosting)
}
}
return out
}
function normalizeSalesRows(payloads: SalesPayloadEnvelope[], products: GridApiRow[], postingDetailsByKey?: Map<string, any>): SalesRow[] {
const productsByOfferId = new Map<string, GridApiRow>()
const productsBySku = new Map<string, GridApiRow>()
for (const product of products) {
const offerId = normalizeTextValue((product as any)?.offer_id)
const sku = normalizeTextValue((product as any)?.sku)
if (offerId && !productsByOfferId.has(offerId)) productsByOfferId.set(offerId, product)
if (sku && !productsBySku.has(sku)) productsBySku.set(sku, product)
}
const dedup = new Map<string, SalesRow>()
const fboOrderPostingMap = new Map<string, Set<string>>()
for (const envelope of payloads) {
if (normalizeSalesEndpointName(envelope.endpoint) !== 'FBO') continue
for (const posting of extractPostingsFromPayload(envelope.payload)) {
const orderKey = normalizeTextValue(pickFirstPresent(posting, ['order_id', 'order_number']))
const postingNumber = normalizeTextValue(pickFirstPresent(posting, ['posting_number', 'postingNumber']))
if (!orderKey || !postingNumber) continue
let bucket = fboOrderPostingMap.get(orderKey)
if (!bucket) {
bucket = new Set<string>()
fboOrderPostingMap.set(orderKey, bucket)
}
bucket.add(postingNumber)
}
}
for (const envelope of payloads) {
const endpointKind = normalizeSalesEndpointName(envelope.endpoint)
for (const posting of extractPostingsFromPayload(envelope.payload)) {
const items = Array.isArray((posting as any)?.products)
? (posting as any).products
: (Array.isArray((posting as any)?.items) ? (posting as any).items : [])
if (items.length === 0) continue
const acceptedAt = normalizeDateValue(pickFirstPresent(posting, ['in_process_at', 'created_at', 'acceptance_date']))
const postingNumber = normalizeTextValue(pickFirstPresent(posting, ['posting_number', 'postingNumber']))
const orderKey = normalizeTextValue(pickFirstPresent(posting, ['order_id', 'order_number']))
const fallbackRelated = endpointKind === 'FBO' && orderKey
? Array.from(fboOrderPostingMap.get(orderKey) ?? []).filter((value) => value !== postingNumber)
: []
const related = buildRelatedPostingsText(posting, fallbackRelated)
const shipmentDate = normalizeDateValue(pickFirstPresent(posting, ['delivering_date', 'shipment_date', 'shipment_date_actual', 'shipped_at', 'changed_state_date']))
const status = translateSalesCodeValue(pickFirstPresent(posting, ['status', 'state', 'result.status', 'result.state']), 'status')
const statusDetails = buildSalesStatusDetailsValue(posting, envelope.endpoint)
const carrierStatusDetails = buildSalesCarrierStatusDetailsValue(posting)
const detailPosting = postingDetailsByKey?.get(getSalesPostingDetailsKey(endpointKind, postingNumber)) ?? null
const deliveredAt = getFactDeliveryDateValue(detailPosting) || getFactDeliveryDateValue(posting)
const deliveryCluster = normalizeTextValue(pickFirstPresent(posting, ['financial_data.cluster_to', 'result.financial_data.cluster_to', 'cluster_to', 'result.cluster_to']))
const deliverySchema = buildDeliveryModelValue(posting, envelope.endpoint)
if (!postingNumber) continue
for (const item of items) {
const sku = normalizeTextValue(pickFirstPresent(item, ['sku', 'sku_id', 'id']))
if (!sku) continue
const offerId = normalizeTextValue(pickFirstPresent(item, ['offer_id', 'offerId', 'article']))
const productMeta = productsByOfferId.get(offerId) ?? productsBySku.get(sku) ?? null
const row: SalesRow = {
...(productMeta ?? {}),
offer_id: offerId || String((productMeta as any)?.offer_id ?? ''),
sku,
name: normalizeTextValue(pickFirstPresent(item, ['name', 'product_name'])) || String((productMeta as any)?.name ?? ''),
in_process_at: acceptedAt || '',
posting_number: postingNumber,
related_postings: related || '',
shipment_date: shipmentDate || '',
status: status || '',
status_details: statusDetails || '',
carrier_status_details: carrierStatusDetails || '',
delivery_date: deliveredAt || '',
delivery_cluster: deliveryCluster || '',
delivery_model: deliverySchema || '',
price: normalizeNumberValue(pickFirstPresent(item, ['price', 'your_price', 'seller_price'])),
quantity: normalizeNumberValue(pickFirstPresent(item, ['quantity', 'qty'])),
paid_by_customer: normalizeNumberValue(pickFirstPresent(item, ['payout', 'paid_by_buyer', 'price'])),
}
const dedupKey = `${postingNumber}|${sku}`
const prev = dedup.get(dedupKey)
if (!prev || shouldReplaceSalesRow(prev, row)) dedup.set(dedupKey, row)
}
}
}
return Array.from(dedup.values()).sort((a, b) => {
const aAccepted = String(a?.in_process_at ?? '')
const bAccepted = String(b?.in_process_at ?? '')
if (aAccepted !== bAccepted) return bAccepted.localeCompare(aAccepted)
const aPosting = String(a?.posting_number ?? '')
const bPosting = String(b?.posting_number ?? '')
if (aPosting !== bPosting) return bPosting.localeCompare(aPosting)
return String(a?.offer_id ?? '').localeCompare(String(b?.offer_id ?? ''), 'ru')
})
}
function getCachedSalesPayloadMap(storeClientId: string | null | undefined): Map<string, SalesPayloadEnvelope> {
const scoped = dbGetLatestApiRawResponses(storeClientId ?? null, SALES_ENDPOINTS as unknown as string[])
const rows = scoped.length > 0 ? scoped : dbGetLatestApiRawResponses(null, SALES_ENDPOINTS as unknown as string[])
const out = new Map<string, SalesPayloadEnvelope>()
for (const row of rows) {
if (out.has(row.endpoint)) continue
const parsed = parseJsonTextSafe(row?.response_body)
if (parsed) out.set(row.endpoint, { endpoint: row.endpoint, payload: parsed })
}
return out
}
function createWindow() {
startupLog('createWindow.begin', { packaged: app.isPackaged, appPath: app.getAppPath(), __dirname })
mainWindow = new BrowserWindow({
width: 1200,
height: 760,
minWidth: 980,
minHeight: 620,
title: 'Озонатор',
show: false,
backgroundColor: '#F5F5F7',
autoHideMenuBar: true,
titleBarOverlay: { color: '#F5F5F7', symbolColor: '#1d1d1f', height: 34 },
webPreferences: {
preload: join(__dirname, '../preload/index.js'),
contextIsolation: true,
nodeIntegration: false,
},
})
if (startupShowTimer) {
clearTimeout(startupShowTimer)
startupShowTimer = null
}
startupShowTimer = setTimeout(() => safeShowMainWindow('show-timeout-fallback'), 2500)
mainWindow.once('ready-to-show', () => {
startupLog('event.ready-to-show')
safeShowMainWindow('ready-to-show')
})
mainWindow.webContents.on('did-finish-load', () => {
startupLog('event.did-finish-load', { url: mainWindow?.webContents?.getURL?.() })
safeShowMainWindow('did-finish-load')
})
mainWindow.webContents.on('did-fail-load', (_e, code, desc, url, isMainFrame) => {
startupLog('event.did-fail-load', { code, desc, url, isMainFrame })
try {
if (isMainFrame && mainWindow && !mainWindow.isDestroyed()) {
        const html = `<!doctype html><html><body style="font-family:Segoe UI,sans-serif;padding:16px">
          <h3>Озонатор не смог загрузить интерфейс</h3>
          <div>Причина: ${String(desc || 'did-fail-load')} (code ${String(code)})</div>
          <div style="margin-top:8px;color:#555">Подробности в файле ozonator-startup.log в папке данных приложения.</div>
        </body></html>`
mainWindow.loadURL('data:text/html;charset=utf-8,' + encodeURIComponent(html)).catch(() => {})
}
} catch {}
safeShowMainWindow('did-fail-load')
})
mainWindow.webContents.on('render-process-gone', (_e, details) => {
startupLog('event.render-process-gone', details)
safeShowMainWindow('render-process-gone')
})
mainWindow.on('unresponsive', () => {
startupLog('event.window-unresponsive')
})
mainWindow.on('closed', () => {
startupLog('event.window-closed')
if (startupShowTimer) {
clearTimeout(startupShowTimer)
startupShowTimer = null
}
mainWindow = null
})
const devUrl = process.env.ELECTRON_RENDERER_URL || process.env.VITE_DEV_SERVER_URL || (!app.isPackaged ? 'http://localhost:5173/' : null)
startupLog('renderer.target', { devUrl, packaged: app.isPackaged })
if (devUrl) {
mainWindow.loadURL(devUrl).catch((e) => startupLog('loadURL.error', e?.message ?? String(e)))
try { mainWindow.webContents.openDevTools({ mode: 'detach' }) } catch {}
} else {
const rendererFile = join(app.getAppPath(), 'out/renderer/index.html')
startupLog('renderer.file', rendererFile)
mainWindow.loadFile(rendererFile).catch((e) => startupLog('loadFile.error', e?.message ?? String(e)))
}
nativeTheme.themeSource = 'light'
}
app.whenReady().then(() => {
try {
startupLog('app.whenReady')
if (!safeStorage.isEncryptionAvailable()) {
console.warn('safeStorage encryption is not available on this machine.')
startupLog('safeStorage.unavailable')
}
ensureDb()
startupLog('ensureDb.ok')
setOzonApiCaptureHook((evt) => {
dbRecordApiRawResponse({
storeClientId: evt.storeClientId,
method: evt.method,
endpoint: evt.endpoint,
requestBody: evt.requestBody,
responseBody: evt.responseBody,
httpStatus: evt.httpStatus,
isSuccess: evt.isSuccess,
errorMessage: evt.errorMessage ?? null,
fetchedAt: evt.fetchedAt,
})
})
dbIngestLifecycleMarkers({ appVersion: app.getVersion() })
startupLog('dbIngestLifecycleMarkers.ok', { version: app.getVersion() })
createWindow()
app.on('activate', () => {
startupLog('app.activate', { windows: BrowserWindow.getAllWindows().length })
if (BrowserWindow.getAllWindows().length === 0) createWindow()
else safeShowMainWindow('app-activate')
})
} catch (e: any) {
startupLog('fatal.startup', e?.stack ?? e?.message ?? String(e))
try {
dialog.showErrorBox('Озонатор — ошибка запуска', String(e?.message ?? e))
} catch {}
try {
if (!mainWindow) {
mainWindow = new BrowserWindow({ width: 900, height: 640, show: true, autoHideMenuBar: true })
        const html = `<!doctype html><html><body style="font-family:Segoe UI,sans-serif;padding:16px">
          <h3>Озонатор не запустился</h3>
          <pre style="white-space:pre-wrap">${String(e?.stack ?? e?.message ?? e)}</pre>
          <div style="color:#555">Подробности: ozonator-startup.log в папке данных приложения.</div>
        </body></html>`
mainWindow.loadURL('data:text/html;charset=utf-8,' + encodeURIComponent(html)).catch(() => {})
}
} catch {}
}
})
process.on('uncaughtException', (e: any) => {
startupLog('process.uncaughtException', e?.stack ?? e?.message ?? String(e))
})
process.on('unhandledRejection', (e: any) => {
startupLog('process.unhandledRejection', e as any)
})
app.on('window-all-closed', () => {
if (process.platform !== 'darwin') app.quit()
})
function checkInternet(timeoutMs = 2500): Promise<boolean> {
return new Promise((resolve) => {
const request = net.request({ method: 'GET', url: 'https://api-seller.ozon.ru' })
const timer = setTimeout(() => {
try { request.abort() } catch {}
resolve(false)
}, timeoutMs)
request.on('response', () => {
clearTimeout(timer)
resolve(true)
})
request.on('error', () => {
clearTimeout(timer)
resolve(false)
})
request.end()
})
}
ipcMain.handle('secrets:status', async () => {
return {
hasSecrets: hasSecrets(),
encryptionAvailable: safeStorage.isEncryptionAvailable(),
}
})
ipcMain.handle('secrets:save', async (_e, secrets: { clientId: string; apiKey: string }) => {
saveSecrets({ clientId: String(secrets.clientId).trim(), apiKey: String(secrets.apiKey).trim() })
return { ok: true }
})
ipcMain.handle('secrets:load', async () => {
const s = loadSecrets()
return { ok: true, secrets: { clientId: s.clientId, apiKey: s.apiKey, storeName: s.storeName ?? null } }
})
ipcMain.handle('secrets:delete', async () => {
deleteSecrets()
return { ok: true }
})
ipcMain.handle('net:check', async () => {
return { online: await checkInternet() }
})
ipcMain.handle('admin:getSettings', async () => {
try {
return { ok: true, ...dbGetAdminSettings() }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), logRetentionDays: 30 }
}
})
ipcMain.handle('admin:saveSettings', async (_e, payload: { logRetentionDays?: number }) => {
try {
const saved = dbSaveAdminSettings({ logRetentionDays: Number(payload?.logRetentionDays) })
return { ok: true, ...saved }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e) }
}
})
ipcMain.handle('ozon:testAuth', async () => {
let storeClientId: string | null = null
try { storeClientId = loadSecrets().clientId } catch {}
const logId = dbLogStart('check_auth', storeClientId)
try {
const secrets = loadSecrets()
await ozonTestAuth(secrets)
try {
const name = await ozonGetStoreName(secrets)
if (name) updateStoreName(name)
} catch {
}
dbLogFinish(logId, { status: 'success', storeClientId: secrets.clientId })
const refreshed = loadSecrets()
return { ok: true, storeName: refreshed.storeName ?? null }
} catch (e: any) {
dbLogFinish(logId, { status: 'error', errorMessage: e?.message ?? String(e), errorDetails: e?.details, storeClientId })
return { ok: false, error: e?.message ?? String(e) }
}
})
ipcMain.handle('ozon:syncProducts', async () => {
let storeClientId: string | null = null
try { storeClientId = loadSecrets().clientId } catch {}
const logId = dbLogStart('sync_products', storeClientId)
try {
const secrets = loadSecrets()
const existingOfferIds = new Set(dbGetProducts(secrets.clientId).map((p: any) => p.offer_id))
const incomingOfferIds = new Set<string>()
let added = 0
let lastId = ''
const limit = 1000
let pages = 0
let total = 0
for (let guard = 0; guard < 200; guard++) {
const { items, lastId: next, total: totalMaybe } = await ozonProductList(secrets, { lastId, limit })
pages += 1
total += items.length
const ids = items.map(i => i.product_id).filter(Boolean) as number[]
const infoList = await ozonProductInfoList(secrets, ids)
const infoMap = new Map<number, typeof infoList[number]>()
for (const p of infoList) infoMap.set(p.product_id, p)
const enriched = items.map((it) => {
const info = it.product_id ? infoMap.get(it.product_id) : undefined
return {
offer_id: it.offer_id,
product_id: it.product_id,
sku: (info?.ozon_sku ?? info?.sku ?? it.sku ?? null),
ozon_sku: (info?.ozon_sku ?? info?.sku ?? it.sku ?? null),
seller_sku: (info?.seller_sku ?? it.offer_id ?? null),
fbo_sku: info?.fbo_sku ?? null,
fbs_sku: info?.fbs_sku ?? null,
barcode: info?.barcode ?? null,
brand: info?.brand ?? null,
category: info?.category ?? null,
type: info?.type ?? null,
name: info?.name ?? null,
photo_url: info?.photo_url ?? null,
is_visible: info?.is_visible ?? null,
hidden_reasons: info?.hidden_reasons ?? null,
created_at: info?.created_at ?? null,
archived: it.archived ?? false,
store_client_id: secrets.clientId,
}
})
for (const it of enriched) {
const offer = String((it as any).offer_id)
if (offer) incomingOfferIds.add(offer)
if (!existingOfferIds.has(offer)) {
existingOfferIds.add(offer)
added += 1
}
}
dbUpsertProducts(enriched)
if (!next) break
if (next === lastId) break
lastId = next
if (typeof totalMaybe === 'number' && total >= totalMaybe) break
}
dbDeleteProductsMissingForStore(secrets.clientId, Array.from(incomingOfferIds))
const syncedCount = dbCountProducts(secrets.clientId)
let placementRowsCount = 0
let placementSyncError: string | null = null
let placementCacheKept = false
try {
const productsForStore = dbGetProducts(secrets.clientId)
const ozonSkuList = Array.from(new Set(productsForStore.map((p) => String(p.sku ?? '').trim()).filter(Boolean)))
const sellerSkuList = Array.from(new Set(productsForStore.map((p) => String(p.offer_id ?? '').trim()).filter(Boolean)))
if (ozonSkuList.length > 0 || sellerSkuList.length > 0) {
const warehouses = await ozonWarehouseList(secrets)
if (!Array.isArray(warehouses) || warehouses.length === 0) {
placementSyncError = 'Ozon не вернул список складов; локальные данные по складам/зонам сохранены без перезаписи.'
placementCacheKept = true
} else {
const allPlacementRows: Array<{
warehouse_id: number
warehouse_name?: string | null
sku: string
ozon_sku?: string | null
seller_sku?: string | null
placement_zone?: string | null
}> = []
const placementRowKeys = new Set<string>()
let placementApiCallCount = 0
const appendPlacementRows = (
warehouseId: number,
warehouseName: string | null,
zones: Array<{
sku: string
ozon_sku?: string | null
seller_sku?: string | null
placement_zone: string | null
}>
) => {
for (const z of zones) {
const rowKey = [
String(warehouseId),
String(z.ozon_sku ?? ''),
String(z.seller_sku ?? ''),
String(z.placement_zone ?? ''),
].join('::')
if (placementRowKeys.has(rowKey)) continue
placementRowKeys.add(rowKey)
allPlacementRows.push({
warehouse_id: warehouseId,
warehouse_name: warehouseName,
sku: z.sku,
ozon_sku: z.ozon_sku ?? null,
seller_sku: z.seller_sku ?? null,
placement_zone: z.placement_zone ?? null,
})
}
}
for (const wh of warehouses) {
const wid = Number(wh.warehouse_id)
if (!Number.isFinite(wid)) continue
for (const part of chunk(ozonSkuList, 500)) {
placementApiCallCount += 1
const zones = await ozonPlacementZoneInfo(secrets, { warehouseId: wid, skus: part })
appendPlacementRows(wid, wh.name ?? null, zones)
}
for (const part of chunk(sellerSkuList, 500)) {
placementApiCallCount += 1
const zones = await ozonPlacementZoneInfo(secrets, { warehouseId: wid, skus: part })
appendPlacementRows(wid, wh.name ?? null, zones)
}
}
if (allPlacementRows.length === 0 && placementApiCallCount > 0) {
placementSyncError = 'Ozon не вернул зоны размещения ни по одному SKU; прежние локальные данные по складам/зонам сохранены.'
placementCacheKept = true
} else {
placementRowsCount = dbReplaceProductPlacementsForStore(secrets.clientId, allPlacementRows)
}
}
} else {
placementRowsCount = dbReplaceProductPlacementsForStore(secrets.clientId, [])
}
} catch (placementErr: any) {
placementSyncError = placementErr?.message ?? String(placementErr)
}
if (!secrets.storeName) {
try {
const name = await ozonGetStoreName(secrets)
if (name) updateStoreName(name)
} catch {
}
}
dbLogFinish(logId, {
status: 'success',
itemsCount: syncedCount,
storeClientId: secrets.clientId,
meta: {
added,
storeClientId: secrets.clientId,
storeName: loadSecrets().storeName ?? null,
placementRowsCount,
placementSyncError,
placementCacheKept,
},
})
return { ok: true, itemsCount: syncedCount, pages, placementRowsCount, placementSyncError }
} catch (e: any) {
dbLogFinish(logId, { status: 'error', errorMessage: e?.message ?? String(e), errorDetails: e?.details, storeClientId })
return { ok: false, error: e?.message ?? String(e) }
}
})
ipcMain.handle('data:getProducts', async () => {
try {
let storeClientId: string | null = null
try {
storeClientId = loadSecrets().clientId
} catch {
storeClientId = null
}
const products = dbGetProducts(storeClientId)
return { ok: true, products }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), products: [] }
}
})
ipcMain.handle('data:getSales', async (_e, args?: { period?: SalesPeriod | null }) => {
try {
let storeClientId: string | null = null
let secrets: ReturnType<typeof loadSecrets> | null = null
try {
secrets = loadSecrets()
storeClientId = secrets.clientId
} catch {
secrets = null
storeClientId = null
}
const requestedPeriod: SalesPeriod | null = args?.period ?? null
const products = dbGetProducts(storeClientId)
const payloads: SalesPayloadEnvelope[] = []
const onlineEndpoints = new Set<string>()
if (secrets?.clientId && secrets?.apiKey) {
try {
const fbsPayloads = await fetchSalesEndpointPages(
(body) => ozonPostingFbsList(secrets as NonNullable<typeof secrets>, body),
requestedPeriod,
'/v3/posting/fbs/list',
)
if (fbsPayloads.length > 0) {
payloads.push(...fbsPayloads)
onlineEndpoints.add('/v3/posting/fbs/list')
}
} catch {}
try {
const fboPayloads = await fetchSalesEndpointPages(
(body) => ozonPostingFboList(secrets as NonNullable<typeof secrets>, body),
requestedPeriod,
'/v2/posting/fbo/list',
)
if (fboPayloads.length > 0) {
payloads.push(...fboPayloads)
onlineEndpoints.add('/v2/posting/fbo/list')
}
} catch {}
}
if (onlineEndpoints.size < SALES_ENDPOINTS.length) {
const cachedByEndpoint = getCachedSalesPayloadMap(storeClientId)
for (const [endpoint, payload] of cachedByEndpoint.entries()) {
if (!onlineEndpoints.has(endpoint)) payloads.push(payload)
}
}
const postingDetailsByKey = secrets?.clientId && secrets?.apiKey && onlineEndpoints.size > 0
? await fetchSalesPostingDetails(secrets as NonNullable<typeof secrets>, payloads)
: new Map<string, any>()
const rows = normalizeSalesRows(payloads, products, postingDetailsByKey)
return { ok: true, rows }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), rows: [] }
}
})
ipcMain.handle('data:getReturns', async () => {
try {
let storeClientId: string | null = null
try {
storeClientId = loadSecrets().clientId
} catch {
storeClientId = null
}
const products = dbGetProducts(storeClientId)
return { ok: true, rows: products }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), rows: [] }
}
})
ipcMain.handle('data:getStocks', async () => {
try {
let storeClientId: string | null = null
try {
storeClientId = loadSecrets().clientId
} catch {
storeClientId = null
}
const rows = dbGetStockViewRows(storeClientId)
return { ok: true, rows }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), rows: [] }
}
})
ipcMain.handle('ui:getGridColumns', async (_e, args: { dataset: 'products' | 'sales' | 'returns' | 'stocks' }) => {
try {
return { ok: true, ...dbGetGridColumns(args?.dataset) }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), dataset: (args?.dataset ?? 'products') as 'products' | 'sales' | 'returns' | 'stocks', cols: null }
}
})
ipcMain.handle('ui:saveGridColumns', async (_e, args: { dataset: 'products' | 'sales' | 'returns' | 'stocks'; cols: Array<{ id: string; w: number; visible: boolean; hiddenBucket: 'main' | 'add' }> }) => {
try {
return { ok: true, ...dbSaveGridColumns(args?.dataset, args?.cols) }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), dataset: (args?.dataset ?? 'products') as 'products' | 'sales' | 'returns' | 'stocks', savedCount: 0 }
}
})
ipcMain.handle('data:getSyncLog', async () => {
try {
let storeClientId: string | null = null
try {
storeClientId = loadSecrets().clientId
} catch {
storeClientId = null
}
const logs = dbGetSyncLog(storeClientId)
return { ok: true, logs }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), logs: [] }
}
})
ipcMain.handle('data:clearLogs', async () => {
try {
dbClearLogs()
return { ok: true }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e) }
}
})
