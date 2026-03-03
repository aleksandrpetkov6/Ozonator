import { ozonPostingFboGet, ozonPostingFbsGet } from './ozon'

export type SalesPeriod = {
  from?: string | null
  to?: string | null
}

export type SalesRow = GridApiRow & {
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

export type SalesPayloadEnvelope = {
  endpoint: string
  payload: any
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
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

function getUnknownSalesText(mode: 'status' | 'detail' | 'provider'): string {
  if (mode === 'provider' || mode === 'detail') return ''
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
  const dateOnlyMatch = raw.match(/^(\d{4}-\d{2}-\d{2})$/)
  if (dateOnlyMatch?.[1]) return dateOnlyMatch[1]
  const midnightMatch = raw.match(/^(\d{4}-\d{2}-\d{2})[T\s]00:00:00(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?$/i)
  if (midnightMatch?.[1]) return midnightMatch[1]
  const parsed = new Date(raw)
  return Number.isNaN(parsed.getTime()) ? '' : raw
}

function normalizeNumberValue(value: any): number | '' {
  if (value == null || value === '') return ''
  const n = Number(value)
  if (!Number.isFinite(n)) return ''
  return n
}

export function extractPostingsFromPayload(payload: any): any[] {
  const fromResult = safeGetByPath(payload, 'result.postings', null)
  if (Array.isArray(fromResult)) return fromResult

  const fromResultItems = safeGetByPath(payload, 'result.items', null)
  if (Array.isArray(fromResultItems)) return fromResultItems

  const fromResultRows = safeGetByPath(payload, 'result.rows', null)
  if (Array.isArray(fromResultRows)) return fromResultRows

  const direct = safeGetByPath(payload, 'postings', null)
  if (Array.isArray(direct)) return direct

  const directItems = safeGetByPath(payload, 'items', null)
  if (Array.isArray(directItems)) return directItems

  const directResult = safeGetByPath(payload, 'result', null)
  if (Array.isArray(directResult)) return directResult

  if (Array.isArray(payload)) return payload
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

function pickFirstPresentFromSources(paths: string[], ...sources: any[]) {
  for (const source of sources) {
    const value = pickFirstPresent(source, paths)
    if (value !== undefined && value !== null && value !== '') return value
  }
  return undefined
}

export type SalesPostingStateEvent = {
  state: string
  changed_state_date: string
}

const SALES_STATE_CHANGED_EVENT_TYPE = 'type_state_changed'
const FBO_SHIPMENT_STATE = 'posting_transferring_to_delivery'
const FBO_SHIPMENT_PRIMARY_STATES = [
  FBO_SHIPMENT_STATE,
  'posting_transfered_to_courier_service',
  'posting_transferred_to_courier_service',
  'posting_driver_pick_up',
] as const
const FBO_SHIPMENT_STATE_SET = new Set<string>(FBO_SHIPMENT_PRIMARY_STATES)
const FBO_SHIPMENT_FALLBACK_STATES = [
  'posting_delivering',
  'delivering',
] as const
const SALES_SHIPMENT_EVENT_PRIMARY_STATES = [
  'shipped',
  'sent_by_seller',
  'handed_over_to_delivery',
  'sent_to_delivery',
  'driver_pickup',
  'posting_sent_by_seller',
  'posting_transfered_to_courier_service',
  'posting_transferred_to_courier_service',
  'posting_driver_pick_up',
] as const
const SALES_SHIPMENT_EVENT_FALLBACK_STATES = [
  'in_transit',
  'transit',
  'on_the_way',
  'on_route',
  'posting_in_carriage',
  'posting_sent_to_city',
  'posting_on_way_to_city',
  'posting_on_way_to_pickup_point',
] as const
const SALES_SHIPMENT_PREFACT_STATUS_KEYS = new Set<string>([
  'awaiting_packaging',
  'awaiting_deliver',
  'awaiting_approve',
  'awaiting_registration',
  'acceptance_in_progress',
  'created',
  'processing',
  'in_process',
  'ready_to_ship',
  'ready_for_pickup',
  'posting_created',
  'posting_created_from_split',
  'posting_registered',
  'posting_accepted',
  'posting_reception_transfer',
  'posting_ready_to_ship',
  'posting_transferring_to_delivery',
])
const SALES_SHIPMENT_FACT_STATUS_KEYS = new Set<string>([
  'shipped',
  'handed_over_to_delivery',
  'sent_to_delivery',
  'sent_by_seller',
  'driver_pickup',
  'in_transit',
  'transit',
  'on_the_way',
  'on_route',
  'delivering',
  'delivery_failed',
  'delivered',
  'delivered_to_customer',
  'customer_received',
  'returned',
  'returning',
  'return_in_progress',
  'return_preparing',
  'return_arrived_to_seller',
  'return_ready_for_seller_pickup',
  'posting_sent_by_seller',
  'posting_transfered_to_courier_service',
  'posting_transferred_to_courier_service',
  'posting_driver_pick_up',
  'posting_in_carriage',
  'posting_sent_to_city',
  'posting_on_way_to_city',
  'posting_on_way_to_pickup_point',
  'posting_arrived_at_pickup_point',
  'posting_in_pickup_point',
  'posting_on_pickup_point',
  'posting_waiting_buyer',
  'posting_conditionally_delivered',
  'posting_delivering',
  'posting_delivered',
  'posting_delivered_to_customer',
  'posting_return_in_progress',
  'posting_returning',
  'posting_returned',
  'posting_returned_to_seller',
  'posting_partial_return',
  'returned_to_seller',
])
const SALES_DELIVERY_EVENT_STATES = [
  'posting_delivered_to_customer',
  'posting_delivered',
  'posting_conditionally_delivered',
  'delivered_to_customer',
  'delivered',
  'customer_received',
] as const
function pickLatestSalesEventDateByStates(events: SalesPostingStateEvent[], states: readonly string[]): string {
  for (const state of states) {
    const latest = events
      .filter((event) => event.state === state)
      .map((event) => event.changed_state_date)
      .sort((left, right) => right.localeCompare(left))[0] ?? ''
    if (latest) return latest
  }
  return ''
}

function buildSalesStateEventCandidate(source: any): SalesPostingStateEvent | null {
  if (!source || typeof source !== 'object') return null

  const explicitNewState = pickFirstPresent(source, ['new_state'])
  const eventType = normalizeSalesLookupKey(pickFirstPresent(source, ['type', 'event_type']))
  const isExplicitStateEvent = explicitNewState !== undefined || eventType === SALES_STATE_CHANGED_EVENT_TYPE
  if (!isExplicitStateEvent) return null

  const state = normalizeSalesLookupKey(
    explicitNewState ?? (eventType === SALES_STATE_CHANGED_EVENT_TYPE
      ? pickFirstPresent(source, ['state', 'status'])
      : undefined),
  )
  const changedStateDate = normalizeDateValue(
    pickFirstPresent(source, ['changed_state_date'])
      ?? (eventType === SALES_STATE_CHANGED_EVENT_TYPE
        ? pickFirstPresent(source, ['date', 'created_at'])
        : undefined),
  )

  if (!state || !changedStateDate) return null
  return { state, changed_state_date: changedStateDate }
}

export function collectSalesStateEvents(...sources: any[]): SalesPostingStateEvent[] {
  const out: SalesPostingStateEvent[] = []
  const seen = new Set<string>()
  const visited = new Set<any>()

  const walk = (value: any) => {
    if (!value || typeof value !== 'object') return
    if (visited.has(value)) return
    visited.add(value)

    const candidate = buildSalesStateEventCandidate(value)
    if (candidate) {
      const key = `${candidate.state}|${candidate.changed_state_date}`
      if (!seen.has(key)) {
        seen.add(key)
        out.push(candidate)
      }
    }

    if (Array.isArray(value)) {
      for (const item of value) walk(item)
      return
    }

    for (const nested of Object.values(value as Record<string, unknown>)) {
      if (!nested || typeof nested !== 'object') continue
      walk(nested)
    }
  }

  for (const source of sources) walk(source)

  out.sort((left, right) => left.changed_state_date.localeCompare(right.changed_state_date))
  return out
}

function resolveSalesCurrentStateDateByStates(stateSet: ReadonlySet<string>, ...sources: any[]): string {
  for (const source of sources) {
    if (!source || typeof source !== 'object') continue
    const state = normalizeSalesLookupKey(pickFirstPresent(source, ['new_state', 'status', 'state', 'result.new_state', 'result.status', 'result.state']))
    const changed = normalizeDateValue(pickFirstPresent(source, ['changed_state_date', 'result.changed_state_date']))
    if (state && changed && stateSet.has(state)) return changed
  }
  return ''
}

function resolveSalesEventDateByStates(
  primaryStates: readonly string[],
  fallbackStates: readonly string[] = [],
  ...sources: any[]
): string {
  const events = collectSalesStateEvents(...sources)
  const fromEvents = pickLatestSalesEventDateByStates(events, primaryStates)
    || pickLatestSalesEventDateByStates(events, fallbackStates)
  if (fromEvents) return fromEvents

  const stateSet = new Set<string>([...primaryStates, ...fallbackStates])
  return resolveSalesCurrentStateDateByStates(stateSet, ...sources)
}

function isSalesShipmentPreFactKey(value: any): boolean {
  const key = normalizeSalesLookupKey(value)
  return Boolean(key && SALES_SHIPMENT_PREFACT_STATUS_KEYS.has(key))
}

function isSalesShipmentFactKey(value: any): boolean {
  const key = normalizeSalesLookupKey(value)
  return Boolean(key && SALES_SHIPMENT_FACT_STATUS_KEYS.has(key))
}

function shouldSuppressShipmentProgressDetail(mainStatusValue: any, detailValue: any): boolean {
  return isSalesShipmentPreFactKey(mainStatusValue) && isSalesShipmentFactKey(detailValue)
}

function resolveDeliveredEventDateFromSources(...sources: any[]): string {
  return resolveSalesEventDateByStates(SALES_DELIVERY_EVENT_STATES, [], ...sources)
}

function toComparableSalesTimestamp(value: string): number | null {
  const raw = String(value ?? '').trim()
  if (!raw) return null
  const parsed = Date.parse(raw)
  return Number.isFinite(parsed) ? parsed : null
}

function isChronologicallyAfter(candidate: string, reference: string): boolean {
  const candidateRaw = String(candidate ?? '').trim()
  if (!candidateRaw) return false
  const referenceRaw = String(reference ?? '').trim()
  if (!referenceRaw) return true

  const candidateTs = toComparableSalesTimestamp(candidateRaw)
  const referenceTs = toComparableSalesTimestamp(referenceRaw)
  if (candidateTs != null && referenceTs != null) return candidateTs > referenceTs
  if (candidateTs != null && referenceTs == null) return true
  if (candidateTs == null && referenceTs != null) return true
  return candidateRaw > referenceRaw
}

export function resolveFboShipmentDateFromSources(...sources: any[]): string {
  return resolveSalesEventDateByStates(FBO_SHIPMENT_PRIMARY_STATES, FBO_SHIPMENT_FALLBACK_STATES, ...sources)
}

function buildRelatedPostingsText(posting: any, fallbackPostings: string[] = [], secondaryPosting: any = null): string {
  const candidates = normalizeTextList([
    safeGetByPath(posting, 'related_postings.related_posting_numbers', undefined),
    safeGetByPath(posting, 'result.related_postings.related_posting_numbers', undefined),
    safeGetByPath(posting, 'related_postings.related_postings', undefined),
    safeGetByPath(posting, 'related_postings_numbers', undefined),
    safeGetByPath(posting, 'related_posting_numbers', undefined),
    safeGetByPath(posting, 'related_postings', undefined),
    safeGetByPath(posting, 'parent_posting_number', undefined),
    safeGetByPath(secondaryPosting, 'related_postings.related_posting_numbers', undefined),
    safeGetByPath(secondaryPosting, 'result.related_postings.related_posting_numbers', undefined),
    safeGetByPath(secondaryPosting, 'related_postings.related_postings', undefined),
    safeGetByPath(secondaryPosting, 'related_postings_numbers', undefined),
    safeGetByPath(secondaryPosting, 'related_posting_numbers', undefined),
    safeGetByPath(secondaryPosting, 'related_postings', undefined),
    safeGetByPath(secondaryPosting, 'parent_posting_number', undefined),
    fallbackPostings,
  ])
  return candidates.join(', ')
}

function normalizeSalesEndpointName(endpoint: string): 'FBS' | 'FBO' | '' {
  if (endpoint.includes('/posting/fbs/')) return 'FBS'
  if (endpoint.includes('/posting/fbo/')) return 'FBO'
  return ''
}

function normalizeDeliveryModelLabel(value: any): 'FBS' | 'rFBS' | 'FBO' | '' {
  const raw = normalizeTextValue(value)
  if (!raw) return ''
  const compact = raw.replace(/[^a-z]/gi, '').toUpperCase()
  if (!compact) return ''
  if (compact.includes('RFBS')) return 'rFBS'
  if (compact.includes('FBO')) return 'FBO'
  if (compact.includes('FBS')) return 'FBS'
  return ''
}

function pickDeliverySchemaFromOperations(source: any): any {
  if (!source || typeof source !== 'object') return undefined
  const operations = Array.isArray((source as any)?.operations)
    ? (source as any).operations
    : (Array.isArray((source as any)?.result?.operations) ? (source as any).result.operations : [])
  for (const operation of operations) {
    const value = pickFirstPresent(operation, ['posting.delivery_schema', 'delivery_schema'])
    if (value !== undefined && value !== null && value !== '') return value
  }
  return undefined
}

function hasRfbsAnalyticsSignal(...sources: any[]): boolean {
  for (const source of sources) {
    if (!source || typeof source !== 'object') continue
    const city = normalizeTextValue(pickFirstPresent(source, ['analytics_data.city', 'city']))
    const region = normalizeTextValue(pickFirstPresent(source, ['analytics_data.region', 'region']))
    if (city || region) return true
  }
  return false
}

function buildDeliveryModelValue(posting: any, detailPosting: any, endpoint: string): string {
  const explicit = normalizeDeliveryModelLabel(
    pickFirstPresent(detailPosting, [
      'delivery_schema',
      'posting.delivery_schema',
      'analytics_data.delivery_schema',
      'result.delivery_schema',
      'result.posting.delivery_schema',
    ])
    ?? pickDeliverySchemaFromOperations(detailPosting)
    ?? pickFirstPresent(posting, [
      'delivery_schema',
      'posting.delivery_schema',
      'analytics_data.delivery_schema',
      'result.delivery_schema',
      'result.posting.delivery_schema',
    ])
    ?? pickDeliverySchemaFromOperations(posting),
  )
  if (explicit) return explicit

  const normalizedEndpoint = normalizeSalesEndpointName(endpoint)
  if (normalizedEndpoint === 'FBO') return 'FBO'
  if (normalizedEndpoint === 'FBS') {
    if (hasRfbsAnalyticsSignal(detailPosting, posting)) return 'rFBS'
    return 'FBS'
  }

  return normalizeDeliveryModelLabel(
    pickFirstPresent(detailPosting, ['delivery_method.name', 'delivery_method', 'delivery_type', 'analytics_data.delivery_type'])
    ?? pickFirstPresent(posting, ['delivery_method.name', 'delivery_method', 'delivery_type', 'analytics_data.delivery_type']),
  )
}

function buildSalesStatusDetailsValue(posting: any, endpoint: string, secondaryPosting: any = null): string {
  const parts: string[] = []
  if (normalizeSalesEndpointName(endpoint) === 'FBO') {
    const nextStateRaw = pickFirstPresentFromSources(['new_state', 'result.new_state'], posting, secondaryPosting)
    const nextStateKey = normalizeSalesLookupKey(nextStateRaw)
    const nextState = nextStateKey && FBO_SHIPMENT_STATE_SET.has(nextStateKey)
      ? 'Передан в доставку'
      : translateSalesCodeValue(nextStateRaw, 'detail')
    pushUniqueSalesPart(parts, nextState)
    pushLabeledSalesPart(parts, 'Дата изменения', pickFirstPresentFromSources(['changed_state_date', 'result.changed_state_date'], posting, secondaryPosting))
    return parts.join(' | ')
  }
  const mainStatusRaw = pickFirstPresentFromSources(['status', 'state', 'result.status', 'result.state'], posting, secondaryPosting)
  const substatusRaw = pickFirstPresentFromSources(['substatus', 'result.substatus'], posting, secondaryPosting)
  if (shouldSuppressShipmentProgressDetail(mainStatusRaw, substatusRaw)) return ''
  const substatus = translateSalesCodeValue(substatusRaw, 'detail')
  pushUniqueSalesPart(parts, substatus)
  return parts.join(' | ')
}

function buildSalesCarrierStatusDetailsValue(posting: any, secondaryPosting: any = null): string {
  const parts: string[] = []
  const mainStatusRaw = pickFirstPresentFromSources(['status', 'state', 'result.status', 'result.state'], posting, secondaryPosting)
  const providerStatusRaw = pickFirstPresentFromSources(['provider_status', 'result.provider_status'], posting, secondaryPosting)
  if (shouldSuppressShipmentProgressDetail(mainStatusRaw, providerStatusRaw)) return ''
  pushUniqueSalesPart(parts, translateSalesCodeValue(providerStatusRaw, 'provider'))
  return parts.join(' | ')
}

function normalizeSalesPeriodDate(value: any): string {
  const raw = typeof value === 'string' ? value.trim() : ''
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : ''
}

export function buildSalesRequestBody(period: SalesPeriod | null | undefined, limit = 1000, offset = 0, endpoint?: string) {
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

export async function fetchSalesEndpointPages(
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

export function getSalesPostingDetailsKey(endpointKind: 'FBS' | 'FBO' | '', postingNumber: string): string {
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

function getFallbackDeliveredDateValue(source: any): string {
  return normalizeDateValue(pickFirstPresent(source, [
    'result.delivered_at',
    'delivered_at',
    'result.delivered_date',
    'delivered_date',
  ]))
}

function getCustomerDeliveryDateValue(source: any): string {
  return normalizeDateValue(pickFirstPresent(source, [
    'result.customer_deliver_date',
    'customer_deliver_date',
  ]))
}

function getShipmentDateValue(detailPosting: any, posting: any, endpointKind: 'FBS' | 'FBO' | ''): string {
  if (endpointKind === 'FBO') {
    return resolveFboShipmentDateFromSources(detailPosting, posting)
  }

  if (endpointKind === 'FBS') {
    const direct = normalizeDateValue(pickFirstPresentFromSources([
      'result.shipment_date_actual',
      'shipment_date_actual',
      'result.shipped_at',
      'shipped_at',
      'result.shipment_date',
      'shipment_date',
    ], detailPosting, posting))
    if (direct) return direct
    return resolveSalesEventDateByStates(SALES_SHIPMENT_EVENT_PRIMARY_STATES, SALES_SHIPMENT_EVENT_FALLBACK_STATES, detailPosting, posting)
  }

  return normalizeDateValue(pickFirstPresentFromSources([
    'result.shipment_date_actual',
    'shipment_date_actual',
    'result.shipped_at',
    'shipped_at',
    'result.shipment_date',
    'shipment_date',
  ], detailPosting, posting))
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

const SALES_DELIVERY_FALLBACK_PATHS = [
  'result.delivered_at',
  'delivered_at',
  'result.delivered_date',
  'delivered_date',
  'result.customer_deliver_date',
  'customer_deliver_date',
]

function hasDeliveryDateSignal(posting: any): boolean {
  return Boolean(normalizeDateValue(pickFirstPresent(posting, SALES_DELIVERY_FALLBACK_PATHS)))
}

function getFallbackDeliveryDateValue(source: any): string {
  return getFallbackDeliveredDateValue(source) || getCustomerDeliveryDateValue(source)
}

function resolvePostingDeliveryDate(
  detailPosting: any,
  posting: any,
  endpointKind: 'FBS' | 'FBO' | '',
  shipmentDate = '',
): string {
  const delivered = hasDeliveredStatusSignal(detailPosting) || hasDeliveredStatusSignal(posting)
  if (!delivered) return ''

  const shipmentBase = String(shipmentDate || getShipmentDateValue(detailPosting, posting, endpointKind) || '').trim()
  const exact = getFactDeliveryDateValue(detailPosting) || getFactDeliveryDateValue(posting)
  if (exact && isChronologicallyAfter(exact, shipmentBase)) return exact

  const fromEvents = resolveDeliveredEventDateFromSources(detailPosting, posting)
  if (fromEvents && isChronologicallyAfter(fromEvents, shipmentBase)) return fromEvents

  const actualFallback = getFallbackDeliveredDateValue(detailPosting) || getFallbackDeliveredDateValue(posting)
  if (actualFallback && isChronologicallyAfter(actualFallback, shipmentBase)) return actualFallback

  const customerFallback = getCustomerDeliveryDateValue(detailPosting) || getCustomerDeliveryDateValue(posting)
  if (customerFallback && isChronologicallyAfter(customerFallback, shipmentBase)) return customerFallback

  return ''
}

function shouldFetchSalesPostingDetails(posting: any, endpointKind: 'FBS' | 'FBO' | ''): boolean {
  if (!posting || typeof posting !== 'object') return false
  if (endpointKind === 'FBO') {
    const hasRelated = Boolean(buildRelatedPostingsText(posting))
    const hasShipmentDate = Boolean(getShipmentDateValue(null, posting, 'FBO'))
    const hasDeliveryCluster = Boolean(normalizeTextValue(pickFirstPresent(posting, [
      'financial_data.cluster_to',
      'result.financial_data.cluster_to',
      'cluster_to',
      'result.cluster_to',
    ])))
    const needsDeliveryBackfill = (getFactDeliveryDateValue(posting) || hasDeliveryDateSignal(posting) || hasDeliveredStatusSignal(posting))
      ? !Boolean(resolvePostingDeliveryDate(posting, posting, 'FBO'))
      : false

    if (!hasRelated || !hasShipmentDate || !hasDeliveryCluster || needsDeliveryBackfill) return true
    return false
  }
  const hasResolvedDelivery = Boolean(resolvePostingDeliveryDate(posting, posting, endpointKind))
  if ((getFactDeliveryDateValue(posting) || hasDeliveryDateSignal(posting) || hasDeliveredStatusSignal(posting)) && !hasResolvedDelivery) return true
  if (!buildRelatedPostingsText(posting)) return true
  if (!getShipmentDateValue(null, posting, endpointKind)) return true
  if (!normalizeTextValue(pickFirstPresent(posting, ['financial_data.cluster_to', 'result.financial_data.cluster_to', 'cluster_to', 'result.cluster_to']))) return true
  return false
}

export async function fetchSalesPostingDetails(
  secrets: any,
  payloads: SalesPayloadEnvelope[],
): Promise<Map<string, any>> {
  const requests: Array<{ endpointKind: 'FBS' | 'FBO'; postingNumber: string }> = []
  const seen = new Set<string>()
  for (const envelope of payloads) {
    const endpointKind = normalizeSalesEndpointName(envelope.endpoint)
    if (endpointKind !== 'FBS' && endpointKind !== 'FBO') continue
    for (const posting of extractPostingsFromPayload(envelope.payload)) {
      const postingNumber = normalizeTextValue(pickFirstPresent(posting, ['posting_number', 'postingNumber']))
      if (!postingNumber || !shouldFetchSalesPostingDetails(posting, endpointKind)) continue
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

export function normalizeSalesRows(payloads: SalesPayloadEnvelope[], products: GridApiRow[], postingDetailsByKey?: Map<string, any>): SalesRow[] {
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
      const detailPosting = postingDetailsByKey?.get(getSalesPostingDetailsKey(endpointKind, postingNumber)) ?? null
      const related = buildRelatedPostingsText(detailPosting, fallbackRelated, posting)
      const shipmentDate = getShipmentDateValue(detailPosting, posting, endpointKind)
      const status = translateSalesCodeValue(pickFirstPresentFromSources(['status', 'state', 'result.status', 'result.state'], detailPosting, posting), 'status')
      const statusDetails = buildSalesStatusDetailsValue(detailPosting, envelope.endpoint, posting)
      const carrierStatusDetails = buildSalesCarrierStatusDetailsValue(detailPosting, posting)
      const deliveredAt = resolvePostingDeliveryDate(detailPosting, posting, endpointKind, shipmentDate)
      const deliveryCluster = normalizeTextValue(pickFirstPresentFromSources(['financial_data.cluster_to', 'result.financial_data.cluster_to', 'cluster_to', 'result.cluster_to'], detailPosting, posting))
      const deliverySchema = buildDeliveryModelValue(posting, detailPosting, envelope.endpoint)
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
    return String(b?.sku ?? '').localeCompare(String(a?.sku ?? ''))
  })
}
