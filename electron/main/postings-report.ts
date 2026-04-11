import type { Secrets } from './types'

const OZON_API_BASE_URL = 'https://api-seller.ozon.ru'
const REPORT_INFO_POLL_ATTEMPTS = 25
const REPORT_INFO_POLL_DELAY_MS = 1500
const CSV_MOSCOW_OFFSET_MS = 3 * 60 * 60 * 1000

type JsonRecord = Record<string, unknown>

export type ReportPeriodInput = {
  from?: string | null
  to?: string | null
}

export type SalesPostingsReportRow = {
  posting_number: string
  order_number: string
  delivery_schema: string
  shipment_date: string
  delivery_date: string
  raw_row: Record<string, string>
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function text(value: unknown): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function normalizePeriodDate(value: unknown): string | null {
  const raw = text(value)
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null
}

function normalizePeriod(period: ReportPeriodInput | null | undefined): { from: string | null; to: string | null } {
  let from = normalizePeriodDate(period?.from)
  let to = normalizePeriodDate(period?.to)
  if (!from && to) from = to
  if (from && !to) to = from
  if (from && to && from > to) [from, to] = [to, from]
  return { from, to }
}

function buildPostingsReportCreateBody(
  period: ReportPeriodInput | null | undefined,
  deliverySchema?: string | null,
) {
  const normalized = normalizePeriod(period)
  const now = new Date()
  const fromDate = normalized.from
    ? new Date(`${normalized.from}T00:00:00.000Z`)
    : new Date(now.getTime() - (90 * 24 * 60 * 60 * 1000))
  const toDate = normalized.to
    ? new Date(`${normalized.to}T23:59:59.999Z`)
    : now

  return {
    filter: {
      processed_at_from: fromDate.toISOString(),
      processed_at_to: toDate.toISOString(),
      delivery_schema: deliverySchema ? [String(deliverySchema).trim().toLowerCase()] : [],
      sku: [],
      cancel_reason_id: [],
      offer_id: '',
      status_alias: [],
      statuses: [],
      title: '',
    },
    language: 'DEFAULT',
  }
}

async function ozonPost(secrets: Secrets, endpoint: string, body: unknown): Promise<JsonRecord> {
  const res = await fetch(`${OZON_API_BASE_URL}${endpoint}`, {
    method: 'POST',
    headers: {
      'Client-Id': secrets.clientId,
      'Api-Key': secrets.apiKey,
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(body ?? {}),
  })

  const rawText = await res.text()
  let parsed: JsonRecord = {}
  if (rawText.trim()) {
    try {
      parsed = JSON.parse(rawText) as JsonRecord
    } catch {
      throw new Error(`Ozon API error: invalid JSON for ${endpoint} (HTTP ${res.status})`)
    }
  }

  if (!res.ok) {
    const message = text((parsed as any)?.message) || text((parsed as any)?.error) || `HTTP ${res.status}`
    throw new Error(`Ozon API error ${endpoint}: ${message}`)
  }

  return parsed
}

async function ozonReportPostingsCreate(secrets: Secrets, body: unknown) {
  return ozonPost(secrets, '/v1/report/postings/create', body ?? {})
}

async function ozonReportInfo(secrets: Secrets, code: string) {
  const normalizedCode = text(code)
  if (!normalizedCode) throw new Error('Не указан code отчёта')
  return ozonPost(secrets, '/v1/report/info', { code: normalizedCode })
}

async function ozonDownloadReportFile(fileUrl: string): Promise<Uint8Array> {
  const normalizedUrl = text(fileUrl)
  if (!normalizedUrl) throw new Error('Не указан URL отчёта')

  const res = await fetch(normalizedUrl, {
    method: 'GET',
    headers: {
      Accept: 'text/csv,application/octet-stream;q=0.9,*/*;q=0.8',
    },
  })

  if (!res.ok) {
    throw new Error(`Ozon report download error: HTTP ${res.status}`)
  }

  const buffer = await res.arrayBuffer()
  return new Uint8Array(buffer)
}

function normalizeReportStatus(value: unknown): string {
  return text(value).toLowerCase().replace(/\s+/g, '_')
}

function looksLikeReadyReportStatus(value: unknown): boolean {
  const status = normalizeReportStatus(value)
  return status === 'success' || status === 'completed' || status === 'done' || status === 'ready'
}

function looksLikeFailedReportStatus(value: unknown): boolean {
  const status = normalizeReportStatus(value)
  return status === 'error' || status === 'failed' || status === 'fail' || status === 'cancelled' || status === 'canceled'
}

function decodeCsvBytes(bytes: Uint8Array): string {
  if (bytes.length === 0) return ''

  const tryDecode = (encoding: string) => {
    try {
      return new TextDecoder(encoding).decode(bytes)
    } catch {
      return ''
    }
  }

  const utf8 = tryDecode('utf-8')
  if (utf8 && !utf8.includes('�')) return utf8

  const win1251 = tryDecode('windows-1251')
  if (win1251) return win1251

  return utf8 || tryDecode('utf-8')
}

function detectCsvDelimiter(line: string): string {
  const candidates = [';', ',', '\t']
  let best = ';'
  let bestScore = -1
  for (const delimiter of candidates) {
    let score = 0
    let inQuotes = false
    for (const ch of line) {
      if (ch === '"') inQuotes = !inQuotes
      else if (ch === delimiter && !inQuotes) score += 1
    }
    if (score > bestScore) {
      bestScore = score
      best = delimiter
    }
  }
  return best
}

function parseCsv(textRaw: string): Array<Record<string, string>> {
  const text = textRaw.replace(/^\uFEFF/, '')
  const firstLine = text.split(/\r?\n/, 1)[0] ?? ''
  const delimiter = detectCsvDelimiter(firstLine)

  const rows: string[][] = []
  let field = ''
  let row: string[] = []
  let inQuotes = false

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i]
    const next = text[i + 1]

    if (ch === '"') {
      if (inQuotes && next === '"') {
        field += '"'
        i += 1
      } else {
        inQuotes = !inQuotes
      }
      continue
    }

    if (!inQuotes && ch === delimiter) {
      row.push(field)
      field = ''
      continue
    }

    if (!inQuotes && (ch === '\n' || ch === '\r')) {
      if (ch === '\r' && next === '\n') i += 1
      row.push(field)
      rows.push(row)
      row = []
      field = ''
      continue
    }

    field += ch
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field)
    rows.push(row)
  }

  if (rows.length === 0) return []

  const headers = rows[0].map((value) => value.trim())
  const out: Array<Record<string, string>> = []
  for (const rawRow of rows.slice(1)) {
    const obj: Record<string, string> = {}
    let hasValue = false
    for (let index = 0; index < headers.length; index += 1) {
      const header = headers[index] ?? `col_${index}`
      const value = String(rawRow[index] ?? '').trim()
      obj[header] = value
      if (value) hasValue = true
    }
    if (hasValue) out.push(obj)
  }
  return out
}

function normalizeHeaderKey(value: unknown): string {
  return text(value)
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/[()]/g, ' ')
    .replace(/[^a-zа-я0-9]+/gi, '_')
    .replace(/^_+|_+$/g, '')
}

function pickRowValue(row: Record<string, string>, aliases: string[]): string {
  const normalizedRow = new Map<string, string>()
  for (const [key, value] of Object.entries(row)) {
    normalizedRow.set(normalizeHeaderKey(key), text(value))
  }

  for (const alias of aliases) {
    const direct = normalizedRow.get(normalizeHeaderKey(alias))
    if (direct) return direct
  }
  return ''
}

function toUtcIsoFromMoscowParts(year: number, month: number, day: number, hour: number, minute: number, second: number) {
  const localMillis = Date.UTC(year, month - 1, day, hour, minute, second, 0)
  return new Date(localMillis - CSV_MOSCOW_OFFSET_MS).toISOString()
}

function parseOzonLocalDateToIso(value: unknown): string {
  const raw = text(value)
  if (!raw) return ''

  const isoParsed = new Date(raw)
  if (!Number.isNaN(isoParsed.getTime()) && /(z|[+-]\d{2}:?\d{2})$/i.test(raw)) {
    return isoParsed.toISOString()
  }

  const dotMatch = raw.match(/^(\d{2})\.(\d{2})\.(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/)
  if (dotMatch) {
    const day = Number(dotMatch[1])
    const month = Number(dotMatch[2])
    const year = Number(dotMatch[3])
    const hour = Number(dotMatch[4] ?? '0')
    const minute = Number(dotMatch[5] ?? '0')
    const second = Number(dotMatch[6] ?? '0')
    return toUtcIsoFromMoscowParts(year, month, day, hour, minute, second)
  }

  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/)
  if (isoMatch) {
    const year = Number(isoMatch[1])
    const month = Number(isoMatch[2])
    const day = Number(isoMatch[3])
    const hour = Number(isoMatch[4] ?? '0')
    const minute = Number(isoMatch[5] ?? '0')
    const second = Number(isoMatch[6] ?? '0')
    return toUtcIsoFromMoscowParts(year, month, day, hour, minute, second)
  }

  if (!Number.isNaN(isoParsed.getTime())) return isoParsed.toISOString()
  return ''
}

function normalizeDeliverySchema(value: unknown): string {
  const compact = text(value).toLowerCase().replace(/[^a-z]/g, '')
  if (!compact) return ''
  if (compact.includes('rfbs')) return 'rFBS'
  if (compact.includes('fbo')) return 'FBO'
  if (compact.includes('fbs')) return 'FBS'
  return text(value)
}

function mapCsvRowToSalesReportRow(row: Record<string, string>): SalesPostingsReportRow | null {
  const postingNumber = pickRowValue(row, ['Номер отправления', 'Отправление', 'posting_number', 'posting number'])
  if (!postingNumber) return null

  const orderNumber = pickRowValue(row, ['Номер заказа', 'order_number', 'order number'])
  const deliverySchema = normalizeDeliverySchema(pickRowValue(row, ['Метод доставки', 'Схема доставки', 'delivery_schema', 'delivery schema']))
  const shipmentDateRaw = pickRowValue(row, [
    'Фактическая дата передачи в доставку',
    'Передан в доставку',
    'Дата отгрузки',
    'shipment_date_actual',
    'shipment_date',
    'shipment date',
  ])
  const shipmentDate = parseOzonLocalDateToIso(shipmentDateRaw)
  const deliveryDate = parseOzonLocalDateToIso(pickRowValue(row, ['Дата доставки', 'delivery_date', 'delivery date']))

  return {
    posting_number: postingNumber,
    order_number: orderNumber,
    delivery_schema: deliverySchema,
    shipment_date: shipmentDate,
    delivery_date: deliveryDate,
    raw_row: row,
  }
}

function parseSalesPostingsReportCsv(csvText: string): SalesPostingsReportRow[] {
  const rawRows = parseCsv(csvText)
  const out = new Map<string, SalesPostingsReportRow>()

  for (const rawRow of rawRows) {
    const mapped = mapCsvRowToSalesReportRow(rawRow)
    if (!mapped) continue
    const key = `${mapped.delivery_schema}|${mapped.posting_number}`
    const prev = out.get(key)
    if (!prev || (!prev.shipment_date && mapped.shipment_date)) {
      out.set(key, mapped)
    }
  }

  return Array.from(out.values())
}

export async function fetchSalesPostingsReportRows(
  secrets: Secrets,
  period: ReportPeriodInput | null | undefined,
  deliverySchema?: string | null,
): Promise<{ reportCode: string; fileUrl: string; rows: SalesPostingsReportRow[] }> {
  const createResponse = await ozonReportPostingsCreate(secrets, buildPostingsReportCreateBody(period, deliverySchema))
  const createResult = (createResponse && typeof createResponse === 'object' && 'result' in createResponse)
    ? (createResponse as any).result
    : createResponse
  const reportCode = text((createResult as any)?.code)
  if (!reportCode) throw new Error('Ozon report error: empty report code for /v1/report/postings/create')

  let fileUrl = ''
  let lastStatus = ''
  let lastError = ''

  for (let attempt = 0; attempt < REPORT_INFO_POLL_ATTEMPTS; attempt += 1) {
    const infoResponse = await ozonReportInfo(secrets, reportCode)
    const infoResult = (infoResponse && typeof infoResponse === 'object' && 'result' in infoResponse)
      ? (infoResponse as any).result
      : infoResponse

    lastStatus = normalizeReportStatus((infoResult as any)?.status)
    lastError = text((infoResult as any)?.error)
    fileUrl = text((infoResult as any)?.file)

    if (fileUrl && (looksLikeReadyReportStatus(lastStatus) || !lastStatus)) break
    if (looksLikeFailedReportStatus(lastStatus)) {
      throw new Error(`Ozon report error: ${lastStatus}${lastError ? ` (${lastError})` : ''}`)
    }
    await sleep(REPORT_INFO_POLL_DELAY_MS)
  }

  if (!fileUrl) {
    throw new Error(`Ozon report error: report file is not ready for code ${reportCode}${lastStatus ? ` (${lastStatus})` : ''}`)
  }

  const bytes = await ozonDownloadReportFile(fileUrl)
  const csvText = decodeCsvBytes(bytes)
  const rows = parseSalesPostingsReportCsv(csvText)

  return { reportCode, fileUrl, rows }
}
