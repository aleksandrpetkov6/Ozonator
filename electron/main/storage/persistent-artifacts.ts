import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { getPersistentRootDir } from './paths'

export type CurrentPersistentArtifactMergeMode = 'replace' | 'csv_append_missing'

export type CurrentPersistentArtifactInput = {
  groupPath: string[]
  slot: string
  content: string
  extension?: string
  suffix?: string
  headers?: string[]
  mergeMode?: CurrentPersistentArtifactMergeMode
  identityHeaders?: string[]
  preserveOtherFiles?: boolean
}

export type CurrentPersistentArtifactSaved = {
  path: string
  slot: string
  fileName: string
  headers: string[]
}

export type SaveCurrentPersistentArtifactsResult = {
  saved: CurrentPersistentArtifactSaved[]
  cleanedLegacyFilesCount: number
}

function normalizeText(value: unknown): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function sanitizeFilePart(value: unknown): string {
  const normalized = normalizeText(value)
    .replace(/[\\/:*?"<>|]+/g, '-')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^-+|-+$/g, '')
  return normalized || 'unknown'
}

function splitCsvSemicolonLine(line: string): string[] {
  const out: string[] = []
  let current = ''
  let inQuotes = false
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index]
    if (char === '"') {
      const nextChar = line[index + 1]
      if (inQuotes && nextChar === '"') {
        current += '"'
        index += 1
        continue
      }
      inQuotes = !inQuotes
      continue
    }
    if (char === ';' && !inQuotes) {
      out.push(current)
      current = ''
      continue
    }
    current += char
  }
  out.push(current)
  return out
}

function escapeCsvSemicolonValue(value: string): string {
  const safe = String(value ?? '').replace(/"/g, '""')
  return `"${safe}"`
}

function splitCsvLinesPreservingQuotes(csvText: string): string[] {
  const out: string[] = []
  let current = ''
  let inQuotes = false
  const safeText = String(csvText ?? '')
  for (let index = 0; index < safeText.length; index += 1) {
    const char = safeText[index]
    if (char === '"') {
      const nextChar = safeText[index + 1]
      if (inQuotes && nextChar === '"') {
        current += '""'
        index += 1
        continue
      }
      inQuotes = !inQuotes
      current += char
      continue
    }
    if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && safeText[index + 1] === '\n') index += 1
      if (current.trim()) out.push(current)
      current = ''
      continue
    }
    current += char
  }
  if (current.trim()) out.push(current)
  return out
}

type ParsedCsv = {
  headers: string[]
  rows: string[][]
}

function parseSemicolonCsv(csvText: string): ParsedCsv {
  const lines = splitCsvLinesPreservingQuotes(csvText)
  if (lines.length === 0) return { headers: [], rows: [] }
  const headers = splitCsvSemicolonLine(lines[0]).map((value) => normalizeText(value))
  const rows = lines.slice(1).map((line) => splitCsvSemicolonLine(line))
  return { headers, rows }
}

function buildCsvIdentity(headers: string[], row: string[], preferredHeaders: string[]): string {
  const normalizedHeaders = headers.map((value) => normalizeText(value))
  const preferredParts = preferredHeaders
    .map((header) => normalizeText(header))
    .filter(Boolean)
    .map((header) => {
      const index = normalizedHeaders.indexOf(header)
      if (index < 0) return ''
      return `${header}=${normalizeText(row[index] ?? '')}`
    })
    .filter(Boolean)

  const hasValue = preferredParts.some((part) => !part.endsWith('='))
  if (preferredParts.length > 0 && hasValue) return preferredParts.join('|')
  return row.map((value) => normalizeText(value)).join('|')
}

function buildHeaderUnion(existingHeaders: string[], incomingHeaders: string[]): string[] {
  const out: string[] = []
  for (const header of [...existingHeaders, ...incomingHeaders]) {
    const normalized = normalizeText(header)
    if (!normalized || out.includes(normalized)) continue
    out.push(normalized)
  }
  return out
}

function readRowByHeader(headers: string[], row: string[]): Record<string, string> {
  const out: Record<string, string> = {}
  headers.forEach((header, index) => {
    const normalized = normalizeText(header)
    if (!normalized) return
    out[normalized] = normalizeText(row[index] ?? '')
  })
  return out
}

function mergeRowByHeaders(args: {
  targetHeaders: string[]
  existingRow?: string[] | null
  incomingRow?: string[] | null
  existingHeaders: string[]
  incomingHeaders: string[]
}): string[] {
  const existingMap = args.existingRow ? readRowByHeader(args.existingHeaders, args.existingRow) : {}
  const incomingMap = args.incomingRow ? readRowByHeader(args.incomingHeaders, args.incomingRow) : {}
  return args.targetHeaders.map((header) => {
    const incomingValue = normalizeText(incomingMap[header] ?? '')
    const existingValue = normalizeText(existingMap[header] ?? '')
    return incomingValue || existingValue || ''
  })
}

function parseSortableDate(value: string): number | null {
  const raw = normalizeText(value)
  if (!raw) return null

  const iso = Date.parse(raw)
  if (Number.isFinite(iso)) return iso

  const match = raw.match(/^(\d{2})\.(\d{2})\.(\d{2,4})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/)
  if (!match) return null

  const day = Number(match[1])
  const month = Number(match[2]) - 1
  let year = Number(match[3])
  if (year < 100) year += 2000
  const hour = Number(match[4] || 0)
  const minute = Number(match[5] || 0)
  const second = Number(match[6] || 0)
  const ts = Date.UTC(year, month, day, hour, minute, second)
  return Number.isFinite(ts) ? ts : null
}

function findPreferredDateHeader(headers: string[]): string | null {
  const normalizedHeaders = headers.map((value) => normalizeText(value))
  const candidates = [
    'Принят в обработку',
    'Дата доставки',
    'Дата отгрузки',
    'Фактическая дата передачи в доставку',
    'created_at',
    'in_process_at',
    'shipment_date',
    'delivery_date',
  ]
  for (const candidate of candidates) {
    if (normalizedHeaders.includes(candidate)) return candidate
  }
  return normalizedHeaders.find(Boolean) ?? null
}

function sortCsvRows(headers: string[], rows: string[][], identityHeaders: string[]): string[][] {
  const preferredDateHeader = findPreferredDateHeader(headers)
  const preferredDateIndex = preferredDateHeader ? headers.findIndex((header) => normalizeText(header) === preferredDateHeader) : -1
  return [...rows].sort((left, right) => {
    const leftDate = preferredDateIndex >= 0 ? parseSortableDate(String(left[preferredDateIndex] ?? '')) : null
    const rightDate = preferredDateIndex >= 0 ? parseSortableDate(String(right[preferredDateIndex] ?? '')) : null
    if (leftDate !== null && rightDate !== null && leftDate !== rightDate) return leftDate - rightDate
    if (leftDate !== null && rightDate === null) return -1
    if (leftDate === null && rightDate !== null) return 1
    const leftKey = buildCsvIdentity(headers, left, identityHeaders)
    const rightKey = buildCsvIdentity(headers, right, identityHeaders)
    return leftKey.localeCompare(rightKey, 'ru')
  })
}

function serializeSemicolonCsv(parsed: ParsedCsv): string {
  if (parsed.headers.length === 0) return ''
  const lines: string[] = []
  lines.push(parsed.headers.map((value) => escapeCsvSemicolonValue(value)).join(';'))
  for (const row of parsed.rows) {
    const normalizedRow = parsed.headers.map((_, index) => escapeCsvSemicolonValue(normalizeText(row[index] ?? '')))
    lines.push(normalizedRow.join(';'))
  }
  return `${lines.join('\n')}\n`
}

function mergeCsvAppendMissing(existingText: string, incomingText: string, identityHeaders: string[]): string {
  const incoming = parseSemicolonCsv(incomingText)
  if (incoming.headers.length === 0) return String(existingText ?? '')

  const existing = parseSemicolonCsv(existingText)
  if (existing.headers.length === 0) {
    return serializeSemicolonCsv({
      headers: incoming.headers,
      rows: sortCsvRows(incoming.headers, incoming.rows, identityHeaders),
    })
  }

  const headers = buildHeaderUnion(existing.headers, incoming.headers)
  const existingByKey = new Map<string, string[]>()
  const incomingByKey = new Map<string, string[]>()

  for (const row of existing.rows) {
    const key = buildCsvIdentity(existing.headers, row, identityHeaders)
    if (!key) continue
    existingByKey.set(key, row)
  }
  for (const row of incoming.rows) {
    const key = buildCsvIdentity(incoming.headers, row, identityHeaders)
    if (!key) continue
    incomingByKey.set(key, row)
  }

  const mergedRows: string[][] = []
  const allKeys = new Set<string>([...existingByKey.keys(), ...incomingByKey.keys()])
  for (const key of allKeys) {
    mergedRows.push(mergeRowByHeaders({
      targetHeaders: headers,
      existingRow: existingByKey.get(key),
      incomingRow: incomingByKey.get(key),
      existingHeaders: existing.headers,
      incomingHeaders: incoming.headers,
    }))
  }

  return serializeSemicolonCsv({
    headers,
    rows: sortCsvRows(headers, mergedRows, identityHeaders),
  })
}

export function saveCurrentPersistentArtifacts(artifacts: CurrentPersistentArtifactInput[]): SaveCurrentPersistentArtifactsResult {
  const safeArtifacts = Array.isArray(artifacts) ? artifacts : []
  if (safeArtifacts.length === 0) return { saved: [], cleanedLegacyFilesCount: 0 }

  const firstGroupPath = Array.isArray(safeArtifacts[0]?.groupPath) ? safeArtifacts[0].groupPath : []
  const root = join(getPersistentRootDir(), ...firstGroupPath)
  mkdirSync(root, { recursive: true })

  const prepared = safeArtifacts.map((artifact) => {
    const extension = sanitizeFilePart(artifact?.extension || 'txt').replace(/^\.+/, '') || 'txt'
    const suffix = sanitizeFilePart(artifact?.suffix || 'report')
    const slot = sanitizeFilePart(artifact?.slot)
    const fileName = `current__${slot}__${suffix}.${extension}`
    const filePath = join(root, fileName)
    const mergeMode: CurrentPersistentArtifactMergeMode = artifact?.mergeMode === 'csv_append_missing' ? 'csv_append_missing' : 'replace'
    const identityHeaders = Array.isArray(artifact?.identityHeaders)
      ? artifact.identityHeaders.map((value) => normalizeText(value)).filter(Boolean)
      : []
    const incomingContent = String(artifact?.content ?? '')
    let finalContent = incomingContent
    if (mergeMode === 'csv_append_missing' && existsSync(filePath)) {
      try {
        const existingContent = readFileSync(filePath, 'utf8')
        finalContent = mergeCsvAppendMissing(existingContent, incomingContent, identityHeaders)
      } catch {
        finalContent = incomingContent
      }
    }
    writeFileSync(filePath, finalContent, 'utf8')
    return {
      path: filePath,
      slot,
      fileName,
      headers: Array.isArray(artifact?.headers) ? artifact.headers.map((value) => normalizeText(value)).filter(Boolean) : [],
    }
  })

  return {
    saved: prepared.map(({ path, slot, fileName, headers }) => ({ path, slot, fileName, headers })),
    cleanedLegacyFilesCount: 0,
  }
}
