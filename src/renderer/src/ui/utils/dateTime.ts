export type DateOnlyBoundary = 'keep' | 'startOfDay' | 'endOfDay'

const DATE_ONLY_RE = /^(\d{4})-(\d{2})-(\d{2})$/

function parseDateOnlyLocal(value: string, boundary: DateOnlyBoundary): Date | null {
  const match = DATE_ONLY_RE.exec(value.trim())
  if (!match) return null

  const year = Number(match[1])
  const month = Number(match[2])
  const day = Number(match[3])
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null

  const isEnd = boundary === 'endOfDay'
  return new Date(
    year,
    month - 1,
    day,
    isEnd ? 23 : 0,
    isEnd ? 59 : 0,
    isEnd ? 59 : 0,
    isEnd ? 999 : 0,
  )
}

function toDate(value: unknown, boundary: DateOnlyBoundary): Date | null {
  if (value == null || value === '') return null

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : new Date(value.getTime())
  }

  if (typeof value === 'string') {
    const localDateOnly = parseDateOnlyLocal(value, boundary)
    if (localDateOnly) return localDateOnly
  }

  const parsed = new Date(value as any)
  return Number.isNaN(parsed.getTime()) ? null : parsed
}

export function formatDateTimeRu(value: unknown, options?: { dateOnlyBoundary?: DateOnlyBoundary }): string {
  if (value == null || value === '') return ''

  const parsed = toDate(value, options?.dateOnlyBoundary ?? 'keep')
  if (!parsed) return String(value)

  const pad = (n: number) => String(n).padStart(2, '0')
  const dd = pad(parsed.getDate())
  const mm = pad(parsed.getMonth() + 1)
  const yy = pad(parsed.getFullYear() % 100)
  const hh = pad(parsed.getHours())
  const mi = pad(parsed.getMinutes())
  const ss = pad(parsed.getSeconds())

  return `${dd}.${mm}.${yy} ${hh}:${mi}:${ss}`
}
