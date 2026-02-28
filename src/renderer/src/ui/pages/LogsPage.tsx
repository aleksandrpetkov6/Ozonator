import React, { useEffect, useMemo, useState } from 'react'
import { formatDateTimeRu } from '../utils/dateTime'
import { getSortButtonTitle, type SortableColumn, type TableSortState, sortTableRows, toggleTableSort } from '../utils/tableSort'

type LogRow = {
  id: number
  type: string
  status: string
  started_at: string
  finished_at: string | null
  items_count: number | null
  error_message: string | null
  error_details: string | null
  meta: string | null
}

type LogSortCol = 'id' | 'type' | 'status' | 'started_at' | 'finished_at' | 'details' | 'error_message'
type LogSortState = TableSortState<LogSortCol>
type LogColDef = SortableColumn<LogRow, LogSortCol> & { id: LogSortCol; title: string }

const TYPE_RU: Record<string, string> = {
  sync_products: 'Синхронизация товаров',
  check_auth: 'Проверка доступа',
  app_install: 'Установка программы',
  app_update: 'Обновление программы',
  app_reinstall: 'Переустановка программы',
  app_uninstall: 'Удаление программы',
  admin_settings: 'Настройки Админ',
}

const STATUS_RU: Record<string, string> = {
  pending: 'Ожидает',
  running: 'В процессе',
  success: 'Успешно',
  error: 'Ошибка',
}

function typeRu(v?: string | null) {
  if (!v) return '-'
  return TYPE_RU[v] ?? v
}

function statusRu(v?: string | null) {
  if (!v) return '-'
  return STATUS_RU[v] ?? v
}

function fmtDt(iso?: string | null) {
  if (!iso) return '-'
  return formatDateTimeRu(iso) || '-'
}

function detailsRu(type?: string | null, meta?: string | null, itemsCount?: number | null): string {
  if (type === 'app_uninstall') return ''
  if (!meta) return '-'
  try {
    const m = JSON.parse(meta)
    if (typeof m?.updated === 'number' || typeof m?.added === 'number') {
      const add = (typeof m?.added === 'number') ? m.added : 0
      const synced = (typeof itemsCount === 'number' && Number.isFinite(itemsCount)) ? itemsCount : Math.max(0, add)
      return `синхронизировано: ${synced}, новых: ${add}`
    }
    if (typeof m?.logRetentionDays === 'number') return `жизнь лога: ${m.logRetentionDays} дн.`
    if (m?.appVersion || m?.previousVersion) {
      if (m?.appVersion && (type === 'app_install' || type === 'app_reinstall' || type === 'app_update')) return `версия: ${m.appVersion}`
      const parts: string[] = []
      if (m?.appVersion) parts.push(`версия: ${m.appVersion}`)
      if (m?.previousVersion) parts.push(`было: ${m.previousVersion}`)
      return parts.join(', ') || meta
    }
    const parts: string[] = []
    if (typeof m?.pages === 'number') parts.push(`страниц: ${m.pages}`)
    if (typeof m?.infoBatches === 'number') parts.push(`батчей: ${m.infoBatches}`)
    if (typeof m?.infoFetched === 'number') parts.push(`расширено: ${m.infoFetched}`)
    return parts.length ? parts.join(', ') : meta
  } catch {
    return meta
  }
}

function toSortTimestamp(value?: string | null): number | null {
  if (!value) return null
  const time = Date.parse(String(value))
  return Number.isFinite(time) ? time : null
}

const LOG_COLUMNS: readonly LogColDef[] = [
  { id: 'id', title: 'ID' },
  { id: 'type', title: 'Тип', getSortValue: (row) => typeRu(row.type) },
  { id: 'status', title: 'Статус', getSortValue: (row) => statusRu(row.status) },
  { id: 'started_at', title: 'Старт', getSortValue: (row) => toSortTimestamp(row.started_at) ?? '' },
  { id: 'finished_at', title: 'Финиш', getSortValue: (row) => toSortTimestamp(row.finished_at) ?? '' },
  { id: 'details', title: 'Детали', getSortValue: (row) => detailsRu(row.type, row.meta, row.items_count) },
  { id: 'error_message', title: 'Ошибка' },
]

export default function LogsPage() {
  const [logs, setLogs] = useState<LogRow[]>([])
  const [sortState, setSortState] = useState<LogSortState>(null)

  async function load() {
    const resp = await window.api.getSyncLog()
    setLogs(resp.logs as any)
  }

  useEffect(() => { load() }, [])
  useEffect(() => {
    const onUpdated = () => load()
    window.addEventListener('ozon:logs-updated', onUpdated)
    return () => window.removeEventListener('ozon:logs-updated', onUpdated)
  }, [])

  const sortedLogs = useMemo(() => sortTableRows(logs, LOG_COLUMNS, sortState), [logs, sortState])

  function toggleSort(colId: LogSortCol) {
    const column = LOG_COLUMNS.find((item) => item.id === colId)
    if (!column || column.sortable === false) return
    setSortState((prev) => toggleTableSort(prev, colId, column.sortable !== false))
  }

  function renderSortHeader(label: string, colId: LogSortCol) {
    const isSorted = sortState?.colId === colId
    return (
      <button
        type="button"
        onClick={() => toggleSort(colId)}
        title={getSortButtonTitle(isSorted, sortState?.dir)}
        style={{ display: 'inline-flex', alignItems: 'center', gap: 6, width: '100%', padding: 0, border: 'none', background: 'transparent', color: 'inherit', font: 'inherit', fontWeight: 'inherit', cursor: 'pointer', textAlign: 'left' }}
      >
        <span style={{ flex: '1 1 auto', minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis' }}>{label}</span>
        {isSorted && <span aria-hidden="true" style={{ fontSize: 10, opacity: 0.72, flex: '0 0 auto' }}>{sortState?.dir === 'asc' ? '▲' : '▼'}</span>}
      </button>
    )
  }

  return (
    <div className="card logsCard">
      <table className="table">
        <thead>
          <tr>{LOG_COLUMNS.map((column) => <th key={column.id}>{renderSortHeader(column.title, column.id)}</th>)}</tr>
        </thead>
        <tbody>
          {sortedLogs.map((l) => (
            <tr key={l.id}>
              <td><div className="cellText" title={String(l.id)}>{l.id}</div></td>
              <td><div className="cellText" title={typeRu(l.type)}>{typeRu(l.type)}</div></td>
              <td><div className="cellText" title={statusRu(l.status)}><span className={`statusText ${l.status ?? ''}`.trim()}>{statusRu(l.status)}</span></div></td>
              <td className="small"><div className="cellText" title={fmtDt(l.started_at)}>{fmtDt(l.started_at)}</div></td>
              <td className="small"><div className="cellText" title={fmtDt(l.finished_at)}>{fmtDt(l.finished_at)}</div></td>
              <td className="small"><div className="cellText" title={detailsRu(l.type, l.meta, l.items_count)}>{detailsRu(l.type, l.meta, l.items_count)}</div></td>
              <td className="small"><div className="cellText" title={l.error_message ?? '-'}>{l.error_message ?? '-'}</div></td>
            </tr>
          ))}
          {sortedLogs.length === 0 && <tr><td colSpan={7} className="small">Пока нет записей.</td></tr>}
        </tbody>
      </table>
    </div>
  )
}
