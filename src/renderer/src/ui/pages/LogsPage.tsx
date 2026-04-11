import React, { useEffect, useMemo, useState } from 'react'
import { formatTemporalCellRu } from '../utils/dateTime'
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
type LogGroup = { row: LogRow; children: LogRow[] }

const TYPE_RU: Record<string, string> = {
  sync_products: 'Синхронизация',
  check_auth: 'Проверка доступа',
  app_install: 'Установка программы',
  app_update: 'Обновление программы',
  app_reinstall: 'Переустановка программы',
  app_uninstall: 'Удаление программы',
  admin_settings: 'Настройки Админ',
  sales_fbo_shipment_trace: 'Трассировка FBO даты отгрузки',
}

const STATUS_RU: Record<string, string> = {
  pending: 'Ожидает',
  running: 'В процессе',
  success: 'Успешно',
  error: 'Ошибка',
}

const TRACE_TYPES = new Set(['sales_fbo_shipment_trace'])
const TRACE_PARENT_TYPES = new Set(['sync_products'])

function typeRu(v?: string | null) {
  if (!v) return '-'
  return TYPE_RU[v] ?? v
}

function statusRu(v?: string | null) {
  if (!v) return '-'
  return STATUS_RU[v] ?? v
}

function fmtDt(colId: 'started_at' | 'finished_at', value?: string | null) {
  if (!value) return '-'
  return formatTemporalCellRu(colId, value) || '-'
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
    if (type === 'sales_fbo_shipment_trace') {
      const parts: string[] = []
      const stage = String(m?.stage ?? '')
      if (m?.stageRu || m?.stage) parts.push(String(m?.stageRu ?? m?.stage))
      if (typeof m?.fboPostingCount === 'number') parts.push(`FBO отправлений: ${m.fboPostingCount}`)
      if (typeof m?.mergedFboDetailCount === 'number') parts.push(`деталей FBO: ${m.mergedFboDetailCount}`)
      if (typeof m?.compatLoadedCount === 'number') parts.push(`compat: ${m.compatLoadedCount}`)
      if (typeof m?.fboRowsWithShipmentDate === 'number') parts.push(`строк с датой: ${m.fboRowsWithShipmentDate}`)
      if (typeof m?.salesRowsCount === 'number') parts.push(`строк продаж: ${m.salesRowsCount}`)
      if (typeof m?.reportRowsCount === 'number') parts.push(`строк отчёта: ${m.reportRowsCount}`)
      if (typeof m?.reportStrategy === 'string' && m.reportStrategy) {
        const strategyLabel = m.reportStrategy === 'chunked-1d'
          ? 'отчёт по дням'
          : (m.reportStrategy === 'chunked-7d' ? 'отчёт по неделям' : 'один отчёт')
        parts.push(`стратегия: ${strategyLabel}`)
      }
      if (typeof m?.reportSegmentsTotal === 'number') parts.push(`сегментов: ${m.reportSegmentsTotal}`)
      if (typeof m?.reportSegmentsSucceeded === 'number') parts.push(`успешно: ${m.reportSegmentsSucceeded}`)
      if (typeof m?.reportSegmentsFailed === 'number') parts.push(`ошибок сегм.: ${m.reportSegmentsFailed}`)
      if (typeof m?.incomingEventsCount === 'number') parts.push(`push событий: ${m.incomingEventsCount}`)
      if (typeof m?.acceptedPushEventCount === 'number') parts.push(`push принято: ${m.acceptedPushEventCount}`)
      const samplePostingNumbers = Array.isArray(m?.samplePostingNumbers) ? m.samplePostingNumbers : []
      if (samplePostingNumbers.length > 0) {
        const label = stage.startsWith('push.ingest') ? 'push sample' : 'FBO sample'
        parts.push(`${label}: ${samplePostingNumbers.slice(0, 3).join(', ')}`)
      }
      if (stage === 'webhook.server.status') {
        if (typeof m?.baseUrl === 'string' && m.baseUrl) parts.push(`сервер: ${m.baseUrl}`)
        if (typeof m?.webhookUrlLocal === 'string' && m.webhookUrlLocal) parts.push(`webhook: ${m.webhookUrlLocal}`)
        if (typeof m?.webhookProbeUrlLocal === 'string' && m.webhookProbeUrlLocal) parts.push(`ping: ${m.webhookProbeUrlLocal}`)
      }
      if (stage === 'webhook.probe.received') {
        if (typeof m?.probeAt === 'string' && m.probeAt) parts.push(`время ping: ${m.probeAt}`)
        if (typeof m?.pathname === 'string' && m.pathname) parts.push(`путь: ${m.pathname}`)
      }
      if (Array.isArray(m?.payloadTopLevelKeys) && m.payloadTopLevelKeys.length > 0) parts.push(`payload keys: ${m.payloadTopLevelKeys.slice(0, 6).join(', ')}`)
      if (m?.trace && typeof m.trace === 'object') {
        if (typeof m.trace?.postingsWithDetail === 'number') parts.push(`с деталями: ${m.trace.postingsWithDetail}`)
        if (typeof m.trace?.postingsWithShipmentTransferEvent === 'number') parts.push(`с event даты: ${m.trace.postingsWithShipmentTransferEvent}`)
        if (typeof m.trace?.postingsWithResolvedShipmentDate === 'number') parts.push(`дата извлечена: ${m.trace.postingsWithResolvedShipmentDate}`)
      }
      if (m?.persisted && typeof m.persisted === 'object') {
        if (typeof m.persisted?.shipmentTransferEventCount === 'number') parts.push(`event в БД: ${m.persisted.shipmentTransferEventCount}`)
        if (typeof m.persisted?.shipmentDateCount === 'number') parts.push(`дат в БД: ${m.persisted.shipmentDateCount}`)
      }
      const failedSegments = Array.isArray(m?.failedSegmentSample) ? m.failedSegmentSample : []
      if (failedSegments.length > 0) {
        const sample = failedSegments
          .slice(0, 2)
          .map((item: any) => {
            const label = typeof item?.label === 'string' ? item.label : ''
            const error = typeof item?.error === 'string' ? item.error : ''
            return [label, error].filter(Boolean).join(' → ')
          })
          .filter(Boolean)
        if (sample.length > 0) parts.push(`fail: ${sample.join(' | ')}`)
      }
      if (typeof m?.reportBuildError === 'string' && m.reportBuildError) parts.push(`ошибка отчёта: ${m.reportBuildError}`)
      const missing = Array.isArray(m?.trace?.missingShipmentDatePostingNumbers) ? m.trace.missingShipmentDatePostingNumbers : []
      if (missing.length > 0) parts.push(`без даты: ${missing.slice(0, 3).join(', ')}`)
      const missingDetails = Array.isArray(m?.trace?.missingDetailPostingNumbers) ? m.trace.missingDetailPostingNumbers : []
      if (missingDetails.length > 0) parts.push(`без detail: ${missingDetails.slice(0, 3).join(', ')}`)
      return parts.length ? parts.join(', ') : meta
    }
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

function getTraceParentId(traceRow: LogRow, syncRows: LogRow[]): number | null {
  const traceTime = toSortTimestamp(traceRow.started_at) ?? toSortTimestamp(traceRow.finished_at)
  let fallbackParentId: number | null = null

  for (const syncRow of syncRows) {
    if (syncRow.id >= traceRow.id) break
    fallbackParentId = syncRow.id

    const syncStart = toSortTimestamp(syncRow.started_at)
    if (traceTime == null || syncStart == null || traceTime < syncStart) continue

    const syncFinishRaw = toSortTimestamp(syncRow.finished_at)
    const syncFinish = syncFinishRaw == null ? Number.POSITIVE_INFINITY : (syncFinishRaw + 60_000)
    if (traceTime <= syncFinish) return syncRow.id
  }

  return fallbackParentId
}

function buildLogGroups(rows: LogRow[]): LogGroup[] {
  const topLevelRows = rows.filter((row) => !TRACE_TYPES.has(row.type))
  const childMap = new Map<number, LogRow[]>()
  const syncRows = topLevelRows
    .filter((row) => TRACE_PARENT_TYPES.has(row.type))
    .slice()
    .sort((a, b) => a.id - b.id)

  const traceRows = rows
    .filter((row) => TRACE_TYPES.has(row.type))
    .slice()
    .sort((a, b) => {
      const timeDiff = (toSortTimestamp(a.started_at) ?? 0) - (toSortTimestamp(b.started_at) ?? 0)
      if (timeDiff !== 0) return timeDiff
      return a.id - b.id
    })

  for (const traceRow of traceRows) {
    const parentId = getTraceParentId(traceRow, syncRows)
    if (parentId == null) continue
    const bucket = childMap.get(parentId) ?? []
    bucket.push(traceRow)
    childMap.set(parentId, bucket)
  }

  return topLevelRows.map((row) => ({
    row,
    children: (childMap.get(row.id) ?? []).slice().sort((a, b) => {
      const timeDiff = (toSortTimestamp(a.started_at) ?? 0) - (toSortTimestamp(b.started_at) ?? 0)
      if (timeDiff !== 0) return timeDiff
      return a.id - b.id
    }),
  }))
}

function hasExpandableDetails(group: LogGroup): boolean {
  return TRACE_PARENT_TYPES.has(group.row.type) && group.children.length > 0
}

function getMainRowDetails(row: LogRow, traceCount: number): string {
  const base = detailsRu(row.type, row.meta, row.items_count)
  if (!traceCount) return base
  if (base && base !== '-') return `${base}, деталей синхронизации: ${traceCount}`
  return `деталей синхронизации: ${traceCount}`
}

function escapeTxtLine(value?: string | null): string {
  return String(value ?? '')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .trim()
}

function formatFileStamp(value?: string | null): string {
  const date = value ? new Date(value) : new Date()
  if (Number.isNaN(date.getTime())) return '00.00.00 00：00：00'
  const dd = String(date.getDate()).padStart(2, '0')
  const mm = String(date.getMonth() + 1).padStart(2, '0')
  const yy = String(date.getFullYear()).slice(-2)
  const hh = String(date.getHours()).padStart(2, '0')
  const min = String(date.getMinutes()).padStart(2, '0')
  const ss = String(date.getSeconds()).padStart(2, '0')
  return `${dd}.${mm}.${yy} ${hh}：${min}：${ss}`
}

function buildSyncReportText(group: LogGroup): string {
  const { row, children } = group
  const lines: string[] = [
    'Синхронизация',
    '',
    `ID: ${row.id}`,
    `Статус: ${statusRu(row.status)}`,
    `Старт: ${fmtDt('started_at', row.started_at)}`,
    `Финиш: ${fmtDt('finished_at', row.finished_at)}`,
    `Детали: ${getMainRowDetails(row, children.length)}`,
    `Ошибка: ${row.error_message ?? '-'}`,
  ]

  if (row.error_details) {
    lines.push('')
    lines.push('Технические детали ошибки:')
    lines.push(escapeTxtLine(row.error_details) || '-')
  }

  lines.push('')
  lines.push('Детали синхронизации:')

  if (children.length === 0) {
    lines.push('— Нет вложенных деталей.')
  } else {
    for (const child of children) {
      lines.push('')
      lines.push(`[${fmtDt('started_at', child.started_at)}] ${statusRu(child.status)}`)
      lines.push(`ID: ${child.id}`)
      lines.push(`Трассировка: ${detailsRu(child.type, child.meta, child.items_count)}`)
      lines.push(`Ошибка: ${child.error_message ?? '-'}`)
      if (child.error_details) {
        lines.push(`Технические детали: ${escapeTxtLine(child.error_details) || '-'}`)
      }
    }
  }

  return `${lines.join('\n').trim()}\n`
}

async function downloadSyncReport(group: LogGroup): Promise<void> {
  const fileName = `Синхронизация ${formatFileStamp(group.row.started_at)}.txt`
  const content = buildSyncReportText(group)
  const resp = await window.api.saveLogReportToDesktop(fileName, content)
  if (!resp?.ok) {
    throw new Error(resp?.error || 'Не удалось сохранить отчёт на Рабочий стол')
  }
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
  const [expandedIds, setExpandedIds] = useState<number[]>([])

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

  const groups = useMemo(() => buildLogGroups(logs), [logs])
  const sortedTopLevelRows = useMemo(
    () => sortTableRows(groups.map((group) => group.row), LOG_COLUMNS, sortState),
    [groups, sortState],
  )

  const sortedGroups = useMemo(() => {
    const byId = new Map(groups.map((group) => [group.row.id, group]))
    return sortedTopLevelRows
      .map((row) => byId.get(row.id))
      .filter((group): group is LogGroup => Boolean(group))
  }, [groups, sortedTopLevelRows])

  useEffect(() => {
    setExpandedIds((prev) => prev.filter((id) => sortedGroups.some((group) => group.row.id === id && hasExpandableDetails(group))))
  }, [sortedGroups])

  function toggleSort(colId: LogSortCol) {
    const column = LOG_COLUMNS.find((item) => item.id === colId)
    if (!column || column.sortable === false) return
    setSortState((prev) => toggleTableSort(prev, colId, column.sortable !== false))
  }

  function toggleExpanded(id: number) {
    setExpandedIds((prev) => (prev.includes(id) ? prev.filter((value) => value !== id) : [...prev, id]))
  }

  function handleTextAction(event: React.KeyboardEvent<HTMLElement>, action: () => void) {
  if (event.key === 'Enter' || event.key === ' ') {
    event.preventDefault()
    action()
  }
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
        <span className="tableHeaderLabel" data-table-header-label="true" style={{ flex: '1 1 auto', minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis' }}>{label}</span>
        <span aria-hidden="true" style={{ flex: '0 0 auto', opacity: isSorted ? 1 : 0.4 }}>{isSorted ? (sortState?.dir === 'asc' ? '▲' : '▼') : '↕'}</span>
      </button>
    )
  }

  return (
    <div className="card logsCard">
      <div className="tableWrap logsTableWrap">
        <div className="tableWrapX">
          <div className="tableWrapY">
            <table className="table logsTable">
              <colgroup>
                <col className="logsColId" />
                <col className="logsColType" />
                <col className="logsColStatus" />
                <col className="logsColStart" />
                <col className="logsColFinish" />
                <col className="logsColDetails" />
                <col className="logsColError" />
              </colgroup>
              <thead>
                <tr>{LOG_COLUMNS.map((column) => <th key={column.id}>{renderSortHeader(column.title, column.id)}</th>)}</tr>
              </thead>
              <tbody>
                {sortedGroups.map((group) => {
                  const { row, children } = group
                  const expandable = hasExpandableDetails(group)
                  const expanded = expandedIds.includes(row.id)
                  const mainDetails = getMainRowDetails(row, children.length)

                  return (
                    <React.Fragment key={row.id}>
                      <tr className={expandable ? 'logMainRow logMainRowExpandable' : 'logMainRow'}>
                        <td><div className="logCellWrap" title={String(row.id)}>{row.id}</div></td>
                        <td>
                          <div className="logTypeCell">
                            {expandable ? (
                              <span
                                role="button"
                                tabIndex={0}
                                className={expanded ? 'logTypeText logTypeTextExpandable expanded' : 'logTypeText logTypeTextExpandable'}
                                onClick={() => toggleExpanded(row.id)}
                                onKeyDown={(event) => handleTextAction(event, () => toggleExpanded(row.id))}
                                aria-expanded={expanded}
                                aria-label={expanded ? 'Свернуть детали синхронизации' : 'Показать детали синхронизации'}
                                title={expanded ? 'Свернуть детали синхронизации' : 'Показать детали синхронизации'}
                              >
                                {typeRu(row.type)}
                              </span>
                            ) : (
                              <span className="logTypeText" title={typeRu(row.type)}>{typeRu(row.type)}</span>
                            )}
                          </div>
                        </td>
                        <td><div className="logCellWrap"><span className={`statusText ${row.status ?? ''}`.trim()}>{statusRu(row.status)}</span></div></td>
                        <td><div className="logCellWrap" title={fmtDt('started_at', row.started_at)}>{fmtDt('started_at', row.started_at)}</div></td>
                        <td><div className="logCellWrap" title={fmtDt('finished_at', row.finished_at)}>{fmtDt('finished_at', row.finished_at)}</div></td>
                        <td><div className="logCellWrap" title={mainDetails}>{mainDetails}</div></td>
                        <td><div className="logCellWrap" title={row.error_message ?? '-'}>{row.error_message ?? '-'}</div></td>
                      </tr>

                      {expandable && expanded && (
                        <tr className="logDetailsRow">
                          <td colSpan={7}>
                            <div className="logDetailsPanel">
                              <div className="logDetailsToolbar">
                                <div className="logDetailsTitle">Детали синхронизации</div>
                                <span
                                  role="button"
                                  tabIndex={0}
                                  className="logActionText"
                                  onClick={() => {
                                    void downloadSyncReport(group).catch((error: any) => {
                                      window.alert(error?.message ?? 'Не удалось сохранить отчёт на Рабочий стол')
                                    })
                                  }}
                                  onKeyDown={(event) => handleTextAction(event, () => {
                                    void downloadSyncReport(group).catch((error: any) => {
                                      window.alert(error?.message ?? 'Не удалось сохранить отчёт на Рабочий стол')
                                    })
                                  })}
                                  aria-label="Скачать отчёт по синхронизации"
                                  title="Скачать отчёт по синхронизации"
                                >
                                  Скачать отчёт
                                </span>
                              </div>

                              <div className="logDetailsSummary">
                                <div><span>Старт:</span> {fmtDt('started_at', row.started_at)}</div>
                                <div><span>Финиш:</span> {fmtDt('finished_at', row.finished_at)}</div>
                                <div><span>Статус:</span> {statusRu(row.status)}</div>
                                <div><span>Записей:</span> {children.length}</div>
                              </div>

                              <div className="logTraceList">
                                {children.map((child) => {
                                  const childDetails = detailsRu(child.type, child.meta, child.items_count)
                                  return (
                                    <div key={child.id} className="logTraceItem">
                                      <div className="logTraceMeta">
                                        <span className="logTraceTime">{fmtDt('started_at', child.started_at)}</span>
                                        <span className={`statusText ${child.status ?? ''}`.trim()}>{statusRu(child.status)}</span>
                                        <span className="logTraceId">ID {child.id}</span>
                                      </div>
                                      <div className="logTraceText">{childDetails}</div>
                                      {child.error_message && child.error_message !== '-' && (
                                        <div className="logTraceError">Ошибка: {child.error_message}</div>
                                      )}
                                    </div>
                                  )
                                })}
                              </div>
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  )
                })}
                {sortedGroups.length === 0 && <tr><td colSpan={7} className="small">Пока нет записей.</td></tr>}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}
