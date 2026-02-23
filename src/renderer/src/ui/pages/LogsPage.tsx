import React, { useEffect, useState } from 'react'

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

// Формат: дд.мм.гг чч:мм:сс (локальное время компьютера)
function fmtDt(iso?: string | null) {
  if (!iso) return '-'
  const d = new Date(iso)
  const pad = (n: number) => String(n).padStart(2, '0')
  const dd = pad(d.getDate())
  const mm = pad(d.getMonth() + 1)
  const yy = pad(d.getFullYear() % 100)
  const hh = pad(d.getHours())
  const mi = pad(d.getMinutes())
  const ss = pad(d.getSeconds())
  return `${dd}.${mm}.${yy} ${hh}:${mi}:${ss}`
}

function detailsRu(meta?: string | null): string {
  if (!meta) return '-'
  try {
    const m = JSON.parse(meta)

    // Новый формат: показываем только обновлено + новых
    if (typeof m?.updated === 'number' || typeof m?.added === 'number') {
      const upd = (typeof m?.updated === 'number') ? m.updated : 0
      const add = (typeof m?.added === 'number') ? m.added : 0
      return `обновлено: ${upd}, новых: ${add}`
    }

    if (typeof m?.logRetentionDays === 'number') {
      return `жизнь лога: ${m.logRetentionDays} дн.`
    }

    if (m?.appVersion || m?.previousVersion) {
      const parts: string[] = []
      if (m?.appVersion) parts.push(`версия: ${m.appVersion}`)
      if (m?.previousVersion) parts.push(`было: ${m.previousVersion}`)
      return parts.join(', ') || meta
    }

    // Старый формат — fallback
    const parts: string[] = []
    if (typeof m?.pages === 'number') parts.push(`страниц: ${m.pages}`)
    if (typeof m?.infoBatches === 'number') parts.push(`батчей: ${m.infoBatches}`)
    if (typeof m?.infoFetched === 'number') parts.push(`расширено: ${m.infoFetched}`)
    return parts.length ? parts.join(', ') : meta
  } catch {
    return meta
  }
}

export default function LogsPage() {
  const [logs, setLogs] = useState<LogRow[]>([])

  async function load() {
    const resp = await window.api.getSyncLog()
    setLogs(resp.logs as any)
  }

  useEffect(() => {
    load()
  }, [])

  // После синхронизации (иконка в шапке) перезагружаем лог
  useEffect(() => {
    const onUpdated = () => load()
    window.addEventListener('ozon:logs-updated', onUpdated)
    return () => window.removeEventListener('ozon:logs-updated', onUpdated)
  }, [])

  return (
    <div className="card logsCard">
      <table className="table">
        <thead>
          <tr>
            <th></th>
            <th>Тип</th>
            <th>Статус</th>
            <th>Старт</th>
            <th>Финиш</th>
            <th>Детали</th>
            <th>Ошибка</th>
          </tr>
        </thead>
        <tbody>
          {logs.map(l => (
            <tr key={l.id}>
              <td><div className="cellText" title={String(l.id)}>{l.id}</div></td>
              <td><div className="cellText" title={typeRu(l.type)}>{typeRu(l.type)}</div></td>
              <td>
                <div className="cellText" title={statusRu(l.status)}>
                  <span className={`statusText ${l.status ?? ''}`.trim()}>{statusRu(l.status)}</span>
                </div>
              </td>
              <td className="small"><div className="cellText" title={fmtDt(l.started_at)}>{fmtDt(l.started_at)}</div></td>
              <td className="small"><div className="cellText" title={fmtDt(l.finished_at)}>{fmtDt(l.finished_at)}</div></td>
              <td className="small"><div className="cellText" title={detailsRu(l.meta)}>{detailsRu(l.meta)}</div></td>
              <td className="small"><div className="cellText" title={l.error_message ?? '-'}>{l.error_message ?? '-'}</div></td>
            </tr>
          ))}
          {logs.length === 0 && <tr><td colSpan={7} className="small">Пока нет записей.</td></tr>}
        </tbody>
      </table>
    </div>
  )
}
