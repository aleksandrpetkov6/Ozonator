import React, { useEffect, useState } from 'react'

function fmt(dtIso: string): string {
  const d = new Date(dtIso)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${pad(d.getDate())}.${pad(d.getMonth() + 1)}.${String(d.getFullYear()).slice(-2)} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

const TYPE_RU: Record<string, string> = {
  check_auth: 'Проверка ключей',
  sync_products: 'Синхронизация товаров',
  app_update: 'Обновление приложения',
}

export default function LogsPage() {
  const [rows, setRows] = useState<any[]>([])

  const load = async () => {
    const r = await window.api.getSyncLog()
    setRows(Array.isArray(r) ? r : [])
  }

  useEffect(() => {
    load()
  }, [])

  const clear = async () => {
    await window.api.clearLogs()
    await load()
  }

  return (
    <div className="page">
      <div className="row">
        <div className="badge">Лог</div>
        <button className="btn" onClick={load}>
          Обновить
        </button>
        <button className="btn" onClick={clear}>
          Очистить
        </button>
        <div className="badge">Записей: {rows.length}</div>
      </div>

      <div className="logList">
        {rows.map((r) => (
          <div className="logItem" key={r.id}>
            <div className="logTop">
              <div className="logType">{TYPE_RU[r.type] ?? r.type}</div>
              <div className="logMeta">
                {fmt(r.created_at)}{r.version ? ` • v${r.version}` : ''} • {r.status}
              </div>
            </div>
            {r.message ? <div className="logMsg">{r.message}</div> : null}
          </div>
        ))}
      </div>
    </div>
  )
}
