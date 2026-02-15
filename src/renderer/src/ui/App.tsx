import React, { useEffect, useMemo, useRef, useState } from 'react'
import { NavLink, Route, Routes, useLocation } from 'react-router-dom'
import SettingsPage from './pages/SettingsPage'
import ProductsPage from './pages/ProductsPage'
import LogsPage from './pages/LogsPage'

function useOnline() {
  const [online, setOnline] = useState<boolean>(true)

  async function check() {
    try {
      const r = await window.api.netCheck()
      setOnline(!!r.online)
    } catch {
      setOnline(false)
    }
  }

  useEffect(() => {
    check()
    const id = setInterval(check, 15000)
    return () => clearInterval(id)
  }, [])

  return online
}

export default function App() {
  const location = useLocation()
  const online = useOnline()

  const [running, setRunning] = useState(false)
  const runningRef = useRef(false)
  useEffect(() => { runningRef.current = running }, [running])

  const [lastOkAt, setLastOkAt] = useState<string | null>(null)
  const [lastError, setLastError] = useState<string | null>(null)

  const dotState = useMemo(() => {
    if (!online) return 'offline'
    if (running) return 'running'
    if (lastError) return 'error'
    return 'ok'
  }, [online, running, lastError])

  async function syncNow(reason: 'manual' | 'auto' = 'manual') {
    if (runningRef.current) return

    setLastError(null)

    // если офлайн — не пытаемся
    if (!online) {
      setLastError('Нет интернета')
      return
    }

    // ключи должны быть сохранены
    const st = await window.api.secretsStatus()
    if (!st.hasSecrets) {
      if (reason === 'manual') setLastError('Ключи не сохранены. Откройте Настройки → Магазин.')
      return
    }

    setRunning(true)

    try {
      const resp = await window.api.syncProducts()
      if (!resp.ok) {
        setLastError(resp.error ?? 'Ошибка синхронизации')
      } else {
        setLastOkAt(new Date().toISOString())
        setLastError(null)

        // Обновляем список товаров + лог
        window.dispatchEvent(new Event('ozon:products-updated'))
        window.dispatchEvent(new Event('ozon:logs-updated'))
      }
    } finally {
      setRunning(false)
    }
  }

  // Автосинхронизация: при запуске и затем каждый час
  useEffect(() => {
    let cancelled = false

    async function runAuto() {
      if (cancelled) return
      try {
        const st = await window.api.secretsStatus()
        if (st.hasSecrets) {
          await syncNow('auto')
        }
      } catch {
        // ignore
      }
    }

    runAuto()

    const id = setInterval(runAuto, 60 * 60 * 1000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [online])

  return (
    <div className="appShell">
      <div className="topbar">
        <div className="topbarInner">
          <NavLink className="appNameLink" to="/">Озонатор</NavLink>

          <div className="topbarSlot">
            <div className="segmented">
              <NavLink to="/products">Товары</NavLink>
              <NavLink to="/logs">Лог</NavLink>
            </div>
          </div>

          <div className="topbarRight">
            <NavLink className="iconLink" to="/settings" title="Настройки">
              ⚙️
            </NavLink>

            <button
              className={`iconBtn syncBtn ${running ? 'running' : ''}`}
              title={online ? (running ? 'Синхронизация…' : 'Синхронизировать сейчас') : 'Нет интернета'}
              onClick={() => syncNow('manual')}
              disabled={!online || running}
            >
              <span className={`syncBtnDot ${dotState}`} aria-hidden>
                {running ? <span className="syncSpinner" /> : <span className="syncCheck" />}
              </span>
            </button>
          </div>
        </div>
      </div>

      <div className={(() => {
        const p = location.pathname || '/'
        const isProducts = p === '/' || p.startsWith('/products')
        return isProducts ? 'container containerWide' : 'container'
      })()}>
        {lastError && <div className="notice error">{lastError}</div>}
        {!lastError && lastOkAt && (
          <div className="notice">
            Синхронизировано: {new Date(lastOkAt).toLocaleString()}
          </div>
        )}

        <Routes>
          <Route path="/" element={<ProductsPage />} />
          <Route path="/products" element={<ProductsPage />} />
          <Route path="/logs" element={<LogsPage />} />
          <Route path="/settings" element={<SettingsPage />} />
        </Routes>
      </div>
    </div>
  )
}
