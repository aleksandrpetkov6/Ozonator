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

  const [lastError, setLastError] = useState<string | null>(null)

  const [storeName, setStoreName] = useState<string | null>(null)
  const [productsQuery, setProductsQuery] = useState('')
  const [productsTotal, setProductsTotal] = useState(0)
  const [productsFiltered, setProductsFiltered] = useState(0)

  const pathname = location.pathname || '/'
  const isProducts = pathname === '/' || pathname.startsWith('/products')

  async function refreshStoreName() {
    try {
      const resp = await window.api.loadSecrets()
      if (resp?.ok) {
        const name = (resp.secrets as any)?.storeName
        const cleaned = (typeof name === 'string' && name.trim()) ? name.trim() : null
        setStoreName(cleaned)
        document.title = cleaned ? `Озонатор — ${cleaned}` : 'Озонатор'
      }
    } catch {
      // ignore
    }
  }

  useEffect(() => {
    refreshStoreName()
    const onStore = () => refreshStoreName()
    window.addEventListener('ozon:store-updated', onStore)
    return () => window.removeEventListener('ozon:store-updated', onStore)
  }, [])

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
      if (reason === 'manual') setLastError('Ключи не сохранены. Откройте Настройки.')
      return
    }

    setRunning(true)

    try {
      const resp = await window.api.syncProducts()
      if (!resp.ok) {
        setLastError(resp.error ?? 'Ошибка синхронизации')
      } else {
        setLastError(null)

        // Обновляем список товаров + лог + имя магазина (если подтянулось)
        window.dispatchEvent(new Event('ozon:products-updated'))
        window.dispatchEvent(new Event('ozon:logs-updated'))
        window.dispatchEvent(new Event('ozon:store-updated'))
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
          <div className="appTitle" title={storeName ? `Подключен магазин: ${storeName}` : undefined}>
            <div className="appName">Озонатор</div>
            {storeName && <div className="appStoreName">{storeName}</div>}
          </div>

          <div className="topbarSlot">
            <div className="segmented" aria-label="Навигация">
              <NavLink to="/products">
                <span>Товары</span>
                <span className="segCount">Всего: {productsTotal}</span>
              </NavLink>
            </div>

            {isProducts && (
              <div className="topbarSearch">
                <input
                  className="searchInput search"
                  value={productsQuery}
                  onChange={(e) => setProductsQuery(e.target.value)}
                  placeholder="Поиск по таблице…"
                />
              </div>
            )}
          </div>

          <div className="topbarRight">
            <NavLink className="iconLink" to="/logs" title="Лог">
              🗒️
            </NavLink>

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

      <div className="pageArea">
        <div className={isProducts ? "container containerWide" : "container"}>
          {lastError && <div className="notice error">{lastError}</div>}

          <Routes>
            <Route
              path="/"
              element={
                <ProductsPage
                  query={productsQuery}
                  onStats={(s) => {
                    setProductsTotal(s.total)
                    setProductsFiltered(s.filtered)
                  }}
                />
              }
            />
            <Route
              path="/products"
              element={
                <ProductsPage
                  query={productsQuery}
                  onStats={(s) => {
                    setProductsTotal(s.total)
                    setProductsFiltered(s.filtered)
                  }}
                />
              }
            />
            <Route path="/logs" element={<LogsPage />} />
            <Route path="/settings" element={<SettingsPage />} />
          </Routes>

          {/* пока не показываем filtered рядом, но оставили стейт под быстрые итерации */}
          {productsFiltered /* noop */ && false}
        </div>
      </div>
    </div>
  )
}
