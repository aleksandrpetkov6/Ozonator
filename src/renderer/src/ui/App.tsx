import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import SettingsPage from './pages/SettingsPage'
import ProductsPage from './pages/ProductsPage'
import LogsPage from './pages/LogsPage'

const baseTitle = 'Озонатор'
const STORE_NAME_LS_KEY = 'ozonator_store_name'

const ProductsPageMemo = React.memo(ProductsPage)

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
  useEffect(() => {
    runningRef.current = running
  }, [running])

  const [lastError, setLastError] = useState<string | null>(null)

  const [storeName, setStoreName] = useState<string>('')
  const [productsQuery, setProductsQuery] = useState('')
  const [productsTotal, setProductsTotal] = useState(0)
  const [productsFiltered, setProductsFiltered] = useState(0)

  const pathname = location.pathname || '/'
  const isProducts = !pathname.startsWith('/logs') && !pathname.startsWith('/settings')

  const onProductStats = useCallback((s: { total: number; filtered: number }) => {
    setProductsTotal(s.total)
    setProductsFiltered(s.filtered)
  }, [])

  async function refreshStoreName() {
    // 1) Пробуем secrets
    try {
      const resp = await window.api.loadSecrets()
      if (resp.ok) {
        const raw = (resp.secrets as any).storeName
        const cleaned = typeof raw === 'string' && raw.trim() ? raw.trim() : ''
        if (cleaned) {
          setStoreName(cleaned)
          try {
            localStorage.setItem(STORE_NAME_LS_KEY, cleaned)
          } catch {
            /* ignore */
          }
          document.title = `${baseTitle} 🤝 ${cleaned}`
          return
        }
      }
    } catch {
      // ignore
    }

    // 2) Fallback: localStorage (если storeName не сохраняется в secrets)
    try {
      const raw = localStorage.getItem(STORE_NAME_LS_KEY) ?? ''
      const cleaned = raw.trim()
      if (cleaned) {
        setStoreName(cleaned)
        document.title = `${baseTitle} 🤝 ${cleaned}`
        return
      }
    } catch {
      // ignore
    }

    setStoreName('')
    document.title = baseTitle
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
          <div className="topbarSlot">
            <div className="segmented" aria-label="Навигация">
              <NavLink to="/products">
                <span>Товары</span>
                <span className="segCount">Всего: {productsTotal}</span>
              </NavLink>
            </div>

            {isProducts && (
              <div className="topbarSearch">
                <div className="searchWrap">
                  <input
                    className="searchInput search"
                    value={productsQuery}
                    onChange={(e) => setProductsQuery(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Escape') {
                        e.preventDefault()
                        e.stopPropagation()
                        setProductsQuery('')
                      }
                    }}
                    placeholder="Поиск по таблице…"
                  />
                  {productsQuery && (
                    <button
                      type="button"
                      className="searchClearBtn"
                      title="Очистить"
                      aria-label="Очистить"
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => setProductsQuery('')}
                    >
                      ×
                    </button>
                  )}
                </div>
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
        <div className={isProducts ? 'container containerWide' : 'container'}>
          {lastError && <div className="notice error">{lastError}</div>}

          {/* Страницы держим смонтированными — переключение без перезагрузки тяжёлой таблицы */}
          <div style={{ display: isProducts ? 'block' : 'none', height: '100%' }}>
            <ProductsPageMemo query={productsQuery} onStats={onProductStats} />
          </div>

          <div style={{ display: pathname.startsWith('/logs') ? 'block' : 'none', height: '100%' }}>
            <LogsPage />
          </div>

          <div style={{ display: pathname.startsWith('/settings') ? 'block' : 'none', height: '100%' }}>
            <SettingsPage />
          </div>

          {/* пока не показываем filtered рядом, но оставили стейт под быстрые итерации */}
          {productsFiltered /* noop */ && false}
        </div>
      </div>
    </div>
  )
}
