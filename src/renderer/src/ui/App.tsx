import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import SettingsPage from './pages/SettingsPage'
import ProductsPage from './pages/ProductsPage'
import LogsPage from './pages/LogsPage'
import AdminPage from './pages/AdminPage'

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

function parseLogLifeDays(value: string): number | null {
  const trimmed = String(value ?? '').trim()
  if (!trimmed) return null
  const n = Number(trimmed)
  if (!Number.isFinite(n)) return null
  const i = Math.trunc(n)
  if (i <= 0) return null
  return i
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

  const [adminLoading, setAdminLoading] = useState(true)
  const [adminSaving, setAdminSaving] = useState(false)
  const [adminLogLifeDraft, setAdminLogLifeDraft] = useState('')
  const [adminLogLifeSaved, setAdminLogLifeSaved] = useState<number>(30)
  const [adminNotice, setAdminNotice] = useState<{ kind: 'success' | 'error'; text: string } | null>(null)

  const pathname = location.pathname || '/'
  const isLogs = pathname.startsWith('/logs')
  const isSettings = pathname.startsWith('/settings')
  const isAdmin = pathname.startsWith('/admin')
  const isProducts = !isLogs && !isSettings && !isAdmin

  const onProductStats = useCallback((s: { total: number; filtered: number }) => {
    setProductsTotal(s.total)
    setProductsFiltered(s.filtered)
  }, [])

  async function refreshStoreName() {
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

  useEffect(() => {
    let cancelled = false

    async function loadAdmin() {
      setAdminLoading(true)
      try {
        const resp = await window.api.getAdminSettings()
        if (cancelled) return
        if (!resp.ok) throw new Error(resp.error ?? 'Не удалось загрузить настройки Админ')

        const days = Math.max(1, Math.trunc(Number(resp.logRetentionDays) || 30))
        setAdminLogLifeSaved(days)
        setAdminLogLifeDraft(String(days))
        setAdminNotice(null)
      } catch (e: any) {
        if (cancelled) return
        setAdminNotice({ kind: 'error', text: e?.message ?? 'Не удалось загрузить настройки Админ' })
      } finally {
        if (!cancelled) setAdminLoading(false)
      }
    }

    loadAdmin()
    return () => {
      cancelled = true
    }
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

    if (!online) {
      setLastError('Нет интернета')
      return
    }

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
        window.dispatchEvent(new Event('ozon:products-updated'))
        window.dispatchEvent(new Event('ozon:logs-updated'))
        window.dispatchEvent(new Event('ozon:store-updated'))
      }
    } finally {
      setRunning(false)
    }
  }

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

  const saveAdmin = useCallback(async () => {
    const parsed = parseLogLifeDays(adminLogLifeDraft)
    if (!parsed) {
      setAdminNotice({ kind: 'error', text: 'Поле «Жизнь лога» должно быть целым числом больше 0.' })
      return
    }

    setAdminSaving(true)
    setAdminNotice(null)

    try {
      const resp = await window.api.saveAdminSettings({ logRetentionDays: parsed })
      if (!resp.ok) throw new Error(resp.error ?? 'Не удалось сохранить настройки Админ')

      const saved = Math.max(1, Math.trunc(Number(resp.logRetentionDays) || parsed))
      setAdminLogLifeSaved(saved)
      setAdminLogLifeDraft(String(saved))
      setAdminNotice({ kind: 'success', text: 'Настройки сохранены. Применено сразу.' })

      window.dispatchEvent(new Event('ozon:logs-updated'))
    } catch (e: any) {
      setAdminNotice({ kind: 'error', text: e?.message ?? 'Не удалось сохранить настройки Админ' })
    } finally {
      setAdminSaving(false)
    }
  }, [adminLogLifeDraft])

  const adminParsed = parseLogLifeDays(adminLogLifeDraft)
  const adminDirty = adminParsed !== null ? adminParsed !== adminLogLifeSaved : adminLogLifeDraft.trim() !== String(adminLogLifeSaved)

  return (
    <div className="appShell">
      <div className="topbar">
        <div className="topbarInner">
          <div className="topbarLeft">
            <NavLink
              end
              to="/"
              className={({ isActive }) => `navChip${isActive ? ' active' : ''}`}
              title="Товары"
            >
              Товары
            </NavLink>

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

            <NavLink className="iconLink" to="/admin" title="Админ">
              🛡️
            </NavLink>

            <NavLink className="iconLink" to="/settings" title="Настройки">
              ⚙️
            </NavLink>

            {isAdmin && (
              <button
                type="button"
                className={`topbarSaveBtn${adminDirty ? ' isDirty' : ''}`}
                onClick={saveAdmin}
                disabled={adminLoading || adminSaving}
                title={adminSaving ? 'Сохранение…' : 'Сохранить настройки Админ'}
              >
                {adminSaving ? 'Сохранение…' : 'Сохранить'}
              </button>
            )}

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

          <div style={{ display: isProducts ? 'block' : 'none', height: '100%' }}>
            <ProductsPageMemo query={productsQuery} onStats={onProductStats} />
          </div>

          <div style={{ display: isLogs ? 'block' : 'none', height: '100%' }}>
            <LogsPage />
          </div>

          <div style={{ display: isAdmin ? 'block' : 'none', height: '100%' }}>
            <AdminPage
              loading={adminLoading}
              saving={adminSaving}
              logLifeDaysValue={adminLogLifeDraft}
              onChangeLogLifeDays={(v) => {
                setAdminLogLifeDraft(v)
                if (adminNotice) setAdminNotice(null)
              }}
              notice={adminNotice}
              currentSavedDays={adminLogLifeSaved}
            />
          </div>

          <div style={{ display: isSettings ? 'block' : 'none', height: '100%' }}>
            <SettingsPage />
          </div>

          {productsTotal /* noop */ && false}
          {productsFiltered /* noop */ && false}
        </div>
      </div>
    </div>
  )
}
