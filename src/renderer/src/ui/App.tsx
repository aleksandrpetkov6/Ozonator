import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import SettingsPage from './pages/SettingsPage'
import ProductsPage from './pages/ProductsPage'
import LogsPage from './pages/LogsPage'
import AdminPage from './pages/AdminPage'
import { formatDateTimeRu } from './utils/dateTime'
import { DEFAULT_UI_DATE_RANGE_DAYS, UI_DATE_RANGE_LS_KEY, getDefaultDateRange, readDateRangeWithDefault, sanitizeDateInput, type UiDateRange } from './utils/dateRangeDefaults'
import { useGlobalTableEnhancements } from './utils/tableEnhancements'

const baseTitle = 'Озонатор'
const STORE_NAME_LS_KEY = 'ozonator_store_name'
const DEMAND_FORECAST_PERIOD_LS_KEY = UI_DATE_RANGE_LS_KEY
const AUTO_SYNC_ENABLED_LS_KEY = 'ozonator_auto_sync_enabled'

type DemandForecastPeriod = UiDateRange

const DEMAND_PERIOD_PRESETS = [DEFAULT_UI_DATE_RANGE_DAYS, 90, 180, 365] as const


function toShortRuDate(value: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return ''
  const [, m, d] = value.split('-')
  return `${d}.${m}`
}

function formatPeriodBoundary(value: string, boundary: 'startOfDay' | 'endOfDay'): string {
  const formatted = formatDateTimeRu(value, { dateOnlyBoundary: boundary })
  return formatted || ''
}

function readDemandForecastPeriod(): DemandForecastPeriod {
  return readDateRangeWithDefault(DEMAND_FORECAST_PERIOD_LS_KEY, DEFAULT_UI_DATE_RANGE_DAYS)
}

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
  useGlobalTableEnhancements()
  const location = useLocation()
  const online = useOnline()

  const [running, setRunning] = useState(false)
  const [autoSyncEnabled, setAutoSyncEnabled] = useState<boolean>(() => {
    try { return localStorage.getItem(AUTO_SYNC_ENABLED_LS_KEY) === '1' } catch { return false }
  })
  const runningRef = useRef(false)
  useEffect(() => {
    runningRef.current = running
  }, [running])

  const [lastError, setLastError] = useState<string | null>(null)

  const [storeName, setStoreName] = useState<string>('')
  const [productsQuery, setProductsQuery] = useState('')
  const [productsTotal, setProductsTotal] = useState(0)
  const [productsFiltered, setProductsFiltered] = useState(0)
  const [demandPeriod, setDemandPeriod] = useState<DemandForecastPeriod>(() => readDemandForecastPeriod())
  const [salesRefreshTick, setSalesRefreshTick] = useState(0)

  const [adminLoading, setAdminLoading] = useState(true)
  const [adminSaving, setAdminSaving] = useState(false)
  const [adminLogLifeDraft, setAdminLogLifeDraft] = useState('')
  const [adminLogLifeSaved, setAdminLogLifeSaved] = useState<number>(30)
  const [adminNotice, setAdminNotice] = useState<{ kind: 'success' | 'error'; text: string } | null>(null)
  const [datePresetOpen, setDatePresetOpen] = useState(false)
  const dateRangeRef = useRef<HTMLDivElement | null>(null)

  const pathname = location.pathname || '/'
  const isLogs = pathname.startsWith('/logs')
  const isSettings = pathname.startsWith('/settings')
  const isAdmin = pathname.startsWith('/admin')
  const isDemandForecast = pathname.startsWith('/forecast-demand')
  const isSales = pathname.startsWith('/sales')
  const isReturns = pathname.startsWith('/returns')
  const isStocks = pathname.startsWith('/stocks')
  const isProducts = !isLogs && !isSettings && !isAdmin && !isDemandForecast && !isSales && !isReturns && !isStocks
  const isDataGridTab = isProducts || isSales || isReturns || isStocks
  const isProductsLike = isDataGridTab || isDemandForecast

  const onProductStats = useCallback((s: { total: number; filtered: number }) => {
    setProductsTotal(s.total)
    setProductsFiltered(s.filtered)
  }, [])

  useEffect(() => {
    try {
      localStorage.setItem(DEMAND_FORECAST_PERIOD_LS_KEY, JSON.stringify(demandPeriod))
    } catch {
      // ignore
    }
  }, [demandPeriod])

  const setDemandPeriodField = useCallback((field: keyof DemandForecastPeriod, value: string) => {
    const normalized = sanitizeDateInput(value)
    setDemandPeriod((prev) => ({ ...prev, [field]: normalized }))
  }, [])

  const applyDemandPreset = useCallback((days: number) => {
    setDemandPeriod(getDefaultDateRange(days))
  }, [])

  const demandPresetDays = useMemo(() => {
    for (const days of DEMAND_PERIOD_PRESETS) {
      const preset = getDefaultDateRange(days)
      if (preset.from === demandPeriod.from && preset.to === demandPeriod.to) return days
    }
    return null
  }, [demandPeriod.from, demandPeriod.to])

  const dateTriggerLabel = useMemo(() => {
    const from = toShortRuDate(demandPeriod.from)
    const to = toShortRuDate(demandPeriod.to)
    if (from && to) return `${from}—${to}`
    if (from) return `с ${from}`
    if (to) return `по ${to}`
    return 'Указать промежуток'
  }, [demandPeriod.from, demandPeriod.to])

  const dateTriggerTitle = useMemo(() => {
    const from = formatPeriodBoundary(demandPeriod.from, 'startOfDay')
    const to = formatPeriodBoundary(demandPeriod.to, 'endOfDay')
    if (from && to) return `${from} — ${to}`
    if (from) return `с ${from}`
    if (to) return `по ${to}`
    return 'Указать промежуток'
  }, [demandPeriod.from, demandPeriod.to])

  useEffect(() => {
    setDatePresetOpen(false)
  }, [pathname])

  useEffect(() => {
    if (!datePresetOpen) return

    const onPointerDown = (ev: MouseEvent) => {
      const host = dateRangeRef.current
      if (!host) return
      if (host.contains(ev.target as Node)) return
      setDatePresetOpen(false)
    }

    const onEscape = (ev: KeyboardEvent) => {
      if (ev.key === 'Escape') setDatePresetOpen(false)
    }

    window.addEventListener('mousedown', onPointerDown)
    window.addEventListener('keydown', onEscape)

    return () => {
      window.removeEventListener('mousedown', onPointerDown)
      window.removeEventListener('keydown', onEscape)
    }
  }, [datePresetOpen])

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
        if (reason === 'manual' && !autoSyncEnabled) {
          setAutoSyncEnabled(true)
          try { localStorage.setItem(AUTO_SYNC_ENABLED_LS_KEY, '1') } catch {}
        }
        setLastError(null)
        setSalesRefreshTick((prev) => prev + 1)
        window.dispatchEvent(new Event('ozon:products-updated'))
        window.dispatchEvent(new Event('ozon:logs-updated'))
        window.dispatchEvent(new Event('ozon:store-updated'))
      }
    } finally {
      setRunning(false)
    }
  }

  useEffect(() => {
    if (!online || !autoSyncEnabled) return

    let cancelled = false

    async function runAuto() {
      if (cancelled || document.hidden) return
      try {
        const st = await window.api.secretsStatus()
        if (cancelled || !st.hasSecrets) return
        await syncNow('auto')
      } catch {
        // ignore
      }
    }

    const id = setInterval(runAuto, 60 * 60 * 1000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoSyncEnabled, online])

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
      window.dispatchEvent(new Event('ozon:logs-updated'))
    } catch (e: any) {
      setAdminNotice({ kind: 'error', text: e?.message ?? 'Не удалось сохранить настройки Админ' })
    } finally {
      setAdminSaving(false)
    }
  }, [adminLogLifeDraft])

  const adminParsed = parseLogLifeDays(adminLogLifeDraft)
  const adminDirty = adminParsed !== null ? adminParsed !== adminLogLifeSaved : adminLogLifeDraft.trim() !== String(adminLogLifeSaved)
  const visibleLastError = lastError && lastError !== 'Нет интернета' ? lastError : null

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

            {isSales ? (
              <div className="topbarDateChipHost topbarDateChipHostSales" ref={dateRangeRef} aria-label="Период продаж">
                <button
                  type="button"
                  className={`topbarDateTrigger${datePresetOpen ? ' open' : ''}`}
                  onClick={() => setDatePresetOpen((v) => !v)}
                  title={dateTriggerTitle}
                  aria-haspopup="dialog"
                  aria-expanded={datePresetOpen}
                >
                  <span className="topbarDateTriggerText">{dateTriggerLabel}</span>
                  <span className="topbarDateTriggerIcon" aria-hidden>📅</span>
                </button>

                {datePresetOpen && (
                  <div className="topbarDatePopover" role="dialog" aria-label="Период продаж">
                    <div className="topbarDatePopoverFields">
                      <label className="topbarDatePopoverField">
                        <span>С</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={demandPeriod.from}
                          onChange={(e) => setDemandPeriodField('from', e.target.value)}
                        />
                      </label>
                      <label className="topbarDatePopoverField">
                        <span>По</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={demandPeriod.to}
                          onChange={(e) => setDemandPeriodField('to', e.target.value)}
                        />
                      </label>
                    </div>

                    <div className="topbarDatePopoverPresets" role="menu" aria-label="Шаблоны периода">
                      {DEMAND_PERIOD_PRESETS.map((days) => (
                        <button
                          key={days}
                          type="button"
                          role="menuitem"
                          className={`topbarDatePresetBtn${demandPresetDays === days ? ' active' : ''}`}
                          onClick={() => {
                            applyDemandPreset(days)
                            setDatePresetOpen(false)
                          }}
                        >
                          {days}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <NavLink
                to="/sales"
                className={({ isActive }) => `navChip${isActive ? ' active' : ''}`}
                title="Продажи"
              >
                Продажи
              </NavLink>
            )}

            {isReturns ? (
              <div className="topbarDateChipHost topbarDateChipHostReturns" ref={dateRangeRef} aria-label="Период возвратов">
                <button
                  type="button"
                  className={`topbarDateTrigger${datePresetOpen ? ' open' : ''}`}
                  onClick={() => setDatePresetOpen((v) => !v)}
                  title={dateTriggerTitle}
                  aria-haspopup="dialog"
                  aria-expanded={datePresetOpen}
                >
                  <span className="topbarDateTriggerText">{dateTriggerLabel}</span>
                  <span className="topbarDateTriggerIcon" aria-hidden>📅</span>
                </button>

                {datePresetOpen && (
                  <div className="topbarDatePopover" role="dialog" aria-label="Период возвратов">
                    <div className="topbarDatePopoverFields">
                      <label className="topbarDatePopoverField">
                        <span>С</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={demandPeriod.from}
                          onChange={(e) => setDemandPeriodField('from', e.target.value)}
                        />
                      </label>
                      <label className="topbarDatePopoverField">
                        <span>По</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={demandPeriod.to}
                          onChange={(e) => setDemandPeriodField('to', e.target.value)}
                        />
                      </label>
                    </div>

                    <div className="topbarDatePopoverPresets" role="menu" aria-label="Шаблоны периода">
                      {DEMAND_PERIOD_PRESETS.map((days) => (
                        <button
                          key={days}
                          type="button"
                          role="menuitem"
                          className={`topbarDatePresetBtn${demandPresetDays === days ? ' active' : ''}`}
                          onClick={() => {
                            applyDemandPreset(days)
                            setDatePresetOpen(false)
                          }}
                        >
                          {days}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <NavLink
                to="/returns"
                className={({ isActive }) => `navChip${isActive ? ' active' : ''}`}
                title="Возвраты"
              >
                Возвраты
              </NavLink>
            )}

            {isDemandForecast ? (
              <div className="topbarDateChipHost topbarDateChipHostForecast" ref={dateRangeRef} aria-label="Период прогноза спроса">
                <button
                  type="button"
                  className={`topbarDateTrigger${datePresetOpen ? ' open' : ''}`}
                  onClick={() => setDatePresetOpen((v) => !v)}
                  title={dateTriggerTitle}
                  aria-haspopup="dialog"
                  aria-expanded={datePresetOpen}
                >
                  <span className="topbarDateTriggerText">{dateTriggerLabel}</span>
                  <span className="topbarDateTriggerIcon" aria-hidden>📅</span>
                </button>

                {datePresetOpen && (
                  <div className="topbarDatePopover" role="dialog" aria-label="Период прогноза спроса">
                    <div className="topbarDatePopoverFields">
                      <label className="topbarDatePopoverField">
                        <span>С</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={demandPeriod.from}
                          onChange={(e) => setDemandPeriodField('from', e.target.value)}
                        />
                      </label>
                      <label className="topbarDatePopoverField">
                        <span>По</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={demandPeriod.to}
                          onChange={(e) => setDemandPeriodField('to', e.target.value)}
                        />
                      </label>
                    </div>

                    <div className="topbarDatePopoverPresets" role="menu" aria-label="Шаблоны периода">
                      {DEMAND_PERIOD_PRESETS.map((days) => (
                        <button
                          key={days}
                          type="button"
                          role="menuitem"
                          className={`topbarDatePresetBtn${demandPresetDays === days ? ' active' : ''}`}
                          onClick={() => {
                            applyDemandPreset(days)
                            setDatePresetOpen(false)
                          }}
                        >
                          {days}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <NavLink
                to="/forecast-demand"
                className={({ isActive }) => `navChip${isActive ? ' active' : ''}`}
                title="Прогноз спроса"
              >
                Прогноз спроса
              </NavLink>
            )}

            <NavLink
              to="/stocks"
              className={({ isActive }) => `navChip${isActive ? ' active' : ''}`}
              title="Остатки"
            >
              Остатки
            </NavLink>

            {isProductsLike && (
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

            <NavLink className="iconLink" to="/admin" title="Админ">
              🛡️
            </NavLink>

            <button
              className={`iconBtn syncBtn ${running ? 'running' : ''}`}
              title={online ? (running ? 'Синхронизация…' : 'Синхронизировать сейчас') : 'Оффлайн'}
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
        <div className={isProductsLike ? 'container containerWide' : 'container'}>
          {visibleLastError && <div className="notice error">{visibleLastError}</div>}

          {isProducts && (
            <div style={{ height: '100%' }}>
              <ProductsPageMemo key="products" dataset="products" query={productsQuery} onStats={onProductStats} />
            </div>
          )}

          {isSales && (
            <div style={{ height: '100%' }}>
              <ProductsPageMemo key={`sales:${demandPeriod.from || "-"}:${demandPeriod.to || "-"}:${salesRefreshTick}`} dataset="sales" query={productsQuery} period={demandPeriod} onStats={onProductStats} />
            </div>
          )}

          {isReturns && (
            <div style={{ height: '100%' }}>
              <ProductsPageMemo key="returns" dataset="returns" query={productsQuery} onStats={onProductStats} />
            </div>
          )}

          {isStocks && (
            <div style={{ height: '100%' }}>
              <ProductsPageMemo key="stocks" dataset="stocks" query={productsQuery} onStats={onProductStats} />
            </div>
          )}

          {isDemandForecast && (
            <div style={{ height: '100%' }}>
              <ProductsPageMemo key="forecast-demand" dataset="products" query={productsQuery} onStats={onProductStats} />
            </div>
          )}

          {isLogs && (
            <div style={{ height: '100%' }}>
              <LogsPage />
            </div>
          )}

          {isAdmin && (
            <div style={{ height: '100%' }}>
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
          )}

          {isSettings && (
            <div style={{ height: '100%' }}>
              <SettingsPage />
            </div>
          )}

          {productsTotal /* noop */ && false}
          {productsFiltered /* noop */ && false}
        </div>
      </div>
    </div>
  )
}
