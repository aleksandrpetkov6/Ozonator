import React, { useEffect, useMemo, useState } from 'react'
import ProductsPage from './pages/ProductsPage'
import LogsPage from './pages/LogsPage'
import SettingsPage from './pages/SettingsPage'
import './styles.css'

type Page = 'products' | 'logs' | 'settings'

export default function App() {
  const [page, setPage] = useState<Page>('products')
  const [storeName, setStoreName] = useState<string | null>(null)
  const [syncing, setSyncing] = useState(false)

  useEffect(() => {
    const load = async () => {
      try {
        const s = await window.api.loadSecrets()
        setStoreName(s.storeName ?? null)
      } catch {
        setStoreName(null)
      }
    }
    load()

    const onStore = (ev: any) => setStoreName(ev?.detail ?? null)
    window.addEventListener('ozonator-store-name', onStore as any)
    return () => window.removeEventListener('ozonator-store-name', onStore as any)
  }, [])

  const title = useMemo(() => {
    if (!storeName) return 'Озонатор'
    return `Озонатор • ${storeName}`
  }, [storeName])

  const syncNow = async () => {
    try {
      setSyncing(true)
      const res = await window.api.syncProducts()
      if (!res?.ok) {
        alert(res?.error || 'Ошибка синхронизации')
      }
    } catch (e: any) {
      alert(String(e?.message ?? e))
    } finally {
      setSyncing(false)
    }
  }

  return (
    <div className="app">
      <div className="header">
        <div className="brand" title={title}>
          <div className="brandTitle">Озонатор</div>
          {storeName ? <div className="brandStore">{storeName}</div> : null}
        </div>

        <div className="headerBtns">
          <button className={`iconBtn ${syncing ? 'iconBtnPrimary' : ''}`} onClick={syncNow} disabled={syncing} title="Синхронизация">
            ⟳
          </button>
          <button className={`iconBtn ${page === 'logs' ? 'iconBtnPrimary' : ''}`} onClick={() => setPage('logs')} title="Лог">
            ☰
          </button>
          <button className={`iconBtn ${page === 'settings' ? 'iconBtnPrimary' : ''}`} onClick={() => setPage('settings')} title="Настройки">
            ⚙
          </button>
          <button className={`iconBtn ${page === 'products' ? 'iconBtnPrimary' : ''}`} onClick={() => setPage('products')} title="Товары">
            ▦
          </button>
        </div>
      </div>

      <div className="main">
        {page === 'products' ? <ProductsPage /> : null}
        {page === 'logs' ? <LogsPage /> : null}
        {page === 'settings' ? <SettingsPage onStoreName={setStoreName} /> : null}
      </div>
    </div>
  )
}
