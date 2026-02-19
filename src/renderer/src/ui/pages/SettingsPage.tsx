import React, { useEffect, useState } from 'react'

const STORE_NAME_LS_KEY = 'ozonator_store_name'

export default function SettingsPage() {
  const [clientId, setClientId] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [storeName, setStoreName] = useState<string>('')

  const [status, setStatus] = useState<string>('')
  const [err, setErr] = useState<string>('')

  async function load() {
    try {
      const resp = await window.api.loadSecrets()
      if (resp.ok) {
        setClientId(resp.secrets.clientId ?? '')
        setApiKey(resp.secrets.apiKey ?? '')
        {
          const name = (resp.secrets as any).storeName
          const cleaned = typeof name === 'string' && name.trim() ? name.trim() : ''
          if (cleaned) {
            setStoreName(cleaned)
          } else {
            // fallback: localStorage (если storeName не сохраняется в secrets)
            try {
              const ls = (localStorage.getItem(STORE_NAME_LS_KEY) ?? '').trim()
              if (ls) setStoreName(ls)
            } catch {
              // ignore
            }
          }
        }
      }
    } catch {
      // ignore
    }
  }

  useEffect(() => {
    load()
  }, [])

  async function onSaveAndTest() {
    setStatus('')
    setErr('')

    try {
      await window.api.saveSecrets({ clientId, apiKey })
      const resp = await window.api.testAuth()

      if (resp.ok) {
        if (typeof resp.storeName === 'string' && resp.storeName.trim()) {
          const cleaned = resp.storeName.trim()
          setStoreName(cleaned)
          try {
            localStorage.setItem(STORE_NAME_LS_KEY, cleaned)
          } catch {
            // ignore
          }
        }

        // обновим поля из локального хранилища (на случай, если storeName подтянулся и сохранился)
        load()

        // обновим заголовок/лог
        window.dispatchEvent(new Event('ozon:store-updated'))
        window.dispatchEvent(new Event('ozon:logs-updated'))
      } else {
        setErr(resp.error ?? 'Ошибка проверки доступа')
      }
    } catch (e: any) {
      setErr(e?.message ?? String(e))
    }
  }

  async function onDelete() {
    setStatus('')
    setErr('')

    try {
      await window.api.deleteSecrets()
      setClientId('')
      setApiKey('')
      setStoreName('')
      setStatus('Ключи удалены.')
      try {
        localStorage.removeItem(STORE_NAME_LS_KEY)
      } catch {
        // ignore
      }
      window.dispatchEvent(new Event('ozon:store-updated'))
    } catch (e: any) {
      setErr(e?.message ?? String(e))
    }
  }

  return (
    <div className="card">
      <div className="h1">Настройки</div>

      <div className="row" style={{ marginTop: 12 }}>
        <div className="col">
          {err ? (
            <span className="pill pillError">{err}</span>
          ) : storeName ? (
            <span className="pill" title="Название магазина">Магазин: {storeName}</span>
          ) : (
            <span className="small muted">Название магазина появится после проверки доступа</span>
          )}
        </div>
      </div>

      <div className="row" style={{ marginTop: 12 }}>
        <div className="col field">
          <label>Client-Id</label>
          <input value={clientId} onChange={(e) => setClientId(e.target.value)} placeholder="например 55201" />
        </div>
        <div className="col field">
          <label>Api-Key</label>
          <input value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="например 9c70..." />
        </div>
      </div>

      <div style={{ display: 'flex', gap: 10, marginTop: 16, flexWrap: 'wrap' }}>
        <button className="primary" onClick={onSaveAndTest}>
          Сохранить и проверить
        </button>
        <button onClick={onDelete}>Стереть ключи</button>
      </div>

      {status && (
        <div className="notice" style={{ marginTop: 12 }}>
          {status}
        </div>
      )}
    </div>
  )
}
