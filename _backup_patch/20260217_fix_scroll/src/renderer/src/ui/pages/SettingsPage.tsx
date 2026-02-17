import React, { useEffect, useState } from 'react'

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
        setStoreName((resp.secrets as any).storeName ?? '')
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
        if (resp.storeName) setStoreName(resp.storeName)
        setStatus('Доступ подтверждён.')

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
      window.dispatchEvent(new Event('ozon:store-updated'))
    } catch (e: any) {
      setErr(e?.message ?? String(e))
    }
  }

  return (
    <div className="card">
      <div className="h1">Настройки</div>

      <div className="row" style={{ marginTop: 16 }}>
        <div className="col field">
          <label>Название магазина</label>
          <input
            value={storeName}
            placeholder="Появится после проверки доступа"
            readOnly
          />
        </div>
      </div>

      <div className="row" style={{ marginTop: 12 }}>
        <div className="col field">
          <label>Client-Id</label>
          <input value={clientId} onChange={e => setClientId(e.target.value)} placeholder="например 55201" />
        </div>
        <div className="col field">
          <label>Api-Key</label>
          <input value={apiKey} onChange={e => setApiKey(e.target.value)} placeholder="например 9c70..." />
        </div>
      </div>

      <div style={{ display: 'flex', gap: 10, marginTop: 16, flexWrap: 'wrap' }}>
        <button className="primary" onClick={onSaveAndTest}>Сохранить и проверить</button>
        <button onClick={onDelete}>Стереть ключи</button>
      </div>

      {status && <div className="notice" style={{ marginTop: 12 }}>{status}</div>}
      {err && <div className="notice error" style={{ marginTop: 12 }}>{err}</div>}
    </div>
  )
}
