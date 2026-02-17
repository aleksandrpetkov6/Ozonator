import React, { useEffect, useState } from 'react'

export default function SettingsPage(props: { onStoreName?: (name: string | null) => void }) {
  const [clientId, setClientId] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [storeName, setStoreName] = useState<string | null>(null)
  const [status, setStatus] = useState<{ hasSecrets: boolean; encryptionAvailable: boolean } | null>(null)
  const [busy, setBusy] = useState(false)

  const load = async () => {
    const st = await window.api.secretsStatus()
    setStatus(st)

    if (st.hasSecrets) {
      try {
        const s = await window.api.loadSecrets()
        setClientId(s.clientId || '')
        setApiKey(s.apiKey || '')
        setStoreName(s.storeName ?? null)
        props.onStoreName?.(s.storeName ?? null)
      } catch {
        // ignore
      }
    }
  }

  useEffect(() => {
    load()
  }, [])

  const saveAndTest = async () => {
    try {
      setBusy(true)
      await window.api.saveSecrets({ clientId, apiKey })
      const res = await window.api.testAuth()
      if (!res?.ok) {
        alert(res?.error || 'Ключи не подошли')
      } else {
        const name = res?.storeName ?? null
        setStoreName(name)
        props.onStoreName?.(name)
      }
      await load()
    } finally {
      setBusy(false)
    }
  }

  const clear = async () => {
    await window.api.deleteSecrets()
    setClientId('')
    setApiKey('')
    setStoreName(null)
    props.onStoreName?.(null)
    await load()
  }

  return (
    <div className="page">
      <div className="row">
        <div className="badge">Настройки</div>
        {status ? (
          <div className="badge">
            Шифрование: {status.encryptionAvailable ? 'доступно' : 'недоступно'} • Ключи: {status.hasSecrets ? 'есть' : 'нет'}
          </div>
        ) : null}
      </div>

      {storeName ? (
        <div className="row">
          <div className="badge">Магазин: {storeName}</div>
        </div>
      ) : (
        <div className="row">
          <div className="badge">Название магазина: не определено</div>
        </div>
      )}

      <div className="row">
        <input className="input" placeholder="Client-Id" value={clientId} onChange={(e) => setClientId(e.target.value)} />
        <input className="input" placeholder="Api-Key" value={apiKey} onChange={(e) => setApiKey(e.target.value)} />
        <button className="btn" onClick={saveAndTest} disabled={busy}>
          Сохранить и проверить
        </button>
        <button className="btn" onClick={clear} disabled={busy}>
          Удалить ключи
        </button>
      </div>

      {!status?.encryptionAvailable ? (
        <div className="badge" style={{ maxWidth: 820 }}>
          SafeStorage недоступен на этой машине — программа не сможет безопасно сохранить ключи. Обычно помогает включить пароль/пин входа в Windows и перезапустить.
        </div>
      ) : null}
    </div>
  )
}
