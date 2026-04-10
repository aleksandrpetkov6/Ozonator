import React, { useEffect, useMemo, useState } from 'react'

const STORE_NAME_LS_KEY = 'ozonator_store_name'
const SETTINGS_DRAFT_LS_KEY = 'ozonator_settings_ui_draft'

type SettingsDraft = {
  clientId?: string
  apiKey?: string
}

type LocalServerInfo = {
  baseUrl?: string
  webhookUrlLocal?: string
}

function readSettingsDraft(): SettingsDraft {
  try {
    const raw = localStorage.getItem(SETTINGS_DRAFT_LS_KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object') return {}
    return {
      clientId: typeof (parsed as SettingsDraft).clientId === 'string' ? (parsed as SettingsDraft).clientId : undefined,
      apiKey: typeof (parsed as SettingsDraft).apiKey === 'string' ? (parsed as SettingsDraft).apiKey : undefined,
    }
  } catch {
    return {}
  }
}

function writeSettingsDraft(draft: SettingsDraft) {
  try {
    const clientId = String(draft.clientId ?? '')
    const apiKey = String(draft.apiKey ?? '')
    if (!clientId.trim() && !apiKey.trim()) {
      localStorage.removeItem(SETTINGS_DRAFT_LS_KEY)
      return
    }
    localStorage.setItem(SETTINGS_DRAFT_LS_KEY, JSON.stringify({
      clientId,
      apiKey,
    }))
  } catch {
    // ignore
  }
}

function clearSettingsDraft() {
  try {
    localStorage.removeItem(SETTINGS_DRAFT_LS_KEY)
  } catch {
    // ignore
  }
}

export default function SettingsPage() {
  const bootDraft = useMemo(() => readSettingsDraft(), [])
  const [clientId, setClientId] = useState(() => bootDraft.clientId ?? '')
  const [apiKey, setApiKey] = useState(() => bootDraft.apiKey ?? '')
  const [storeName, setStoreName] = useState<string>('')
  const [localServerInfo, setLocalServerInfo] = useState<LocalServerInfo>({})

  const [status, setStatus] = useState<string>('')
  const [err, setErr] = useState<string>('')

  async function load() {
    try {
      const [resp, serverResp] = await Promise.all([
        window.api.loadSecrets(),
        window.api.localServerConfig().catch(() => ({ ok: false })),
      ])
      if (resp.ok) {
        setClientId((prev) => prev.trim() ? prev : (resp.secrets.clientId ?? ''))
        setApiKey((prev) => prev.trim() ? prev : (resp.secrets.apiKey ?? ''))
        {
          const name = (resp.secrets as any).storeName
          const cleaned = typeof name === 'string' && name.trim() ? name.trim() : ''
          if (cleaned) {
            setStoreName(cleaned)
          } else {
            try {
              const ls = (localStorage.getItem(STORE_NAME_LS_KEY) ?? '').trim()
              if (ls) setStoreName(ls)
            } catch {
              // ignore
            }
          }
        }
      }
      if (serverResp && serverResp.ok) {
        setLocalServerInfo({
          baseUrl: typeof serverResp.baseUrl === 'string' ? serverResp.baseUrl : '',
          webhookUrlLocal: typeof serverResp.webhookUrlLocal === 'string' ? serverResp.webhookUrlLocal : '',
        })
      }
    } catch {
      // ignore
    }
  }

  useEffect(() => {
    load()
  }, [])

  useEffect(() => {
    writeSettingsDraft({ clientId, apiKey })
  }, [clientId, apiKey])

  useEffect(() => {
    const flushDraft = () => writeSettingsDraft({ clientId, apiKey })
    window.addEventListener('ozon:prepare-install-exit', flushDraft)
    return () => window.removeEventListener('ozon:prepare-install-exit', flushDraft)
  }, [clientId, apiKey])

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

        clearSettingsDraft()

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
      clearSettingsDraft()
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
      <div className="row" style={{ alignItems: 'center', flexWrap: 'wrap' }}>
        <div className="settingsStoreInline" title={storeName || 'Название появится после проверки доступа'}>
          
          {storeName ? (
            <span className="settingsStoreValue">{storeName}</span>
          ) : (
            <span className="settingsStorePlaceholder">Название появится после проверки доступа</span>
          )}
        </div>

        <div className="col field" style={{ minWidth: 220 }}>
          <label>Client-Id</label>
          <input value={clientId} onChange={(e) => setClientId(e.target.value)} placeholder="например 55201" />
        </div>
        <div className="col field" style={{ minWidth: 220 }}>
          <label>Api-Key</label>
          <input value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="например 9c70..." />
        </div>
      </div>

      <div className="row" style={{ marginTop: 16, gap: 16, flexWrap: 'wrap' }}>
        <div className="col field" style={{ minWidth: 280 }}>
          <label>Локальный HTTP сервер</label>
          <input value={localServerInfo.baseUrl ?? ''} readOnly placeholder="запускается автоматически" />
        </div>
        <div className="col field" style={{ minWidth: 420, flex: 1 }}>
          <label>Локальный webhook для FBO push</label>
          <input value={localServerInfo.webhookUrlLocal ?? ''} readOnly placeholder="нужен внешний HTTPS-туннель до этого адреса" />
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

      {err && (
        <div className="notice error" style={{ marginTop: 12 }}>
          {err}
        </div>
      )}
    </div>
  )
}
