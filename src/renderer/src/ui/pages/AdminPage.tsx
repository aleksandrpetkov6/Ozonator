import React from 'react'

type Props = {
  loading: boolean
  saving: boolean
  logLifeDaysValue: string
  currentSavedDays: number
  onChangeLogLifeDays: (value: string) => void
  notice: { kind: 'success' | 'error'; text: string } | null
}

export default function AdminPage(props: Props) {
  const {
    loading,
    saving,
    logLifeDaysValue,
    currentSavedDays,
    onChangeLogLifeDays,
    notice,
  } = props

  return (
    <div className="adminWrap">
      <div className="card adminCard">
        <div className="adminCardHead">
          <div className="adminCardIcon" aria-hidden>🛡️</div>
          <div>
            <div className="adminTitle">Админ</div>
            <div className="adminSub">Служебные настройки приложения</div>
          </div>
        </div>

        {notice && <div className={`notice ${notice.kind === 'error' ? 'error' : ''}`}>{notice.text}</div>}

        {loading ? (
          <div className="muted">Загрузка настроек…</div>
        ) : (
          <div className="adminGrid">
            <label className="adminField">
              <span className="adminFieldLabel">Жизнь лога</span>
              <input
                type="number"
                min={1}
                step={1}
                inputMode="numeric"
                className="searchInput adminNumberInput"
                value={logLifeDaysValue}
                onChange={(e) => onChangeLogLifeDays(e.target.value)}
                placeholder="Например: 10"
                disabled={saving}
              />
              <span className="adminHint">
                Срок хранения записей в днях. На следующий день после истечения срока старые записи удаляются автоматически.
              </span>
              <span className="adminHint">Сейчас сохранено: {currentSavedDays} дн.</span>
            </label>
          </div>
        )}
      </div>
    </div>
  )
}
