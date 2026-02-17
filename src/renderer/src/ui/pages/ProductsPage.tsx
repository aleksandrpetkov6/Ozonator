import React, { useEffect, useMemo, useRef, useState } from 'react'

// note: types from backend
export type Product = {
  product_id: number
  offer_id: string | null
  name: string | null
  brand: string | null
  category: string | null
  type: string | null
  created_at: string | null
  archived: number
}

type Col = {
  id: keyof Product
  label: string
  w: number
  visible: boolean
}

const STORAGE_KEY = 'ozonator.columns.v4'

const DEFAULT_COLS: Col[] = [
  { id: 'offer_id', label: 'Артикул', w: 160, visible: true },
  { id: 'product_id', label: 'ID товара', w: 110, visible: true },
  { id: 'name', label: 'Название', w: 320, visible: true },
  { id: 'brand', label: 'Бренд', w: 180, visible: true },
  { id: 'category', label: 'Категория', w: 220, visible: true },
  { id: 'type', label: 'Тип', w: 180, visible: true },
  { id: 'created_at', label: 'Дата создан', w: 150, visible: true },
]

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n))
}

function fmtRuDateTime(iso: any) {
  const s = String(iso ?? '').trim()
  if (!s) return '-'
  const d = new Date(s)
  if (Number.isNaN(d.getTime())) return s
  const pad = (x: number) => String(x).padStart(2, '0')
  const yy = String(d.getFullYear()).slice(-2)
  return `${pad(d.getDate())}.${pad(d.getMonth() + 1)}.${yy} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

export default function ProductsPage() {
  const [products, setProducts] = useState<Product[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [cols, setCols] = useState<Col[]>(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY)
      if (!raw) return DEFAULT_COLS
      const parsed = JSON.parse(raw)
      if (!Array.isArray(parsed)) return DEFAULT_COLS

      const safe: Col[] = []
      const byId = new Map(DEFAULT_COLS.map((c) => [c.id, c]))

      for (const item of parsed) {
        if (!item || typeof item !== 'object') continue
        const id = (item as any).id
        if (!byId.has(id)) continue
        const base = byId.get(id)!
        safe.push({
          id,
          label: base.label,
          w: Number.isFinite((item as any).w) ? clamp(Number((item as any).w), 80, 900) : base.w,
          visible: typeof (item as any).visible === 'boolean' ? (item as any).visible : base.visible,
        })
      }

      // add any missing
      for (const c of DEFAULT_COLS) {
        if (!safe.find((x) => x.id === c.id)) safe.push(c)
      }

      return safe
    } catch {
      return DEFAULT_COLS
    }
  })

  const [query, setQuery] = useState('')

  const draggingIdRef = useRef<string | null>(null)
  const [dropHint, setDropHint] = useState<{ targetId: string; side: 'left' | 'right' } | null>(null)

  const wrapRef = useRef<HTMLDivElement | null>(null)
  const [resizeLineX, setResizeLineX] = useState<number | null>(null)
  const resizeRafRef = useRef<number | null>(null)

  // persist columns
  useEffect(() => {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(cols)) } catch {}
  }, [cols])

  // load products
  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        setLoading(true)
        setError(null)
        const res = await window.api.getProducts()
        if (!cancelled) setProducts((res as any)?.products ?? [])
      } catch (e: any) {
        if (!cancelled) setError(e?.message ?? 'Ошибка загрузки')
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => { cancelled = true }
  }, [])

  const visibleCols = useMemo(() => cols.filter((c) => c.visible), [cols])
  const hiddenCols = useMemo(() => cols.filter((c) => !c.visible), [cols])

  const tableWidth = useMemo(() => {
    const sum = visibleCols.reduce((acc, c) => acc + c.w, 0)
    return Math.max(860, sum)
  }, [visibleCols])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return products

    const fields: (keyof Product)[] = visibleCols.map((c) => c.id)

    return products.filter((p) => {
      for (const f of fields) {
        const v = (p as any)[f]
        if (v == null) continue
        if (String(v).toLowerCase().includes(q)) return true
      }
      return false
    })
  }, [products, query, visibleCols])

  function cellText(p: Product, colId: keyof Product) {
    const v = (p as any)[colId]
    if (v == null || v === '') return '-'
    if (colId === 'created_at') return fmtRuDateTime(v)
    if (colId === 'archived') return v ? 'Да' : 'Нет'
    return String(v)
  }

  function toggleCol(id: keyof Product) {
    setCols((prev) => prev.map((c) => (c.id === id ? { ...c, visible: !c.visible } : c)))
  }

  function startResize(e: React.MouseEvent, colId: keyof Product) {
    e.preventDefault()
    e.stopPropagation()

    const startX = e.clientX
    const col = cols.find((c) => c.id === colId)
    if (!col) return
    const startW = col.w

    const getWrapLeft = () => wrapRef.current?.getBoundingClientRect().left ?? 0
    const setLine = (clientX: number) => setResizeLineX(Math.round(clientX - getWrapLeft()))

    setLine(e.clientX)

    const onMove = (ev: MouseEvent) => {
      const dx = ev.clientX - startX
      const nextW = clamp(startW + dx, 80, 900)

      if (resizeRafRef.current != null) cancelAnimationFrame(resizeRafRef.current)
      resizeRafRef.current = requestAnimationFrame(() => {
        setCols((prev) => prev.map((c) => (c.id === colId ? { ...c, w: nextW } : c)))
        setLine(ev.clientX)
      })
    }

    const onUp = () => {
      if (resizeRafRef.current != null) cancelAnimationFrame(resizeRafRef.current)
      resizeRafRef.current = null
      setResizeLineX(null)
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }

    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }

  function autoSizeCol(colId: keyof Product) {
    const sample = filtered.slice(0, 80)
    const header = cols.find((c) => c.id === colId)?.label ?? String(colId)

    const canvas = document.createElement('canvas')
    const ctx = canvas.getContext('2d')
    if (!ctx) return
    ctx.font = '13px system-ui, -apple-system, Segoe UI, Roboto, sans-serif'

    const pad = 44
    let maxPx = ctx.measureText(header).width + pad

    for (const p of sample) {
      const t = cellText(p, colId)
      const w = ctx.measureText(t).width + pad
      if (w > maxPx) maxPx = w
    }

    const nextW = clamp(Math.ceil(maxPx), 90, 520)
    setCols((prev) => prev.map((c) => (c.id === colId ? { ...c, w: nextW } : c)))
  }

  function onDragStartHeader(e: React.DragEvent, colId: keyof Product) {
    draggingIdRef.current = String(colId)
    e.dataTransfer.effectAllowed = 'move'
    try { e.dataTransfer.setData('text/plain', String(colId)) } catch {}
  }

  function onDragEndHeader() {
    draggingIdRef.current = null
    setDropHint(null)
  }

  function onDragOverHeader(e: React.DragEvent, targetId: keyof Product) {
    e.preventDefault()

    const dragging = draggingIdRef.current
    if (!dragging || dragging === String(targetId)) {
      setDropHint(null)
      return
    }

    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect()
    const x = e.clientX - rect.left
    const mid = rect.width / 2
    const threshold = 14

    setDropHint((prev) => {
      if (prev?.targetId !== String(targetId)) {
        const side = x < mid ? 'left' : 'right'
        return { targetId: String(targetId), side }
      }

      // гистерезис: в зоне ±threshold от середины не меняем сторону (убирает "прыжок" на миллиметр)
      if (x < mid - threshold) return { targetId: String(targetId), side: 'left' }
      if (x > mid + threshold) return { targetId: String(targetId), side: 'right' }
      return prev
    })
  }

  function onDropHeader(e: React.DragEvent, targetId: keyof Product) {
    e.preventDefault()

    const dragging = draggingIdRef.current
    if (!dragging || dragging === String(targetId)) {
      setDropHint(null)
      return
    }

    const fromIdx = cols.findIndex((c) => String(c.id) === dragging)
    const targetIdx = cols.findIndex((c) => c.id === targetId)
    if (fromIdx < 0 || targetIdx < 0) {
      setDropHint(null)
      return
    }

    const side = dropHint?.targetId === String(targetId) ? dropHint.side : 'right'

    setCols((prev) => {
      const arr = [...prev]
      const [moved] = arr.splice(fromIdx, 1)
      let insertIdx = targetIdx

      // если мы вытащили элемент из массива слева — targetIdx изменился
      if (fromIdx < targetIdx) insertIdx = targetIdx - 1

      if (side === 'right') insertIdx += 1
      insertIdx = clamp(insertIdx, 0, arr.length)
      arr.splice(insertIdx, 0, moved)
      return arr
    })

    setDropHint(null)
  }

  const totalLabel = useMemo(() => `Всего: ${products.length}`, [products.length])

  return (
    <div className="card productsCard">
      <div className="productsHeader">
        <div>
          <div className="h2">Товары</div>
          <div className="muted">{totalLabel}</div>
        </div>
      </div>

      <div className="productsTableArea">
        {hiddenCols.length > 0 && (
          <div className="collapsedBar">
            <div className="collapsedLeft">Скрытые столбцы:</div>
            <div className="collapsedRight">
              {hiddenCols.map((c) => (
                <button key={String(c.id)} className="chip chipSmall" onClick={() => toggleCol(c.id)}>
                  {c.label}
                </button>
              ))}
            </div>
          </div>
        )}

        <div className="tableWrap" ref={wrapRef}>
          {resizeLineX != null && <div className="colResizeLine" style={{ left: resizeLineX }} />}

          <div className="tableScroller">
            <table className="table tableFixed" style={{ width: tableWidth, minWidth: tableWidth }}>
              <colgroup>
                {visibleCols.map((c) => (
                  <col key={String(c.id)} style={{ width: c.w }} />
                ))}
              </colgroup>

              <thead>
                <tr>
                  {visibleCols.map((c) => {
                    const hint = dropHint?.targetId === String(c.id) ? dropHint.side : null
                    const thClass = [
                      hint ? (hint === 'left' ? 'thDropLeft' : 'thDropRight') : '',
                    ].filter(Boolean).join(' ')

                    return (
                      <th
                        key={String(c.id)}
                        className={thClass}
                        draggable
                        onDragStart={(e) => onDragStartHeader(e, c.id)}
                        onDragEnd={onDragEndHeader}
                        onDragOver={(e) => onDragOverHeader(e, c.id)}
                        onDrop={(e) => onDropHeader(e, c.id)}
                        title="Перетащите, чтобы поменять порядок"
                      >
                        <div className="thInner">
                          <span className="thLabel">{c.label}</span>
                          <div className="thActions">
                            <button className="colToggle" onClick={() => toggleCol(c.id)} title="Скрыть столбец">
                              ⨯
                            </button>
                          </div>
                        </div>
                        <div
                          className="thResizer"
                          onMouseDown={(e) => startResize(e, c.id)}
                          onDoubleClick={() => autoSizeCol(c.id)}
                          title="Тяните для изменения ширины. Двойной клик — автоширина"
                        />
                      </th>
                    )
                  })}
                </tr>
              </thead>

              <tbody>
                {loading && (
                  <tr>
                    <td colSpan={visibleCols.length} className="tdCenter">Загрузка...</td>
                  </tr>
                )}

                {!loading && error && (
                  <tr>
                    <td colSpan={visibleCols.length} className="tdCenter">{error}</td>
                  </tr>
                )}

                {!loading && !error && filtered.length === 0 && (
                  <tr>
                    <td colSpan={visibleCols.length} className="tdCenter">Нет данных</td>
                  </tr>
                )}

                {!loading && !error && filtered.map((p) => (
                  <tr key={p.product_id}>
                    {visibleCols.map((c) => (
                      <td key={String(c.id)}>
                        <div className="cellText" title={cellText(p, c.id)}>
                          {cellText(p, c.id)}
                        </div>
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}
