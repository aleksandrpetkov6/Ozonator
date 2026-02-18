import React, { useEffect, useMemo, useRef, useState } from 'react'

type Product = {
  offer_id: string
  sku?: string | null
  barcode?: string | null
  brand?: string | null
  category?: string | null
  type?: string | null
  name?: string | null
  is_visible?: number | boolean | null
  hidden_reasons?: string | null
  created_at?: string | null
  updated_at?: string | null
}

type ColDef = {
  id: keyof Product | 'archived'
  title: string
  w: number
  visible: boolean
}

type Props = {
  query?: string
  onStats?: (s: { total: number; filtered: number }) => void
}

const DEFAULT_COLS: ColDef[] = [
  { id: 'offer_id', title: 'Артикул', w: 160, visible: true },
  { id: 'name', title: 'Наименование', w: 320, visible: true },
  { id: 'category', title: 'Категория', w: 280, visible: true },
  { id: 'brand', title: 'Бренд', w: 180, visible: true },
  { id: 'sku', title: 'SKU', w: 140, visible: true },
  { id: 'barcode', title: 'Штрихкод', w: 170, visible: true },
  { id: 'type', title: 'Тип', w: 220, visible: true },
  { id: 'is_visible', title: 'Видимость', w: 140, visible: true },
  { id: 'created_at', title: 'Создан', w: 180, visible: true },
  { id: 'updated_at', title: 'Обновлён', w: 180, visible: false },
]

const AUTO_MIN_W = 80
const AUTO_PAD = 34
const AUTO_MAX_W: Record<string, number> = {
  offer_id: 240,
  sku: 220,
  barcode: 260,
  brand: 220,
  is_visible: 180,
  created_at: 240,
  updated_at: 240,
  type: 320,
  category: 380,
  name: 460,
}

function readCols(): ColDef[] {
  try {
    const raw = localStorage.getItem('ozonator_cols')
    if (!raw) return DEFAULT_COLS

    const parsed = JSON.parse(raw) as Partial<ColDef>[]
    const map = new Map<string, Partial<ColDef>>()
    for (const x of parsed) {
      if (x?.id) map.set(String(x.id), x)
    }

    // Мержим с дефолтом, чтобы новые колонки появлялись автоматически
    const merged: ColDef[] = []
    for (const d of DEFAULT_COLS) {
      const p = map.get(String(d.id))
      merged.push({
        id: d.id,
        title: d.title,
        w: (typeof p?.w === 'number' && p.w > 60) ? p.w : d.w,
        visible: (typeof p?.visible === 'boolean') ? p.visible : d.visible,
      })
      map.delete(String(d.id))
    }

    // Если в localStorage были старые/лишние колонки — игнорируем
    return merged
  } catch {
    return DEFAULT_COLS
  }
}

function saveCols(cols: ColDef[]) {
  localStorage.setItem('ozonator_cols', JSON.stringify(cols))
}

function toText(v: any): string {
  if (v == null) return ''
  if (typeof v === 'string') return v
  if (typeof v === 'number' || typeof v === 'boolean') return String(v)
  try { return JSON.stringify(v) } catch { return String(v) }
}

function visibilityText(p: Product): string {
  const v = p.is_visible
  if (v === true || v === 1) return 'Виден'
  if (v === false || v === 0) return 'Скрыт'
  if (p.hidden_reasons && String(p.hidden_reasons).trim()) return 'Скрыт'
  return 'Неизвестно'
}

export default function ProductsPage({ query = '', onStats }: Props) {
  const [products, setProducts] = useState<Product[]>([])
  const [cols, setCols] = useState<ColDef[]>(readCols)

  const [draggingId, setDraggingId] = useState<string | null>(null)
  const [dropHint, setDropHint] = useState<{ id: string; side: 'left' | 'right'; x: number } | null>(null)

  const resizingRef = useRef<{ id: string; startX: number; startW: number } | null>(null)
  const measureCanvasRef = useRef<HTMLCanvasElement | null>(null)
  const didAutoInitRef = useRef(false)

  const hasStoredCols = useMemo(() => {
    try { return !!localStorage.getItem('ozonator_cols') } catch { return true }
  }, [])

  async function load() {
    const resp = await window.api.getProducts()
    if (resp.ok) setProducts(resp.products as any)
  }

  useEffect(() => {
    load()
  }, [])

  useEffect(() => {
    const onUpdated = () => load()
    window.addEventListener('ozon:products-updated', onUpdated)
    return () => window.removeEventListener('ozon:products-updated', onUpdated)
  }, [])

  useEffect(() => {
    saveCols(cols)
  }, [cols])

  const visibleCols = useMemo(() => cols.filter(c => c.visible), [cols])
  const hiddenCols = useMemo(() => cols.filter(c => !c.visible), [cols])

  const filtered = useMemo(() => {
    const q = String(query ?? '').trim().toLowerCase()
    if (!q) return products

    return products.filter((p) => {
      const hay = visibleCols
        .map((c) => {
          if (c.id === 'archived') return ''
          if (c.id === 'is_visible') return visibilityText(p)
          if (c.id === 'brand') return (p.brand && String(p.brand).trim()) ? String(p.brand).trim() : 'Не указан'
          if (c.id === 'name') return (p.name && String(p.name).trim()) ? String(p.name).trim() : 'Без названия'
          return toText((p as any)[c.id])
        })
        .join(' ')
        .toLowerCase()

      return hay.includes(q)
    })
  }, [products, query, visibleCols])

  useEffect(() => {
    onStats?.({ total: products.length, filtered: filtered.length })
  }, [products.length, filtered.length, onStats])

  function hideCol(id: string) {
    setCols(prev => prev.map(c => String(c.id) === id ? { ...c, visible: false } : c))
  }

  function showCol(id: string) {
    setCols(prev => prev.map(c => String(c.id) === id ? { ...c, visible: true } : c))
  }

  function onDragStart(e: React.DragEvent, id: string) {
    setDraggingId(id)
    e.dataTransfer.setData('text/plain', id)
    e.dataTransfer.effectAllowed = 'move'
  }


function onDragOverHeader(e: React.DragEvent) {
  e.preventDefault()
  e.dataTransfer.dropEffect = 'move'

  const head = headScrollRef.current
  const row = headerRowRef.current
  if (!head || !row) return
  if (visibleCols.length === 0) return

  const headRect = head.getBoundingClientRect()
  const x = (e.clientX - headRect.left) + head.scrollLeft

  const cells = Array.from(row.children) as HTMLElement[]
  if (cells.length === 0) return

  let targetId = String(visibleCols[0].id)
  let side: 'left' | 'right' = 'left'
  let lineX = 0

  for (let i = 0; i < cells.length; i++) {
    const cell = cells[i]
    const left = cell.offsetLeft
    const w = cell.offsetWidth
    const mid = left + (w / 2)

    if (x < mid) {
      targetId = String(visibleCols[i].id)
      side = 'left'
      lineX = left
      break
    }

    if (i === cells.length - 1) {
      targetId = String(visibleCols[i].id)
      side = 'right'
      lineX = left + w
    }
  }

  const next = { id: targetId, side, x: Math.round(lineX) }

  setDropHint((prev) => {
    if (!prev) return next

    // если линия «дрожит» на 1px — фиксируем положение, но обновляем цель
    const stableX = (Math.abs(next.x - prev.x) <= 1) ? prev.x : next.x
    const stable = { ...next, x: stableX }

    if (prev.id === stable.id && prev.side === stable.side && prev.x === stable.x) return prev
    return stable
  })
}



  function onDrop(e: React.DragEvent) {
  e.preventDefault()

  const draggedId = e.dataTransfer.getData('text/plain')
  if (!draggedId) return

  const hint = dropHint
  if (!hint) {
    setDraggingId(null)
    setDropHint(null)
    return
  }

  const targetId = hint.id
  const side = hint.side

  setCols(prev => {
    const fromIdx = prev.findIndex(c => String(c.id) === draggedId)
    const toIdxRaw = prev.findIndex(c => String(c.id) === targetId)
    if (fromIdx < 0 || toIdxRaw < 0 || fromIdx === toIdxRaw) return prev

    const insertBase = toIdxRaw + (side === 'right' ? 1 : 0)

    const next = [...prev]
    const [moved] = next.splice(fromIdx, 1)

    let insertIdx = insertBase
    // если элемент забрали слева, индексы сдвинулись
    if (fromIdx < insertIdx) insertIdx -= 1
    if (insertIdx < 0) insertIdx = 0
    if (insertIdx > next.length) insertIdx = next.length

    next.splice(insertIdx, 0, moved)
    return next
  })

  setDraggingId(null)
  setDropHint(null)
}

  function onDragEnd() {
    setDraggingId(null)
    setDropHint(null)
  }

  function startResize(e: React.MouseEvent, colId: string) {
    e.preventDefault()
    e.stopPropagation()

    const col = cols.find(c => String(c.id) === colId)
    if (!col) return

    resizingRef.current = { id: colId, startX: e.clientX, startW: col.w }

    const onMove = (ev: MouseEvent) => {
      const r = resizingRef.current
      if (!r) return
      const dx = ev.clientX - r.startX
      const w = Math.max(AUTO_MIN_W, Math.round(r.startW + dx))
      setCols(prev => prev.map(c => String(c.id) === r.id ? { ...c, w } : c))
    }

    const onUp = () => {
      resizingRef.current = null
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }

    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }

  function cellText(p: Product, colId: ColDef['id']): { text: string; title?: string } {
    if (colId === 'offer_id') return { text: p.offer_id }
    if (colId === 'name') return { text: (p.name && String(p.name).trim()) ? String(p.name).trim() : 'Без названия' }
    if (colId === 'brand') return { text: (p.brand && String(p.brand).trim()) ? String(p.brand).trim() : 'Не указан' }

    if (colId === 'is_visible') {
      const txt = visibilityText(p)
      const title = (p.hidden_reasons && String(p.hidden_reasons).trim()) ? String(p.hidden_reasons) : undefined
      return { text: txt, title }
    }

    const v = (p as any)[colId]
    return { text: (v == null || v === '') ? '-' : String(v) }
  }

  function measureTextWidth(text: string): number {
    const canvas = measureCanvasRef.current ?? (measureCanvasRef.current = document.createElement('canvas'))
    const ctx = canvas.getContext('2d')
    if (!ctx) return text.length * 7

    const cs = window.getComputedStyle(document.body)
    const fontSize = cs.fontSize || '13px'
    const fontFamily = cs.fontFamily || 'system-ui'
    const fontWeight = cs.fontWeight || '400'
    ctx.font = `${fontWeight} ${fontSize} ${fontFamily}`

    return ctx.measureText(text).width
  }

  function getCellString(p: Product, colId: ColDef['id']): string {
    if (colId === 'archived') return ''
    if (colId === 'is_visible') return visibilityText(p)
    if (colId === 'brand') return (p.brand && String(p.brand).trim()) ? String(p.brand).trim() : 'Не указан'
    if (colId === 'name') return (p.name && String(p.name).trim()) ? String(p.name).trim() : 'Без названия'
    return toText((p as any)[colId])
  }

  function autoSizeColumn(colId: string, rows: Product[]) {
    const col = cols.find(c => String(c.id) === colId)
    if (!col) return

    const cap = AUTO_MAX_W[colId] ?? 320

    let max = measureTextWidth(col.title)
    const sample = rows.length > 1600 ? rows.slice(0, 1600) : rows
    for (const p of sample) {
      const s = getCellString(p, col.id)
      if (!s) continue
      const w = measureTextWidth(s)
      if (w > max) max = w
    }

    const nextW = Math.max(AUTO_MIN_W, Math.min(cap, Math.round(max + AUTO_PAD)))
    setCols(prev => prev.map(c => String(c.id) === colId ? { ...c, w: nextW } : c))
  }

  // Первичная авто-ширина (если пользователь ещё ничего не сохранял)
  useEffect(() => {
    if (didAutoInitRef.current) return
    if (hasStoredCols) return
    if (products.length === 0) return

    didAutoInitRef.current = true

    // авто-подгоняем только видимые дефолтные столбцы
    const next = cols.map((c) => {
      if (!c.visible) return c
      const cap = AUTO_MAX_W[String(c.id)] ?? 320

      let max = measureTextWidth(c.title)
      const sample = products.length > 1600 ? products.slice(0, 1600) : products
      for (const p of sample) {
        const s = getCellString(p, c.id)
        if (!s) continue
        const w = measureTextWidth(s)
        if (w > max) max = w
      }

      const nextW = Math.max(AUTO_MIN_W, Math.min(cap, Math.round(max + AUTO_PAD)))
      return { ...c, w: nextW }
    })

    setCols(next)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [products.length])

  const tableMinWidth = useMemo(() => Math.max(860, visibleCols.reduce((s, c) => s + c.w, 0)), [visibleCols])

  const headScrollRef = useRef<HTMLDivElement | null>(null)
  const bodyScrollRef = useRef<HTMLDivElement | null>(null)
  const scrollSyncLockRef = useRef(false)
  const headerRowRef = useRef<HTMLTableRowElement | null>(null)

  useEffect(() => {
    const head = headScrollRef.current
    const body = bodyScrollRef.current
    if (!head || !body) return

    const syncFromBody = () => {
      if (scrollSyncLockRef.current) return
      scrollSyncLockRef.current = true
      head.scrollLeft = body.scrollLeft
      scrollSyncLockRef.current = false
    }

    const syncFromHead = () => {
      if (scrollSyncLockRef.current) return
      scrollSyncLockRef.current = true
      body.scrollLeft = head.scrollLeft
      scrollSyncLockRef.current = false
    }

    body.addEventListener('scroll', syncFromBody, { passive: true })
    head.addEventListener('scroll', syncFromHead, { passive: true })

    // первичная синхронизация
    head.scrollLeft = body.scrollLeft

    return () => {
      body.removeEventListener('scroll', syncFromBody)
      head.removeEventListener('scroll', syncFromHead)
    }
  }, [visibleCols.length])

  return (
    <div className="card productsCard">
      {hiddenCols.length > 0 && (
        <div className="collapsedBar">
          <span className="small">Скрытые столбцы:</span>
          {hiddenCols.map(c => (
            <span key={String(c.id)} className="chip">
              {c.title}{' '}
              <button className="colToggle" onClick={() => showCol(String(c.id))} title="Показать">+</button>
            </span>
          ))}
        </div>
      )}

      <div className="productsTableArea">
        <div className="tableWrap" style={{ marginTop: 12 }}>
          <div className="tableHeadX" ref={headScrollRef}>
            <div className="tableWrapY tableHeadInner" style={{ minWidth: tableMinWidth }}>
              {dropHint && <div className="dropIndicator" style={{ left: dropHint.x }} />}
              <table className="table tableFixed tableHead" style={{ minWidth: tableMinWidth }}>
                <colgroup>
                  {visibleCols.map(c => (
                    <col key={String(c.id)} style={{ width: c.w }} />
                  ))}
                </colgroup>
                <thead onDragOver={onDragOverHeader} onDrop={onDrop}>
                  <tr ref={headerRowRef}>
                    {visibleCols.map(c => {
                      const id = String(c.id)
                      return (
                        <th
                          key={id}
                          draggable
                          onDragStart={(e) => onDragStart(e, id)}
                          onDragEnd={onDragEnd}
                          className={`thDraggable ${draggingId === id ? 'thDragging' : ''}`.trim()}
                        >
                          <div className="thInner">
                            <button className="colToggle" onClick={() => hideCol(id)} title="Скрыть">−</button>
                            <span>{c.title}</span>
                            <span className="thGrip" title="Перетащите, чтобы поменять столбцы местами">⋮⋮</span>
                          </div>
                          <div
                            className="thResizer"
                            title="Изменить ширину (двойной клик — по содержимому)"
                            onMouseDown={(e) => startResize(e, id)}
                            onDoubleClick={(e) => {
                              e.preventDefault()
                              e.stopPropagation()
                              autoSizeColumn(id, filtered)
                            }}
                          />
                        </th>
                      )
                    })}
                  </tr>
                </thead>
              </table>
            </div>
          </div>

          <div className="tableWrapX" ref={bodyScrollRef}>
            <div className="tableWrapY" style={{ minWidth: tableMinWidth }}>
              <table className="table tableFixed tableBody" style={{ minWidth: tableMinWidth }}>
                <colgroup>
                  {visibleCols.map(c => (
                    <col key={String(c.id)} style={{ width: c.w }} />
                  ))}
                </colgroup>
                <tbody>
                  {filtered.map(p => (
                    <tr key={p.offer_id}>
                      {visibleCols.map(c => {
                        const id = String(c.id)
                        const { text, title } = cellText(p, c.id)
                        return (
                          <td key={id}>
                            <div className="cellText" title={title ?? text}>{text}</div>
                          </td>
                        )
                      })}
                    </tr>
                  ))}

                  {filtered.length === 0 && (
                    <tr>
                      <td colSpan={visibleCols.length} className="empty">Ничего не найдено.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
