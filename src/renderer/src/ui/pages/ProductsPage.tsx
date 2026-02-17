import React, { useEffect, useMemo, useRef, useState } from 'react'

type ColId =
  | 'offer_id'
  | 'product_id'
  | 'name'
  | 'brand'
  | 'category'
  | 'type'
  | 'sku'
  | 'barcode'
  | 'created_at'
  | 'archived'

type Col = { id: ColId; title: string; w: number; visible: boolean }

const DEFAULT_COLS: Col[] = [
  { id: 'offer_id', title: 'Артикул', w: 180, visible: true },
  { id: 'name', title: 'Название', w: 320, visible: true },
  { id: 'brand', title: 'Бренд', w: 160, visible: true },
  { id: 'category', title: 'Категория', w: 220, visible: true },
  { id: 'type', title: 'Тип', w: 180, visible: true },
  { id: 'sku', title: 'SKU', w: 130, visible: true },
  { id: 'barcode', title: 'Barcode', w: 160, visible: false },
  { id: 'product_id', title: 'Product ID', w: 120, visible: false },
  { id: 'created_at', title: 'Создан', w: 170, visible: true },
  { id: 'archived', title: 'Архив', w: 90, visible: false },
]

function fmt(dtIso: string | null): string {
  if (!dtIso) return ''
  const d = new Date(dtIso)
  if (Number.isNaN(d.getTime())) return dtIso
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${pad(d.getDate())}.${pad(d.getMonth() + 1)}.${String(d.getFullYear()).slice(-2)} ${pad(d.getHours())}:${pad(
    d.getMinutes()
  )}:${pad(d.getSeconds())}`
}

const ROW_H = 34
const OVERSCAN = 12

export default function ProductsPage() {
  const [all, setAll] = useState<any[]>([])
  const [q, setQ] = useState('')
  const [cols, setCols] = useState<Col[]>(() => {
    try {
      const saved = localStorage.getItem('ozonator-cols-v2')
      if (saved) return JSON.parse(saved)
    } catch {}
    return DEFAULT_COLS
  })

  const [scrollTop, setScrollTop] = useState(0)
  const [viewportH, setViewportH] = useState(400)
  const wrapRef = useRef<HTMLDivElement | null>(null)

  // resize
  const [resizeGuideX, setResizeGuideX] = useState<number | null>(null)
  const resizeRef = useRef<{ id: ColId; startX: number; startW: number; raf?: number } | null>(null)

  // drag reorder
  const [dropGuide, setDropGuide] = useState<{ id: ColId; side: 'left' | 'right'; x: number } | null>(null)
  const dragRef = useRef<{ draggingId: ColId; raf?: number; lastKey?: string } | null>(null)

  const visibleCols = useMemo(() => cols.filter((c) => c.visible), [cols])
  const hiddenCols = useMemo(() => cols.filter((c) => !c.visible), [cols])

  const gridTemplate = useMemo(() => visibleCols.map((c) => `${c.w}px`).join(' '), [visibleCols])
  const tableW = useMemo(() => visibleCols.reduce((s, c) => s + c.w, 0), [visibleCols])

  useEffect(() => {
    try {
      localStorage.setItem('ozonator-cols-v2', JSON.stringify(cols))
    } catch {}
  }, [cols])

  const load = async () => {
    const rows = await window.api.getProducts()
    setAll(Array.isArray(rows) ? rows : [])
  }

  useEffect(() => {
    load()
  }, [])

  useEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const onScroll = () => {
      setScrollTop(el.scrollTop)
    }
    onScroll()
    el.addEventListener('scroll', onScroll, { passive: true })
    const ro = new ResizeObserver(() => {
      setViewportH(el.clientHeight)
    })
    ro.observe(el)
    return () => {
      el.removeEventListener('scroll', onScroll as any)
      ro.disconnect()
    }
  }, [])

  const filtered = useMemo(() => {
    const qq = q.trim().toLowerCase()
    if (!qq) return all
    return all.filter((r) => {
      const hay = [
        r.offer_id,
        r.name,
        r.brand,
        r.category,
        r.type,
        r.sku,
        r.barcode,
        String(r.product_id ?? ''),
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return hay.includes(qq)
    })
  }, [all, q])

  const totalH = filtered.length * ROW_H
  const start = Math.max(0, Math.floor(scrollTop / ROW_H) - OVERSCAN)
  const end = Math.min(filtered.length, Math.ceil((scrollTop + viewportH) / ROW_H) + OVERSCAN)
  const slice = filtered.slice(start, end)

  // ===== column actions =====
  const hideCol = (id: ColId) => setCols((prev) => prev.map((c) => (c.id === id ? { ...c, visible: false } : c)))
  const showCol = (id: ColId) => setCols((prev) => prev.map((c) => (c.id === id ? { ...c, visible: true } : c)))

  // ===== resize =====
  const onResizeDown = (id: ColId, e: React.MouseEvent) => {
    e.preventDefault()
    e.stopPropagation()

    const col = cols.find((c) => c.id === id)
    if (!col) return

    resizeRef.current = { id, startX: e.clientX, startW: col.w }

    const onMove = (ev: MouseEvent) => {
      const r = resizeRef.current
      if (!r) return
      const dx = ev.clientX - r.startX
      const w = Math.max(60, r.startW + dx)

      // guide line (вниз по всей таблице)
      const wrap = wrapRef.current
      if (wrap) {
        const rect = wrap.getBoundingClientRect()
        const x = Math.min(Math.max(0, (r.startW + dx) + visibleColsBeforeWidth(r.id, cols) - wrap.scrollLeft), tableW)
        setResizeGuideX(rect.left + x)
      }

      // throttled width update
      if (r.raf) cancelAnimationFrame(r.raf)
      r.raf = requestAnimationFrame(() => {
        setCols((prev) => prev.map((c) => (c.id === id ? { ...c, w } : c)))
      })
    }

    const onUp = () => {
      resizeRef.current = null
      setResizeGuideX(null)
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }

    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }

  // ===== drag reorder =====
  const onDragStart = (id: ColId) => {
    dragRef.current = { draggingId: id }
  }

  const onDragEnd = () => {
    dragRef.current = null
    setDropGuide(null)
  }

  const onDragOverHeader = (targetId: ColId, e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault()
    const drag = dragRef.current
    if (!drag) return
    if (drag.draggingId === targetId) return

    const rect = (e.currentTarget as HTMLDivElement).getBoundingClientRect()
    const x = e.clientX - rect.left
    const side: 'left' | 'right' = x < rect.width / 2 ? 'left' : 'right'

    // анти-джиттер: обновляем только если реально изменилось (и чуть гистерезис)
    const key = `${targetId}:${side}`
    if (drag.lastKey === key) return
    drag.lastKey = key

    if (drag.raf) cancelAnimationFrame(drag.raf)
    drag.raf = requestAnimationFrame(() => {
      // x-position for guide within whole table
      const targetLeft = visibleColsBeforeWidth(targetId, cols)
      const guideX = side === 'left' ? targetLeft : targetLeft + (cols.find((c) => c.id === targetId)?.w ?? 0)
      setDropGuide({ id: targetId, side, x: guideX })
    })
  }

  const onDropHeader = (targetId: ColId, e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault()
    const drag = dragRef.current
    if (!drag) return
    const fromId = drag.draggingId
    if (fromId === targetId) return

    const rect = (e.currentTarget as HTMLDivElement).getBoundingClientRect()
    const x = e.clientX - rect.left
    const side: 'left' | 'right' = x < rect.width / 2 ? 'left' : 'right'

    setCols((prev) => reorder(prev, fromId, targetId, side))
    setDropGuide(null)
  }

  // ===== render =====
  return (
    <div className="page">
      <div className="row">
        <div className="badge">Товары: {filtered.length}</div>
        <input className="input" value={q} onChange={(e) => setQ(e.target.value)} placeholder="Поиск по любому полю..." />
        <button className="btn" onClick={load}>
          Обновить список
        </button>
      </div>

      {hiddenCols.length ? (
        <div className="hiddenBar">
          {hiddenCols.map((c) => (
            <div className="hiddenChip" key={c.id} title="Показать столбец">
              {c.title}
              <button className="hiddenChipBtn" onClick={() => showCol(c.id)}>
                +
              </button>
            </div>
          ))}
        </div>
      ) : null}

      <div className="tableWrap" ref={wrapRef}>
        <div className="tableInner" style={{ minWidth: tableW }}>
          {/* header */}
          <div className="headerRowSticky">
            <div className="gridRow" style={{ gridTemplateColumns: gridTemplate }}>
              {visibleCols.map((c) => (
                <div
                  key={c.id}
                  className="headerCell"
                  draggable
                  onDragStart={() => onDragStart(c.id)}
                  onDragEnd={onDragEnd}
                  onDragOver={(e) => onDragOverHeader(c.id, e)}
                  onDrop={(e) => onDropHeader(c.id, e)}
                  title={c.title}
                >
                  {c.title}
                  <span className="resizeHandle" onMouseDown={(e) => onResizeDown(c.id, e)} />
                  <span
                    style={{ marginLeft: 8, color: '#9ca3af', cursor: 'pointer' }}
                    onClick={(e) => {
                      e.stopPropagation()
                      hideCol(c.id)
                    }}
                    title="Скрыть столбец"
                  >
                    ×
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* body virtualized */}
          <div style={{ height: totalH, position: 'relative' }}>
            <div style={{ transform: `translateY(${start * ROW_H}px)` }}>
              {slice.map((r, idx) => {
                const i = start + idx
                const alt = i % 2 === 1
                return (
                  <div
                    key={r.offer_id + ':' + i}
                    className={`gridRow ${alt ? 'rowAlt' : ''}`}
                    style={{ gridTemplateColumns: gridTemplate }}
                  >
                    {visibleCols.map((c) => (
                      <div className="cell" key={c.id}>
                        {renderCell(c.id, r)}
                      </div>
                    ))}
                  </div>
                )
              })}
            </div>
          </div>

          {/* guides */}
          {dropGuide ? (
            <div
              className="dropGuide"
              style={{
                left: dropGuide.x - (wrapRef.current?.scrollLeft ?? 0),
              }}
            />
          ) : null}

          {resizeGuideX !== null && wrapRef.current ? (
            <div
              className="resizeGuide"
              style={{
                left: resizeGuideX - wrapRef.current.getBoundingClientRect().left,
              }}
            />
          ) : null}
        </div>
      </div>
    </div>
  )
}

function renderCell(id: ColId, r: any): string {
  switch (id) {
    case 'offer_id':
      return r.offer_id ?? ''
    case 'product_id':
      return String(r.product_id ?? '')
    case 'name':
      return r.name ?? ''
    case 'brand':
      return r.brand ?? ''
    case 'category':
      return r.category ?? ''
    case 'type':
      return r.type ?? ''
    case 'sku':
      return r.sku ?? ''
    case 'barcode':
      return r.barcode ?? ''
    case 'created_at':
      return fmt(r.created_at ?? null)
    case 'archived':
      return r.archived ? 'Да' : ''
    default:
      return ''
  }
}

function visibleColsBeforeWidth(targetId: ColId, cols: Col[]): number {
  let sum = 0
  for (const c of cols) {
    if (!c.visible) continue
    if (c.id === targetId) break
    sum += c.w
  }
  return sum
}

function reorder(cols: Col[], fromId: ColId, targetId: ColId, side: 'left' | 'right'): Col[] {
  const next = [...cols]
  const fromIdx = next.findIndex((c) => c.id === fromId)
  const targetIdx = next.findIndex((c) => c.id === targetId)
  if (fromIdx === -1 || targetIdx === -1) return cols
  const [moved] = next.splice(fromIdx, 1)
  const insertAt = side === 'left' ? targetIdx : targetIdx + 1
  next.splice(insertAt > fromIdx ? insertAt - 1 : insertAt, 0, moved)
  return next
}
