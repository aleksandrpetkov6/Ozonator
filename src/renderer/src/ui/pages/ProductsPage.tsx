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

// Консервативные лимиты для первичной авто-инициализации (при первом запуске без localStorage)
const AUTO_MAX_W: Record<string, number> = {
  offer_id: 260,
  sku: 240,
  barcode: 300,
  brand: 280,
  is_visible: 200,
  created_at: 260,
  updated_at: 260,
  type: 420,
  category: 560,
  name: 760,
}

// Более широкие лимиты именно для двойного клика по разделителю (ручная команда пользователя)
const AUTO_MAX_W_DBL: Record<string, number> = {
  offer_id: 360,
  sku: 320,
  barcode: 420,
  brand: 360,
  is_visible: 240,
  created_at: 320,
  updated_at: 320,
  type: 900,
  category: 1200,
  name: 1600,
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

function toText(v: unknown): string {
  if (v == null) return ''
  if (typeof v === 'string') return v
  if (typeof v === 'number' || typeof v === 'boolean') return String(v)
  try {
    return JSON.stringify(v)
  } catch {
    return String(v)
  }
}

function visibilityText(p: Product): string {
  const v = p.is_visible
  if (v === true || v === 1) return 'Виден'
  if (v === false || v === 0) return 'Скрыт'
  if (p.hidden_reasons && String(p.hidden_reasons).trim()) return 'Скрыт'
  return 'Неизвестно'
}

let PRODUCTS_CACHE: Product[] | null = null
let PRODUCTS_CACHE_AT = 0
let PRODUCTS_INFLIGHT: Promise<Product[]> | null = null
const PRODUCTS_CACHE_TTL_MS = 60_000

async function fetchProductsCached(force = false): Promise<Product[]> {
  const now = Date.now()
  if (!force && PRODUCTS_CACHE && (now - PRODUCTS_CACHE_AT) < PRODUCTS_CACHE_TTL_MS) return PRODUCTS_CACHE
  if (PRODUCTS_INFLIGHT) return PRODUCTS_INFLIGHT

  PRODUCTS_INFLIGHT = (async () => {
    try {
      const resp = await window.api.getProducts()
      if (resp.ok) {
        const list = (resp.products as unknown) as Product[]
        PRODUCTS_CACHE = list
        PRODUCTS_CACHE_AT = Date.now()
        return list
      }
      return PRODUCTS_CACHE ?? []
    } catch {
      return PRODUCTS_CACHE ?? []
    } finally {
      PRODUCTS_INFLIGHT = null
    }
  })()

  return PRODUCTS_INFLIGHT
}

export default function ProductsPage({ query = '', onStats }: Props) {
  const [products, setProducts] = useState<Product[]>(() => PRODUCTS_CACHE ?? [])
  const [cols, setCols] = useState<ColDef[]>(readCols)
  const [draggingId, setDraggingId] = useState<string | null>(null)
  const [dropHint, setDropHint] = useState<{ id: string; side: 'left' | 'right'; x: number } | null>(null)
  const [collapsedOpen, setCollapsedOpen] = useState(false)
  const [bodyScrollTop, setBodyScrollTop] = useState(0)
  const [bodyViewportH, setBodyViewportH] = useState(600)

  const collapsedBtnRef = useRef<HTMLButtonElement | null>(null)
  const collapsedMenuRef = useRef<HTMLDivElement | null>(null)
  const scrollTopRafRef = useRef<number | null>(null)
  const lastScrollTopRef = useRef(0)

  const resizingRef = useRef<{
    id: string
    startX: number
    startW: number
    startRight: number
    startTableW: number
    colIdx: number
    headCol?: HTMLTableColElement | null
    bodyCol?: HTMLTableColElement | null
  } | null>(null)

  const headInnerRef = useRef<HTMLDivElement | null>(null)
  const bodyInnerRef = useRef<HTMLDivElement | null>(null)
  const headTableRef = useRef<HTMLTableElement | null>(null)
  const bodyTableRef = useRef<HTMLTableElement | null>(null)
  const resizeIndicatorRef = useRef<HTMLDivElement | null>(null)
  const measureCanvasRef = useRef<HTMLCanvasElement | null>(null)
  const didAutoInitRef = useRef(false)

  const hasStoredCols = useMemo(() => {
    try {
      return !!localStorage.getItem('ozonator_cols')
    } catch {
      return true
    }
  }, [])

  async function load(force = false) {
    const list = await fetchProductsCached(force)
    if (Array.isArray(list)) setProducts(list)
  }

  useEffect(() => { void load() }, [])

  useEffect(() => {
    const onUpdated = () => { void load(true) }
    window.addEventListener('ozon:products-updated', onUpdated)
    return () => window.removeEventListener('ozon:products-updated', onUpdated)
  }, [])

  useEffect(() => {
    const id = window.setTimeout(() => saveCols(cols), 250)
    return () => window.clearTimeout(id)
  }, [cols])

  const visibleCols = useMemo(() => cols.filter(c => c.visible), [cols])
  const hiddenCols = useMemo(() => cols.filter(c => !c.visible), [cols])

  useEffect(() => {
    if (!collapsedOpen) return

    const onDown = (ev: MouseEvent) => {
      const t = ev.target as Node | null
      if (!t) return
      if (collapsedMenuRef.current?.contains(t)) return
      if (collapsedBtnRef.current?.contains(t)) return
      setCollapsedOpen(false)
    }

    const onKey = (ev: KeyboardEvent) => {
      if (ev.key === 'Escape') setCollapsedOpen(false)
    }

    window.addEventListener('mousedown', onDown)
    window.addEventListener('keydown', onKey)
    return () => {
      window.removeEventListener('mousedown', onDown)
      window.removeEventListener('keydown', onKey)
    }
  }, [collapsedOpen])

  useEffect(() => {
    if (collapsedOpen && hiddenCols.length === 0) setCollapsedOpen(false)
  }, [collapsedOpen, hiddenCols.length])

  // Для поиска не учитываем ширины столбцов (чтобы ресайз не тормозил)
  const visibleSearchKey = useMemo(
    () => cols.map(c => `${c.id}:${c.visible ? 1 : 0}`).join('|'),
    [cols],
  )

  const visibleSearchCols = useMemo(
    () => cols.filter(c => c.visible).map(c => c.id),
    [visibleSearchKey],
  )

  const filtered = useMemo(() => {
    const q = String(query ?? '').trim().toLowerCase()
    if (!q) return products

    return products.filter((p) => {
      const hay = visibleSearchCols
        .map((colId) => {
          if (colId === 'archived') return ''
          if (colId === 'is_visible') return visibilityText(p)
          if (colId === 'brand') return (p.brand && String(p.brand).trim()) ? String(p.brand).trim() : 'Не указан'
          if (colId === 'name') return (p.name && String(p.name).trim()) ? String(p.name).trim() : 'Без названия'
          return toText((p as Record<string, unknown>)[colId as string])
        })
        .join(' ')
        .toLowerCase()

      return hay.includes(q)
    })
  }, [products, query, visibleSearchKey])

  useEffect(() => {
    onStats?.({ total: products.length, filtered: filtered.length })
  }, [products.length, filtered.length, onStats])

  function hideCol(id: string) {
    setCols(prev => prev.map(c => String(c.id) === id ? { ...c, visible: false } : c))
  }

  function showCol(id: string) {
    setCols(prev => prev.map(c => String(c.id) === id ? { ...c, visible: true } : c))
  }

  const headScrollRef = useRef<HTMLDivElement | null>(null)
  const bodyScrollRef = useRef<HTMLDivElement | null>(null)
  const scrollSyncLockRef = useRef(false)
  const headerRowRef = useRef<HTMLTableRowElement | null>(null)

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
      // если линия «дрожит» на 3px — фиксируем положение, но обновляем цель
      const stableX = (Math.abs(next.x - prev.x) <= 3) ? prev.x : next.x
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

  const tableWidth = useMemo(
    () => Math.max(1, visibleCols.reduce((s, c) => s + c.w, 0)),
    [visibleCols],
  )

  function startResize(e: React.MouseEvent, colId: string) {
    e.preventDefault()
    e.stopPropagation()

    const col = cols.find(c => String(c.id) === colId)
    if (!col) return

    const head = headScrollRef.current
    const row = headerRowRef.current
    const cell = row?.querySelector(`th[data-col-id="${colId}"]`) as HTMLTableCellElement | null
    if (!head || !cell) return

    const colIdx = visibleCols.findIndex(c => String(c.id) === colId)
    if (colIdx < 0) return

    const startRight = cell.offsetLeft + cell.offsetWidth
    const headCols = headTableRef.current?.querySelectorAll('colgroup col') ?? []
    const bodyCols = bodyTableRef.current?.querySelectorAll('colgroup col') ?? []
    const headCol = (headCols[colIdx] as HTMLTableColElement | undefined) ?? null
    const bodyCol = (bodyCols[colIdx] as HTMLTableColElement | undefined) ?? null

    resizingRef.current = {
      id: colId,
      startX: e.clientX,
      startW: col.w,
      startRight,
      startTableW: tableWidth,
      colIdx,
      headCol,
      bodyCol,
    }

    const indicator = resizeIndicatorRef.current
    if (indicator) {
      indicator.style.display = 'block'
      indicator.style.left = `${Math.round(startRight - (head.scrollLeft ?? 0))}px`
    }

    const prevCursor = document.body.style.cursor
    const prevSelect = document.body.style.userSelect
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'

    let raf: number | null = null
    let pendingDx = 0
    let lastW = col.w

    const flush = () => {
      raf = null
      const r = resizingRef.current
      if (!r) return

      const w = Math.max(AUTO_MIN_W, Math.round(r.startW + pendingDx))
      const delta = w - r.startW
      const newTableW = Math.max(1, Math.round(r.startTableW + delta))

      if (w !== lastW) {
        lastW = w
        if (r.headCol) r.headCol.style.width = `${w}px`
        if (r.bodyCol) r.bodyCol.style.width = `${w}px`
        if (headInnerRef.current) headInnerRef.current.style.width = `${newTableW}px`
        if (bodyInnerRef.current) bodyInnerRef.current.style.width = `${newTableW}px`
        if (headTableRef.current) headTableRef.current.style.width = `${newTableW}px`
        if (bodyTableRef.current) bodyTableRef.current.style.width = `${newTableW}px`
      }

      const sl = headScrollRef.current?.scrollLeft ?? 0
      if (indicator) indicator.style.left = `${Math.round(r.startRight + delta - sl)}px`
    }

    const schedule = () => {
      if (raf != null) return
      raf = window.requestAnimationFrame(flush)
    }

    const onMove = (ev: MouseEvent) => {
      const r = resizingRef.current
      if (!r) return
      pendingDx = ev.clientX - r.startX
      schedule()
    }

    const onUp = () => {
      if (raf != null) {
        window.cancelAnimationFrame(raf)
        raf = null
      }

      const r = resizingRef.current
      resizingRef.current = null

      document.body.style.cursor = prevCursor
      document.body.style.userSelect = prevSelect
      if (indicator) indicator.style.display = 'none'

      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)

      if (!r) return
      const finalW = Math.max(AUTO_MIN_W, Math.round(r.startW + pendingDx))
      setCols(prev => prev.map(c => String(c.id) === r.id ? { ...c, w: finalW } : c))
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
    const v = (p as Record<string, unknown>)[colId as string]
    return { text: (v == null || v === '') ? '-' : String(v) }
  }

  function measureTextWidth(text: string, styleSource?: HTMLElement | null): number {
    const canvas = measureCanvasRef.current ?? (measureCanvasRef.current = document.createElement('canvas'))
    const ctx = canvas.getContext('2d')
    if (!ctx) return text.length * 7

    const fallback = window.getComputedStyle(document.body)
    const cs = styleSource ? window.getComputedStyle(styleSource) : fallback

    const font = cs.font && cs.font !== 'normal' ? cs.font : `${cs.fontWeight || '400'} ${cs.fontSize || '13px'} ${cs.fontFamily || 'system-ui'}`
    ctx.font = font

    let width = ctx.measureText(text).width
    const letterSpacing = Number.parseFloat(cs.letterSpacing || '0')
    if (Number.isFinite(letterSpacing) && letterSpacing > 0 && text.length > 1) {
      width += letterSpacing * (text.length - 1)
    }
    return width
  }

  function getCellString(p: Product, colId: ColDef['id']): string {
    if (colId === 'archived') return ''
    if (colId === 'is_visible') return visibilityText(p)
    if (colId === 'brand') return (p.brand && String(p.brand).trim()) ? String(p.brand).trim() : 'Не указан'
    if (colId === 'name') return (p.name && String(p.name).trim()) ? String(p.name).trim() : 'Без названия'
    return toText((p as Record<string, unknown>)[colId as string])
  }

  function measureColumnContentWidth(colId: string, rows: Product[]): number {
    const th = headerRowRef.current?.querySelector(`th[data-col-id="${colId}"]`) as HTMLTableCellElement | null
    const anyTd = bodyTableRef.current?.querySelector(`td[data-col-id="${colId}"]`) as HTMLTableCellElement | null
    const styleSource = anyTd ?? th

    const headerLabelEl = th?.querySelector('.thTitle') as HTMLElement | null
    const headerLabelText = headerLabelEl?.textContent?.trim() || cols.find(c => String(c.id) === colId)?.title || ''
    let maxText = measureTextWidth(headerLabelText, headerLabelEl ?? th ?? styleSource)

    const sample = rows.length > 5000 ? rows.slice(0, 5000) : rows
    const col = cols.find(c => String(c.id) === colId)
    if (!col) return maxText

    for (const p of sample) {
      const s = getCellString(p, col.id)
      if (!s) continue
      const w = measureTextWidth(s, anyTd ?? styleSource)
      if (w > maxText) maxText = w
    }

    const tdCS = anyTd ? window.getComputedStyle(anyTd) : null
    const thCS = th ? window.getComputedStyle(th) : null
    const padX = [tdCS, thCS]
      .filter(Boolean)
      .map((cs) => {
        const c = cs as CSSStyleDeclaration
        const pl = Number.parseFloat(c.paddingLeft || '0') || 0
        const pr = Number.parseFloat(c.paddingRight || '0') || 0
        return pl + pr
      })
      .reduce((m, v) => Math.max(m, v), 0)

    // запас под кнопки/иконки в заголовке и под погрешность измерения
    const headerControlsReserve = 34
    return maxText + Math.max(AUTO_PAD, padX) + headerControlsReserve
  }

  function autoSizeColumn(colId: string, rows: Product[]) {
    const col = cols.find(c => String(c.id) === colId)
    if (!col) return

    const conservativeCap = AUTO_MAX_W[colId] ?? 480
    const userCap = AUTO_MAX_W_DBL[colId] ?? Math.max(conservativeCap, 900)
    const measured = measureColumnContentWidth(colId, rows)
    const nextW = Math.max(AUTO_MIN_W, Math.min(userCap, Math.round(measured)))

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
      const cap = AUTO_MAX_W[String(c.id)] ?? 480
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

  // Виртуализация строк: резко снижает лаги при переключении вкладок и при ресайзе колонок
  useEffect(() => {
    const body = bodyScrollRef.current
    if (!body) return

    const updateViewport = () => setBodyViewportH(body.clientHeight || 0)
    updateViewport()

    const ro = new ResizeObserver(() => updateViewport())
    ro.observe(body)
    return () => ro.disconnect()
  }, [])

  useEffect(() => {
    const body = bodyScrollRef.current
    if (!body) return

    const onScroll = () => {
      lastScrollTopRef.current = body.scrollTop
      if (scrollTopRafRef.current != null) return

      scrollTopRafRef.current = window.requestAnimationFrame(() => {
        scrollTopRafRef.current = null
        setBodyScrollTop(lastScrollTopRef.current)
      })
    }

    body.addEventListener('scroll', onScroll, { passive: true })
    onScroll()

    return () => {
      body.removeEventListener('scroll', onScroll)
      if (scrollTopRafRef.current != null) {
        window.cancelAnimationFrame(scrollTopRafRef.current)
        scrollTopRafRef.current = null
      }
    }
  }, [])

  const ROW_H = 28
  const OVERSCAN = 12
  const totalRows = filtered.length
  const viewH = bodyViewportH || 600
  const startRow = Math.max(0, Math.floor(bodyScrollTop / ROW_H) - OVERSCAN)
  const endRow = Math.min(totalRows, startRow + Math.ceil(viewH / ROW_H) + (OVERSCAN * 2))
  const visibleRows = useMemo(() => filtered.slice(startRow, endRow), [filtered, startRow, endRow])
  const topSpace = startRow * ROW_H
  const bottomSpace = Math.max(0, (totalRows - endRow) * ROW_H)

  return (
    <div className="productsPage" style={{ display: 'flex', flexDirection: 'column', minHeight: 0, height: '100%' }}>
      {hiddenCols.length > 0 && (
        <div className="collapsedColsBar" style={{ position: 'relative', marginBottom: 8 }}>
          <button
            ref={collapsedBtnRef}
            type="button"
            className="collapsedColsBtn"
            onMouseDown={(e) => e.preventDefault()}
            onClick={() => setCollapsedOpen(v => !v)}
          >
            + Скрытые столбцы ({hiddenCols.length})
          </button>

          {collapsedOpen && (
            <div
              ref={collapsedMenuRef}
              className="collapsedColsMenu"
              style={{
                position: 'absolute',
                zIndex: 20,
                top: '100%',
                left: 0,
                marginTop: 4,
                minWidth: 220,
                maxHeight: 260,
                overflow: 'auto',
                background: 'var(--bg-elevated, #fff)',
                border: '1px solid var(--border, #ddd)',
                borderRadius: 8,
                boxShadow: '0 8px 24px rgba(0,0,0,0.12)',
                padding: 6,
              }}
            >
              {hiddenCols.map(c => (
                <button
                  key={`show-${String(c.id)}`}
                  type="button"
                  className="collapsedColItem"
                  style={{ display: 'block', width: '100%', textAlign: 'left' }}
                  onMouseDown={(e) => e.preventDefault()}
                  onClick={() => {
                    showCol(String(c.id))
                    setCollapsedOpen(false)
                  }}
                >
                  {c.title}
                </button>
              ))}
            </div>
          )}
        </div>
      )}

      <div className="productsTableWrap" style={{ position: 'relative', minHeight: 0, display: 'flex', flexDirection: 'column', flex: 1 }}>
        {dropHint && (
          <div
            className="dropHintLine"
            style={{
              position: 'absolute',
              top: 0,
              bottom: 0,
              left: dropHint.x,
              width: 2,
              background: 'var(--accent, #2f7cff)',
              pointerEvents: 'none',
              zIndex: 5,
              transform: `translateX(-${headScrollRef.current?.scrollLeft ?? 0}px)`,
            }}
          />
        )}

        <div
          ref={headScrollRef}
          className="productsHeadScroll"
          style={{ overflowX: 'auto', overflowY: 'hidden', position: 'relative' }}
        >
          <div ref={headInnerRef} style={{ width: tableWidth, minWidth: tableWidth, position: 'relative' }}>
            <div
              ref={resizeIndicatorRef}
              style={{
                display: 'none',
                position: 'absolute',
                top: 0,
                bottom: 0,
                width: 2,
                background: 'var(--accent, #2f7cff)',
                pointerEvents: 'none',
                zIndex: 4,
              }}
            />

            <table
              ref={headTableRef}
              className="productsTable productsHeadTable"
              style={{ width: tableWidth, tableLayout: 'fixed', borderCollapse: 'collapse' }}
            >
              <colgroup>
                {visibleCols.map(c => (
                  <col key={`head-col-${String(c.id)}`} style={{ width: c.w }} />
                ))}
              </colgroup>

              <thead>
                <tr
                  ref={headerRowRef}
                  onDragOver={onDragOverHeader}
                  onDrop={onDrop}
                >
                  {visibleCols.map(c => {
                    const id = String(c.id)
                    return (
                      <th key={`th-${id}`} data-col-id={id} className="thCell" style={{ position: 'relative' }}>
                        <div
                          draggable
                          onDragStart={(e) => onDragStart(e, id)}
                          onDragEnd={onDragEnd}
                          className={`thDraggable ${draggingId === id ? 'thDragging' : ''}`.trim()}
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 6,
                            minWidth: 0,
                            paddingRight: 12,
                            userSelect: 'none',
                          }}
                        >
                          <button
                            type="button"
                            className="thHideBtn"
                            onMouseDown={(e) => e.preventDefault()}
                            onClick={() => hideCol(id)}
                            title="Скрыть"
                            style={{ flex: '0 0 auto' }}
                          >
                            −
                          </button>
                          <span className="thTitle" style={{ minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {c.title}
                          </span>
                        </div>

                        <div
                          className="resizeHandle"
                          role="separator"
                          aria-orientation="vertical"
                          onMouseDown={(e) => startResize(e, id)}
                          onDoubleClick={(e) => {
                            e.preventDefault()
                            e.stopPropagation()
                            autoSizeColumn(id, filtered)
                          }}
                          style={{
                            position: 'absolute',
                            top: 0,
                            right: -3,
                            bottom: 0,
                            width: 8,
                            cursor: 'col-resize',
                            zIndex: 2,
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

        <div
          ref={bodyScrollRef}
          className="productsBodyScroll"
          style={{ overflow: 'auto', minHeight: 0, flex: 1 }}
        >
          <div ref={bodyInnerRef} style={{ width: tableWidth, minWidth: tableWidth }}>
            <table
              ref={bodyTableRef}
              className="productsTable productsBodyTable"
              style={{ width: tableWidth, tableLayout: 'fixed', borderCollapse: 'collapse' }}
            >
              <colgroup>
                {visibleCols.map(c => (
                  <col key={`body-col-${String(c.id)}`} style={{ width: c.w }} />
                ))}
              </colgroup>

              <tbody>
                {topSpace > 0 && (
                  <tr aria-hidden="true" className="tbodySpacer">
                    <td colSpan={Math.max(1, visibleCols.length)} style={{ height: topSpace, padding: 0, border: 0 }} />
                  </tr>
                )}

                {visibleRows.map((p) => (
                  <tr key={`${p.offer_id}::${p.sku ?? ''}::${p.barcode ?? ''}`} className="productsRow" style={{ height: ROW_H }}>
                    {visibleCols.map(c => {
                      const id = String(c.id)
                      const { text, title } = cellText(p, c.id)
                      return (
                        <td
                          key={`${p.offer_id}-${id}`}
                          data-col-id={id}
                          title={title ?? text}
                          className="productsCell"
                          style={{
                            minWidth: 0,
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                          }}
                        >
                          {text}
                        </td>
                      )
                    })}
                  </tr>
                ))}

                {bottomSpace > 0 && (
                  <tr aria-hidden="true" className="tbodySpacer">
                    <td colSpan={Math.max(1, visibleCols.length)} style={{ height: bottomSpace, padding: 0, border: 0 }} />
                  </tr>
                )}

                {filtered.length === 0 && (
                  <tr className="productsEmptyRow">
                    <td colSpan={Math.max(1, visibleCols.length)} className="productsEmptyCell" style={{ padding: '18px 12px' }}>
                      Ничего не найдено.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}
