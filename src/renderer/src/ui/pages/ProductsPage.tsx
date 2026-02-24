import React, { useEffect, useMemo, useRef, useState } from 'react'

type Product = {
  offer_id: string
  sku?: string | null
  barcode?: string | null
  brand?: string | null
  category?: string | null
  type?: string | null
  name?: string | null
  photo_url?: string | null
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
  { id: 'photo_url', title: 'Фото', w: 80, visible: true },
  { id: 'name', title: 'Наименование', w: 320, visible: true },
  { id: 'brand', title: 'Бренд', w: 180, visible: true },
  { id: 'sku', title: 'SKU', w: 140, visible: true },
  { id: 'barcode', title: 'Штрихкод', w: 170, visible: true },
  { id: 'type', title: 'Категория', w: 280, visible: true },
  { id: 'is_visible', title: 'Видимость', w: 140, visible: true },
  { id: 'hidden_reasons', title: 'Причина скрытия', w: 320, visible: true },
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
  hidden_reasons: 440,
  created_at: 240,
  updated_at: 240,
  type: 380,
  name: 460,
  photo_url: 80,
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

function formatDateTimeRu(v: any): string {
  if (v == null || v === '') return ''

  const d = (v instanceof Date) ? v : new Date(v)
  if (Number.isNaN(d.getTime())) return String(v)

  const dd = String(d.getDate()).padStart(2, '0')
  const mm = String(d.getMonth() + 1).padStart(2, '0')
  const yy = String(d.getFullYear()).slice(-2)
  const hh = String(d.getHours()).padStart(2, '0')
  const mi = String(d.getMinutes()).padStart(2, '0')
  const ss = String(d.getSeconds()).padStart(2, '0')

  return `${dd}.${mm}.${yy} ${hh}.${mi}.${ss}`
}

const VISIBILITY_REASON_MAP_RU: Record<string, string> = {
  double_without_merger_offer: 'Дубль товара',
  image_absent_with_shipment: 'Нет фото в карточке товара',
  image_absent: 'Нет фото в карточке товара',
  no_stock: 'Нет остатков',
  empty_stock: 'Нет остатков',
  archived: 'Товар в архиве',
  disabled_by_seller: 'Скрыт продавцом',
  blocked: 'Заблокирован',
  banned: 'Заблокирован',
}

function visibilityReasonText(v: any): string {
  if (v == null || v === '') return '-'
  const mapOne = (s: string) => {
    const key = s.trim()
    if (!key) return ''
    if (key === 'Нет изображения при наличии отгрузок') return 'Нет фото в карточке товара'
    if (key === 'Дубль товара без объединения карточек') return 'Дубль товара'
    if (VISIBILITY_REASON_MAP_RU[key]) return VISIBILITY_REASON_MAP_RU[key]
    if (/^[a-z0-9_]+$/i.test(key)) return 'Другая причина скрытия'
    return key
  }
  let raw: any = v
  if (typeof raw === 'string') {
    const s = raw.trim()
    if (!s) return '-'
    try { raw = JSON.parse(s) } catch { raw = s }
  }
  const out: string[] = []
  const pushVal = (val: any) => {
    if (val == null) return
    if (Array.isArray(val)) { for (const x of val) pushVal(x); return }
    if (typeof val === 'object') {
      const cand = (val.reason ?? val.code ?? val.value ?? val.name)
      if (cand != null) pushVal(String(cand))
      return
    }
    const s = String(val)
    for (const part of s.split(',')) {
      const mapped = mapOne(part)
      if (mapped) out.push(mapped)
    }
  }
  pushVal(raw)
  const uniq = Array.from(new Set(out))
  return uniq.length ? uniq.join(', ') : '-'
}

function visibilityText(p: Product): string {
  const v = p.is_visible
  if (v === true || v === 1) return 'Виден'
  if (v === false || v === 0) return 'Скрыт'
  if (p.hidden_reasons && String(p.hidden_reasons).trim()) return 'Скрыт'
  return 'Виден'
}

let PRODUCTS_CACHE: Product[] | null = null
let PRODUCTS_CACHE_AT = 0
let PRODUCTS_INFLIGHT: Promise<Product[] | null> | null = null
const PRODUCTS_CACHE_TTL_MS = 60_000

async function fetchProductsCached(force = false): Promise<Product[] | null> {
  const now = Date.now()
  if (!force && PRODUCTS_CACHE && (now - PRODUCTS_CACHE_AT) < PRODUCTS_CACHE_TTL_MS) return PRODUCTS_CACHE
  if (PRODUCTS_INFLIGHT) return PRODUCTS_INFLIGHT

  PRODUCTS_INFLIGHT = (async () => {
    try {
      const resp = await window.api.getProducts()
      if (resp.ok) {
        const list = (resp.products as any) as Product[]
        PRODUCTS_CACHE = list
        PRODUCTS_CACHE_AT = Date.now()
        return list
      }
      return PRODUCTS_CACHE
    } catch {
      return PRODUCTS_CACHE
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
  const [bodyWindowAnchorRow, setBodyWindowAnchorRow] = useState(0)
  const [bodyViewportH, setBodyViewportH] = useState(600)

  const collapsedBtnRef = useRef<HTMLButtonElement | null>(null)
  const collapsedMenuRef = useRef<HTMLDivElement | null>(null)

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
  const photoHoverTimerRef = useRef<number | null>(null)
  const photoHoverLastPosRef = useRef<{ x: number; y: number }>({ x: 0, y: 0 })
  const photoHoverPendingRef = useRef<{ url: string; title: string } | null>(null)
  const [photoPreview, setPhotoPreview] = useState<{ url: string; title: string; x: number; y: number } | null>(null)

  const hasStoredCols = useMemo(() => {
    try { return !!localStorage.getItem('ozonator_cols') } catch { return true }
  }, [])

  async function load(force = false) {
    const list = await fetchProductsCached(force)
    if (Array.isArray(list)) setProducts(list)
  }

  useEffect(() => {
    load()
  }, [])

  useEffect(() => {
    const onUpdated = () => load(true)
    window.addEventListener('ozon:products-updated', onUpdated)
    return () => window.removeEventListener('ozon:products-updated', onUpdated)
  }, [])

  useEffect(() => {
    const clearPreviewNow = () => {
      if (photoHoverTimerRef.current != null) {
        window.clearTimeout(photoHoverTimerRef.current)
        photoHoverTimerRef.current = null
      }
      photoHoverPendingRef.current = null
      setPhotoPreview(prev => (prev ? null : prev))
    }

    window.addEventListener('scroll', clearPreviewNow, true)
    return () => {
      window.removeEventListener('scroll', clearPreviewNow, true)
      clearPreviewNow()
    }
  }, [])

  useEffect(() => {
    const id = window.setTimeout(() => saveCols(cols), 250)
    return () => window.clearTimeout(id)
  }, [cols])

  const visibleCols = useMemo(() => cols.filter(c => c.visible), [cols])
  const rowH = useMemo(() => (visibleCols.some(c => c.id === 'photo_url') ? 60 : 28), [visibleCols])
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
    [cols]
  )

  const visibleSearchCols = useMemo(
    () => cols.filter(c => c.visible).map(c => c.id),
    [visibleSearchKey]
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
          if (colId === 'photo_url') return ''
          return toText((p as any)[colId])
        })
        .join(' ')
        .toLowerCase()

      return hay.includes(q)
    })
  }, [products, query, visibleSearchKey])


  useEffect(() => {
    onStats?.({ total: products.length, filtered: filtered.length })
  }, [products.length, filtered.length, onStats])

  function clearPhotoHoverTimer() {
    if (photoHoverTimerRef.current != null) {
      window.clearTimeout(photoHoverTimerRef.current)
      photoHoverTimerRef.current = null
    }
  }

  function getPhotoPreviewPos(clientX: number, clientY: number) {
    const size = 200
    const margin = 12
    const offsetX = 16
    const offsetY = 16

    let x = clientX + offsetX
    let y = clientY - size - offsetY

    const maxX = Math.max(margin, window.innerWidth - size - margin)
    const maxY = Math.max(margin, window.innerHeight - size - margin)

    if (y < margin) y = Math.min(maxY, clientY + offsetY)
    if (x > maxX) x = maxX
    if (y > maxY) y = maxY
    if (x < margin) x = margin
    if (y < margin) y = margin

    return { x, y }
  }

  function hidePhotoPreview() {
    clearPhotoHoverTimer()
    photoHoverPendingRef.current = null
    setPhotoPreview(prev => (prev ? null : prev))
  }

  function onPhotoMouseEnter(e: React.MouseEvent, url: string, title: string) {
    photoHoverLastPosRef.current = { x: e.clientX, y: e.clientY }
    clearPhotoHoverTimer()
    photoHoverPendingRef.current = { url, title }
    photoHoverTimerRef.current = window.setTimeout(() => {
      const pending = photoHoverPendingRef.current
      if (!pending) return
      const { x, y } = getPhotoPreviewPos(photoHoverLastPosRef.current.x, photoHoverLastPosRef.current.y)
      setPhotoPreview({ ...pending, x, y })
      photoHoverTimerRef.current = null
    }, 1000)
  }

  function onPhotoMouseMove(e: React.MouseEvent) {
    photoHoverLastPosRef.current = { x: e.clientX, y: e.clientY }
    const pos = getPhotoPreviewPos(e.clientX, e.clientY)
    setPhotoPreview(prev => (prev ? { ...prev, ...pos } : prev))
  }

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
  function startResize(e: React.MouseEvent, colId: string) {
    e.preventDefault()
    e.stopPropagation()

    const col = cols.find(c => String(c.id) === colId)
    if (!col) return

    const head = headScrollRef.current
    const row = headerRowRef.current
    const cell = row?.querySelector<HTMLElement>(`th[data-col-id="${colId}"]`)
    if (!head || !cell) return

    const colIdx = visibleCols.findIndex(c => String(c.id) === colId)
    if (colIdx < 0) return

    const startRight = cell.offsetLeft + cell.offsetWidth

    const headCols = headTableRef.current?.querySelectorAll('colgroup col') ?? []
    const bodyCols = bodyTableRef.current?.querySelectorAll('colgroup col') ?? []
    const headCol = (headCols[colIdx] as any) as HTMLTableColElement | null
    const bodyCol = (bodyCols[colIdx] as any) as HTMLTableColElement | null

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
        if (r.headCol) (r.headCol as any).style.width = `${w}px`
        if (r.bodyCol) (r.bodyCol as any).style.width = `${w}px`

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
    if (colId === 'photo_url') return { text: '', title: (p.photo_url && String(p.photo_url).trim()) ? String(p.photo_url).trim() : 'Нет фото' }

    if (colId === 'is_visible') {
      const txt = visibilityText(p)
      const rs = visibilityReasonText(p.hidden_reasons)
      const title = rs !== '-' ? rs : undefined
      return { text: txt, title }
    }

    const v = (p as any)[colId]
    if (colId === 'hidden_reasons') {
      const rs = visibilityReasonText(v)
      return { text: rs, title: rs !== '-' ? rs : undefined }
    }
    if (colId === 'created_at' || colId === 'updated_at') {
      const f = formatDateTimeRu(v)
      return { text: f || '-', title: (v == null || v === '') ? undefined : String(v) }
    }
    return { text: (v == null || v === '') ? '-' : String(v) }
  }

  function measureTextWidth(text: string, kind: 'cell' | 'header' = 'cell'): number {
    const canvas = measureCanvasRef.current ?? (measureCanvasRef.current = document.createElement('canvas'))
    const ctx = canvas.getContext('2d')
    if (!ctx) return text.length * 7

    const probe = document.querySelector(kind === 'header' ? '.thTitle' : '.cellText') as HTMLElement | null
    const cs = window.getComputedStyle(probe ?? document.body)
    const fontStyle = cs.fontStyle || 'normal'
    const fontVariant = cs.fontVariant || 'normal'
    const fontWeight = cs.fontWeight || '400'
    const fontSize = cs.fontSize || '13px'
    const lineHeight = cs.lineHeight && cs.lineHeight !== 'normal' ? `/${cs.lineHeight}` : ''
    const fontFamily = cs.fontFamily || 'system-ui'
    ctx.font = `${fontStyle} ${fontVariant} ${fontWeight} ${fontSize}${lineHeight} ${fontFamily}`

    return ctx.measureText(text).width
  }

  function getCellString(p: Product, colId: ColDef['id']): string {
    if (colId === 'archived') return ''
    if (colId === 'is_visible') return visibilityText(p)
    if (colId === 'hidden_reasons') return visibilityReasonText((p as any)[colId])
    if (colId === 'brand') return (p.brand && String(p.brand).trim()) ? String(p.brand).trim() : 'Не указан'
    if (colId === 'name') return (p.name && String(p.name).trim()) ? String(p.name).trim() : 'Без названия'
    if (colId === 'photo_url') return ''
    if (colId === 'created_at' || colId === 'updated_at') return formatDateTimeRu((p as any)[colId])
    return toText((p as any)[colId])
  }

  function autoSizeColumn(colId: string, rows: Product[], mode: 'default' | 'fit' = 'default') {
    const col = cols.find(c => String(c.id) === colId)
    if (!col) return
    if (colId === 'photo_url') {
      setCols(prev => prev.map(c => String(c.id) === colId ? { ...c, w: 120 } : c))
      return
    }

    const headerExtra = 44 // кнопка скрытия + внутренние отступы в th
    const baseCap = AUTO_MAX_W[colId] ?? 320
    const cap = mode === 'fit' ? 4000 : baseCap

    let max = measureTextWidth(col.title, 'header') + headerExtra
    const sample = mode === 'fit' ? rows : (rows.length > 1600 ? rows.slice(0, 1600) : rows)
    for (const p of sample) {
      const s = getCellString(p, col.id)
      if (!s) continue
      const w = measureTextWidth(s, 'cell')
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
      if (String(c.id) === 'photo_url') return { ...c, w: 80 }
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

  const tableWidth = useMemo(() => Math.max(1, visibleCols.reduce((s, c) => s + c.w, 0)), [visibleCols])

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


  // Виртуализация строк: резко снижает лаги при переключении вкладок и при ресайзе колонок
  useEffect(() => {
    const body = bodyScrollRef.current
    if (!body) return

    const updateViewport = () => setBodyViewportH(body.clientHeight || 0)
    updateViewport()

    // eslint-disable-next-line no-undef
    const ro = new ResizeObserver(() => updateViewport())
    ro.observe(body)
    return () => ro.disconnect()
  }, [])

  useEffect(() => {
    const body = bodyScrollRef.current
    if (!body) return

    const viewH = bodyViewportH || 600
    const viewportRows = Math.max(1, Math.ceil(viewH / rowH))
    const windowStepRows = Math.max(12, Math.ceil(viewportRows / 2))

    const onScroll = () => {
      const nextTop = Math.max(0, body.scrollTop || 0)
      const anchorRow = Math.floor(nextTop / rowH)
      const nextWindowAnchorRow = Math.floor(anchorRow / windowStepRows) * windowStepRows
      setBodyWindowAnchorRow((prev) => (prev === nextWindowAnchorRow ? prev : nextWindowAnchorRow))
    }

    body.addEventListener('scroll', onScroll, { passive: true })
    onScroll()

    return () => {
      body.removeEventListener('scroll', onScroll)
    }
  }, [bodyViewportH, rowH])

  const totalRows = filtered.length
  const viewH = bodyViewportH || 600
  const viewportRows = Math.max(1, Math.ceil(viewH / rowH))
  const OVERSCAN = Math.max(64, viewportRows * 3)
  const WINDOW_STEP_ROWS = Math.max(12, Math.ceil(viewportRows / 2))
  const startRow = Math.max(0, bodyWindowAnchorRow - OVERSCAN)
  const endRow = Math.min(totalRows, bodyWindowAnchorRow + viewportRows + (OVERSCAN * 2) + WINDOW_STEP_ROWS)

  const visibleRows = useMemo(() => filtered.slice(startRow, endRow), [filtered, startRow, endRow])
  const topSpace = startRow * rowH
  const bottomSpace = Math.max(0, (totalRows - endRow) * rowH)

  const getHeaderTitleText = (c: ColDef): string => {
    if (String(c.id) === 'offer_id') return `${c.title} ${totalRows}`
    return c.title
  }

  return (
    <div className="productsCard">
      <div className="productsTableArea">
        <div className="tableWrap" style={{ marginTop: 0, position: 'relative' }}>
          <div className="resizeIndicator" ref={resizeIndicatorRef} style={{ display: 'none' }} />
          {hiddenCols.length > 0 && (
            <div className="collapsedCorner" style={{ position: 'absolute', top: 6, right: 6, zIndex: 5 }}>
              <button
                type="button"
                className="colToggle colTogglePlus"
                ref={collapsedBtnRef}
                title="Показать скрытый столбец"
                aria-haspopup="menu"
                aria-expanded={collapsedOpen}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => setCollapsedOpen(v => !v)}
              >
                +
              </button>

              {collapsedOpen && (
                <div
                  className="collapsedMenu"
                  ref={collapsedMenuRef}
                  role="menu"
                  style={{ position: 'absolute', top: 0, right: 'calc(100% + 6px)', left: 'auto', zIndex: 6 }}
                >
                  {hiddenCols.map(c => (
                    <button
                      type="button"
                      key={String(c.id)}
                      className="collapsedMenuItem"
                      role="menuitem"
                      style={{ padding: '6px 10px', lineHeight: 1.1 }}
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => {
                        showCol(String(c.id))
                      }}
                    >
                      {c.title}
                    </button>
                  ))}
                </div>
              )}
            </div>
          )}
          <div className="tableHeadX" ref={headScrollRef}>
            <div className="tableWrapY tableHeadInner" ref={headInnerRef} style={{ width: tableWidth }}>
              {dropHint && <div className="dropIndicator" style={{ left: dropHint.x }} />}
              <table ref={headTableRef} className="table tableFixed tableHead" style={{ width: tableWidth }}>
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
                          data-col-id={id}
                          draggable
                          onDragStart={(e) => onDragStart(e, id)}
                          onDragEnd={onDragEnd}
                          className={`thDraggable ${draggingId === id ? 'thDragging' : ''}`.trim()}
                        >
                          <div className="thInner">
                            <button className="colToggle" onClick={() => hideCol(id)} title="Скрыть">−</button>
                            <span className="thTitle" title={getHeaderTitleText(c)}>{getHeaderTitleText(c)}</span>
                          </div>
                          <div
                            className="thResizer"
                            title="Изменить ширину (двойной клик — по содержимому)"
                            onMouseDown={(e) => startResize(e, id)}
                            onDoubleClick={(e) => {
                              e.preventDefault()
                              e.stopPropagation()
                              autoSizeColumn(id, filtered, 'fit')
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
            <div className="tableWrapY" ref={bodyInnerRef} style={{ width: tableWidth }}>
              <table ref={bodyTableRef} className="table tableFixed tableBody" style={{ width: tableWidth }}>
                <colgroup>
                  {visibleCols.map(c => (
                    <col key={String(c.id)} style={{ width: c.w }} />
                  ))}
                </colgroup>
                <tbody>
                  {topSpace > 0 && (
                    <tr className="spacerRow">
                      <td colSpan={visibleCols.length} style={{ height: topSpace, padding: 0, border: 'none' }} />
                    </tr>
                  )}

                  {visibleRows.map(p => (
                    <tr key={p.offer_id}>
                      {visibleCols.map(c => {
                        const id = String(c.id)
                        const { text, title } = cellText(p, c.id)
                        if (id === 'photo_url') {
                          const url = (p.photo_url && String(p.photo_url).trim()) ? String(p.photo_url).trim() : ''
                          return (
                            <td key={id} className="tdPhoto">
                              <div
                                className="photoCell"
                                onMouseEnter={url ? (e) => onPhotoMouseEnter(e, url, p.offer_id) : undefined}
                                onMouseMove={url ? onPhotoMouseMove : undefined}
                                onMouseLeave={hidePhotoPreview}
                                onMouseDown={hidePhotoPreview}
                              >
                                {url ? (
                                  <>
                                    <img
                                      className="photoThumb"
                                      src={url}
                                      alt={p.offer_id}
                                      loading="lazy"
                                      onError={(e) => {
                                        hidePhotoPreview()
                                        const img = e.currentTarget
                                        img.style.display = 'none'
                                        const fb = img.parentElement?.querySelector<HTMLElement>('.photoThumbFallback')
                                        if (fb) fb.style.display = 'flex'
                                      }}
                                    />
                                    <div className="photoThumbFallback" style={{ display: 'none' }}>Нет фото</div>
                                  </>
                                ) : (
                                  <div className="photoThumbFallback">Нет фото</div>
                                )}
                              </div>
                            </td>
                          )
                        }
                        return (
                          <td key={id}>
                            <div className="cellText" title={title ?? text}>{text}</div>
                          </td>
                        )
                      })}
                    </tr>
                  ))}

                  {bottomSpace > 0 && (
                    <tr className="spacerRow">
                      <td colSpan={visibleCols.length} style={{ height: bottomSpace, padding: 0, border: 'none' }} />
                    </tr>
                  )}


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
      {photoPreview && (
        <div className="photoHoverPreview" style={{ left: photoPreview.x, top: photoPreview.y }} aria-hidden>
          <img
            src={photoPreview.url}
            alt={photoPreview.title}
            onError={() => setPhotoPreview(null)}
          />
        </div>
      )}
    </div>
  )
}
