import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { formatDateTimeRu } from '../utils/dateTime'
import { type TableSortState, sortTableRows, toggleTableSort } from '../utils/tableSort'
import ProductsGridView from './products/ProductsGridView'
import {
  type ColDef,
  type DataSet,
  type GridColId,
  type GridRow,
  type HiddenBucket,
  buildDefaultCols,
  colsStorageKey,
  fetchRowsCached,
  getCachedRows,
  mergeColsWithDefaults,
  readCols,
  saveCols,
  toText,
  visibilityReasonText,
  visibilityText,
} from './products/shared'

type SortState = TableSortState<GridColId>

type Props = {
  dataset?: DataSet
  query?: string
  onStats?: (s: { total: number; filtered: number }) => void
}

const PHOTO_PREVIEW_SIZE = 200
const PHOTO_PREVIEW_DELAY_MS = 1000
const AUTO_MIN_W = 80
const AUTO_PAD = 34
const AUTO_MAX_W: Record<string, number> = {
  offer_id: 240,
  product_id: 120,
  ozon_sku: 220,
  seller_sku: 240,
  fbo_sku: 220,
  fbs_sku: 220,
  sku: 220,
  barcode: 260,
  brand: 220,
  is_visible: 180,
  hidden_reasons: 440,
  created_at: 240,
  updated_at: 240,
  in_process_at: 240,
  warehouse_name: 240,
  placement_zone: 320,
  type: 380,
  name: 460,
  photo_url: 90,
}

export default function ProductsPage({ dataset = 'products', query = '', onStats }: Props) {
  const [products, setProducts] = useState<GridRow[]>(() => getCachedRows(dataset))
  const [cols, setCols] = useState<ColDef[]>(() => readCols(dataset))
  const [sortState, setSortState] = useState<SortState>(null)
  const [draggingId, setDraggingId] = useState<string | null>(null)
  const [dropHint, setDropHint] = useState<{ id: string; side: 'left' | 'right'; x: number } | null>(null)
  const [collapsedOpen, setCollapsedOpen] = useState(false)
  const [addColumnMenuOpen, setAddColumnMenuOpen] = useState(false)
  const [bodyWindowAnchorRow, setBodyWindowAnchorRow] = useState(0)
  const [bodyViewportH, setBodyViewportH] = useState(600)
  const [photoPreview, setPhotoPreview] = useState<{ url: string; alt: string; x: number; y: number } | null>(null)
  const [colsSyncReady, setColsSyncReady] = useState(false)
  const [hasStoredCols, setHasStoredCols] = useState<boolean>(() => {
    try { return !!localStorage.getItem(colsStorageKey(dataset)) } catch { return true }
  })

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
  const photoHoverPendingRef = useRef<{ url: string; alt: string; clientX: number; clientY: number } | null>(null)
  const headScrollRef = useRef<HTMLDivElement | null>(null)
  const bodyScrollRef = useRef<HTMLDivElement | null>(null)
  const scrollSyncLockRef = useRef(false)
  const headerRowRef = useRef<HTMLTableRowElement | null>(null)

  const clearPhotoHoverTimer = useCallback(() => {
    if (photoHoverTimerRef.current != null) {
      window.clearTimeout(photoHoverTimerRef.current)
      photoHoverTimerRef.current = null
    }
  }, [])

  function getPhotoPreviewPos(clientX: number, clientY: number) {
    const offsetX = 18
    const offsetY = 14
    const margin = 10
    const box = PHOTO_PREVIEW_SIZE + 16
    let x = clientX + offsetX
    let y = clientY - box - offsetY
    const maxX = Math.max(margin, window.innerWidth - box - margin)
    const maxY = Math.max(margin, window.innerHeight - box - margin)
    if (y < margin) y = clientY + offsetY
    if (x > maxX) x = maxX
    if (y > maxY) y = maxY
    if (x < margin) x = margin
    if (y < margin) y = margin
    return { x, y }
  }

  function queuePhotoPreview(url: string, alt: string, clientX: number, clientY: number) {
    photoHoverPendingRef.current = { url, alt, clientX, clientY }
    clearPhotoHoverTimer()
    photoHoverTimerRef.current = window.setTimeout(() => {
      const pending = photoHoverPendingRef.current
      if (!pending) return
      const pos = getPhotoPreviewPos(pending.clientX, pending.clientY)
      setPhotoPreview({ url: pending.url, alt: pending.alt, x: pos.x, y: pos.y })
      photoHoverTimerRef.current = null
    }, PHOTO_PREVIEW_DELAY_MS)
  }

  function movePhotoPreview(clientX: number, clientY: number) {
    if (photoHoverPendingRef.current) photoHoverPendingRef.current = { ...photoHoverPendingRef.current, clientX, clientY }
    setPhotoPreview((prev) => {
      if (!prev) return prev
      const pos = getPhotoPreviewPos(clientX, clientY)
      return { ...prev, x: pos.x, y: pos.y }
    })
  }

  const hidePhotoPreview = useCallback(() => {
    clearPhotoHoverTimer()
    photoHoverPendingRef.current = null
    setPhotoPreview(null)
  }, [clearPhotoHoverTimer])

  useEffect(() => {
    let active = true
    setColsSyncReady(false)
    ;(async () => {
      const localCols = (() => {
        try {
          const raw = localStorage.getItem(colsStorageKey(dataset))
          if (!raw) return null
          return JSON.parse(raw)
        } catch {
          return null
        }
      })()

      try {
        const dbResp = await window.api.getGridColumns(dataset)
        if (!active) return
        if (Array.isArray(dbResp?.cols) && dbResp.cols.length > 0) {
          const merged = mergeColsWithDefaults(dataset, dbResp.cols)
          setCols(merged)
          setHasStoredCols(true)
          try { saveCols(dataset, merged) } catch {}
          setColsSyncReady(true)
          return
        }
        if (localCols) {
          const merged = mergeColsWithDefaults(dataset, localCols)
          setCols(merged)
          setHasStoredCols(true)
          try { await window.api.saveGridColumns(dataset, merged.map((c) => ({ id: String(c.id), w: c.w, visible: c.visible, hiddenBucket: c.hiddenBucket }))) } catch {}
          setColsSyncReady(true)
          return
        }
        setCols(buildDefaultCols(dataset))
        setHasStoredCols(false)
        setColsSyncReady(true)
      } catch {
        if (!active) return
        if (localCols) {
          const merged = mergeColsWithDefaults(dataset, localCols)
          setCols(merged)
          setHasStoredCols(true)
        } else {
          setCols(buildDefaultCols(dataset))
          setHasStoredCols(false)
        }
        setColsSyncReady(true)
      }
    })()

    return () => { active = false }
  }, [dataset])

  const load = useCallback(async (force = false) => {
    const list = await fetchRowsCached(dataset, force)
    if (Array.isArray(list)) setProducts(list)
  }, [dataset])

  useEffect(() => { load() }, [load])

  useEffect(() => {
    if (dataset !== 'products') return
    const onUpdated = () => load(true)
    window.addEventListener('ozon:products-updated', onUpdated)
    return () => window.removeEventListener('ozon:products-updated', onUpdated)
  }, [dataset, load])

  useEffect(() => () => { clearPhotoHoverTimer() }, [clearPhotoHoverTimer])

  useEffect(() => {
    if (!colsSyncReady) return
    const id = window.setTimeout(() => {
      const payload = cols.map((c) => ({ id: String(c.id), w: c.w, visible: c.visible, hiddenBucket: c.hiddenBucket }))
      try { saveCols(dataset, cols) } catch {}
      window.api.saveGridColumns(dataset, payload).catch(() => {})
    }, 250)
    return () => window.clearTimeout(id)
  }, [dataset, cols, colsSyncReady])

  const visibleCols = useMemo(() => cols.filter((c) => c.visible), [cols])
  const rowH = useMemo(() => (visibleCols.some((c) => c.id === 'photo_url') ? 58 : 28), [visibleCols])
  const hiddenCols = useMemo(() => cols.filter((c) => !c.visible), [cols])
  const primaryHiddenCols = useMemo(() => hiddenCols.filter((c) => c.hiddenBucket !== 'add'), [hiddenCols])
  const addMenuHiddenCols = useMemo(() => hiddenCols.filter((c) => c.hiddenBucket === 'add'), [hiddenCols])

  useEffect(() => {
    if (!collapsedOpen) return
    const onDown = (ev: MouseEvent) => {
      const t = ev.target as Node | null
      if (!t) return
      if (collapsedMenuRef.current?.contains(t)) return
      if (collapsedBtnRef.current?.contains(t)) return
      setCollapsedOpen(false)
      setAddColumnMenuOpen(false)
    }
    const onKey = (ev: KeyboardEvent) => {
      if (ev.key === 'Escape') {
        setCollapsedOpen(false)
        setAddColumnMenuOpen(false)
      }
    }
    window.addEventListener('mousedown', onDown)
    window.addEventListener('keydown', onKey)
    return () => {
      window.removeEventListener('mousedown', onDown)
      window.removeEventListener('keydown', onKey)
    }
  }, [collapsedOpen])

  useEffect(() => {
    if (collapsedOpen && hiddenCols.length === 0) {
      setCollapsedOpen(false)
      setAddColumnMenuOpen(false)
    }
  }, [collapsedOpen, hiddenCols.length])

  useEffect(() => {
    if (!addColumnMenuOpen) return
    if (addMenuHiddenCols.length > 0) return
    setAddColumnMenuOpen(false)
  }, [addColumnMenuOpen, addMenuHiddenCols.length])

  useEffect(() => {
    setCollapsedOpen(false)
    setAddColumnMenuOpen(false)
  }, [dataset])

  const visibleSearchKey = useMemo(() => cols.map((c) => `${c.id}:${c.visible ? 1 : 0}`).join('|'), [cols])
  const visibleSearchCols = useMemo(
    () => visibleSearchKey.split('|').filter(Boolean).flatMap((entry) => {
      const splitAt = entry.lastIndexOf(':')
      if (splitAt < 0) return []
      return entry.slice(splitAt + 1) === '1' ? [entry.slice(0, splitAt)] : []
    }),
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
          if (colId === 'ozon_sku') {
            const v = p.ozon_sku ?? p.sku
            return (v == null || String(v).trim() === '') ? '-' : String(v)
          }
          if (colId === 'seller_sku') {
            const v = p.seller_sku ?? p.offer_id
            return (v == null || String(v).trim() === '') ? '-' : String(v)
          }
          if (colId === 'fbo_sku' || colId === 'fbs_sku') {
            const v = (p as any)[colId]
            return (v == null || String(v).trim() === '') ? '-' : String(v)
          }
          if (colId === 'in_process_at') return formatDateTimeRu((p as any)[colId])
          if (colId === 'photo_url') return ''
          return toText((p as any)[colId])
        })
        .join(' ')
        .toLowerCase()
      return hay.includes(q)
    })
  }, [products, query, visibleSearchCols])

  const sortedRows = useMemo(() => sortTableRows(filtered, cols, sortState), [filtered, cols, sortState])

  useEffect(() => {
    onStats?.({ total: products.length, filtered: filtered.length })
  }, [products.length, filtered.length, onStats])

  function toggleSort(id: string) {
    const col = cols.find((item) => String(item.id) === id)
    if (!col || col.sortable === false) return
    setSortState((prev) => toggleTableSort(prev, col.id, col.sortable !== false))
  }

  function hideCol(id: string) {
    setCols((prev) => prev.map((c) => String(c.id) === id ? { ...c, visible: false } : c))
    setSortState((prev) => (prev?.colId === id ? null : prev))
  }

  function showCol(id: string) {
    setCols((prev) => prev.map((c) => String(c.id) === id ? { ...c, visible: true } : c))
  }

  function moveHiddenColToBucket(id: string, hiddenBucket: HiddenBucket) {
    setCols((prev) => prev.map((c) => (String(c.id) === id ? { ...c, hiddenBucket } : c)))
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
    if (!head || !row || visibleCols.length === 0) return

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

    setCols((prev) => {
      const fromIdx = prev.findIndex((c) => String(c.id) === draggedId)
      const toIdxRaw = prev.findIndex((c) => String(c.id) === hint.id)
      if (fromIdx < 0 || toIdxRaw < 0 || fromIdx === toIdxRaw) return prev
      const insertBase = toIdxRaw + (hint.side === 'right' ? 1 : 0)
      const next = [...prev]
      const [moved] = next.splice(fromIdx, 1)
      let insertIdx = insertBase
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
    const col = cols.find((c) => String(c.id) === colId)
    if (!col) return
    const head = headScrollRef.current
    const row = headerRowRef.current
    const cell = row?.querySelector<HTMLElement>(`th[data-col-id="${colId}"]`)
    if (!head || !cell) return

    const colIdx = visibleCols.findIndex((c) => String(c.id) === colId)
    if (colIdx < 0) return
    const startRight = cell.offsetLeft + cell.offsetWidth
    const headCols = headTableRef.current?.querySelectorAll('colgroup col') ?? []
    const bodyCols = bodyTableRef.current?.querySelectorAll('colgroup col') ?? []
    const headCol = (headCols[colIdx] as any) as HTMLTableColElement | null
    const bodyCol = (bodyCols[colIdx] as any) as HTMLTableColElement | null

    resizingRef.current = { id: colId, startX: e.clientX, startW: col.w, startRight, startTableW: tableWidth, colIdx, headCol, bodyCol }

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
      setCols((prev) => prev.map((c) => String(c.id) === r.id ? { ...c, w: finalW } : c))
    }

    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }

  function cellText(p: GridRow, colId: ColDef['id']): { text: string; title?: string } {
    if (colId === 'offer_id') return { text: p.offer_id }
    if (colId === 'name') return { text: (p.name && String(p.name).trim()) ? String(p.name).trim() : 'Без названия' }
    if (colId === 'brand') return { text: (p.brand && String(p.brand).trim()) ? String(p.brand).trim() : 'Не указан' }
    if (colId === 'photo_url') return { text: '', title: (p.photo_url && String(p.photo_url).trim()) ? String(p.photo_url).trim() : 'Нет фото' }
    if (colId === 'ozon_sku') {
      const v = p.ozon_sku ?? p.sku
      return { text: (v == null || String(v).trim() === '') ? '-' : String(v) }
    }
    if (colId === 'seller_sku') {
      const v = p.seller_sku ?? p.offer_id
      return { text: (v == null || String(v).trim() === '') ? '-' : String(v) }
    }
    if (colId === 'fbo_sku' || colId === 'fbs_sku') {
      const v = (p as any)[colId]
      return { text: (v == null || String(v).trim() === '') ? '-' : String(v) }
    }
    if (colId === 'is_visible') {
      const txt = visibilityText(p)
      const rs = visibilityReasonText(p.hidden_reasons)
      return { text: txt, title: rs !== '-' ? rs : undefined }
    }

    const v = (p as any)[colId]
    if (colId === 'hidden_reasons') {
      const rs = visibilityReasonText(v)
      return { text: rs, title: rs !== '-' ? rs : undefined }
    }
    if (colId === 'created_at' || colId === 'updated_at' || colId === 'in_process_at') {
      const f = formatDateTimeRu(v)
      return { text: f || '-', title: f || undefined }
    }
    if (colId === 'warehouse_name') {
      const rawName = (p.warehouse_name == null ? '' : String(p.warehouse_name)).trim()
      if (rawName) return { text: rawName }
      const rawId = (p.warehouse_id == null ? '' : String(p.warehouse_id)).trim()
      return { text: rawId ? `Склад #${rawId}` : 'Нет данных синхронизации' }
    }
    if (colId === 'placement_zone') {
      const zone = (p.placement_zone == null ? '' : String(p.placement_zone)).trim()
      return { text: zone || 'Нет данных синхронизации' }
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

  function getCellString(p: GridRow, colId: ColDef['id']): string {
    if (colId === 'archived') return ''
    if (colId === 'is_visible') return visibilityText(p)
    if (colId === 'hidden_reasons') return visibilityReasonText((p as any)[colId])
    if (colId === 'brand') return (p.brand && String(p.brand).trim()) ? String(p.brand).trim() : 'Не указан'
    if (colId === 'name') return (p.name && String(p.name).trim()) ? String(p.name).trim() : 'Без названия'
    if (colId === 'ozon_sku') {
      const v = p.ozon_sku ?? p.sku
      return (v == null || String(v).trim() === '') ? '-' : String(v)
    }
    if (colId === 'seller_sku') {
      const v = p.seller_sku ?? p.offer_id
      return (v == null || String(v).trim() === '') ? '-' : String(v)
    }
    if (colId === 'fbo_sku' || colId === 'fbs_sku') {
      const v = (p as any)[colId]
      return (v == null || String(v).trim() === '') ? '-' : String(v)
    }
    if (colId === 'photo_url') return ''
    if (colId === 'warehouse_name') {
      const rawName = (p.warehouse_name == null ? '' : String(p.warehouse_name)).trim()
      if (rawName) return rawName
      const rawId = (p.warehouse_id == null ? '' : String(p.warehouse_id)).trim()
      return rawId ? `Склад #${rawId}` : 'Нет данных синхронизации'
    }
    if (colId === 'placement_zone') {
      const zone = (p.placement_zone == null ? '' : String(p.placement_zone)).trim()
      return zone || 'Нет данных синхронизации'
    }
    if (colId === 'created_at' || colId === 'updated_at' || colId === 'in_process_at') return formatDateTimeRu((p as any)[colId])
    return toText((p as any)[colId])
  }

  function autoSizeColumn(colId: string, rows: GridRow[], mode: 'default' | 'fit' = 'default') {
    const col = cols.find((c) => String(c.id) === colId)
    if (!col) return
    if (colId === 'photo_url') {
      setCols((prev) => prev.map((c) => String(c.id) === colId ? { ...c, w: 120 } : c))
      return
    }

    const headerExtra = 44
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
    setCols((prev) => prev.map((c) => String(c.id) === colId ? { ...c, w: nextW } : c))
  }

  useEffect(() => {
    if (!colsSyncReady) return
    if (didAutoInitRef.current) return
    if (hasStoredCols) {
      didAutoInitRef.current = true
      return
    }

    didAutoInitRef.current = true
    setCols((prev) => prev.map((c) => {
      const id = String(c.id)
      if (id === 'photo_url') return { ...c, w: 120 }
      const headerExtra = 44
      const baseCap = AUTO_MAX_W[id] ?? 320
      let max = measureTextWidth(c.title, 'header') + headerExtra
      const sample = products.length > 1600 ? products.slice(0, 1600) : products
      for (const p of sample) {
        const s = getCellString(p, c.id)
        if (!s) continue
        const w = measureTextWidth(s)
        if (w > max) max = w
      }
      return { ...c, w: Math.max(AUTO_MIN_W, Math.min(baseCap, Math.round(max + AUTO_PAD))) }
    }))
  }, [products.length, colsSyncReady, hasStoredCols, dataset])

  const tableWidth = useMemo(() => Math.max(1, visibleCols.reduce((s, c) => s + c.w, 0)), [visibleCols])

  useEffect(() => {
    const head = headScrollRef.current
    const body = bodyScrollRef.current
    if (!head || !body) return

    const syncFromBody = () => {
      hidePhotoPreview()
      if (scrollSyncLockRef.current) return
      scrollSyncLockRef.current = true
      head.scrollLeft = body.scrollLeft
      scrollSyncLockRef.current = false
    }

    const syncFromHead = () => {
      hidePhotoPreview()
      if (scrollSyncLockRef.current) return
      scrollSyncLockRef.current = true
      body.scrollLeft = head.scrollLeft
      scrollSyncLockRef.current = false
    }

    body.addEventListener('scroll', syncFromBody, { passive: true })
    head.addEventListener('scroll', syncFromHead, { passive: true })
    head.scrollLeft = body.scrollLeft

    return () => {
      body.removeEventListener('scroll', syncFromBody)
      head.removeEventListener('scroll', syncFromHead)
    }
  }, [visibleCols.length, hidePhotoPreview])

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
    return () => body.removeEventListener('scroll', onScroll)
  }, [bodyViewportH, rowH])

  const totalRows = sortedRows.length
  const viewH = bodyViewportH || 600
  const viewportRows = Math.max(1, Math.ceil(viewH / rowH))
  const OVERSCAN = Math.max(64, viewportRows * 3)
  const WINDOW_STEP_ROWS = Math.max(12, Math.ceil(viewportRows / 2))
  const startRow = Math.max(0, bodyWindowAnchorRow - OVERSCAN)
  const endRow = Math.min(totalRows, bodyWindowAnchorRow + viewportRows + (OVERSCAN * 2) + WINDOW_STEP_ROWS)
  const visibleRows = useMemo(() => sortedRows.slice(startRow, endRow), [sortedRows, startRow, endRow])
  const topSpace = startRow * rowH
  const bottomSpace = Math.max(0, (totalRows - endRow) * rowH)

  const getHeaderTitleText = useCallback((c: ColDef): string => {
    if (String(c.id) === 'offer_id' && dataset === 'products') return `${c.title} ${totalRows}`
    return c.title
  }, [dataset, totalRows])

  const getRowKey = useCallback((p: GridRow, absoluteRowIndex: number): string => {
    if (dataset === 'stocks') return `${p.offer_id}__${p.sku ?? ''}__${p.warehouse_id ?? ''}__${(p.placement_zone ?? '').toString().trim()}`
    if (dataset === 'sales') {
      const row = p as any
      return `${row.posting_number ?? ''}__${p.offer_id}__${p.sku ?? ''}__${row.in_process_at ?? row.created_at ?? ''}__${absoluteRowIndex}`
    }
    if (dataset === 'returns') {
      const row = p as any
      return `${row.return_id ?? ''}__${p.offer_id}__${p.sku ?? ''}__${row.created_at ?? ''}__${absoluteRowIndex}`
    }
    return p.offer_id
  }, [dataset])

  return (
    <ProductsGridView
      hiddenCols={hiddenCols}
      collapsedOpen={collapsedOpen}
      addColumnMenuOpen={addColumnMenuOpen}
      addMenuHiddenCols={addMenuHiddenCols}
      primaryHiddenCols={primaryHiddenCols}
      visibleCols={visibleCols}
      draggingId={draggingId}
      dropHint={dropHint}
      tableWidth={tableWidth}
      visibleRows={visibleRows}
      startRow={startRow}
      topSpace={topSpace}
      bottomSpace={bottomSpace}
      empty={sortedRows.length === 0}
      sortColId={sortState?.colId ?? null}
      sortDir={sortState?.dir}
      photoPreview={photoPreview}
      collapsedBtnRef={collapsedBtnRef}
      collapsedMenuRef={collapsedMenuRef}
      resizeIndicatorRef={resizeIndicatorRef}
      headScrollRef={headScrollRef}
      headInnerRef={headInnerRef}
      headTableRef={headTableRef}
      headerRowRef={headerRowRef}
      bodyScrollRef={bodyScrollRef}
      bodyInnerRef={bodyInnerRef}
      bodyTableRef={bodyTableRef}
      getHeaderTitleText={getHeaderTitleText}
      getRowKey={getRowKey}
      cellText={cellText}
      setCollapsedOpen={setCollapsedOpen}
      setAddColumnMenuOpen={setAddColumnMenuOpen}
      onShowCol={showCol}
      onHideCol={hideCol}
      onMoveHiddenColToBucket={moveHiddenColToBucket}
      onDragStart={onDragStart}
      onDragOverHeader={onDragOverHeader}
      onDrop={onDrop}
      onDragEnd={onDragEnd}
      onToggleSort={toggleSort}
      onStartResize={startResize}
      onAutoSize={(id) => autoSizeColumn(id, sortedRows, 'fit')}
      queuePhotoPreview={queuePhotoPreview}
      movePhotoPreview={movePhotoPreview}
      hidePhotoPreview={hidePhotoPreview}
    />
  )
}
