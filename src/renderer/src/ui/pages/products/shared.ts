import { type SortableColumn } from '../../utils/tableSort'

export type GridRow = {
  offer_id: string
  product_id?: number | null
  sku?: string | null
  ozon_sku?: string | null
  seller_sku?: string | null
  fbo_sku?: string | null
  fbs_sku?: string | null
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
  in_process_at?: string | null
  posting_number?: string | null
  related_postings?: string | null
  delivery_model?: string | null
  shipment_date?: string | null
  warehouse_id?: number | null
  warehouse_name?: string | null
  placement_zone?: string | null
}

export type DataSet = 'products' | 'sales' | 'returns' | 'stocks'
export type HiddenBucket = 'main' | 'add'
export type GridColId = keyof GridRow | 'archived'

export type ColDef = SortableColumn<GridRow, GridColId> & {
  id: GridColId
  title: string
  w: number
  visible: boolean
  hiddenBucket: HiddenBucket
}

type PersistedColLayout = Pick<ColDef, 'id' | 'w' | 'visible' | 'hiddenBucket'>

const asMainCol = (col: Omit<ColDef, 'hiddenBucket'>): ColDef => ({ sortable: true, ...col, hiddenBucket: 'main' })

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

export function toText(v: unknown): string {
  if (v == null) return ''
  if (typeof v === 'string') return v
  if (typeof v === 'number' || typeof v === 'boolean') return String(v)
  try { return JSON.stringify(v) } catch { return String(v) }
}

export function visibilityReasonText(v: unknown): string {
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

  let raw: unknown = v
  if (typeof raw === 'string') {
    const s = raw.trim()
    if (!s) return '-'
    try { raw = JSON.parse(s) } catch { raw = s }
  }

  const out: string[] = []
  const pushVal = (val: unknown) => {
    if (val == null) return
    if (Array.isArray(val)) {
      for (const x of val) pushVal(x)
      return
    }
    if (typeof val === 'object') {
      const cand = (val as any).reason ?? (val as any).code ?? (val as any).value ?? (val as any).name
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

export function visibilityText(p: GridRow): string {
  const v = p.is_visible
  if (v === true || v === 1) return 'Виден'
  if (v === false || v === 0) return 'Скрыт'
  if (p.hidden_reasons && String(p.hidden_reasons).trim()) return 'Скрыт'
  return 'Виден'
}

export function toSortTimestamp(value: unknown): number | null {
  if (value == null || value === '') return null
  const time = Date.parse(String(value))
  return Number.isFinite(time) ? time : null
}

function getOfferIdSortBucket(value: unknown): number {
  const text = toText(value).trim()
  if (!text) return 2
  const firstLetter = text.match(/[A-Za-zА-ЯЁа-яё]/)?.[0] ?? text[0]
  return /[А-ЯЁа-яё]/.test(firstLetter) ? 0 : 1
}

function compareOfferIdsRuFirst(a: unknown, b: unknown): number {
  const left = toText(a).trim()
  const right = toText(b).trim()
  const leftEmpty = left === ''
  const rightEmpty = right === ''
  if (leftEmpty || rightEmpty) {
    if (leftEmpty && rightEmpty) return 0
    return leftEmpty ? 1 : -1
  }

  const bucketDiff = getOfferIdSortBucket(left) - getOfferIdSortBucket(right)
  if (bucketDiff) return bucketDiff

  return left.localeCompare(right, 'ru', { numeric: true, sensitivity: 'base' })
}

function compareRowsByDefaultSort(dataset: DataSet, left: GridRow, right: GridRow): number {
  if (dataset === 'sales') {
    const leftAt = toSortTimestamp(left.in_process_at)
    const rightAt = toSortTimestamp(right.in_process_at)
    const leftEmpty = leftAt == null
    const rightEmpty = rightAt == null
    if (leftEmpty || rightEmpty) {
      if (leftEmpty && rightEmpty) return 0
      return leftEmpty ? 1 : -1
    }
    return rightAt - leftAt
  }

  if (dataset === 'products' || dataset === 'stocks') {
    return compareOfferIdsRuFirst(left.offer_id, right.offer_id)
  }

  return 0
}

function sortRowsForDefaultView(dataset: DataSet, rows: GridRow[]): GridRow[] {
  if (rows.length < 2) return rows
  if (dataset === 'returns') return rows

  return rows
    .map((row, index) => ({ row, index }))
    .sort((left, right) => {
      const compared = compareRowsByDefaultSort(dataset, left.row, right.row)
      return compared || (left.index - right.index)
    })
    .map((entry) => entry.row)
}

export function buildDefaultCols(dataset: DataSet): ColDef[] {
  const base: ColDef[] = [
    asMainCol({ id: 'offer_id', title: 'Артикул', w: 160, visible: true }),
    asMainCol({ id: 'product_id', title: 'ID', w: 110, visible: true }),
    asMainCol({ id: 'ozon_sku', title: 'SKU Ozon', w: 150, visible: true, getSortValue: (row) => row.ozon_sku ?? row.sku ?? '' }),
    asMainCol({ id: 'seller_sku', title: 'SKU продавца', w: 180, visible: true, getSortValue: (row) => row.seller_sku ?? row.offer_id ?? '' }),
    asMainCol({ id: 'fbo_sku', title: 'SKU FBO', w: 150, visible: true }),
    asMainCol({ id: 'fbs_sku', title: 'SKU FBS', w: 150, visible: true }),
    asMainCol({ id: 'photo_url', title: 'Фото', w: 74, visible: true, getSortValue: (row) => ((row.photo_url && String(row.photo_url).trim()) ? 1 : 0) }),
    asMainCol({ id: 'name', title: 'Наименование', w: 320, visible: true }),
    asMainCol({ id: 'brand', title: 'Бренд', w: 180, visible: true }),
    asMainCol({ id: 'sku', title: 'SKU', w: 140, visible: true }),
    asMainCol({ id: 'barcode', title: 'Штрихкод', w: 170, visible: true }),
    asMainCol({ id: 'type', title: 'Категория', w: 280, visible: true }),
    asMainCol({ id: 'is_visible', title: 'Видимость', w: 140, visible: true, getSortValue: (row) => visibilityText(row) }),
    asMainCol({ id: 'hidden_reasons', title: 'Причина скрытия', w: 320, visible: true, getSortValue: (row) => visibilityReasonText(row.hidden_reasons) }),
    asMainCol({ id: 'created_at', title: 'Создан', w: 180, visible: true, getSortValue: (row) => toSortTimestamp(row.created_at) ?? '' }),
  ]

  if (dataset === 'sales') {
    base.push(
      asMainCol({ id: 'in_process_at', title: 'Принят в обработку', w: 180, visible: true, getSortValue: (row) => toSortTimestamp(row.in_process_at) ?? '' }),
      asMainCol({ id: 'posting_number', title: 'Номер отправления', w: 220, visible: true }),
      asMainCol({ id: 'related_postings', title: 'Связанные отправления', w: 300, visible: true }),
      asMainCol({ id: 'delivery_model', title: 'Метод доставки', w: 150, visible: true }),
      asMainCol({ id: 'shipment_date', title: 'Дата отгрузки', w: 180, visible: true, getSortValue: (row) => toSortTimestamp(row.shipment_date) ?? '' }),
    )
  }

  if (dataset === 'stocks') {
    base.push(
      asMainCol({ id: 'warehouse_name', title: 'Склад', w: 180, visible: true, getSortValue: (row) => {
        const rawName = (row.warehouse_name == null ? '' : String(row.warehouse_name)).trim()
        if (rawName) return rawName
        const rawId = (row.warehouse_id == null ? '' : String(row.warehouse_id)).trim()
        return rawId ? `Склад #${rawId}` : ''
      } }),
      asMainCol({ id: 'placement_zone', title: 'Зона размещения', w: 220, visible: true, getSortValue: (row) => {
        const zone = (row.placement_zone == null ? '' : String(row.placement_zone)).trim()
        return zone || ''
      } }),
    )
  }

  if (dataset === 'products') {
    base.push(asMainCol({ id: 'updated_at', title: 'Обновлён', w: 180, visible: false, getSortValue: (row) => toSortTimestamp(row.updated_at) ?? '' }))
  }

  return base
}

export function colsStorageKey(dataset: DataSet) {
  return `ozonator_cols_${dataset}`
}

function normalizePersistedCols(value: unknown): PersistedColLayout[] {
  if (!Array.isArray(value)) return []
  const out: PersistedColLayout[] = []
  const seen = new Set<string>()

  for (const raw of value) {
    if (!raw || typeof raw !== 'object') continue
    const id = String((raw as any).id ?? '').trim()
    if (!id || seen.has(id)) continue

    const wNum = Number((raw as any).w)
    const w = Number.isFinite(wNum) ? Math.max(60, Math.min(2000, Math.round(wNum))) : 120
    const visible = typeof (raw as any).visible === 'boolean' ? (raw as any).visible : true
    const hiddenBucket: HiddenBucket = (raw as any).hiddenBucket === 'add' ? 'add' : 'main'

    out.push({ id: id as ColDef['id'], w, visible, hiddenBucket })
    seen.add(id)
    if (out.length >= 200) break
  }

  return out
}

export function mergeColsWithDefaults(dataset: DataSet, persistedRaw: unknown): ColDef[] {
  const defaults = buildDefaultCols(dataset)
  const persisted = normalizePersistedCols(persistedRaw)
  if (persisted.length === 0) return defaults

  const defaultsById = new Map<string, ColDef>()
  for (const d of defaults) defaultsById.set(String(d.id), d)

  const out: ColDef[] = []
  const used = new Set<string>()
  for (const p of persisted) {
    const d = defaultsById.get(String(p.id))
    if (!d) continue
    out.push({ ...d, w: p.w, visible: p.visible, hiddenBucket: p.hiddenBucket })
    used.add(String(d.id))
  }

  for (const d of defaults) {
    const key = String(d.id)
    if (used.has(key)) continue
    out.push(d)
  }

  return out
}

export function readCols(dataset: DataSet): ColDef[] {
  try {
    const raw = localStorage.getItem(colsStorageKey(dataset))
    if (!raw) return buildDefaultCols(dataset)
    return mergeColsWithDefaults(dataset, JSON.parse(raw))
  } catch {
    return buildDefaultCols(dataset)
  }
}

export function saveCols(dataset: DataSet, cols: ColDef[]) {
  const payload: PersistedColLayout[] = cols.map((c) => ({ id: c.id, w: c.w, visible: c.visible, hiddenBucket: c.hiddenBucket }))
  localStorage.setItem(colsStorageKey(dataset), JSON.stringify(payload))
}

const DATASET_CACHE: Record<DataSet, GridRow[] | null> = {
  products: null,
  sales: null,
  returns: null,
  stocks: null,
}

const DATASET_CACHE_AT: Record<DataSet, number> = {
  products: 0,
  sales: 0,
  returns: 0,
  stocks: 0,
}

const DATASET_INFLIGHT: Record<DataSet, Promise<GridRow[] | null> | null> = {
  products: null,
  sales: null,
  returns: null,
  stocks: null,
}

const PRODUCTS_CACHE_TTL_MS = 60_000

export function getCachedRows(dataset: DataSet): GridRow[] {
  return DATASET_CACHE[dataset] ?? []
}

export async function fetchRowsCached(dataset: DataSet, force = false): Promise<GridRow[] | null> {
  const now = Date.now()
  if (!force && DATASET_CACHE[dataset] && (now - DATASET_CACHE_AT[dataset]) < PRODUCTS_CACHE_TTL_MS) return DATASET_CACHE[dataset]
  if (DATASET_INFLIGHT[dataset]) return DATASET_INFLIGHT[dataset]

  DATASET_INFLIGHT[dataset] = (async () => {
    try {
      let list: GridRow[] = []
      if (dataset === 'products') {
        const resp = await window.api.getProducts()
        if (resp.ok) list = (resp.products as any) as GridRow[]
        else return DATASET_CACHE[dataset]
      } else if (dataset === 'sales') {
        const resp = await window.api.getSales({ from: '2026-02-01' })
        if (resp.ok) list = (resp.rows as any) as GridRow[]
        else return DATASET_CACHE[dataset]
      } else if (dataset === 'returns') {
        const resp = await window.api.getReturns()
        if (resp.ok) list = (resp.rows as any) as GridRow[]
        else return DATASET_CACHE[dataset]
      } else {
        const resp = await window.api.getStocks()
        if (resp.ok) list = (resp.rows as any) as GridRow[]
        else return DATASET_CACHE[dataset]
      }
      const sortedList = sortRowsForDefaultView(dataset, list)
      DATASET_CACHE[dataset] = sortedList
      DATASET_CACHE_AT[dataset] = Date.now()
      return sortedList
    } catch {
      return DATASET_CACHE[dataset]
    } finally {
      DATASET_INFLIGHT[dataset] = null
    }
  })()

  return DATASET_INFLIGHT[dataset]
}
