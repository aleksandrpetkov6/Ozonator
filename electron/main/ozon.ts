import type { Secrets } from './types'

/**
 * Ozon Seller API client (без внешних зависимостей).
 * Цель: получить расширенные поля по товарам (SKU/штрихкод/бренд/категория/видимость/дата создания/наименование),
 * чтобы в интерфейсе не было прочерков.
 */

const OZON_BASE = 'https://api-seller.ozon.ru'

function headers(secrets: Secrets) {
  return {
    'Client-Id': secrets.clientId,
    'Api-Key': secrets.apiKey,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  }
}

function normalizeError(message: string, details?: any) {
  const err: any = new Error(message)
  err.details = details
  return err
}

async function parseJsonSafe(text: string) {
  try { return JSON.parse(text) } catch { return null }
}

async function ozonRequest(secrets: Secrets, method: 'GET'|'POST', endpoint: string, body?: any) {
  const url = `${OZON_BASE}${endpoint}`
  const res = await fetch(url, {
    method,
    headers: headers(secrets) as any,
    body: method === 'POST' ? JSON.stringify(body ?? {}) : undefined,
  })

  const text = await res.text()
  const json = await parseJsonSafe(text)

  if (!res.ok) {
    throw normalizeError(`Ozon API error: HTTP ${res.status}`, { status: res.status, endpoint, body, response: json ?? text })
  }

  return json
}

async function ozonPost(secrets: Secrets, endpoint: string, body: any) {
  return ozonRequest(secrets, 'POST', endpoint, body)
}

async function ozonGet(secrets: Secrets, endpoint: string) {
  return ozonRequest(secrets, 'GET', endpoint)
}

// ---------------- Types ----------------

type ListItemV3 = {
  offer_id: string
  product_id?: number
  sku?: string
  archived?: boolean
}

export type OzonProductInfo = {
  product_id: number
  offer_id: string
  sku: string | null
  barcode: string | null
  brand: string | null
  category: string | null
  type: string | null
  name: string | null
  is_visible: boolean | number | null
  hidden_reasons: string | null
  created_at: string | null
}

type ProductInfoV2 = {
  id?: number
  product_id?: number
  offer_id?: string
  sku?: number | string
  barcode?: string
  barcodes?: string[]
  category_id?: number
  created_at?: string
  visible?: boolean
  description_category_id?: number
  name?: string
  product_name?: string
  title?: string
  status?: {
    decline_reasons?: any[]
    item_errors?: any[]
  }
  errors?: any[]
}

type AttrValue = { value?: string; dictionary_value_id?: number }

type Attribute = { id: number; values?: AttrValue[] }

type ProductAttributesV3 = {
  id?: number
  offer_id?: string
  barcode?: string
  category_id?: number
  attributes?: Attribute[]
}

// ---------------- Helpers ----------------

function chunk<T>(arr: T[], size: number) {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}

function stringifyReason(x: any): string {
  if (x == null) return ''
  if (typeof x === 'string') return x
  if (typeof x === 'number' || typeof x === 'boolean') return String(x)
  if (typeof x === 'object') {
    if (typeof (x as any).message === 'string') return (x as any).message
    if (typeof (x as any).error === 'string') return (x as any).error
    try { return JSON.stringify(x) } catch { return String(x) }
  }
  return String(x)
}

function buildHiddenReasons(info: ProductInfoV2): string | null {
  const parts: string[] = []

  const dr = info.status?.decline_reasons
  if (Array.isArray(dr)) {
    for (const r of dr) {
      const s = stringifyReason(r).trim()
      if (s) parts.push(s)
    }
  }

  const ie = info.status?.item_errors
  if (Array.isArray(ie)) {
    for (const r of ie) {
      const s = stringifyReason(r).trim()
      if (s) parts.push(s)
    }
  }

  const e = info.errors
  if (Array.isArray(e)) {
    for (const r of e) {
      const s = stringifyReason(r).trim()
      if (s) parts.push(s)
    }
  }

  if (!parts.length) return null
  return Array.from(new Set(parts)).slice(0, 12).join('; ')
}

async function fetchAttributesMap(secrets: Secrets, productIds: number[]) {
  const map = new Map<number, { brand?: string | null; barcode?: string | null; category?: string | null }>()

  // Частые id атрибута "бренд" в разных категориях/кабинетах.
  // (85 — самый распространённый, остальные — редкие вариации.)
  const BRAND_ATTR_IDS = [85, 8229, 31, 10096]

  // Встречаются /v3/products/info/attributes и /v4/products/info/attributes.
  // Делаем основной запрос в /v3, а /v4 используем как fallback.
  async function callWithFallback(body: any) {
    try {
      return await ozonPost(secrets, '/v3/products/info/attributes', body)
    } catch (e: any) {
      if (e?.details?.status !== 404) throw e
      return await ozonPost(secrets, '/v4/products/info/attributes', body)
    }
  }

  for (const ids of chunk(productIds, 900)) {
    let last_id = ''
    for (let guard = 0; guard < 20; guard++) {
      const body = {
        filter: { visibility: 'ALL', product_id: ids },
        limit: 1000,
        last_id,
      }

      const json: any = await callWithFallback(body)

      const data = json?.data ?? json?.result ?? json ?? {}

      // Встречались варианты ответа:
      // 1) { result: { items: [...], last_id: "" } }
      // 2) { result: [...], last_id: "" }
      // 3) { result: [...] }
      // 4) { items: [...] }
      // 5) просто массив (редко)
      const list: ProductAttributesV3[] = Array.isArray(data)
        ? (data as any)
        : Array.isArray((data as any)?.items)
          ? (data as any).items
          : Array.isArray((data as any)?.result)
            ? (data as any).result
            : Array.isArray((data as any)?.result?.items)
              ? (data as any).result.items
              : []

      for (const x of list) {
        const pid = Number((x as any).id ?? (x as any).product_id ?? (x as any).productId)
        if (!pid) continue

        const attrs = Array.isArray(x.attributes) ? x.attributes : []

        // Бренд: сначала по известным id, если value пустой — используем dictionary_value_id.
        let brand: string | null = null
        for (const id of BRAND_ATTR_IDS) {
          const a = attrs.find(a => Number(a.id) === id)
          const v = a?.values?.[0]
          if (!v) continue
          const val = (v.value ?? '').toString().trim()
          if (val) { brand = val; break }
          if (v.dictionary_value_id != null) { brand = String(v.dictionary_value_id); break }
        }

        const barcode = x.barcode ? String(x.barcode) : null
        const category = (x.category_id != null) ? String(x.category_id) : null

        const prev = map.get(pid) ?? {}
        map.set(pid, {
          brand: prev.brand ?? brand,
          barcode: prev.barcode ?? barcode,
          category: prev.category ?? category,
        })
      }

      const next = (!Array.isArray(data))
        ? ((data as any).last_id ?? (data as any).lastId ?? (data as any).result?.last_id ?? (data as any).result?.lastId)
        : undefined
      if (!next) break
      if (next === last_id) break
      last_id = String(next)
    }
  }

  return map
}

function pickFirstString(...vals: any[]): string | null {
  for (const v of vals) {
    if (typeof v === 'string' && v.trim()) return v.trim()
  }
  return null
}

function isReasonableStoreName(s: string): boolean {
  const t = s.trim()
  if (!t) return false
  // Название магазина обычно короткое. Длинные строки часто оказываются "мусором" (токены/идентификаторы).
  if (t.length > 120) return false
  // Похоже на base64/hex/токен — не принимаем как имя магазина.
  if (/^[A-Za-z0-9+/_-]{40,}$/.test(t)) return false
  return true
}

function deepPickFirstString(root: any, keys: string[]): string | null {
  const want = new Set(keys.map(k => k.toLowerCase()))
  const seen = new Set<any>()
  const q: any[] = [root]
  let guard = 0

  while (q.length && guard < 5000) {
    const cur = q.shift()
    guard++

    if (cur == null) continue

    if (typeof cur !== 'object') continue

    if (seen.has(cur)) continue
    seen.add(cur)

    if (Array.isArray(cur)) {
      for (const it of cur) {
        if (it && typeof it === 'object') q.push(it)
      }
      continue
    }

    for (const [kRaw, v] of Object.entries(cur)) {
      const k = String(kRaw).toLowerCase()

      if (want.has(k)) {
        if (typeof v === 'string' && isReasonableStoreName(v)) return v.trim()

        if (v && typeof v === 'object') {
          const candidate = pickFirstString(
            (v as any).name,
            (v as any).title,
            (v as any).value,
            (v as any).company_name,
            (v as any).companyName,
            (v as any).seller_name,
            (v as any).sellerName,
            (v as any).shop_name,
            (v as any).shopName,
          )
          if (candidate && isReasonableStoreName(candidate)) return candidate.trim()
        }
      }

      if (v && typeof v === 'object' && !seen.has(v)) q.push(v)
    }
  }

  return null
}


// ---------------- Public API ----------------

export async function ozonTestAuth(secrets: Secrets) {
  // Лёгкий запрос — просто проверка, что ключи валидны
  await ozonPost(secrets, '/v3/product/list', { filter: {}, last_id: '', limit: 1 })
  return true
}

export async function ozonGetStoreName(secrets: Secrets): Promise<string | null> {
  // Ozon API периодически меняет версию/эндпойнт: поэтому делаем несколько попыток.
  const candidates: Array<() => Promise<any>> = [
    () => ozonGet(secrets, '/v1/seller/info'),
    () => ozonPost(secrets, '/v1/seller/info', {}),
    () => ozonGet(secrets, '/v1/client/info'),
    () => ozonPost(secrets, '/v1/client/info', {}),
  ]

  for (const fn of candidates) {
    try {
      const j: any = await fn()
      const r = j?.result ?? j?.data ?? j
      const name = pickFirstString(
        r?.name,
        r?.company_name,
        r?.companyName,
        r?.seller_name,
        r?.sellerName,
        r?.shop_name,
        r?.shopName,
        j?.name,
        j?.company_name,
        j?.companyName,
      )
      if (name) return name

      // На части кабинетов нужное поле лежит глубже (например result.company.name).
      // Поэтому делаем «глубокий» поиск по дереву ответа, но с защитой от мусорных длинных строк.
      const deep = deepPickFirstString(r, [
        'name',
        'company_name',
        'companyName',
        'seller_name',
        'sellerName',
        'shop_name',
        'shopName'
      ]) ?? deepPickFirstString(j, [
        'name',
        'company_name',
        'companyName',
        'seller_name',
        'sellerName',
        'shop_name',
        'shopName'
      ])
      if (deep) return deep
    } catch (e: any) {
      const st = e?.details?.status
      if (st && st !== 404) {
        // если это не "не найдено" — считаем, что сеть/ключи/прокси и т.п.
        // не фейлим весь вызов — просто попробуем следующий вариант
      }
    }
  }

  return null
}

export async function ozonProductList(secrets: Secrets, opts: { lastId: string; limit: number }) {
  const json = await ozonPost(secrets, '/v3/product/list', {
    filter: {},
    last_id: opts.lastId,
    limit: opts.limit,
  })

  const result = json?.result
  const itemsRaw: any[] = result?.items ?? []
  const lastId: string = String(result?.last_id ?? result?.lastId ?? '')
  const total: number | null = (typeof result?.total === 'number') ? result.total : null

  // В разных версиях API / прокси product_id может прийти как number ИЛИ как строка.
  // Если не привести строку к числу — ids окажутся пустыми, и расширенная информация не подтянется.
  const items: ListItemV3[] = (Array.isArray(itemsRaw) ? itemsRaw : []).map((x) => {
    const pidRaw = (x.product_id ?? x.productId ?? x.id) as any
    const pidNum =
      (typeof pidRaw === 'number') ? pidRaw :
      (typeof pidRaw === 'string' && pidRaw.trim() !== '') ? Number(pidRaw) :
      NaN

    return {
      offer_id: String(x.offer_id ?? ''),
      product_id: Number.isFinite(pidNum) ? pidNum : undefined,
      sku: (typeof x.sku === 'string' || typeof x.sku === 'number') ? String(x.sku) : undefined,
      archived: (typeof x.archived === 'boolean') ? x.archived : undefined,
    }
  }).filter((x) => x.offer_id)

  return { items, lastId, total }
}

export async function ozonProductInfoList(secrets: Secrets, productIds: number[]): Promise<OzonProductInfo[]> {
  if (!productIds.length) return []

  const out: OzonProductInfo[] = []

  function extractItems(json: any): ProductInfoV2[] {
    const r = json?.result
    if (Array.isArray(r)) return r as any
    if (Array.isArray(r?.items)) return r.items as any
    if (Array.isArray(json?.items)) return json.items as any
    if (Array.isArray(r?.result)) return r.result as any
    return []
  }

  async function fetchInfoChunk(ids: number[]) {
    // На части аккаунтов /v2/product/info/list возвращает 404.
    // Поэтому основной путь — /v3/product/info/list, а /v2 используем как fallback.
    try {
      const j3 = await ozonPost(secrets, '/v3/product/info/list', { product_id: ids })
      const items3 = extractItems(j3)
      if (items3.length) return items3
    } catch (e: any) {
      if (e?.details?.status !== 404) throw e
    }

    const j2 = await ozonPost(secrets, '/v2/product/info/list', { product_id: ids })
    return extractItems(j2)
  }

  for (const ids of chunk(productIds, 200)) {
    const items = await fetchInfoChunk(ids)

    for (const x of items) {
      const pid = Number(x.id ?? x.product_id)
      if (!pid) continue

      const barcode = (x.barcode && String(x.barcode)) || (Array.isArray(x.barcodes) && x.barcodes[0]) || null

      const categoryId = (x as any).category_id ?? (x as any).categoryId ?? (x as any).category?.id ?? null
      const brandRaw = (x as any).brand ?? (x as any).brand_name ?? (x as any).brandName ?? null
      const visibleRaw = (x as any).visible ?? (x as any).is_visible ?? (x as any).isVisible ?? (x as any).visibility?.visible ?? null
      const isVisible = (typeof visibleRaw === 'boolean') ? visibleRaw : ((visibleRaw == null) ? null : Boolean(visibleRaw))

      const name = pickFirstString((x as any).name, (x as any).product_name, (x as any).productName, (x as any).title)

      out.push({
        product_id: pid,
        offer_id: String(x.offer_id ?? ''),
        sku: x.sku != null ? String(x.sku) : null,
        barcode,
        brand: (brandRaw != null && String(brandRaw).trim().length) ? String(brandRaw).trim() : null,
        category: categoryId != null ? String(categoryId) : null,
        type: x.description_category_id != null ? String(x.description_category_id) : null,
        name,
        is_visible: isVisible,
        hidden_reasons: buildHiddenReasons(x),
        created_at: x.created_at ?? null,
      })
    }
  }

  // Атрибуты: бренд (и иногда barcode/category)
  try {
    const attrMap = await fetchAttributesMap(secrets, productIds)
    for (const p of out) {
      const a = attrMap.get(p.product_id)
      if (!a) continue
      if (!p.brand && a.brand) p.brand = a.brand
      if (!p.barcode && a.barcode) p.barcode = a.barcode
      if (!p.category && a.category) p.category = a.category
    }
  } catch {
    // атрибуты не критичны — если упали, оставляем базовые поля
  }

  return out
}
