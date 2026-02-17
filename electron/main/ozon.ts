import type { Secrets } from './types'

const BASE_URL = 'https://api-seller.ozon.ru'

async function ozonPost<T>(secrets: Secrets, path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Client-Id': secrets.clientId,
      'Api-Key': secrets.apiKey,
    },
    body: JSON.stringify(body ?? {}),
  })

  const text = await res.text()
  let json: any = null
  try {
    json = text ? JSON.parse(text) : null
  } catch {
    json = null
  }

  if (!res.ok) {
    const msg = json?.message || json?.error?.message || text || `HTTP ${res.status}`
    throw new Error(msg)
  }

  // иногда API возвращает 200 + error внутри
  if (json?.error || json?.errors) {
    const msg = json?.error?.message || json?.errors?.[0]?.message || 'Ошибка API'
    throw new Error(msg)
  }

  return json as T
}

export async function ozonTestAuth(secrets: Secrets): Promise<void> {
  // минимальный запрос, чтобы проверить ключи
  await ozonPost<any>(secrets, '/v3/product/list', { filter: { visibility: 'ALL' }, last_id: '', limit: 1 })
}

type DescriptionMaps = {
  catNameById: Map<number, string>
  typeNameById: Map<number, string>
}

let cachedMaps: { at: number; maps: DescriptionMaps } | null = null
const MAP_TTL_MS = 6 * 60 * 60 * 1000

async function ozonGetCategoryTypeMaps(secrets: Secrets): Promise<DescriptionMaps> {
  if (cachedMaps && Date.now() - cachedMaps.at < MAP_TTL_MS) return cachedMaps.maps

  // В документации встречаются оба пути (/v1/description_category/tree и /v1/description-category/tree)
  const tryPaths = ['/v1/description_category/tree', '/v1/description-category/tree']

  let treeRes: any = null
  let lastErr: any = null
  for (const p of tryPaths) {
    try {
      treeRes = await ozonPost<any>(secrets, p, {})
      break
    } catch (e) {
      lastErr = e
    }
  }
  if (!treeRes) throw lastErr ?? new Error('Не удалось получить дерево категорий')

  const cat = new Map<number, string>()
  const type = new Map<number, string>()

  const nodes: any[] = treeRes?.result ?? treeRes?.items ?? []
  const walk = (n: any) => {
    const cid = n?.description_category_id ?? n?.category_id
    const cname = n?.category_name ?? n?.title ?? n?.name
    if (typeof cid === 'number' && typeof cname === 'string' && cname.trim()) cat.set(cid, cname.trim())

    const types = Array.isArray(n?.types) ? n.types : n?.type_id && n?.type_name ? [{ type_id: n.type_id, type_name: n.type_name }] : []
    for (const t of types) {
      if (typeof t?.type_id === 'number' && typeof t?.type_name === 'string' && t.type_name.trim()) {
        type.set(t.type_id, t.type_name.trim())
      }
    }

    const children = Array.isArray(n?.children) ? n.children : []
    for (const ch of children) walk(ch)
  }

  for (const n of nodes) walk(n)

  const maps = { catNameById: cat, typeNameById: type }
  cachedMaps = { at: Date.now(), maps }
  return maps
}

function pickTextBrand(item: any): string | null {
  const b = item?.brand
  if (typeof b === 'string' && b.trim()) return b.trim()

  // иногда бренд лежит в attributes
  const attrs: any[] = Array.isArray(item?.attributes) ? item.attributes : []
  for (const a of attrs) {
    const name = String(a?.name ?? '').toLowerCase()
    if (name === 'бренд' || name === 'brand') {
      const vals = Array.isArray(a?.values) ? a.values : []
      const v = vals[0]?.value ?? vals[0]
      if (typeof v === 'string' && v.trim() && !/^\d+$/.test(v.trim())) return v.trim()
    }
  }

  return null
}

export type OzonProductRow = {
  offer_id: string
  product_id: number
  sku: string | null
  barcode: string | null
  name: string | null
  brand: string | null
  category: string | null
  type: string | null
  is_visible: boolean | null
  hidden_reasons: string | null
  created_at: string | null
  archived: boolean
}

export async function ozonListAllProducts(secrets: Secrets): Promise<Array<{ offer_id: string; product_id: number; archived: boolean }>> {
  const all: Array<{ offer_id: string; product_id: number; archived: boolean }> = []
  let lastId = ''
  for (;;) {
    const res = await ozonPost<any>(secrets, '/v3/product/list', {
      filter: { visibility: 'ALL' },
      last_id: lastId,
      limit: 1000,
    })
    const items: any[] = res?.result?.items ?? res?.items ?? []
    for (const it of items) {
      if (typeof it?.offer_id === 'string' && typeof it?.product_id === 'number') {
        all.push({ offer_id: it.offer_id, product_id: it.product_id, archived: !!it.archived })
      }
    }
    const nextLastId = res?.result?.last_id ?? res?.last_id ?? ''
    if (!nextLastId || items.length === 0) break
    lastId = String(nextLastId)
  }
  return all
}

export async function ozonProductInfoList(secrets: Secrets, productIds: number[]): Promise<OzonProductRow[]> {
  if (!productIds.length) return []
  const maps = await ozonGetCategoryTypeMaps(secrets)

  const res = await ozonPost<any>(secrets, '/v3/product/info/list', { product_id: productIds })
  const items: any[] = res?.result?.items ?? res?.items ?? []

  return items
    .filter((it) => typeof it?.offer_id === 'string' && typeof it?.product_id === 'number')
    .map((it) => {
      const categoryId = it?.description_category_id ?? it?.category_id ?? null
      const typeId = it?.type_id ?? null

      const categoryName =
        (typeof it?.category_name === 'string' && it.category_name.trim()) ||
        (typeof it?.description_category_name === 'string' && it.description_category_name.trim()) ||
        (typeof categoryId === 'number' ? maps.catNameById.get(categoryId) : null) ||
        null

      const typeName =
        (typeof it?.type_name === 'string' && it.type_name.trim()) ||
        (typeof it?.product_type === 'string' && it.product_type.trim()) ||
        (typeof typeId === 'number' ? maps.typeNameById.get(typeId) : null) ||
        null

      const hiddenReasonsArr: string[] = Array.isArray(it?.hidden_reasons) ? it.hidden_reasons.map(String) : []
      const hidden_reasons = hiddenReasonsArr.length ? hiddenReasonsArr.join(', ') : null

      return {
        offer_id: it.offer_id,
        product_id: it.product_id,
        sku: typeof it?.sku === 'string' || typeof it?.sku === 'number' ? String(it.sku) : null,
        barcode: typeof it?.barcode === 'string' ? it.barcode : null,
        name: typeof it?.name === 'string' ? it.name : null,
        brand: pickTextBrand(it),
        category: categoryName || null,
        type: typeName || null,
        is_visible: typeof it?.is_visible === 'boolean' ? it.is_visible : it?.visible === undefined ? null : !!it?.visible,
        hidden_reasons,
        created_at: typeof it?.created_at === 'string' ? it.created_at : null,
        archived: !!it?.archived,
      } satisfies OzonProductRow
    })
}

export async function ozonGetStoreName(secrets: Secrets): Promise<string | null> {
  // Не у всех аккаунтов есть публичный метод. Пытаемся максимально мягко.
  const tryPaths = ['/v1/seller/info', '/v2/seller/info', '/v1/seller/company', '/v1/seller/company/info']
  for (const p of tryPaths) {
    try {
      const res = await ozonPost<any>(secrets, p, {})
      const name =
        res?.result?.name || res?.result?.company_name || res?.result?.seller_name || res?.name || res?.company_name || null
      if (typeof name === 'string' && name.trim()) return name.trim()
    } catch {
      // ignore
    }
  }
  return null
}

export function chunkNumbers(arr: number[], size: number): number[][] {
  const out: number[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}
