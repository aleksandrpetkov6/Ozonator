import type { Secrets } from './types'

const OZON_BASE = 'https://api-seller.ozon.ru'

function headers(secrets: Secrets) {
  return {
    'Client-Id': secrets.clientId,
    'Api-Key': secrets.apiKey,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  }
}

function uniquePostingNumbers(values: string[]): string[] {
  const out: string[] = []
  const seen = new Set<string>()
  for (const value of values) {
    const postingNumber = String(value ?? '').trim()
    if (!postingNumber || seen.has(postingNumber)) continue
    seen.add(postingNumber)
    out.push(postingNumber)
  }
  return out
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}

async function parseJsonSafe(text: string) {
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}

async function postFboGet(secrets: Secrets, body: any): Promise<any> {
  const res = await fetch(`${OZON_BASE}/v2/posting/fbo/get`, {
    method: 'POST',
    headers: headers(secrets) as any,
    body: JSON.stringify(body ?? {}),
  })
  const text = await res.text()
  const json = await parseJsonSafe(text)
  if (!res.ok) {
    const err: any = new Error(`Ozon FBO get failed: ${res.status}`)
    err.response = json ?? text
    throw err
  }
  return json
}

function extractResult(payload: any): any {
  if (payload?.result && typeof payload.result === 'object') return payload.result
  return payload && typeof payload === 'object' ? payload : null
}

async function fetchOneCompat(secrets: Secrets, postingNumber: string): Promise<any | null> {
  const bodies = [
    { posting_number: postingNumber, with: { financial_data: true, analytics_data: true } },
    { posting_number: postingNumber, with: { financial_data: true } },
    { posting_number: postingNumber },
  ]

  for (const body of bodies) {
    try {
      const payload = await postFboGet(secrets, body)
      const result = extractResult(payload)
      if (result) return result
    } catch {
      // пробуем следующий совместимый body
    }
  }

  return null
}

export async function fetchFboPostingDetailsCompat(secrets: Secrets, postingNumbers: string[]): Promise<Map<string, any>> {
  const list = uniquePostingNumbers(postingNumbers)
  if (list.length === 0) return new Map<string, any>()

  const out = new Map<string, any>()
  for (const batch of chunk(list, 10)) {
    const settled = await Promise.allSettled(batch.map(async (postingNumber) => ({
      postingNumber,
      payload: await fetchOneCompat(secrets, postingNumber),
    })))
    for (const result of settled) {
      if (result.status !== 'fulfilled') continue
      const postingNumber = String(result.value?.postingNumber ?? '').trim()
      const payload = result.value?.payload ?? null
      if (!postingNumber || !payload) continue
      out.set(postingNumber, payload)
    }
  }

  return out
}
