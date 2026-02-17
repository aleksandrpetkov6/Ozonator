export type Secrets = {
  clientId: string
  apiKey: string
  /** Название магазина (не секрет) */
  storeName?: string | null
}

export type ProductRow = {
  offer_id: string
  product_id: number | null
  sku: string | null
  barcode: string | null
  brand: string | null
  category: string | null
  type: string | null
  name: string | null
  is_visible: boolean | null
  hidden_reasons: string | null
  created_at: string | null
  archived: boolean
  store_client_id: string
}

export type SyncLogType = 'check_auth' | 'sync_products' | 'app_update'
export type SyncLogStatus = 'started' | 'success' | 'error'

export type SyncLogRow = {
  id: number
  type: SyncLogType
  status: SyncLogStatus
  message: string | null
  details: unknown | null
  created_at: string
  version: string | null
  store_client_id: string | null
}
