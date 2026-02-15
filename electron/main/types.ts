export type Secrets = {
  clientId: string
  apiKey: string
  /**
   * Название магазина (не секрет). Кэшируем локально, чтобы не спрашивать Ozon каждый раз.
   * Может быть null/undefined, если ещё не удалось получить.
   */
  storeName?: string | null
}

export type ProductRow = {
  offer_id: string
  product_id?: number | null
  sku?: string | null
  barcode?: string | null
  brand?: string | null
  category?: string | null
  type?: string | null
  name?: string | null
  is_visible?: number | boolean | null
  hidden_reasons?: string | null
  created_at?: string | null

  /**
   * Чтобы не смешивать товары разных кабинетов (если вы меняете ключи).
   * Равно Client-Id активного магазина на момент синхронизации.
   */
  store_client_id?: string | null

  archived?: number | null
  updated_at: string
}

export type SyncLogRow = {
  id: number
  type: 'check_auth' | 'sync_products'
  status: 'success' | 'error'
  started_at: string
  finished_at: string | null
  items_count: number | null
  error_message: string | null
  error_details: string | null
  meta: string | null
}
