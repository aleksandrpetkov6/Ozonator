export {}

declare global {
  type GridApiRow = {
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
    store_client_id?: string | null
    warehouse_id?: number | null
    warehouse_name?: string | null
    placement_zone?: string | null
    in_process_at?: string | null
    posting_number?: string | null
    related_postings?: string | null
    shipment_date?: string | null
    status?: string | null
    delivery_date?: string | null
    delivery_model?: string | null
    price?: number | string | null
    quantity?: number | string | null
    paid_by_customer?: number | string | null
  }

  interface Window {
    api: {
      secretsStatus: () => Promise<{ hasSecrets: boolean; encryptionAvailable: boolean }>
      saveSecrets: (secrets: { clientId: string; apiKey: string; storeName?: string }) => Promise<{ ok: boolean }>
      loadSecrets: () => Promise<{ ok: boolean; secrets: { clientId: string; apiKey: string; storeName?: string | null } }>
      deleteSecrets: () => Promise<{ ok: boolean }>

      netCheck: () => Promise<{ online: boolean }>

      getAdminSettings: () => Promise<{ ok: boolean; error?: string; logRetentionDays: number }>
      saveAdminSettings: (payload: { logRetentionDays: number }) => Promise<{ ok: boolean; error?: string; logRetentionDays: number }>

      testAuth: () => Promise<{ ok: boolean; storeName?: string | null; error?: string }>
      syncProducts: () => Promise<{ ok: boolean; itemsCount?: number; pages?: number; placementRowsCount?: number; placementSyncError?: string | null; error?: string }>

      getProducts: () => Promise<{ ok: boolean; error?: string; products: GridApiRow[] }>
      getSales: () => Promise<{ ok: boolean; error?: string; rows: GridApiRow[] }>
      getReturns: () => Promise<{ ok: boolean; error?: string; rows: GridApiRow[] }>
      getStocks: () => Promise<{ ok: boolean; error?: string; rows: GridApiRow[] }>
      getGridColumns: (dataset: 'products' | 'sales' | 'returns' | 'stocks') => Promise<{ ok: boolean; error?: string; dataset: 'products' | 'sales' | 'returns' | 'stocks'; cols: Array<{ id: string; w: number; visible: boolean }> | null }>
      saveGridColumns: (dataset: 'products' | 'sales' | 'returns' | 'stocks', cols: Array<{ id: string; w: number; visible: boolean }>) => Promise<{ ok: boolean; error?: string; dataset: 'products' | 'sales' | 'returns' | 'stocks'; savedCount: number }>
      getSyncLog: () => Promise<{ ok: boolean; logs: any[] }>
      clearLogs: () => Promise<{ ok: boolean }>
    }
  }
}
