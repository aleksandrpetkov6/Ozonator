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
    status_details?: string | null
    carrier_status_details?: string | null
    delivery_date?: string | null
    delivery_cluster?: string | null
    delivery_model?: string | null
    price?: number | string | null
    quantity?: number | string | null
    paid_by_customer?: number | string | null
  }

  interface Window {
    api: {
      localServerConfig: () => Promise<{ ok: boolean; baseUrl?: string; healthUrlLocal?: string; token?: string; webhookPath?: string; webhookUrlLocal?: string; webhookProbePath?: string; webhookProbeUrlLocal?: string; webhookToken?: string; serverStartedAt?: string; lastProbeAt?: string; lastPushHitAt?: string; lastPushAcceptedAt?: string; lastPushAcceptedEvents?: number; error?: string }>
      localServerProbe: () => Promise<{ ok: boolean; status?: string; probeAt?: string; httpStatus?: number; webhookProbeUrlLocal?: string; error?: string }>
      secretsStatus: () => Promise<{ hasSecrets: boolean; encryptionAvailable: boolean }>
      saveSecrets: (secrets: { clientId: string; apiKey: string; storeName?: string }) => Promise<{ ok: boolean }>
      loadSecrets: () => Promise<{ ok: boolean; secrets: { clientId: string; apiKey: string; storeName?: string | null } }>
      deleteSecrets: () => Promise<{ ok: boolean }>

      netCheck: () => Promise<{ online: boolean }>

      getAdminSettings: () => Promise<{ ok: boolean; error?: string; logRetentionDays: number }>
      saveAdminSettings: (payload: { logRetentionDays: number }) => Promise<{ ok: boolean; error?: string; logRetentionDays: number }>

      testAuth: () => Promise<{ ok: boolean; storeName?: string | null; error?: string }>
      syncProducts: (salesPeriod?: { from?: string; to?: string } | null) => Promise<{ ok: boolean; itemsCount?: number; pages?: number; placementRowsCount?: number; placementSyncError?: string | null; error?: string }>
      refreshSales: (period?: { from?: string; to?: string } | null) => Promise<{ ok: boolean; error?: string; rowsCount?: number; rateLimited?: boolean }>

      getDatasetRows: (dataset: string, options?: { period?: { from?: string; to?: string } | null }) => Promise<{ ok: boolean; error?: string; dataset: string; rows: GridApiRow[] }>
      getProducts: () => Promise<{ ok: boolean; error?: string; products: GridApiRow[] }>
      getSales: (period?: { from?: string; to?: string }) => Promise<{ ok: boolean; error?: string; rows: GridApiRow[] }>
      getReturns: () => Promise<{ ok: boolean; error?: string; rows: GridApiRow[] }>
      getStocks: () => Promise<{ ok: boolean; error?: string; rows: GridApiRow[] }>
      getGridColumns: (dataset: string) => Promise<{ ok: boolean; error?: string; dataset: string; cols: Array<{ id: string; w: number; visible: boolean; hiddenBucket: 'main' | 'add' }> | null }>
      saveGridColumns: (dataset: string, cols: Array<{ id: string; w: number; visible: boolean; hiddenBucket: 'main' | 'add' }>) => Promise<{ ok: boolean; error?: string; dataset: string; savedCount: number }>
      getSyncLog: () => Promise<{ ok: boolean; logs: any[] }>
      clearLogs: () => Promise<{ ok: boolean }>
    }
  }
}
