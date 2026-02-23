export {}

declare global {
  interface Window {
    api: {
      secretsStatus: () => Promise<{ hasSecrets: boolean; encryptionAvailable: boolean }>
      saveSecrets: (secrets: { clientId: string; apiKey: string }) => Promise<{ ok: boolean }>
      loadSecrets: () => Promise<{ ok: boolean; secrets: { clientId: string; apiKey: string; storeName?: string | null } }>
      deleteSecrets: () => Promise<{ ok: boolean }>

      netCheck: () => Promise<{ online: boolean }>

      getAdminSettings: () => Promise<{ ok: boolean; error?: string; logRetentionDays: number }>
      saveAdminSettings: (payload: { logRetentionDays: number }) => Promise<{ ok: boolean; error?: string; logRetentionDays: number }>

      testAuth: () => Promise<{ ok: boolean; storeName?: string | null; error?: string }>
      syncProducts: () => Promise<{ ok: boolean; itemsCount?: number; pages?: number; error?: string }>

      getProducts: () => Promise<{ ok: boolean; error?: string; products: Array<{
        offer_id: string
        sku?: string | null
        barcode?: string | null
        brand?: string | null
        category?: string | null
        type?: string | null
        name?: string | null
        is_visible?: number | boolean | null
        hidden_reasons?: string | null
        created_at?: string | null
        store_client_id?: string | null
        updated_at?: string
      }> }>
      getSyncLog: () => Promise<{ ok: boolean; logs: any[] }>
      clearLogs: () => Promise<{ ok: boolean }>
    }
  }
}
