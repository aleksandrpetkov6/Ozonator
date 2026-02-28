import { contextBridge, ipcRenderer } from 'electron'

contextBridge.exposeInMainWorld('api', {
  secretsStatus: () => ipcRenderer.invoke('secrets:status'),
  saveSecrets: (secrets: { storeName?: string; clientId: string; apiKey: string }) => ipcRenderer.invoke('secrets:save', secrets),
  loadSecrets: () => ipcRenderer.invoke('secrets:load'),
  deleteSecrets: () => ipcRenderer.invoke('secrets:delete'),
  netCheck: () => ipcRenderer.invoke('net:check'),

  getAdminSettings: () => ipcRenderer.invoke('admin:getSettings'),
  saveAdminSettings: (payload: { logRetentionDays: number }) => ipcRenderer.invoke('admin:saveSettings', payload),

  testAuth: () => ipcRenderer.invoke('ozon:testAuth'),
  syncProducts: () => ipcRenderer.invoke('ozon:syncProducts'),

  getProducts: () => ipcRenderer.invoke('data:getProducts'),
  getSales: (period?: { from?: string; to?: string }) => ipcRenderer.invoke('data:getSales', { period: period ?? null }),
  getReturns: () => ipcRenderer.invoke('data:getReturns'),
  getStocks: () => ipcRenderer.invoke('data:getStocks'),
  getGridColumns: (dataset: 'products' | 'sales' | 'returns' | 'stocks') => ipcRenderer.invoke('ui:getGridColumns', { dataset }),
  saveGridColumns: (dataset: 'products' | 'sales' | 'returns' | 'stocks', cols: Array<{ id: string; w: number; visible: boolean; hiddenBucket: 'main' | 'add' }>) => ipcRenderer.invoke('ui:saveGridColumns', { dataset, cols }),
  getSyncLog: () => ipcRenderer.invoke('data:getSyncLog'),
  clearLogs: () => ipcRenderer.invoke('data:clearLogs'),
})
