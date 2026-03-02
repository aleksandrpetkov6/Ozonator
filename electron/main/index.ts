import { app, BrowserWindow, ipcMain, nativeTheme, safeStorage, net, dialog } from 'electron'
import { join } from 'path'
import { appendFileSync, mkdirSync } from 'fs'
import { ensureDb, dbGetAdminSettings, dbSaveAdminSettings, dbIngestLifecycleMarkers, dbGetLatestApiRawResponses, dbGetProducts, dbGetSyncLog, dbClearLogs, dbLogFinish, dbLogStart, dbUpsertProducts, dbDeleteProductsMissingForStore, dbCountProducts, dbGetStockViewRows, dbReplaceProductPlacementsForStore, dbRecordApiRawResponse, dbGetGridColumns, dbSaveGridColumns } from './storage/db'
import { deleteSecrets, hasSecrets, loadSecrets, saveSecrets, updateStoreName } from './storage/secrets'
import { ozonGetStoreName, ozonPlacementZoneInfo, ozonPostingFboList, ozonPostingFbsList, ozonProductInfoList, ozonProductList, ozonTestAuth, ozonWarehouseList, setOzonApiCaptureHook } from './ozon'
import { type SalesPayloadEnvelope, type SalesPeriod, fetchSalesEndpointPages, fetchSalesPostingDetails, normalizeSalesRows } from './sales-sync'
let mainWindow: BrowserWindow | null = null
let startupShowTimer: NodeJS.Timeout | null = null
function startupLog(...args: any[]) {
try {
const dir = app?.isReady?.() ? app.getPath('userData') : app.getPath('temp')
mkdirSync(dir, { recursive: true })
const line = `[${new Date().toISOString()}] ` + args.map((a) => {
try { return typeof a === 'string' ? a : JSON.stringify(a) } catch { return String(a) }
}).join(' ') + '\n'
appendFileSync(join(dir, 'ozonator-startup.log'), line, 'utf8')
} catch {}
try { console.log('[startup]', ...args) } catch {}
}
function safeShowMainWindow(reason: string) {
try {
if (!mainWindow || mainWindow.isDestroyed()) return
startupLog('safeShowMainWindow', { reason, visible: mainWindow.isVisible() })
if (!mainWindow.isVisible()) {
try { mainWindow.show() } catch {}
}
try { mainWindow.focus() } catch {}
try { mainWindow.maximize() } catch {}
} catch (e: any) {
startupLog('safeShowMainWindow.error', e?.message ?? String(e))
}
}
function chunk<T>(arr: T[], size: number): T[][] {
const out: T[][] = []
for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
return out
}
const SALES_CACHE_SNAPSHOT_ENDPOINTS = {
  fbs: '/__local__/sales-cache/fbs',
  fbo: '/__local__/sales-cache/fbo',
  details: '/__local__/sales-cache/posting-details',
} as const
const SALES_CACHE_SNAPSHOT_KEYS = [
  SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs,
  SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo,
  SALES_CACHE_SNAPSHOT_ENDPOINTS.details,
] as const
const SALES_LEGACY_ENDPOINTS = ['/v3/posting/fbs/list', '/v2/posting/fbo/list'] as const

function parseJsonTextSafe(text: string | null | undefined) {
if (typeof text !== 'string' || !text.trim()) return null
try {
return JSON.parse(text)
} catch {
return null
}
}

function normalizeSalesPeriodCacheValue(value: any): string | null {
const raw = typeof value === 'string' ? value.trim() : ''
return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null
}

function normalizeSalesPeriodCache(period: SalesPeriod | null | undefined): { from: string | null; to: string | null } {
let from = normalizeSalesPeriodCacheValue(period?.from)
let to = normalizeSalesPeriodCacheValue(period?.to)
if (!from && to) from = to
if (from && !to) to = from
if (from && to && from > to) [from, to] = [to, from]
return { from, to }
}

function sameSalesPeriodCache(
left: { from?: string | null; to?: string | null } | null | undefined,
right: { from?: string | null; to?: string | null } | null | undefined,
) {
return (left?.from ?? null) === (right?.from ?? null) && (left?.to ?? null) === (right?.to ?? null)
}

function getCachedSalesSnapshotMap(storeClientId: string | null | undefined) {
const scoped = dbGetLatestApiRawResponses(storeClientId ?? null, SALES_CACHE_SNAPSHOT_KEYS as unknown as string[])
const rows = scoped.length > 0 ? scoped : dbGetLatestApiRawResponses(null, SALES_CACHE_SNAPSHOT_KEYS as unknown as string[])
const out = new Map<string, any>()
for (const row of rows) {
if (out.has(row.endpoint)) continue
const parsed = parseJsonTextSafe(row?.response_body)
if (parsed) out.set(row.endpoint, parsed)
}
return out
}

function getCachedSalesPayloadMap(storeClientId: string | null | undefined): Map<string, SalesPayloadEnvelope> {
const scoped = dbGetLatestApiRawResponses(storeClientId ?? null, SALES_LEGACY_ENDPOINTS as unknown as string[])
const rows = scoped.length > 0 ? scoped : dbGetLatestApiRawResponses(null, SALES_LEGACY_ENDPOINTS as unknown as string[])
const out = new Map<string, SalesPayloadEnvelope>()
for (const row of rows) {
if (out.has(row.endpoint)) continue
const parsed = parseJsonTextSafe(row?.response_body)
if (parsed) out.set(row.endpoint, { endpoint: row.endpoint, payload: parsed })
}
return out
}

function buildSalesPayloadsFromLocalCache(
cacheByEndpoint: Map<string, any>,
requestedPeriod: SalesPeriod | null | undefined,
): SalesPayloadEnvelope[] {
const normalizedRequestedPeriod = normalizeSalesPeriodCache(requestedPeriod)
const fbsSnapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs)
const fboSnapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo)
const out: SalesPayloadEnvelope[] = []
const pushSnapshot = (snapshot: any, fallbackEndpoint: string) => {
if (!snapshot || typeof snapshot !== 'object') return
const snapshotPeriod = normalizeSalesPeriodCache(snapshot?.period ?? null)
if (!sameSalesPeriodCache(snapshotPeriod, normalizedRequestedPeriod)) return
const payloads = Array.isArray(snapshot?.payloads) ? snapshot.payloads : []
const sourceEndpoint = String(snapshot?.sourceEndpoint ?? '').trim() || fallbackEndpoint
for (const payload of payloads) out.push({ endpoint: sourceEndpoint, payload })
}
pushSnapshot(fbsSnapshot, '/v3/posting/fbs/list')
pushSnapshot(fboSnapshot, '/v2/posting/fbo/list')
return out
}

function buildSalesPostingDetailsFromLocalCache(
cacheByEndpoint: Map<string, any>,
requestedPeriod: SalesPeriod | null | undefined,
): Map<string, any> {
const detailsSnapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.details)
if (!detailsSnapshot || typeof detailsSnapshot !== 'object') return new Map<string, any>()
const normalizedRequestedPeriod = normalizeSalesPeriodCache(requestedPeriod)
const detailsPeriod = normalizeSalesPeriodCache(detailsSnapshot?.period ?? null)
if (!sameSalesPeriodCache(detailsPeriod, normalizedRequestedPeriod)) {
return new Map<string, any>()
}
const out = new Map<string, any>()
const items = Array.isArray(detailsSnapshot?.items) ? detailsSnapshot.items : []
for (const item of items) {
const key = String(item?.key ?? '').trim()
if (!key) continue
out.set(key, item?.payload ?? null)
}
return out
}

async function refreshSalesCacheFromApi(
secrets: ReturnType<typeof loadSecrets>,
requestedPeriod: SalesPeriod | null | undefined,
) {
const normalizedRequestedPeriod = normalizeSalesPeriodCache(requestedPeriod)
const fbsPayloads = await fetchSalesEndpointPages(
(body) => ozonPostingFbsList(secrets, body),
requestedPeriod,
'/v3/posting/fbs/list',
)
const fboPayloads = await fetchSalesEndpointPages(
(body) => ozonPostingFboList(secrets, body),
requestedPeriod,
'/v2/posting/fbo/list',
)
const payloads = [...fbsPayloads, ...fboPayloads]
const postingDetailsByKey = payloads.length > 0
? await fetchSalesPostingDetails(secrets, payloads)
: new Map<string, any>()
const fetchedAt = new Date().toISOString()
const persistSnapshot = (endpoint: string, responseBody: any) => {
  dbRecordApiRawResponse({
    storeClientId: secrets.clientId,
    method: 'LOCAL',
    endpoint,
    requestBody: {
      mode: 'sales-cache-snapshot',
      period: normalizedRequestedPeriod,
    },
    responseBody,
    httpStatus: 200,
    isSuccess: true,
    fetchedAt,
  })
}
persistSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs, {
  sourceEndpoint: '/v3/posting/fbs/list',
  period: normalizedRequestedPeriod,
  payloads: fbsPayloads.map((item) => item.payload),
})
persistSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo, {
  sourceEndpoint: '/v2/posting/fbo/list',
  period: normalizedRequestedPeriod,
  payloads: fboPayloads.map((item) => item.payload),
})
persistSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.details, {
  period: normalizedRequestedPeriod,
  items: Array.from(postingDetailsByKey.entries()).map(([key, payload]) => ({ key, payload })),
})
}
function createWindow() {
startupLog('createWindow.begin', { packaged: app.isPackaged, appPath: app.getAppPath(), __dirname })
mainWindow = new BrowserWindow({
width: 1200,
height: 760,
minWidth: 980,
minHeight: 620,
title: 'Озонатор',
show: false,
backgroundColor: '#F5F5F7',
autoHideMenuBar: true,
titleBarOverlay: { color: '#F5F5F7', symbolColor: '#1d1d1f', height: 34 },
webPreferences: {
preload: join(__dirname, '../preload/index.js'),
contextIsolation: true,
nodeIntegration: false,
},
})
if (startupShowTimer) {
clearTimeout(startupShowTimer)
startupShowTimer = null
}
startupShowTimer = setTimeout(() => safeShowMainWindow('show-timeout-fallback'), 2500)
mainWindow.once('ready-to-show', () => {
startupLog('event.ready-to-show')
safeShowMainWindow('ready-to-show')
})
mainWindow.webContents.on('did-finish-load', () => {
startupLog('event.did-finish-load', { url: mainWindow?.webContents?.getURL?.() })
safeShowMainWindow('did-finish-load')
})
mainWindow.webContents.on('did-fail-load', (_e, code, desc, url, isMainFrame) => {
startupLog('event.did-fail-load', { code, desc, url, isMainFrame })
try {
if (isMainFrame && mainWindow && !mainWindow.isDestroyed()) {
        const html = `<!doctype html><html><body style="font-family:Segoe UI,sans-serif;padding:16px">
          <h3>Озонатор не смог загрузить интерфейс</h3>
          <div>Причина: ${String(desc || 'did-fail-load')} (code ${String(code)})</div>
          <div style="margin-top:8px;color:#555">Подробности в файле ozonator-startup.log в папке данных приложения.</div>
        </body></html>`
mainWindow.loadURL('data:text/html;charset=utf-8,' + encodeURIComponent(html)).catch(() => {})
}
} catch {}
safeShowMainWindow('did-fail-load')
})
mainWindow.webContents.on('render-process-gone', (_e, details) => {
startupLog('event.render-process-gone', details)
safeShowMainWindow('render-process-gone')
})
mainWindow.on('unresponsive', () => {
startupLog('event.window-unresponsive')
})
mainWindow.on('closed', () => {
startupLog('event.window-closed')
if (startupShowTimer) {
clearTimeout(startupShowTimer)
startupShowTimer = null
}
mainWindow = null
})
const devUrl = process.env.ELECTRON_RENDERER_URL || process.env.VITE_DEV_SERVER_URL || (!app.isPackaged ? 'http://localhost:5173/' : null)
startupLog('renderer.target', { devUrl, packaged: app.isPackaged })
if (devUrl) {
mainWindow.loadURL(devUrl).catch((e) => startupLog('loadURL.error', e?.message ?? String(e)))
try { mainWindow.webContents.openDevTools({ mode: 'detach' }) } catch {}
} else {
const rendererFile = join(app.getAppPath(), 'out/renderer/index.html')
startupLog('renderer.file', rendererFile)
mainWindow.loadFile(rendererFile).catch((e) => startupLog('loadFile.error', e?.message ?? String(e)))
}
nativeTheme.themeSource = 'light'
}
app.whenReady().then(() => {
try {
startupLog('app.whenReady')
if (!safeStorage.isEncryptionAvailable()) {
console.warn('safeStorage encryption is not available on this machine.')
startupLog('safeStorage.unavailable')
}
ensureDb()
startupLog('ensureDb.ok')
setOzonApiCaptureHook((evt) => {
dbRecordApiRawResponse({
storeClientId: evt.storeClientId,
method: evt.method,
endpoint: evt.endpoint,
requestBody: evt.requestBody,
responseBody: evt.responseBody,
httpStatus: evt.httpStatus,
isSuccess: evt.isSuccess,
errorMessage: evt.errorMessage ?? null,
fetchedAt: evt.fetchedAt,
})
})
dbIngestLifecycleMarkers({ appVersion: app.getVersion() })
startupLog('dbIngestLifecycleMarkers.ok', { version: app.getVersion() })
createWindow()
app.on('activate', () => {
startupLog('app.activate', { windows: BrowserWindow.getAllWindows().length })
if (BrowserWindow.getAllWindows().length === 0) createWindow()
else safeShowMainWindow('app-activate')
})
} catch (e: any) {
startupLog('fatal.startup', e?.stack ?? e?.message ?? String(e))
try {
dialog.showErrorBox('Озонатор — ошибка запуска', String(e?.message ?? e))
} catch {}
try {
if (!mainWindow) {
mainWindow = new BrowserWindow({ width: 900, height: 640, show: true, autoHideMenuBar: true })
        const html = `<!doctype html><html><body style="font-family:Segoe UI,sans-serif;padding:16px">
          <h3>Озонатор не запустился</h3>
          <pre style="white-space:pre-wrap">${String(e?.stack ?? e?.message ?? e)}</pre>
          <div style="color:#555">Подробности: ozonator-startup.log в папке данных приложения.</div>
        </body></html>`
mainWindow.loadURL('data:text/html;charset=utf-8,' + encodeURIComponent(html)).catch(() => {})
}
} catch {}
}
})
process.on('uncaughtException', (e: any) => {
startupLog('process.uncaughtException', e?.stack ?? e?.message ?? String(e))
})
process.on('unhandledRejection', (e: any) => {
startupLog('process.unhandledRejection', e as any)
})
app.on('window-all-closed', () => {
if (process.platform !== 'darwin') app.quit()
})
function checkInternet(timeoutMs = 2500): Promise<boolean> {
return new Promise((resolve) => {
const request = net.request({ method: 'GET', url: 'https://api-seller.ozon.ru' })
const timer = setTimeout(() => {
try { request.abort() } catch {}
resolve(false)
}, timeoutMs)
request.on('response', () => {
clearTimeout(timer)
resolve(true)
})
request.on('error', () => {
clearTimeout(timer)
resolve(false)
})
request.end()
})
}
ipcMain.handle('secrets:status', async () => {
return {
hasSecrets: hasSecrets(),
encryptionAvailable: safeStorage.isEncryptionAvailable(),
}
})
ipcMain.handle('secrets:save', async (_e, secrets: { clientId: string; apiKey: string }) => {
saveSecrets({ clientId: String(secrets.clientId).trim(), apiKey: String(secrets.apiKey).trim() })
return { ok: true }
})
ipcMain.handle('secrets:load', async () => {
const s = loadSecrets()
return { ok: true, secrets: { clientId: s.clientId, apiKey: s.apiKey, storeName: s.storeName ?? null } }
})
ipcMain.handle('secrets:delete', async () => {
deleteSecrets()
return { ok: true }
})
ipcMain.handle('net:check', async () => {
return { online: await checkInternet() }
})
ipcMain.handle('admin:getSettings', async () => {
try {
return { ok: true, ...dbGetAdminSettings() }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), logRetentionDays: 30 }
}
})
ipcMain.handle('admin:saveSettings', async (_e, payload: { logRetentionDays?: number }) => {
try {
const saved = dbSaveAdminSettings({ logRetentionDays: Number(payload?.logRetentionDays) })
return { ok: true, ...saved }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e) }
}
})
ipcMain.handle('ozon:testAuth', async () => {
let storeClientId: string | null = null
try { storeClientId = loadSecrets().clientId } catch {}
const logId = dbLogStart('check_auth', storeClientId)
try {
const secrets = loadSecrets()
await ozonTestAuth(secrets)
try {
const name = await ozonGetStoreName(secrets)
if (name) updateStoreName(name)
} catch {
}
dbLogFinish(logId, { status: 'success', storeClientId: secrets.clientId })
const refreshed = loadSecrets()
return { ok: true, storeName: refreshed.storeName ?? null }
} catch (e: any) {
dbLogFinish(logId, { status: 'error', errorMessage: e?.message ?? String(e), errorDetails: e?.details, storeClientId })
return { ok: false, error: e?.message ?? String(e) }
}
})
ipcMain.handle('ozon:syncProducts', async (_e, args?: { salesPeriod?: SalesPeriod | null }) => {
let storeClientId: string | null = null
try { storeClientId = loadSecrets().clientId } catch {}
const logId = dbLogStart('sync_products', storeClientId)
try {
const secrets = loadSecrets()
const existingOfferIds = new Set(dbGetProducts(secrets.clientId).map((p: any) => p.offer_id))
const incomingOfferIds = new Set<string>()
let added = 0
let lastId = ''
const limit = 1000
let pages = 0
let total = 0
for (let guard = 0; guard < 200; guard++) {
const { items, lastId: next, total: totalMaybe } = await ozonProductList(secrets, { lastId, limit })
pages += 1
total += items.length
const ids = items.map(i => i.product_id).filter(Boolean) as number[]
const infoList = await ozonProductInfoList(secrets, ids)
const infoMap = new Map<number, typeof infoList[number]>()
for (const p of infoList) infoMap.set(p.product_id, p)
const enriched = items.map((it) => {
const info = it.product_id ? infoMap.get(it.product_id) : undefined
return {
offer_id: it.offer_id,
product_id: it.product_id,
sku: (info?.ozon_sku ?? info?.sku ?? it.sku ?? null),
ozon_sku: (info?.ozon_sku ?? info?.sku ?? it.sku ?? null),
seller_sku: (info?.seller_sku ?? it.offer_id ?? null),
fbo_sku: info?.fbo_sku ?? null,
fbs_sku: info?.fbs_sku ?? null,
barcode: info?.barcode ?? null,
brand: info?.brand ?? null,
category: info?.category ?? null,
type: info?.type ?? null,
name: info?.name ?? null,
photo_url: info?.photo_url ?? null,
is_visible: info?.is_visible ?? null,
hidden_reasons: info?.hidden_reasons ?? null,
created_at: info?.created_at ?? null,
archived: it.archived ?? false,
store_client_id: secrets.clientId,
}
})
for (const it of enriched) {
const offer = String((it as any).offer_id)
if (offer) incomingOfferIds.add(offer)
if (!existingOfferIds.has(offer)) {
existingOfferIds.add(offer)
added += 1
}
}
dbUpsertProducts(enriched)
if (!next) break
if (next === lastId) break
lastId = next
if (typeof totalMaybe === 'number' && total >= totalMaybe) break
}
dbDeleteProductsMissingForStore(secrets.clientId, Array.from(incomingOfferIds))
const syncedCount = dbCountProducts(secrets.clientId)
let placementRowsCount = 0
let placementSyncError: string | null = null
let placementCacheKept = false
try {
const productsForStore = dbGetProducts(secrets.clientId)
const ozonSkuList = Array.from(new Set(productsForStore.map((p) => String(p.sku ?? '').trim()).filter(Boolean)))
const sellerSkuList = Array.from(new Set(productsForStore.map((p) => String(p.offer_id ?? '').trim()).filter(Boolean)))
if (ozonSkuList.length > 0 || sellerSkuList.length > 0) {
const warehouses = await ozonWarehouseList(secrets)
if (!Array.isArray(warehouses) || warehouses.length === 0) {
placementSyncError = 'Ozon не вернул список складов; локальные данные по складам/зонам сохранены без перезаписи.'
placementCacheKept = true
} else {
const allPlacementRows: Array<{
warehouse_id: number
warehouse_name?: string | null
sku: string
ozon_sku?: string | null
seller_sku?: string | null
placement_zone?: string | null
}> = []
const placementRowKeys = new Set<string>()
let placementApiCallCount = 0
const appendPlacementRows = (
warehouseId: number,
warehouseName: string | null,
zones: Array<{
sku: string
ozon_sku?: string | null
seller_sku?: string | null
placement_zone: string | null
}>
) => {
for (const z of zones) {
const rowKey = [
String(warehouseId),
String(z.ozon_sku ?? ''),
String(z.seller_sku ?? ''),
String(z.placement_zone ?? ''),
].join('::')
if (placementRowKeys.has(rowKey)) continue
placementRowKeys.add(rowKey)
allPlacementRows.push({
warehouse_id: warehouseId,
warehouse_name: warehouseName,
sku: z.sku,
ozon_sku: z.ozon_sku ?? null,
seller_sku: z.seller_sku ?? null,
placement_zone: z.placement_zone ?? null,
})
}
}
for (const wh of warehouses) {
const wid = Number(wh.warehouse_id)
if (!Number.isFinite(wid)) continue
for (const part of chunk(ozonSkuList, 500)) {
placementApiCallCount += 1
const zones = await ozonPlacementZoneInfo(secrets, { warehouseId: wid, skus: part })
appendPlacementRows(wid, wh.name ?? null, zones)
}
for (const part of chunk(sellerSkuList, 500)) {
placementApiCallCount += 1
const zones = await ozonPlacementZoneInfo(secrets, { warehouseId: wid, skus: part })
appendPlacementRows(wid, wh.name ?? null, zones)
}
}
if (allPlacementRows.length === 0 && placementApiCallCount > 0) {
placementSyncError = 'Ozon не вернул зоны размещения ни по одному SKU; прежние локальные данные по складам/зонам сохранены.'
placementCacheKept = true
} else {
placementRowsCount = dbReplaceProductPlacementsForStore(secrets.clientId, allPlacementRows)
}
}
} else {
placementRowsCount = dbReplaceProductPlacementsForStore(secrets.clientId, [])
}
} catch (placementErr: any) {
placementSyncError = placementErr?.message ?? String(placementErr)
}
try {
await refreshSalesCacheFromApi(secrets, args?.salesPeriod ?? null)
} catch {
}
if (!secrets.storeName) {
try {
const name = await ozonGetStoreName(secrets)
if (name) updateStoreName(name)
} catch {
}
}
dbLogFinish(logId, {
status: 'success',
itemsCount: syncedCount,
storeClientId: secrets.clientId,
meta: {
added,
storeClientId: secrets.clientId,
storeName: loadSecrets().storeName ?? null,
placementRowsCount,
placementSyncError,
placementCacheKept,
},
})
return { ok: true, itemsCount: syncedCount, pages, placementRowsCount, placementSyncError }
} catch (e: any) {
dbLogFinish(logId, { status: 'error', errorMessage: e?.message ?? String(e), errorDetails: e?.details, storeClientId })
return { ok: false, error: e?.message ?? String(e) }
}
})
ipcMain.handle('data:getProducts', async () => {
try {
let storeClientId: string | null = null
try {
storeClientId = loadSecrets().clientId
} catch {
storeClientId = null
}
const products = dbGetProducts(storeClientId)
return { ok: true, products }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), products: [] }
}
})
ipcMain.handle('data:getSales', async (_e, args?: { period?: SalesPeriod | null }) => {
try {
let storeClientId: string | null = null
try {
storeClientId = loadSecrets().clientId
} catch {
storeClientId = null
}
const requestedPeriod: SalesPeriod | null = args?.period ?? null
const products = dbGetProducts(storeClientId)
const cacheByEndpoint = getCachedSalesSnapshotMap(storeClientId)
const payloads = buildSalesPayloadsFromLocalCache(cacheByEndpoint, requestedPeriod)
const postingDetailsByKey = buildSalesPostingDetailsFromLocalCache(cacheByEndpoint, requestedPeriod)
if (payloads.length === 0 && cacheByEndpoint.size === 0) {
const legacyPayloads = getCachedSalesPayloadMap(storeClientId)
for (const payload of legacyPayloads.values()) payloads.push(payload)
}
const rows = normalizeSalesRows(payloads, products, postingDetailsByKey)
return { ok: true, rows }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), rows: [] }
}
})
ipcMain.handle('data:getReturns', async () => {
try {
let storeClientId: string | null = null
try {
storeClientId = loadSecrets().clientId
} catch {
storeClientId = null
}
const products = dbGetProducts(storeClientId)
return { ok: true, rows: products }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), rows: [] }
}
})
ipcMain.handle('data:getStocks', async () => {
try {
let storeClientId: string | null = null
try {
storeClientId = loadSecrets().clientId
} catch {
storeClientId = null
}
const rows = dbGetStockViewRows(storeClientId)
return { ok: true, rows }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), rows: [] }
}
})
ipcMain.handle('ui:getGridColumns', async (_e, args: { dataset: 'products' | 'sales' | 'returns' | 'stocks' }) => {
try {
return { ok: true, ...dbGetGridColumns(args?.dataset) }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), dataset: (args?.dataset ?? 'products') as 'products' | 'sales' | 'returns' | 'stocks', cols: null }
}
})
ipcMain.handle('ui:saveGridColumns', async (_e, args: { dataset: 'products' | 'sales' | 'returns' | 'stocks'; cols: Array<{ id: string; w: number; visible: boolean; hiddenBucket: 'main' | 'add' }> }) => {
try {
return { ok: true, ...dbSaveGridColumns(args?.dataset, args?.cols) }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), dataset: (args?.dataset ?? 'products') as 'products' | 'sales' | 'returns' | 'stocks', savedCount: 0 }
}
})
ipcMain.handle('data:getSyncLog', async () => {
try {
let storeClientId: string | null = null
try {
storeClientId = loadSecrets().clientId
} catch {
storeClientId = null
}
const logs = dbGetSyncLog(storeClientId)
return { ok: true, logs }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), logs: [] }
}
})
ipcMain.handle('data:clearLogs', async () => {
try {
dbClearLogs()
return { ok: true }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e) }
}
})
