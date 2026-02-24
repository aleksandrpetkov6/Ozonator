import { app, BrowserWindow, ipcMain, nativeTheme, safeStorage, net } from 'electron'
import { join } from 'path'
import { ensureDb, dbGetAdminSettings, dbSaveAdminSettings, dbIngestLifecycleMarkers, dbGetProducts, dbGetSyncLog, dbClearLogs, dbLogFinish, dbLogStart, dbUpsertProducts, dbDeleteProductsMissingForStore, dbCountProducts } from './storage/db'
import { deleteSecrets, hasSecrets, loadSecrets, saveSecrets, updateStoreName } from './storage/secrets'
import { ozonGetStoreName, ozonProductInfoList, ozonProductList, ozonTestAuth } from './ozon'

let mainWindow: BrowserWindow | null = null

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 760,
    minWidth: 980,
    minHeight: 620,
    title: 'Озонатор',

    // чтобы окно не «моргало»
    show: false,
    backgroundColor: '#F5F5F7',
    autoHideMenuBar: true,

    // macOS-like titlebar overlay (на Windows 11 выглядит аккуратно)
    titleBarOverlay: { color: '#F5F5F7', symbolColor: '#1d1d1f', height: 34 },

    webPreferences: {
      preload: join(__dirname, '../preload/index.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  })

  mainWindow.once('ready-to-show', () => {
    try {
      // Стартуем развернутыми на весь экран (maximize), но не в fullscreen/kiosk.
      mainWindow?.maximize()
      mainWindow?.show()
    } catch {}
  })

  mainWindow.webContents.on('did-fail-load', (_e, code, desc, url) => {
    console.error('did-fail-load', { code, desc, url })
  })

  // В этом проекте dev-url задаётся как ELECTRON_RENDERER_URL.
  // На случай другой сборки оставляем fallback на VITE_DEV_SERVER_URL.
  const devUrl = process.env.ELECTRON_RENDERER_URL || process.env.VITE_DEV_SERVER_URL || (!app.isPackaged ? "http://localhost:5173/" : null)
  if (devUrl) {
    mainWindow.loadURL(devUrl)
    mainWindow.webContents.openDevTools({ mode: 'detach' })
  } else {
    // В проде renderer лежит в out/renderer/index.html
    mainWindow.loadFile(join(app.getAppPath(), 'out/renderer/index.html'))
  }

  nativeTheme.themeSource = 'light'
}

app.whenReady().then(() => {
  if (!safeStorage.isEncryptionAvailable()) {
    console.warn('safeStorage encryption is not available on this machine.')
  }

  ensureDb()
  dbIngestLifecycleMarkers({ appVersion: app.getVersion() })
  createWindow()

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow()
  })
})

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit()
})

// -------- IPC --------

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

    // Пытаемся подтянуть название магазина и сохранить локально (не секрет)
    try {
      const name = await ozonGetStoreName(secrets)
      if (name) updateStoreName(name)
    } catch {
      // не критично
    }

    dbLogFinish(logId, { status: 'success', storeClientId: secrets.clientId })

    const refreshed = loadSecrets()
    return { ok: true, storeName: refreshed.storeName ?? null }
  } catch (e: any) {
    dbLogFinish(logId, { status: 'error', errorMessage: e?.message ?? String(e), errorDetails: e?.details, storeClientId })
    return { ok: false, error: e?.message ?? String(e) }
  }
})

ipcMain.handle('ozon:syncProducts', async () => {
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

    // Идём по страницам
    for (let guard = 0; guard < 200; guard++) {
      const { items, lastId: next, total: totalMaybe } = await ozonProductList(secrets, { lastId, limit })
      pages += 1
      // items.length — сколько пришло с API
      total += items.length

      const ids = items.map(i => i.product_id).filter(Boolean) as number[]

      // Вытягиваем расширенную информацию
      const infoList = await ozonProductInfoList(secrets, ids)
      const infoMap = new Map<number, typeof infoList[number]>()
      for (const p of infoList) infoMap.set(p.product_id, p)

      const enriched = items.map((it) => {
        const info = it.product_id ? infoMap.get(it.product_id) : undefined
        return {
          offer_id: it.offer_id,
          product_id: it.product_id,
          sku: (info?.sku ?? it.sku ?? null),
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

      // если API отдаёт total, можно досрочно остановиться
      if (typeof totalMaybe === 'number' && total >= totalMaybe) break
    }

    dbDeleteProductsMissingForStore(secrets.clientId, Array.from(incomingOfferIds))
    const syncedCount = dbCountProducts(secrets.clientId)

    // Обновляем storeName в фоне, если ещё не было
    if (!secrets.storeName) {
      try {
        const name = await ozonGetStoreName(secrets)
        if (name) updateStoreName(name)
      } catch {
        // ignore
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
      },
    })

    return { ok: true, itemsCount: syncedCount, pages }
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
