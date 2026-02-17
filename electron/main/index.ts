import { BrowserWindow, app, ipcMain, nativeTheme, safeStorage } from 'electron'
import { copyFileSync, existsSync, mkdirSync } from 'fs'
import { join } from 'path'
import { chunkNumbers, ozonGetStoreName, ozonListAllProducts, ozonProductInfoList, ozonTestAuth } from './ozon'
import { dbClearLogs, dbGetMeta, dbGetProducts, dbGetSyncLog, dbLogFinish, dbLogStart, dbSetMeta, dbUpsertProducts } from './storage/db'
import { deleteSecrets, hasSecrets, loadSecrets, saveSecrets, updateStoreName } from './storage/secrets'
import type { ProductRow } from './types'

/**
 * Важно:
 * - по ТЗ и "Старт" userData должен быть %APPDATA%\Озонатор
 * - именно здесь задаём путь до app.whenReady()
 */
const USER_DATA_DIR = join(app.getPath('appData'), 'Озонатор')
try {
  mkdirSync(USER_DATA_DIR, { recursive: true })
} catch {}
app.setPath('userData', USER_DATA_DIR)

/**
 * Миграция старых данных (если раньше userData был другим именем).
 * Если в новой папке нет БД, но она есть в старой — копируем.
 */
function migrateLegacyDbIfNeeded(): void {
  const newDb = join(app.getPath('userData'), 'app.db')
  if (existsSync(newDb)) return

  const candidates = [
    join(app.getPath('appData'), 'ozon-seller-os-mvp0', 'app.db'),
    join(app.getPath('appData'), 'Ozon Seller OS (MVP0)', 'app.db'),
  ]
  for (const oldDb of candidates) {
    if (existsSync(oldDb)) {
      try {
        copyFileSync(oldDb, newDb)
      } catch {}
      return
    }
  }
}

function createWindow(): BrowserWindow {
  const win = new BrowserWindow({
    width: 1280,
    height: 800,
    backgroundColor: nativeTheme.shouldUseDarkColors ? '#111' : '#fff',
    webPreferences: {
      preload: join(__dirname, '../preload/index.js'),
    },
  })

  if (process.env.ELECTRON_RENDERER_URL) {
    win.loadURL(process.env.ELECTRON_RENDERER_URL)
  } else {
    win.loadFile(join(__dirname, '../renderer/index.html'))
  }

  return win
}

async function logAppUpdateIfNeeded(): Promise<void> {
  const current = typeof app.getVersion === 'function' ? app.getVersion() : 'unknown'
  const prev = dbGetMeta('last_version')
  if (!prev) {
    dbSetMeta('last_version', current)
    return
  }
  if (prev !== current) {
    const id = dbLogStart('app_update', null)
    dbLogFinish(id, { status: 'success', message: `Обновление: ${prev} → ${current}`, details: { from: prev, to: current }, storeClientId: null })
    dbSetMeta('last_version', current)
  }
}

app.whenReady().then(async () => {
  migrateLegacyDbIfNeeded()
  // ensure DB + log updates
  await logAppUpdateIfNeeded()

  const win = createWindow()

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow()
  })

  // ===== IPC: Secrets =====
  ipcMain.handle('secrets:status', () => {
    return {
      hasSecrets: hasSecrets(),
      encryptionAvailable: safeStorage.isEncryptionAvailable(),
    }
  })

  ipcMain.handle('secrets:load', () => {
    const s = loadSecrets()
    return { clientId: s.clientId, apiKey: s.apiKey, storeName: s.storeName ?? null }
  })

  ipcMain.handle('secrets:save', (_evt, payload: { clientId: string; apiKey: string }) => {
    saveSecrets({ clientId: payload.clientId, apiKey: payload.apiKey })
    return true
  })

  ipcMain.handle('secrets:delete', () => {
    deleteSecrets()
    return true
  })

  // ===== IPC: Ozon API =====
  ipcMain.handle('ozon:testAuth', async () => {
    const storeClientId = hasSecrets() ? loadSecrets().clientId : null
    const logId = dbLogStart('check_auth', storeClientId)
    try {
      const secrets = loadSecrets()
      await ozonTestAuth(secrets)

      const storeName = await ozonGetStoreName(secrets)
      if (storeName) updateStoreName(storeName)

      dbLogFinish(logId, { status: 'success', message: storeName ? `OK • ${storeName}` : 'OK', storeClientId })
      // пушим в UI
      win.webContents.send('storeName:updated', storeName ?? null)

      return { ok: true, storeName: storeName ?? null }
    } catch (e: any) {
      dbLogFinish(logId, { status: 'error', message: String(e?.message ?? e), details: { error: String(e) }, storeClientId })
      return { ok: false, error: String(e?.message ?? e) }
    }
  })

  ipcMain.handle('ozon:syncProducts', async () => {
    const storeClientId = hasSecrets() ? loadSecrets().clientId : null
    const logId = dbLogStart('sync_products', storeClientId)

    try {
      const secrets = loadSecrets()

      // 1) список товаров
      const list = await ozonListAllProducts(secrets)
      const ids = list.map((x) => x.product_id)

      // 2) детальная инфа пачками
      const chunks = chunkNumbers(ids, 100)
      const allInfo: any[] = []
      for (const c of chunks) {
        const rows = await ozonProductInfoList(secrets, c)
        allInfo.push(...rows)
      }

      // 3) upsert
      const existing = new Set(dbGetProducts(storeClientId).map((p) => p.offer_id))
      const toSave: ProductRow[] = allInfo.map((p) => ({
        offer_id: p.offer_id,
        product_id: p.product_id,
        sku: p.sku ?? null,
        barcode: p.barcode ?? null,
        brand: p.brand ?? null,
        category: p.category ?? null,
        type: p.type ?? null,
        name: p.name ?? null,
        is_visible: p.is_visible ?? null,
        hidden_reasons: p.hidden_reasons ?? null,
        created_at: p.created_at ?? null,
        archived: !!p.archived,
        store_client_id: secrets.clientId,
      }))

      dbUpsertProducts(toSave)

      const added = toSave.filter((x) => !existing.has(x.offer_id)).length
      const updated = toSave.length - added

      const storeName = await ozonGetStoreName(secrets)
      if (storeName) updateStoreName(storeName)

      dbLogFinish(logId, {
        status: 'success',
        message: `Синхронизация OK • +${added} / ~${updated} • всего ${toSave.length}`,
        details: { added, updated, total: toSave.length },
        storeClientId,
      })

      win.webContents.send('products:updated')
      if (storeName) win.webContents.send('storeName:updated', storeName)

      return { ok: true, added, updated, total: toSave.length }
    } catch (e: any) {
      dbLogFinish(logId, { status: 'error', message: String(e?.message ?? e), details: { error: String(e) }, storeClientId })
      return { ok: false, error: String(e?.message ?? e) }
    }
  })

  // ===== IPC: Data =====
  ipcMain.handle('data:getProducts', () => {
    const storeClientId = hasSecrets() ? loadSecrets().clientId : null
    return dbGetProducts(storeClientId)
  })

  ipcMain.handle('data:getSyncLog', () => {
    const storeClientId = hasSecrets() ? loadSecrets().clientId : null
    return dbGetSyncLog(storeClientId)
  })

  ipcMain.handle('data:clearLogs', () => {
    const storeClientId = hasSecrets() ? loadSecrets().clientId : null
    dbClearLogs(storeClientId)
    return true
  })
})

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit()
})
