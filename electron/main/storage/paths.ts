import { app } from 'electron'
import { copyFileSync, existsSync, mkdirSync } from 'fs'
import { dirname, join } from 'path'

const LEGACY_VENDOR_ROOT_SEGMENTS = ['Clothes Hub', 'OzonatorPersistent']
const INSTALL_LOCAL_STORAGE_DIRNAME = 'data'

export function getLegacyUserDataDir() {
  return app.getPath('userData')
}

export function getLegacyPersistentRootDir() {
  return join(app.getPath('appData'), ...LEGACY_VENDOR_ROOT_SEGMENTS)
}

export function getLifecycleMarkerRootDir() {
  return getLegacyPersistentRootDir()
}

function getInstallRootDir() {
  return dirname(app.getPath('exe'))
}

export function getPersistentRootDir() {
  if (app.isPackaged) {
    return join(getInstallRootDir(), INSTALL_LOCAL_STORAGE_DIRNAME)
  }
  return getLegacyPersistentRootDir()
}

export function getPersistentDbPath() {
  return join(getPersistentRootDir(), 'app.db')
}

export function getPersistentSecretsPath() {
  return join(getPersistentRootDir(), 'secrets.json')
}

export function getLifecycleMarkerPath(kind: 'installer' | 'uninstall') {
  return join(getLifecycleMarkerRootDir(), `${kind}.marker`)
}

function tryCopyFileIfMissing(src: string, dst: string) {
  if (existsSync(dst) || !existsSync(src)) return false
  copyFileSync(src, dst)
  return true
}

export function ensurePersistentStorageReady() {
  const root = getPersistentRootDir()
  mkdirSync(root, { recursive: true })

  const targetDb = getPersistentDbPath()
  if (!existsSync(targetDb)) {
    const candidates = [
      join(getLegacyPersistentRootDir(), 'app.db'),
      join(getLegacyUserDataDir(), 'app.db'),
    ]

    for (const legacyDb of candidates) {
      if (tryCopyFileIfMissing(legacyDb, targetDb)) {
        for (const name of ['app.db-wal', 'app.db-shm']) {
          const legacyDir = dirname(legacyDb)
          const src = join(legacyDir, name)
          const dst = join(root, name)
          if (!existsSync(dst) && existsSync(src)) {
            try {
              copyFileSync(src, dst)
            } catch {
              // не критично — БД обычно уже консистентна без wal/shm
            }
          }
        }
        break
      }
    }
  }
}
