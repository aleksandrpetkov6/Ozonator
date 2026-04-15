import { app } from 'electron'
import { mkdirSync } from 'fs'
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
  const exeDir = dirname(app.getPath('exe'))
  const dirName = basename(exeDir).trim().toLowerCase()
  if (dirName === 'ozon-seller-os-mvp0') {
    return dirname(exeDir)
  }
  return exeDir
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

export function ensurePersistentStorageReady() {
  mkdirSync(getPersistentRootDir(), { recursive: true })
}

export function readPersistentStorageBootstrapState() {
  const root = getPersistentRootDir()
  const dbPath = join(root, 'app.db')
  const secretsPath = join(root, 'secrets.json')

  return {
    root,
    dbPath,
    secretsPath,
  }
}
