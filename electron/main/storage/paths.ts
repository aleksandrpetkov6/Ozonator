import { app } from 'electron'
import { copyFileSync, existsSync, mkdirSync } from 'fs'
import { join } from 'path'

export function getLegacyUserDataDir() {
  return app.getPath('userData')
}

export function getPersistentRootDir() {
  // Отдельная папка в Roaming, не привязанная к userData текущей сборки/идентификатору приложения.
  // Это помогает сохранить лог при обновлении/переустановке/удалении.
  return join(app.getPath('appData'), 'Clothes Hub', 'OzonatorPersistent')
}

export function getPersistentDbPath() {
  return join(getPersistentRootDir(), 'app.db')
}

export function getLifecycleMarkerPath(kind: 'installer' | 'uninstall') {
  return join(getPersistentRootDir(), `${kind}.marker`)
}

export function ensurePersistentStorageReady() {
  const root = getPersistentRootDir()
  mkdirSync(root, { recursive: true })

  const targetDb = getPersistentDbPath()
  if (!existsSync(targetDb)) {
    const legacyDir = getLegacyUserDataDir()
    const legacyDb = join(legacyDir, 'app.db')

    if (existsSync(legacyDb)) {
      copyFileSync(legacyDb, targetDb)

      const extras = ['app.db-wal', 'app.db-shm']
      for (const name of extras) {
        const src = join(legacyDir, name)
        const dst = join(root, name)
        if (existsSync(src)) {
          try {
            copyFileSync(src, dst)
          } catch {
            // не критично — БД обычно уже консистентна без wal/shm
          }
        }
      }
    }
  }
}
