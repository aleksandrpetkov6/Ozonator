import { app, safeStorage } from 'electron'
import { existsSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from 'fs'
import { join } from 'path'
import type { Secrets } from '../types'

function secretsPath(): string {
  return join(app.getPath('userData'), 'secrets.json')
}

function ensureDir(): void {
  try {
    mkdirSync(app.getPath('userData'), { recursive: true })
  } catch {}
}

function decryptMaybe(buf: Buffer): string {
  if (safeStorage.isEncryptionAvailable()) {
    return safeStorage.decryptString(buf)
  }
  // Fallback: если файл был сохранён в открытом виде (для dev/ручной правки)
  return buf.toString('utf8')
}

export function hasSecrets(): boolean {
  return existsSync(secretsPath())
}

export function loadSecrets(): Secrets {
  if (!existsSync(secretsPath())) {
    throw new Error('Ключи не сохранены. Откройте Настройки.')
  }
  const raw = readFileSync(secretsPath())
  const jsonStr = decryptMaybe(raw)
  const data = JSON.parse(jsonStr)

  return {
    clientId: String(data.clientId ?? '').trim(),
    apiKey: String(data.apiKey ?? '').trim(),
    storeName: data.storeName ? String(data.storeName) : null,
  }
}

export function saveSecrets(secrets: { clientId: string; apiKey: string; storeName?: string | null }): void {
  if (!safeStorage.isEncryptionAvailable()) {
    throw new Error(
      'Шифрование safeStorage недоступно на этой машине.\n' +
        'Нельзя безопасно сохранить Client-Id и Api-Key.\n\n' +
        'Решение: включите пароль/пин для входа в Windows и перезапустите программу.'
    )
  }

  ensureDir()

  const payload = JSON.stringify({
    clientId: String(secrets.clientId).trim(),
    apiKey: String(secrets.apiKey).trim(),
    storeName: secrets.storeName ?? null,
  })

  const enc = safeStorage.encryptString(payload)
  writeFileSync(secretsPath(), enc)
}

export function deleteSecrets(): void {
  if (existsSync(secretsPath())) {
    unlinkSync(secretsPath())
  }
}

export function updateStoreName(storeName: string): void {
  try {
    const s = loadSecrets()
    saveSecrets({ clientId: s.clientId, apiKey: s.apiKey, storeName })
  } catch {
    // ignore
  }
}
