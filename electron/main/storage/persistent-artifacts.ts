import { mkdirSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { getPersistentRootDir } from './paths'

export type CurrentPersistentArtifactInput = {
  groupPath: string[]
  slot: string
  content: string
  extension?: string
  suffix?: string
  headers?: string[]
}

export type CurrentPersistentArtifactSaved = {
  path: string
  slot: string
  fileName: string
  headers: string[]
  cleanedLegacyFilesCount: number
}

function normalizeText(value: unknown): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function sanitizeFilePart(value: unknown): string {
  const normalized = normalizeText(value)
    .replace(/[\\/:*?"<>|]+/g, '-')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^-+|-+$/g, '')
  return normalized || 'unknown'
}

export function saveCurrentPersistentArtifacts(artifacts: CurrentPersistentArtifactInput[]): CurrentPersistentArtifactSaved[] {
  const safeArtifacts = Array.isArray(artifacts) ? artifacts : []
  if (safeArtifacts.length === 0) return []

  const firstGroupPath = Array.isArray(safeArtifacts[0]?.groupPath) ? safeArtifacts[0].groupPath : []
  const root = join(getPersistentRootDir(), ...firstGroupPath)
  mkdirSync(root, { recursive: true })

  const prepared = safeArtifacts.map((artifact) => {
    const extension = sanitizeFilePart(artifact?.extension || 'txt').replace(/^\.+/, '') || 'txt'
    const suffix = sanitizeFilePart(artifact?.suffix || 'report')
    const slot = sanitizeFilePart(artifact?.slot)
    const fileName = `current__${slot}__${suffix}.${extension}`
    const filePath = join(root, fileName)
    writeFileSync(filePath, String(artifact?.content ?? ''), 'utf8')
    return {
      path: filePath,
      slot,
      fileName,
      headers: Array.isArray(artifact?.headers) ? artifact.headers.map((value) => normalizeText(value)).filter(Boolean) : [],
    }
  })

  const keepNames = new Set(prepared.map((item) => item.fileName))
  let cleanedLegacyFilesCount = 0
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    if (!entry.isFile()) continue
    if (keepNames.has(entry.name)) continue
    try {
      rmSync(join(root, entry.name), { force: true })
      cleanedLegacyFilesCount += 1
    } catch {
      // ignore cleanup errors; current files are already written
    }
  }

  return prepared.map((item) => ({
    ...item,
    cleanedLegacyFilesCount,
  }))
}
