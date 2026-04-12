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
  artifactKey?: string
}

export type CurrentPersistentArtifactSaved = {
  path: string
  slot: string
  fileName: string
  headers: string[]
  artifactKey: string
}

export type SaveCurrentPersistentArtifactsResult = {
  saved: CurrentPersistentArtifactSaved[]
  cleanedLegacyFilesCount: number
}

function normalizeText(value: unknown): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function sanitizeFilePart(value: unknown): string {
  const normalized = normalizeText(value)
    .replace(/[\/:*?"<>|]+/g, '-')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^-+|-+$/g, '')
  return normalized || 'unknown'
}

function shouldDeleteLegacyArtifactFile(fileName: string, slot: string, suffix: string, extension: string, keepName: string): boolean {
  if (!fileName || fileName === keepName) return false
  const slotMarker = `__${slot}__`
  const suffixMarker = `__${suffix}.${extension}`
  const currentFileName = `current__${slot}__${suffix}.${extension}`

  if (fileName === currentFileName) return true
  if (!fileName.endsWith(`.${extension}`)) return false
  if (!fileName.includes(slotMarker)) return false
  if (fileName.endsWith(suffixMarker)) return true
  return false
}

export function saveCurrentPersistentArtifacts(artifacts: CurrentPersistentArtifactInput[]): SaveCurrentPersistentArtifactsResult {
  const safeArtifacts = Array.isArray(artifacts) ? artifacts : []
  if (safeArtifacts.length === 0) return { saved: [], cleanedLegacyFilesCount: 0 }

  const firstGroupPath = Array.isArray(safeArtifacts[0]?.groupPath) ? safeArtifacts[0].groupPath : []
  const root = join(getPersistentRootDir(), ...firstGroupPath)
  mkdirSync(root, { recursive: true })

  const prepared = safeArtifacts.map((artifact) => {
    const extension = sanitizeFilePart(artifact?.extension || 'txt').replace(/^\.+/, '') || 'txt'
    const suffix = sanitizeFilePart(artifact?.suffix || 'report')
    const slot = sanitizeFilePart(artifact?.slot)
    const artifactKey = sanitizeFilePart(artifact?.artifactKey || `${firstGroupPath.join('/')}:${slot}:${suffix}:${extension}`)
    const fileName = `current__${slot}__${suffix}.${extension}`
    const filePath = join(root, fileName)
    writeFileSync(filePath, String(artifact?.content ?? ''), 'utf8')
    return {
      path: filePath,
      slot,
      fileName,
      suffix,
      extension,
      artifactKey,
      headers: Array.isArray(artifact?.headers) ? artifact.headers.map((value) => normalizeText(value)).filter(Boolean) : [],
    }
  })

  let cleanedLegacyFilesCount = 0
  for (const artifact of prepared) {
    for (const entry of readdirSync(root, { withFileTypes: true })) {
      if (!entry.isFile()) continue
      if (!shouldDeleteLegacyArtifactFile(entry.name, artifact.slot, artifact.suffix, artifact.extension, artifact.fileName)) continue
      try {
        rmSync(join(root, entry.name), { force: true })
        cleanedLegacyFilesCount += 1
      } catch {
        // ignore cleanup errors; current files are already written
      }
    }
  }

  return {
    saved: prepared.map((item) => ({
      path: item.path,
      slot: item.slot,
      fileName: item.fileName,
      headers: item.headers,
      artifactKey: item.artifactKey,
    })),
    cleanedLegacyFilesCount,
  }
}
