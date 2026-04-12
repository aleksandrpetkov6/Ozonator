export type DataEntityKind = 'snapshot' | 'domain_table' | 'event_log' | 'raw_cache' | 'persistent_file_artifact'

export type DataMergeStrategy = 'replace' | 'incremental_upsert_backfill' | 'authoritative_upsert_prune_backfill' | 'append_only' | 'current_only_replace'

export type DataEntityContract = {
  entityKey: string
  entityKind: DataEntityKind
  schemaVersion: number
  defaultMergeStrategy: DataMergeStrategy
  stableKeyGroups?: string[][]
  notes?: string
}

const SNAPSHOT_CONTRACTS: Record<string, DataEntityContract> = {
  products: {
    entityKey: 'products',
    entityKind: 'snapshot',
    schemaVersion: 2,
    defaultMergeStrategy: 'authoritative_upsert_prune_backfill',
    stableKeyGroups: [['offer_id'], ['product_id'], ['sku'], ['ozon_sku'], ['seller_sku']],
    notes: 'Projection snapshot fed from products domain table.',
  },
  stocks: {
    entityKey: 'stocks',
    entityKind: 'snapshot',
    schemaVersion: 2,
    defaultMergeStrategy: 'authoritative_upsert_prune_backfill',
    stableKeyGroups: [['warehouse_id', 'sku'], ['warehouse_id', 'offer_id'], ['warehouse_id', 'ozon_sku'], ['warehouse_id', 'seller_sku']],
    notes: 'Projection snapshot fed from stock view.',
  },
  returns: {
    entityKey: 'returns',
    entityKind: 'snapshot',
    schemaVersion: 2,
    defaultMergeStrategy: 'authoritative_upsert_prune_backfill',
    stableKeyGroups: [['offer_id'], ['product_id'], ['sku'], ['ozon_sku'], ['seller_sku']],
    notes: 'Projection snapshot derived from products until dedicated returns source is introduced.',
  },
  'forecast-demand': {
    entityKey: 'forecast-demand',
    entityKind: 'snapshot',
    schemaVersion: 2,
    defaultMergeStrategy: 'authoritative_upsert_prune_backfill',
    stableKeyGroups: [['offer_id'], ['product_id'], ['sku'], ['ozon_sku'], ['seller_sku']],
    notes: 'Projection snapshot derived from products until dedicated demand source is introduced.',
  },
  sales: {
    entityKey: 'sales',
    entityKind: 'snapshot',
    schemaVersion: 2,
    defaultMergeStrategy: 'incremental_upsert_backfill',
    stableKeyGroups: [['delivery_model', 'posting_number', 'sku', 'offer_id', 'name'], ['posting_number', 'sku'], ['posting_number', 'offer_id']],
    notes: 'Sales snapshot keeps authoritative append/update semantics by posting/item key and allows backfill of later fields.',
  },
}

const DOMAIN_CONTRACTS: Record<string, DataEntityContract> = {
  products_table: {
    entityKey: 'products_table',
    entityKind: 'domain_table',
    schemaVersion: 2,
    defaultMergeStrategy: 'authoritative_upsert_prune_backfill',
    stableKeyGroups: [['offer_id']],
    notes: 'Products domain table: preserve meaningful existing values, upsert fresh values, prune stale rows by sync scope.',
  },
  product_placements_table: {
    entityKey: 'product_placements_table',
    entityKind: 'domain_table',
    schemaVersion: 2,
    defaultMergeStrategy: 'authoritative_upsert_prune_backfill',
    stableKeyGroups: [['store_client_id', 'warehouse_id', 'sku']],
    notes: 'Product placements domain table: scoped authoritative upsert with prune of removed keys only.',
  },
  fbo_postings_table: {
    entityKey: 'fbo_postings_table',
    entityKind: 'domain_table',
    schemaVersion: 2,
    defaultMergeStrategy: 'authoritative_upsert_prune_backfill',
    stableKeyGroups: [['store_client_id', 'period_key', 'posting_number']],
    notes: 'FBO postings table: period-scoped authoritative upsert/backfill.',
  },
  fbo_posting_items_table: {
    entityKey: 'fbo_posting_items_table',
    entityKind: 'domain_table',
    schemaVersion: 2,
    defaultMergeStrategy: 'authoritative_upsert_prune_backfill',
    stableKeyGroups: [['store_client_id', 'period_key', 'posting_number', 'line_no']],
    notes: 'FBO posting items table: period-scoped authoritative upsert/backfill.',
  },
  fbo_postings_report_table: {
    entityKey: 'fbo_postings_report_table',
    entityKind: 'domain_table',
    schemaVersion: 2,
    defaultMergeStrategy: 'authoritative_upsert_prune_backfill',
    stableKeyGroups: [['store_client_id', 'period_key', 'posting_number']],
    notes: 'Persisted FBO report rows: period-scoped authoritative upsert/backfill.',
  },
  posting_state_events_table: {
    entityKey: 'posting_state_events_table',
    entityKind: 'event_log',
    schemaVersion: 2,
    defaultMergeStrategy: 'authoritative_upsert_prune_backfill',
    stableKeyGroups: [['store_client_id', 'period_key', 'posting_number', 'event_key']],
    notes: 'State events act as append/update by stable event key inside a scoped period.',
  },
  api_raw_cache: {
    entityKey: 'api_raw_cache',
    entityKind: 'raw_cache',
    schemaVersion: 2,
    defaultMergeStrategy: 'append_only',
    stableKeyGroups: [['store_client_id', 'registry_key', 'fetched_at']],
    notes: 'Raw cache is append-only and should be governed by retention/TTL, not by replace.',
  },
}

const FILE_ARTIFACT_CONTRACTS: Record<string, DataEntityContract> = {
  postings_report_fbo_current_file: {
    entityKey: 'postings_report_fbo_current_file',
    entityKind: 'persistent_file_artifact',
    schemaVersion: 2,
    defaultMergeStrategy: 'current_only_replace',
    stableKeyGroups: [['artifact_key'], ['delivery_schema']],
    notes: 'Current-only persisted CSV artifact for FBO postings report.',
  },
  postings_report_fbs_current_file: {
    entityKey: 'postings_report_fbs_current_file',
    entityKind: 'persistent_file_artifact',
    schemaVersion: 2,
    defaultMergeStrategy: 'current_only_replace',
    stableKeyGroups: [['artifact_key'], ['delivery_schema']],
    notes: 'Current-only persisted CSV artifact for FBS postings report.',
  },
}

const DATA_ENTITY_CONTRACTS = {
  ...SNAPSHOT_CONTRACTS,
  ...DOMAIN_CONTRACTS,
  ...FILE_ARTIFACT_CONTRACTS,
} as const

export function listDataEntityContracts(): DataEntityContract[] {
  return Object.values(DATA_ENTITY_CONTRACTS).map((contract) => ({
    ...contract,
    stableKeyGroups: contract.stableKeyGroups?.map((group) => [...group]),
  }))
}

export function getDataEntityContract(entityKey: string): DataEntityContract | null {
  const normalized = String(entityKey ?? '').trim().toLowerCase()
  if (!normalized) return null
  const contract = (DATA_ENTITY_CONTRACTS as Record<string, DataEntityContract | undefined>)[normalized]
  if (!contract) return null
  return {
    ...contract,
    stableKeyGroups: contract.stableKeyGroups?.map((group) => [...group]),
  }
}

export function getDatasetSnapshotContract(dataset: string): DataEntityContract | null {
  const normalized = String(dataset ?? '').trim().toLowerCase()
  if (!normalized) return null
  const contract = SNAPSHOT_CONTRACTS[normalized]
  return contract ? { ...contract, stableKeyGroups: contract.stableKeyGroups?.map((group) => [...group]) } : null
}

export function getDatasetSnapshotSchemaVersion(dataset: string): number {
  return getDatasetSnapshotContract(dataset)?.schemaVersion ?? 1
}

export function getDatasetSnapshotDefaultMergeStrategy(dataset: string, sourceKind?: string | null): DataMergeStrategy {
  const contract = getDatasetSnapshotContract(dataset)
  if (contract) return contract.defaultMergeStrategy

  const normalizedSourceKind = String(sourceKind ?? '').trim().toLowerCase()
  if (normalizedSourceKind === 'db-table' || normalizedSourceKind === 'db-view' || normalizedSourceKind === 'derived-products') {
    return 'authoritative_upsert_prune_backfill'
  }
  return 'incremental_upsert_backfill'
}

function normalizeKeyPart(value: unknown): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  return ''
}

function getPathValue(source: Record<string, any>, path: string): unknown {
  const parts = String(path ?? '').split('.').map((part) => part.trim()).filter(Boolean)
  let current: any = source
  for (const part of parts) {
    if (!current || typeof current !== 'object' || !(part in current)) return undefined
    current = current[part]
  }
  return current
}

function buildKeyFromGroups(entityKey: string, row: Record<string, any>, groups: string[][]): string | null {
  for (const group of groups) {
    const parts = group.map((path) => normalizeKeyPart(getPathValue(row, path)))
    if (parts.every(Boolean)) return `${entityKey}::${group.join('+')}::${parts.join('::')}`
  }
  return null
}

export function inferStableRowKey(entityKey: string, row: any): string | null {
  if (!row || typeof row !== 'object' || Array.isArray(row)) return null
  const contract = getDataEntityContract(entityKey)
  const contractGroups = contract?.stableKeyGroups ?? []
  const contractKey = buildKeyFromGroups(String(contract?.entityKey ?? entityKey).trim() || 'entity', row, contractGroups)
  if (contractKey) return contractKey

  const fallbackGroups: string[][] = [
    ['id'],
    ['artifact_key'],
    ['posting_number', 'sku'],
    ['posting_number', 'offer_id'],
    ['warehouse_id', 'sku'],
    ['warehouse_id', 'offer_id'],
    ['offer_id'],
    ['product_id'],
    ['sku'],
    ['ozon_sku'],
    ['seller_sku'],
    ['fbo_sku'],
    ['fbs_sku'],
  ]

  return buildKeyFromGroups(String(contract?.entityKey ?? entityKey).trim() || 'entity', row, fallbackGroups)
}
