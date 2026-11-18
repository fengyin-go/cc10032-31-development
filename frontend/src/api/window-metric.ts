/** 窗口指标账本接口：窗口页面看受影响范围，指标页面核对重算输出与补算清单。 */
import { request } from './client'

export type MetricWindow = {
  window_key: string
  policy: string
  source: string
  window_start: number
  window_end: number
  version: number
  watermark_before: number
  watermark_after: number
  publish_status: string
  pending_recompute?: boolean
  recomputed?: boolean
  locked: boolean
  consumable: boolean
  row_count: number
  late_row_count?: number
  origin: string
  aggregates?: Record<string, number>
  late_aggregates?: Record<string, number>
  recompute_id?: string
}

export type AffectedScope = {
  ok: boolean
  windows: MetricWindow[]
  total: number
  pending_windows: string[]
  failed_windows: string[]
  recompute_count: number
  published_count: number
  pending_count: number
  failed_count: number
}

export type RecomputeWindow = {
  window_key: string
  policy: string
  source: string
  previous_version: number
  locked_version: number | null
  current_version: number | null
  watermark_before: number
  watermark_after: number
  publish_status: string
  origin: string
  row_count_before?: number
  row_count_after?: number
  is_consumable?: boolean
  is_latest?: boolean
}

export type RecomputeEntry = {
  id: number
  recompute_id: string
  kind: string
  policy: string
  source: string
  strategy: string
  index_name?: string
  group_keys?: string[]
  late_count: number
  watermark_before: number
  watermark_after: number
  affected_windows: RecomputeWindow[]
  locked_versions: Record<string, number>
  status: string
  duplicate_hits: number
  error: string
}

export type MetricView = {
  ok: boolean
  items: MetricWindow[]
  total: number
  index?: string
  group_keys?: string[]
  recompute_entries?: RecomputeEntry[]
}

function query(params: Record<string, string | undefined>): string {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value) search.set(key, value)
  }
  const text = search.toString()
  return text ? `?${text}` : ''
}

async function post<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  if (!response.ok) throw new Error(`接口返回 ${response.status}，补算未生效`)
  return (await response.json()) as T
}

export async function loadAffectedScope(
  filters: { policy?: string; source?: string } = {},
): Promise<AffectedScope> {
  const response = await request(`/api/window-metrics/scope${query(filters)}`)
  if (!response.ok) throw new Error('受影响范围读取失败')
  return await response.json()
}

export async function loadMetricView(
  filters: { policy?: string; source?: string; window_key?: string } = {},
): Promise<MetricView> {
  const response = await request(`/api/window-metrics${query(filters)}`)
  if (!response.ok) throw new Error('窗口指标数据读取失败')
  return await response.json()
}

export async function loadRecomputeList(
  filters: { status?: string; policy?: string; source?: string; index_name?: string } = {},
): Promise<{ items: RecomputeEntry[]; total: number }> {
  const response = await request(`/api/window-metrics/recompute-list${query(filters)}`)
  if (!response.ok) throw new Error('迟到补算清单读取失败')
  return await response.json()
}

export async function executeRecompute(recomputeId: string): Promise<{ ok: boolean; error?: string }> {
  return post(`/api/window-metrics/${encodeURIComponent(recomputeId)}/execute`, {})
}

export async function submitLateRecompute(payload: {
  source: string
  policy?: string
  events: { event_id?: string; event_time: number; payload?: Record<string, unknown> }[]
  strategy?: string
  watermark?: number
}): Promise<{ ok: boolean; error?: string; version_created?: boolean }> {
  return post('/api/window-metrics/late-recompute', payload)
}

export async function submitGroupRecompute(payload: {
  index_name: string
  group_keys?: string[]
  group_filter?: Record<string, string>
  window_keys?: string[]
  policy?: string
  source?: string
}): Promise<{ ok: boolean; error?: string; version_created?: boolean; affected_windows?: RecomputeWindow[] }> {
  return post('/api/window-metrics/group-recompute', payload)
}
