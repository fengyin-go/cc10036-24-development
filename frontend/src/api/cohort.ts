/** 同期群持久窗口进度的前端视图类型与请求封装。 */
import { request } from './client'

export type CohortWindow = {
  run_id: string
  cohort_id: string
  period_index: number
  denominator: number
  numerator: number
  rate: number
  status: string
  delivered_at: string
}

export type CohortRun = {
  run_id: string
  config_id: number
  config_name: string
  status: string
  data_version: string
  watermark: number
  event_count: number
  stop_reason: string | null
  created_at: string
  completed_at: string | null
}

export type CohortProgressView = {
  ok: boolean
  config_id: number
  config_name: string
  message?: string
  run: CohortRun | null
  counts?: { total: number; delivered: number; pending: number }
  windows: CohortWindow[]
  pending_windows: { cohort_id: string; period_index: number; denominator: number }[]
  attempts: {
    attempt_no: number
    mode: string
    status: string
    windows_written: number
    detail: string | null
  }[]
  runs: CohortRun[]
}

export type CohortRetentionView = {
  ok: boolean
  config_id: number
  config_name: string
  run_id?: string
  data_version?: string
  run_status?: string
  complete: boolean
  cohort_ids: string[]
  period_labels: string[]
  retention_matrix: (number | null)[][]
  numerators: (number | null)[][]
  denominators: number[]
  counts?: { total: number; delivered: number; pending: number }
  superseded_runs?: string[]
  message?: string
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}，数据未更新`)
  return (await response.json()) as T
}

export function loadCohortProgress(configId: number): Promise<CohortProgressView> {
  return getJson<CohortProgressView>(`/api/cohort/${configId}/progress`)
}

export function loadCohortRetention(configId: number): Promise<CohortRetentionView> {
  return getJson<CohortRetentionView>(`/api/cohort/${configId}/retention`)
}

export async function runCohort(
  configId: number,
  stopAfterWindows?: number,
): Promise<{ ok: boolean; message: string; run_status?: string }> {
  const query = new URLSearchParams({ config_id: String(configId) })
  if (stopAfterWindows !== undefined) {
    query.set('stop_after_windows', String(stopAfterWindows))
  }
  const response = await request(`/api/cohort/runs?${query}`, { method: 'POST' })
  return (await response.json()) as { ok: boolean; message: string; run_status?: string }
}

export async function recoverCohortWindows(
  configId: number,
): Promise<{ ok: boolean; message: string; retried: number }> {
  const response = await request(`/api/cohort/recovery-plan/retry?config_id=${configId}`, {
    method: 'POST',
  })
  return (await response.json()) as { ok: boolean; message: string; retried: number }
}
