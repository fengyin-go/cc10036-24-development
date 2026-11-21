<template>
  <section class="cohort-panel" data-testid="cohort-panel">
    <header class="page-head">
      <div>
        <h3>持久窗口进度</h3>
        <p class="page-desc">
          中断后重新进入时核对已交付窗口；已交付的留存依据按批次（data_version）锁定。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="cohort-run" type="button" @click="run(false)">
          开始分析
        </button>
        <button class="btn" data-testid="cohort-run-step" type="button" @click="run(true)">
          计算一周后中断
        </button>
        <button class="btn" data-testid="cohort-recover" type="button" @click="recover">
          恢复未完成窗口
        </button>
      </div>
    </header>

    <p v-if="message" class="page-desc" data-testid="cohort-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div v-if="progress?.counts" class="stat-row">
      <article class="stat-card">
        <span class="stat-label">窗口总数</span>
        <strong class="stat-value">{{ progress.counts.total }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已交付</span>
        <strong class="stat-value">{{ progress.counts.delivered }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待处理</span>
        <strong class="stat-value">{{ progress.counts.pending }}</strong>
      </article>
    </div>

    <p v-if="progress?.run" class="status-legend" data-testid="cohort-run-meta">
      <span class="legend-item">运行：{{ progress.run.run_id }}</span>
      <span class="legend-item">状态：{{ progress.run.status }}</span>
      <span class="legend-item">批次：{{ progress.run.data_version.slice(0, 12) }}</span>
      <span v-if="progress.run.stop_reason" class="legend-item">
        中断原因：{{ progress.run.stop_reason }}
      </span>
    </p>

    <table v-if="retention?.cohort_ids.length" class="data-table" data-testid="cohort-matrix">
      <thead>
        <tr>
          <th>同期群</th>
          <th>分母</th>
          <th v-for="label in retention.period_labels" :key="label">{{ label }}</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(cohort, index) in retention.cohort_ids" :key="cohort">
          <td>{{ cohort }}</td>
          <td>{{ retention.denominators[index] }}</td>
          <td v-for="(label, period) in retention.period_labels" :key="label">
            <template v-if="retention.retention_matrix[index][period] !== null">
              {{ retention.retention_matrix[index][period] }}%
              <small>（{{ retention.numerators[index][period] }}/{{ retention.denominators[index] }}）</small>
            </template>
            <span v-else class="pending-cell">待处理</span>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-else class="empty-state">{{ retention?.message ?? '暂无已交付窗口' }}</p>

    <p v-if="progress && progress.attempts.length" class="page-desc">
      尝试记录：
      <span v-for="attempt in progress.attempts" :key="attempt.attempt_no" class="legend-item">
        #{{ attempt.attempt_no }} {{ attempt.mode }}/{{ attempt.status }}/{{ attempt.windows_written }} 窗口
      </span>
    </p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  loadCohortProgress,
  loadCohortRetention,
  recoverCohortWindows,
  runCohort,
  type CohortProgressView,
  type CohortRetentionView,
} from '@/api/cohort'

const props = defineProps<{ configId: number }>()
const progress = ref<CohortProgressView | null>(null)
const retention = ref<CohortRetentionView | null>(null)
const message = ref('')
const errorMessage = ref('')

async function reload() {
  errorMessage.value = ''
  try {
    const [progressView, retentionView] = await Promise.all([
      loadCohortProgress(props.configId),
      loadCohortRetention(props.configId),
    ])
    progress.value = progressView
    retention.value = retentionView
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '读取同期群进度失败'
  }
}

async function run(step: boolean) {
  errorMessage.value = ''
  try {
    const result = await runCohort(props.configId, step ? 1 : undefined)
    message.value = result.message
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分析执行失败'
  }
}

async function recover() {
  errorMessage.value = ''
  try {
    const result = await recoverCohortWindows(props.configId)
    message.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.pending-cell { color: var(--muted); }
.cohort-panel { margin-top: 20px; border-top: 1px solid var(--border); padding-top: 12px; }
</style>
