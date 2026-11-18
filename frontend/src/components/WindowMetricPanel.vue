<template>
  <section class="ledger-panel" :data-view="mode">
    <header class="ledger-head">
      <div>
        <h3>{{ mode === 'window' ? '窗口指标数据 · 受影响范围' : '窗口指标数据 · 重算输出' }}</h3>
        <p class="ledger-desc">
          每个受影响窗口只保留一个当前版本；水位边界取自迟到补算清单，可核对重算前后口径。
        </p>
      </div>
      <div class="ledger-summary">
        <span>窗口 {{ scope?.total ?? 0 }}</span>
        <span>已发布 {{ scope?.published_count ?? 0 }}</span>
        <span>待补 {{ scope?.pending_count ?? 0 }}</span>
        <span v-if="scope?.failed_count">失败 {{ scope.failed_count }}</span>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div class="ledger-toolbar">
      <label>
        <span>策略</span>
        <input v-model="policy" placeholder="按策略过滤" @keyup.enter="reload" />
      </label>
      <label>
        <span>源端</span>
        <input v-model="source" placeholder="按源端过滤" @keyup.enter="reload" />
      </label>
      <label v-if="mode === 'metric'">
        <span>指标</span>
        <input v-model="indexName" placeholder="按指标名称重算" @keyup.enter="reload" />
      </label>
      <button class="btn" type="button" @click="reload">刷新</button>
      <button
        v-if="mode === 'window'"
        class="btn primary"
        type="button"
        :disabled="!editablePolicy || loading"
        @click="runLateRecompute"
      >
        按迟到水位重算
      </button>
      <button
        v-else
        class="btn primary"
        type="button"
        :disabled="!editablePolicy || loading"
        @click="runGroupRecompute"
      >
        重算当前分组
      </button>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th>窗口</th>
          <th>策略 / 源端</th>
          <th>当前版本</th>
          <th>水位 before → after</th>
          <th>发布状态</th>
          <th>可消费</th>
          <th>行数</th>
          <th>{{ mode === 'window' ? '受影响' : '重算输出' }}</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="item in windows"
          :key="`${item.window_key}-${item.version}`"
          :data-testid="`metric-window-${item.window_key}`"
          :data-status="item.publish_status"
        >
          <td class="mono">{{ item.window_key }}</td>
          <td>{{ item.policy }}<br /><small>{{ item.source }}</small></td>
          <td>v{{ item.version }}</td>
          <td class="mono">{{ item.watermark_before }} → {{ item.watermark_after }}</td>
          <td>
            <span :class="['badge', statusClass(item)]">{{ item.publish_status }}</span>
            <span v-if="item.pending_recompute" class="badge warn">待补算排队</span>
          </td>
          <td>{{ item.consumable ? '是' : '否' }}</td>
          <td>{{ item.row_count }}<small v-if="item.late_row_count"> (+{{ item.late_row_count }} 迟到)</small></td>
          <td class="mono">
            <span v-if="mode === 'window'">{{ item.recomputed ? '已重算' : '初始版本' }}</span>
            <span v-else>{{ summarize(item) }}</span>
          </td>
        </tr>
        <tr v-if="!windows.length">
          <td colspan="8" class="empty-state">暂无窗口指标数据，先在窗口配置中生成窗口</td>
        </tr>
      </tbody>
    </table>

    <template v-if="mode === 'metric'">
      <h4 class="ledger-subtitle">迟到补算清单</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>清单</th>
            <th>类型</th>
            <th>策略 / 源端</th>
            <th>水位 before → after</th>
            <th>受影响窗口 (锁定版本)</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="entry in recomputes"
            :key="entry.recompute_id"
            :data-testid="`recompute-${entry.recompute_id}`"
            :data-status="entry.status"
          >
            <td class="mono">{{ entry.recompute_id }}</td>
            <td>{{ entry.kind === 'sliding_index_group' ? '指标分组重算' : '迟到补算' }}</td>
            <td>{{ entry.policy || '—' }}<br /><small>{{ entry.source || '—' }}</small></td>
            <td class="mono">{{ entry.watermark_before }} → {{ entry.watermark_after }}</td>
            <td>
              <div v-for="win in entry.affected_windows" :key="win.window_key" class="mono">
                {{ win.window_key }}
                <small>
                  v{{ win.previous_version }} → v{{ win.locked_version ?? '—' }}
                  <span v-if="win.is_consumable === false">（未锁定）</span>
                </small>
              </div>
            </td>
            <td><span :class="['badge', entryClass(entry.status)]">{{ entry.status }}</span></td>
            <td>
              <button
                v-if="entry.status !== '已发布'"
                class="link"
                type="button"
                :data-testid="`execute-${entry.recompute_id}`"
                @click="execute(entry)"
              >
                执行补算
              </button>
              <span v-else>已锁定 v{{
                Object.values(entry.locked_versions || {}).join(', ') || '—'
              }}</span>
            </td>
          </tr>
          <tr v-if="!recomputes.length">
            <td colspan="7" class="empty-state">暂无迟到补算记录</td>
          </tr>
        </tbody>
      </table>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import {
  executeRecompute,
  loadAffectedScope,
  loadMetricView,
  loadRecomputeList,
  submitGroupRecompute,
  submitLateRecompute,
} from '@/api/window-metric'
import type {
  AffectedScope,
  MetricWindow,
  RecomputeEntry,
} from '@/api/window-metric'

const props = defineProps<{ mode: 'window' | 'metric'; moduleKey: string }>()

const scope = ref<AffectedScope | null>(null)
const windows = ref<MetricWindow[]>([])
const recomputes = ref<RecomputeEntry[]>([])
const policy = ref('')
const source = ref('')
const indexName = ref('')
const loading = ref(false)
const errorMessage = ref('')

const editablePolicy = computed(() => policy.value.trim())

function statusClass(item: MetricWindow) {
  if (item.publish_status === '已发布') return item.recomputed ? 'ok' : 'plain'
  if (item.publish_status === '已失败') return 'bad'
  return 'warn'
}

function entryClass(status: string) {
  if (status === '已发布') return 'ok'
  if (status === '已失败') return 'bad'
  return 'warn'
}

/** 迟到处理入口登记的清单可能没有策略名，命中受影响窗口即算匹配。 */
function eventPolicyMatched(entry: RecomputeEntry, policyValue: string): boolean {
  if (entry.policy === policyValue) return true
  return entry.affected_windows.some((win) => win.policy === policyValue)
}

function summarize(item: MetricWindow): string {
  const entries = Object.entries(item.aggregates ?? {})
  if (!entries.length) return '—'
  const preview = entries.slice(0, 2).map(([key, value]) => `${key}=${format(value)}`)
  return entries.length > 2 ? `${preview.join('，')} …` : preview.join('，')
}

function format(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(2)
}

async function reload() {
  loading.value = true
  errorMessage.value = ''
  try {
    const filters = { policy: policy.value.trim() || undefined, source: source.value.trim() || undefined }
    const listFilters = {
      ...filters,
      index_name: props.mode === 'metric' ? indexName.value.trim() || undefined : undefined,
    }
    const [scopePayload, list] = await Promise.all([
      loadAffectedScope(filters),
      loadRecomputeList(listFilters),
    ])
    scope.value = scopePayload
    recomputes.value = list.items.filter((entry) =>
      !filters.policy || eventPolicyMatched(entry, filters.policy),
    )
    // 首次加载时用首个窗口回填策略/源端，巡检按钮无需手工输入即可用。
    const first = scopePayload.windows[0]
    if (!policy.value.trim() && first) policy.value = first.policy
    if (!source.value.trim() && first) source.value = first.source
    if (props.mode === 'metric') {
      const view = await loadMetricView(filters)
      windows.value = view.items
    } else {
      windows.value = scopePayload.windows
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '窗口指标数据读取失败'
  } finally {
    loading.value = false
  }
}

async function runLateRecompute() {
  // 以当前窗口范围内的事件时间构造迟到事件，确保落到已发布窗口上。
  const anchor = windows.value[0]
  if (!anchor) {
    errorMessage.value = '当前没有可重算的窗口指标数据'
    return
  }
  const eventTime = Math.floor((anchor.window_start + anchor.window_end) / 2)
  const watermark = Math.floor(Math.max(...windows.value.map((item) => item.window_end))) + 30
  loading.value = true
  errorMessage.value = ''
  try {
    const result = await submitLateRecompute({
      source: source.value.trim() || anchor.source,
      policy: editablePolicy.value,
      strategy: 'reopen_segment',
      watermark,
      events: [{
        event_id: `ui_late_${eventTime}_${Date.now()}`,
        event_time: eventTime,
        payload: { user_id: 'u1', region: 'r0', amount: 20 },
      }],
    })
    if (!result.ok) throw new Error(result.error ?? '迟到补算未生效')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迟到补算失败'
  } finally {
    loading.value = false
  }
}

async function runGroupRecompute() {
  if (!editablePolicy.value) {
    errorMessage.value = '先填入要重算的指标策略'
    return
  }
  loading.value = true
  errorMessage.value = ''
  try {
    const result = await submitGroupRecompute({
      index_name: indexName.value.trim() || '滑动指标样例2',
      group_keys: ['user_id'],
      policy: editablePolicy.value,
      source: source.value.trim() || undefined,
    })
    if (!result.ok) throw new Error(result.error ?? '分组重算未生效')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分组重算失败'
  } finally {
    loading.value = false
  }
}

async function execute(entry: RecomputeEntry) {
  loading.value = true
  errorMessage.value = ''
  try {
    const result = await executeRecompute(entry.recompute_id)
    if (!result.ok) throw new Error(result.error ?? '补算执行失败')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补算执行失败'
  } finally {
    loading.value = false
  }
}

watch(() => props.moduleKey, () => void reload())
onMounted(reload)
</script>

<style scoped>
.ledger-panel { margin: 16px 0 24px; }
.ledger-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.ledger-desc { color: #64748b; font-size: 12px; margin: 4px 0 0; }
.ledger-summary { display: flex; gap: 12px; font-size: 12px; color: #475569; white-space: nowrap; }
.ledger-toolbar { display: flex; gap: 10px; align-items: flex-end; flex-wrap: wrap; margin: 10px 0; }
.ledger-toolbar label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: #64748b; }
.ledger-toolbar input { padding: 6px 10px; border: 1px solid #d8dee6; border-radius: 6px; }
.ledger-subtitle { margin: 18px 0 8px; font-size: 14px; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
.badge { display: inline-block; padding: 1px 6px; border-radius: 4px; font-size: 11px; background: #e2e8f0; color: #334155; }
.badge.ok { background: #dcfce7; color: #166534; }
.badge.warn { background: #fef3c7; color: #92400e; }
.badge.bad { background: #fee2e2; color: #991b1b; }
.badge.plain { background: #e2e8f0; color: #334155; }
small { color: #64748b; }
</style>
