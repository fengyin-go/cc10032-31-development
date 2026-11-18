"""窗口指标账本：窗口指标数据与迟到补算清单的统一读写层（序号31）。

背景：窗口配置链路原先只读取源端批次并生成窗口定义，迟到事件只走旁路输出，
不会回到指标重算，指标页面一直显示旧窗口结果。本模块把「按迟到水位更新受影响
窗口」的数据链收拢到一处，供窗口配置、迟到处理、水位追踪、滑动指标四个入口复用。

约束（与题面一一对应）：
  * 每个受影响窗口只能有一个当前指标版本；历史版本保留但不参与消费。
  * 迟到补算清单记录受影响窗口、重算前后的水位边界与发布状态。
  * 同一迟到事件重复到达（含并发重复执行）只复用已有版本，不再次扩大补算范围。
  * 迟到事件同时跨越多个已发布窗口时，支持整段重开重算或只追加迟到旁路指标，
    清单最终为每个窗口锁定一个可消费版本。
  * 事务失败时回滚窗口指标数据，保留原口径并留下失败清单记录。
  * 滑动指标读取窗口指标数据重算指定分组，清单记录受影响窗口和发布状态。

判定与聚合口径与 app.pipeline 中原有实现保持一致，重构前后接口和历史数据行为不变。
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
from typing import Any, Iterable

from app.store import store

# 内部账本表：不进入业务模块导航 / 运营概览。
WINDOW_TABLE = "window_metric_data"
LATE_TABLE = "late_recompute_list"

# 补算清单发布状态
STATUS_PENDING = "待补算"
STATUS_RUNNING = "重算中"
STATUS_PUBLISHED = "已发布"
STATUS_FAILED = "已失败"
STATUS_HISTORY = "历史版本"

# 发布策略：整段重开重算 / 只追加迟到旁路指标
STRATEGY_REOPEN = "reopen_segment"
STRATEGY_BYPASS = "append_bypass"
VALID_STRATEGIES = (STRATEGY_REOPEN, STRATEGY_BYPASS)

AGG_FUNCS = ("count", "sum", "avg", "max", "min")
DEFAULT_GROUP_KEYS = ["user_id", "region"]

# 统一处理路径标记：所有入口写同一个标记，便于重构前后对比。
PROCESS_PATH = "window_metric_ledger.v1"


# ---------------------------------------------------------------------------
# 共用判定与聚合
# ---------------------------------------------------------------------------

def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def classify_events(
    events: list[dict[str, Any]],
    watermark: float,
    allowed_lateness_sec: float = 10.0,
) -> dict[str, Any]:
    """统一迟到判定：事件时间早于水位截止即为迟到。

    窗口配置、迟到处理、水位追踪、管道编排都调用这一份判定，不再各写一遍。
    """
    cutoff = watermark - allowed_lateness_sec
    late: list[dict[str, Any]] = []
    on_time: list[dict[str, Any]] = []
    for event in events:
        event_time = _as_float(event.get("event_time"), 0.0)
        (late if event_time < cutoff else on_time).append(event)
    timestamps = [_as_float(event.get("event_time"), 0.0) for event in late]
    return {
        "cutoff": cutoff,
        "watermark": watermark,
        "allowed_lateness": allowed_lateness_sec,
        "late": late,
        "on_time": on_time,
        "late_count": len(late),
        "on_time_count": len(on_time),
        "late_ratio": round(len(late) / max(len(events), 1), 4),
        "late_distribution": (
            {"min_ts": min(timestamps), "max_ts": max(timestamps), "count": len(timestamps)}
            if timestamps else None
        ),
    }


def _event_amount(event: dict[str, Any]) -> float:
    payload = event.get("payload") or {}
    if not isinstance(payload, dict):
        return 0.0
    return _as_float(payload.get("amount", payload.get("value", 0.0)), 0.0)


def _sample_key(event: dict[str, Any], group_keys: list[str]) -> str:
    payload = event.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    return "|".join(str(payload.get(name, "")) for name in group_keys) or "__all__"


def _sample_dims(event: dict[str, Any], group_keys: list[str]) -> dict[str, str]:
    payload = event.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    return {name: str(payload.get(name, "")) for name in group_keys}


def aggregate_samples(
    samples: dict[str, list[float]],
    agg_funcs: Iterable[str] | None = None,
) -> dict[str, float]:
    funcs = list(agg_funcs or AGG_FUNCS)
    aggregates: dict[str, float] = {}
    for key, values in samples.items():
        for func in funcs:
            aggregates[f"{key}_{func}"] = _aggregate_value(func, values)
    return aggregates


def _aggregate_value(func: str, values: list[float]) -> float:
    if func == "count":
        return float(len(values))
    if func == "sum":
        return float(sum(values))
    if func == "avg":
        return float(sum(values) / len(values)) if values else 0.0
    if func == "max":
        return float(max(values)) if values else 0.0
    if func == "min":
        return float(min(values)) if values else 0.0
    return 0.0


def aggregate_events(
    events: Iterable[dict[str, Any]],
    group_keys: list[str] | None = None,
    agg_funcs: Iterable[str] | None = None,
) -> tuple[dict[str, float], dict[str, list[float]]]:
    """按分组维度聚合事件，返回 (聚合结果, 分组样本)。

    分组样本随窗口指标数据一起保存，迟到重算无需回查原始批次。
    """
    keys = list(group_keys or DEFAULT_GROUP_KEYS)
    samples: dict[str, list[float]] = {}
    for event in events:
        samples.setdefault(_sample_key(event, keys), []).append(_event_amount(event))
    return aggregate_samples(samples, agg_funcs), samples


def event_signature(event: dict[str, Any]) -> str:
    """迟到事件幂等签名：优先 event_id，其次事件时间 + 载荷内容。"""
    event_id = event.get("event_id")
    if event_id not in (None, ""):
        return f"id:{event_id}"
    payload = event.get("payload") or {}
    digest = hashlib.md5(
        repr(sorted(payload.items())).encode() if isinstance(payload, dict) else b""
    ).hexdigest()[:12]
    return f"t:{_as_float(event.get('event_time'), 0.0)}:p:{digest}"


def _next_id(table: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(table)), default=0) + 1


def _row_identity(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("policy", "")), str(row.get("source", "")), str(row.get("window_key", "")))


class WindowMetricLedger:
    """窗口指标数据 + 迟到补算清单的唯一写入者。"""

    def __init__(self) -> None:
        # 同一进程内的重复/并发执行串行化，保证每个窗口只产生一版当前指标。
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 读取
    # ------------------------------------------------------------------

    def current_windows(
        self,
        *,
        policy: str | None = None,
        source: str | None = None,
        window_key: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = [
            row for row in store.rows(WINDOW_TABLE)
            if row.get("is_current")
            and (policy is None or row.get("policy") == policy)
            and (source is None or row.get("source") == source)
            and (window_key is None or row.get("window_key") == window_key)
        ]
        return sorted(rows, key=lambda r: (_as_float(r.get("window_start")), str(r.get("policy"))))

    def window_versions(
        self,
        window_key: str,
        *,
        policy: str | None = None,
        source: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = [
            row for row in store.rows(WINDOW_TABLE)
            if row.get("window_key") == window_key
            and (policy is None or row.get("policy") == policy)
            and (source is None or row.get("source") == source)
        ]
        return sorted(rows, key=lambda r: int(r.get("version", 0)))

    def locate_affected_windows(
        self,
        event_time: float,
        *,
        policy: str | None = None,
        source: str | None = None,
    ) -> list[dict[str, Any]]:
        """定位事件时间落入的已发布当前窗口；滑动窗口可同时命中多个。"""
        moment = _as_float(event_time)
        return [
            row for row in self.current_windows(policy=policy, source=source)
            if _as_float(row.get("window_start")) <= moment < _as_float(row.get("window_end"))
        ]

    def affected_scope(
        self,
        *,
        policy: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        """窗口页面使用的受影响范围：当前版本、水位边界与发布/待补状态。"""
        windows = self.current_windows(policy=policy, source=source)
        entries = self.list_recomputes(policy=policy, source=source)
        pending = [e for e in entries if e.get("status") in (STATUS_PENDING, STATUS_RUNNING)]
        pending_windows = {
            str(item.get("window_key"))
            for entry in pending
            for item in entry.get("affected_windows", [])
        }
        failed_windows = {
            str(item.get("window_key"))
            for entry in entries if entry.get("status") == STATUS_FAILED
            for item in entry.get("affected_windows", [])
        }
        items = []
        for row in windows:
            key = str(row.get("window_key"))
            status = row.get("publish_status", STATUS_PUBLISHED)
            if key in failed_windows:
                status = STATUS_FAILED
            items.append({
                "window_key": key,
                "policy": row.get("policy"),
                "source": row.get("source"),
                "window_start": row.get("window_start"),
                "window_end": row.get("window_end"),
                "version": row.get("version"),
                "watermark_before": row.get("watermark_before"),
                "watermark_after": row.get("watermark_after"),
                "publish_status": status,
                # 当前版本已发布但仍有新的补算清单待执行：待补窗口。
                "pending_recompute": key in pending_windows,
                "recomputed": int(row.get("version", 1)) > 1,
                "locked": bool(row.get("locked")),
                "consumable": bool(row.get("locked") and row.get("publish_status") == STATUS_PUBLISHED),
                "row_count": row.get("row_count"),
                "late_row_count": row.get("late_row_count", 0),
                "origin": row.get("origin"),
            })
        return {
            "ok": True,
            "windows": items,
            "total": len(items),
            "pending_windows": sorted(pending_windows),
            "failed_windows": sorted(failed_windows),
            "recompute_count": len(entries),
            "published_count": sum(1 for e in entries if e.get("status") == STATUS_PUBLISHED),
            "pending_count": len(pending),
            "failed_count": sum(1 for e in entries if e.get("status") == STATUS_FAILED),
        }

    def list_recomputes(
        self,
        *,
        status: str | None = None,
        policy: str | None = None,
        source: str | None = None,
        index_name: str | None = None,
    ) -> list[dict[str, Any]]:
        entries = [
            entry for entry in store.rows(LATE_TABLE)
            if (status is None or entry.get("status") == status)
            and (policy is None or entry.get("policy") == policy)
            and (source is None or entry.get("source") == source)
            and (index_name is None or entry.get("index_name") == index_name)
        ]
        return sorted(entries, key=lambda e: int(e.get("id", 0)))

    def get_recompute(self, recompute_id: str | int) -> dict[str, Any] | None:
        for entry in store.rows(LATE_TABLE):
            if str(entry.get("recompute_id")) == str(recompute_id) or str(entry.get("id")) == str(recompute_id):
                return entry
        return None

    def metric_view(
        self,
        *,
        policy: str | None = None,
        source: str | None = None,
        window_key: str | None = None,
    ) -> dict[str, Any]:
        """指标页面使用的重算输出：只暴露当前可消费版本。"""
        rows = self.current_windows(policy=policy, source=source, window_key=window_key)
        items = [{
            "window_key": row.get("window_key"),
            "policy": row.get("policy"),
            "source": row.get("source"),
            "window_start": row.get("window_start"),
            "window_end": row.get("window_end"),
            "version": row.get("version"),
            "watermark_before": row.get("watermark_before"),
            "watermark_after": row.get("watermark_after"),
            "aggregates": copy.deepcopy(row.get("aggregates", {})),
            "late_aggregates": copy.deepcopy(row.get("late_aggregates", {})),
            "row_count": row.get("row_count"),
            "late_row_count": row.get("late_row_count", 0),
            "origin": row.get("origin"),
            "publish_status": row.get("publish_status", STATUS_PUBLISHED),
            "locked": bool(row.get("locked")),
            "consumable": bool(row.get("locked") and row.get("publish_status") == STATUS_PUBLISHED),
            "recompute_id": row.get("recompute_id"),
        } for row in rows]
        return {"ok": True, "items": items, "total": len(items)}

    def verify_single_current(self) -> dict[str, Any]:
        """校验每个窗口只有一版当前指标，供测试和巡检使用。"""
        counts: dict[tuple[str, str, str], int] = {}
        for row in store.rows(WINDOW_TABLE):
            if row.get("is_current"):
                key = _row_identity(row)
                counts[key] = counts.get(key, 0) + 1
        duplicates = [
            {"policy": key[0], "source": key[1], "window_key": key[2], "current_versions": count}
            for key, count in counts.items() if count > 1
        ]
        return {"ok": not duplicates, "duplicates": duplicates, "window_count": len(counts)}

    # ------------------------------------------------------------------
    # 初始窗口指标（窗口配置入口）
    # ------------------------------------------------------------------

    def materialize_windows(
        self,
        *,
        policy_name: str,
        source_name: str,
        windows: list[dict[str, Any]],
        events: list[dict[str, Any]],
        group_keys: list[str] | None = None,
        agg_funcs: Iterable[str] | None = None,
        batch_id: str = "",
        allowed_lateness: float = 10.0,
    ) -> dict[str, Any]:
        """把窗口配置生成的窗口定义物化为窗口指标数据。

        同一策略 + 源端 + 批次只物化一次，重复消费上游批次不会产生重复版本。
        """
        keys = list(group_keys or DEFAULT_GROUP_KEYS)
        with self._lock:
            if batch_id:
                existing = [
                    row for row in store.rows(WINDOW_TABLE)
                    if row.get("batch_id") == batch_id
                    and row.get("policy") == policy_name
                    and row.get("source") == source_name
                ]
                if existing:
                    return {
                        "ok": True,
                        "duplicate": True,
                        "materialized": 0,
                        "window_keys": sorted({str(row.get("window_key")) for row in existing}),
                    }

            created: list[str] = []
            for window in windows:
                start = _as_float(window.get("window_start"))
                end = _as_float(window.get("window_end"))
                window_events = [
                    event for event in events
                    if start <= _as_float(event.get("event_time")) < end
                ]
                aggregates, samples = aggregate_events(window_events, keys, agg_funcs)
                sample_dims = {
                    key: _sample_dims(event, keys) for event in window_events
                    for key in [_sample_key(event, keys)]
                }
                watermark = end - allowed_lateness
                window_key = str(window.get("window_key") or f"w_{int(start)}_{int(end)}")
                store.rows(WINDOW_TABLE).append({
                    "id": _next_id(WINDOW_TABLE),
                    "window_key": window_key,
                    "policy": policy_name,
                    "source": source_name,
                    "batch_id": batch_id,
                    "window_start": start,
                    "window_end": end,
                    "watermark_before": watermark,
                    "watermark_after": watermark,
                    "allowed_lateness": allowed_lateness,
                    "group_keys": keys,
                    "aggregates": aggregates,
                    "late_aggregates": {},
                    "samples": copy.deepcopy(samples),
                    "sample_dims": sample_dims,
                    "row_count": len(window_events),
                    "late_row_count": 0,
                    "version": 1,
                    "is_current": True,
                    "locked": True,
                    "publish_status": STATUS_PUBLISHED,
                    "origin": "initial",
                    "process_path": PROCESS_PATH,
                    "recompute_id": "",
                    "applied_event_signatures": sorted({event_signature(e) for e in window_events}),
                    "events": copy.deepcopy(window_events),
                    "created_at": time.time(),
                    "updated_at": time.time(),
                })
                created.append(window_key)
            return {
                "ok": True,
                "duplicate": False,
                "materialized": len(created),
                "window_keys": created,
            }

    # ------------------------------------------------------------------
    # 迟到补算：登记 / 执行 / 一步完成
    # ------------------------------------------------------------------

    def plan_late_recompute(
        self,
        events: list[dict[str, Any]],
        *,
        source: str,
        policy: str | None = None,
        strategy: str = STRATEGY_REOPEN,
        allowed_lateness: float = 10.0,
        watermark_after: float | None = None,
    ) -> dict[str, Any]:
        """登记迟到补算清单（待补算），不产生新指标版本。

        重复到达的迟到事件只命中已有清单条目，不会扩大受影响窗口范围。
        """
        usable = [event for event in events if event.get("event_time") is not None]
        if not usable:
            return {"ok": False, "error": "迟到事件为空", "affected_windows": [], "version_created": False}
        if strategy not in VALID_STRATEGIES:
            return {"ok": False, "error": f"未知重算策略「{strategy}」",
                    "affected_windows": [], "version_created": False}

        signatures = [event_signature(event) for event in usable]
        with self._lock:
            target = self._locate_targets(usable, policy=policy, source=source)
            if not target:
                return {
                    "ok": True, "recomputed": False, "duplicate": False,
                    "planned": False, "reason": "迟到事件未落入已发布窗口",
                    "affected_windows": [], "version_created": False,
                }

            fresh = {
                key: [
                    event for event in usable
                    if self._covers(row, event)
                    and event_signature(event) not in set(row.get("applied_event_signatures", []))
                ]
                for key, row in target.items()
            }
            target_keys = sorted(
                str(row.get("window_key")) for row in target.values()
            )
            if not any(fresh.values()):
                existing = self._find_entry(source, policy, signatures)
                if existing is not None:
                    existing["duplicate_hits"] = int(existing.get("duplicate_hits", 0)) + 1
                return {
                    "ok": True, "recomputed": False, "duplicate": True,
                    "reason": "迟到事件已补算，复用已有版本",
                    "recompute": existing,
                    "affected_windows": self._entry_windows(existing),
                    "version_created": False,
                }

            # 已有清单已覆盖同样的受影响窗口时直接复用，不重复登记。
            existing = self._find_entry(
                source, policy, signatures, window_keys=target_keys
            )
            if existing is not None:
                if existing.get("status") == STATUS_FAILED:
                    # 失败重试复用同一条清单，不新建、不扩围。
                    existing.update({
                        "status": STATUS_PENDING,
                        "events": copy.deepcopy(usable),
                        "error": "",
                        "updated_at": time.time(),
                    })
                    return {
                        "ok": True, "recomputed": False, "duplicate": False,
                        "planned": True, "retry": True,
                        "recompute": existing,
                        "affected_windows": self._entry_windows(existing),
                        "version_created": False,
                    }
                existing["duplicate_hits"] = int(existing.get("duplicate_hits", 0)) + 1
                return {
                    "ok": True, "recomputed": False, "duplicate": True,
                    "reason": "既有补算清单覆盖该迟到事件",
                    "recompute": existing,
                    "affected_windows": self._entry_windows(existing),
                    "version_created": False,
                }

            affected_rows = [target[key] for key in sorted(fresh) if fresh[key]]
            before = min(_as_float(row.get("watermark_after")) for row in affected_rows)
            latest = max(_as_float(event.get("event_time")) for event in usable)
            after = max(
                max(_as_float(row.get("watermark_after")) for row in affected_rows),
                latest + allowed_lateness,
                _as_float(watermark_after, 0.0) if watermark_after is not None else 0.0,
            )
            key = self._idempotency_key(source, policy, signatures)
            entry = {
                "id": _next_id(LATE_TABLE),
                "recompute_id": key,
                "idempotency_key": key,
                "kind": "late_recompute",
                "policy": policy or "",
                "source": source,
                "strategy": strategy,
                "event_ids": [
                    str(event.get("event_id") or event_signature(event)) for event in usable
                ],
                "event_signatures": signatures,
                "events": copy.deepcopy(usable),
                "late_count": len(usable),
                "watermark_before": before,
                "watermark_after": after,
                "affected_windows": [
                    self._pending_scope(row, strategy) for row in affected_rows
                ],
                "locked_versions": {},
                "status": STATUS_PENDING,
                "duplicate_hits": 0,
                "error": "",
                "created_at": time.time(),
                "updated_at": time.time(),
            }
            store.rows(LATE_TABLE).append(entry)
            return {
                "ok": True, "recomputed": False, "duplicate": False, "planned": True,
                "recompute": entry,
                "affected_windows": self._entry_windows(entry),
                "version_created": False,
            }

    def execute_recompute(self, recompute_id: str | int) -> dict[str, Any]:
        """执行补算清单：每个受影响窗口写入一版当前指标，并锁定可消费版本。"""
        with self._lock:
            entry = self.get_recompute(recompute_id)
            if entry is None:
                return {"ok": False, "error": f"补算清单「{recompute_id}」不存在",
                        "affected_windows": [], "version_created": False}
            if entry.get("status") == STATUS_PUBLISHED:
                return {
                    "ok": True, "duplicate": True, "recomputed": False,
                    "reason": "补算清单已发布，复用当前版本",
                    "recompute": entry,
                    "affected_windows": self._entry_windows(entry),
                    "version_created": False,
                }
            if entry.get("status") not in (STATUS_PENDING, STATUS_FAILED):
                return {"ok": False, "error": f"补算清单状态「{entry.get('status')}」不可执行",
                        "affected_windows": [], "version_created": False}

            events = [event for event in entry.get("events", []) if event.get("event_time") is not None]
            signatures = list(entry.get("event_signatures", []))
            policy = entry.get("policy") or None
            source = str(entry.get("source", ""))
            target = self._locate_targets(events, policy=policy, source=source)
            fresh = {
                key: [
                    event for event in events
                    if self._covers(row, event)
                    and event_signature(event) not in set(row.get("applied_event_signatures", []))
                ]
                for key, row in target.items()
            }
            if not any(fresh.values()):
                entry["status"] = STATUS_PUBLISHED
                entry["duplicate_hits"] = int(entry.get("duplicate_hits", 0)) + 1
                entry["updated_at"] = time.time()
                return {
                    "ok": True, "duplicate": True, "recomputed": False,
                    "reason": "迟到事件已补算，复用已有版本",
                    "recompute": entry,
                    "affected_windows": self._entry_windows(entry),
                    "version_created": False,
                }

            original_versions = {
                str(row.get("window_key")): int(row.get("version", 0))
                for row in store.rows(WINDOW_TABLE) if row.get("is_current")
            }
            snapshot = self._snapshot()
            entry["status"] = STATUS_RUNNING
            entry["updated_at"] = time.time()
            try:
                affected: list[dict[str, Any]] = []
                locked: dict[str, int] = {}
                for key in sorted(fresh):
                    late_events = fresh[key]
                    if not late_events:
                        continue
                    new_row = self._write_window_version(
                        target[key],
                        late_events=late_events,
                        signatures=[event_signature(e) for e in late_events],
                        strategy=entry.get("strategy", STRATEGY_REOPEN),
                        recompute_id=str(entry.get("recompute_id", "")),
                        watermark_after=entry.get("watermark_after"),
                    )
                    affected.append(self._published_scope(target[key], new_row))
                    locked[str(new_row["window_key"])] = int(new_row["version"])

                if not affected:
                    entry["status"] = STATUS_PUBLISHED
                    entry["updated_at"] = time.time()
                    return {
                        "ok": True, "duplicate": True, "recomputed": False,
                        "reason": "无可重算窗口",
                        "recompute": entry,
                        "affected_windows": self._entry_windows(entry),
                        "version_created": False,
                    }

                entry["affected_windows"] = affected
                entry["locked_versions"] = locked
                entry["status"] = STATUS_PUBLISHED
                entry["error"] = ""
                entry["updated_at"] = time.time()
                return {
                    "ok": True, "duplicate": False, "recomputed": True,
                    "recompute": entry,
                    "affected_windows": self._entry_windows(entry),
                    "version_created": True,
                }
            except Exception as exc:  # noqa: BLE001 - 事务失败保留原口径
                self._restore(snapshot)
                failed = self.get_recompute(recompute_id)
                if failed is not None:
                    # 保留受影响窗口范围（标记失败），重试时复用同一条清单。
                    failed.update({
                        "status": STATUS_FAILED,
                        "error": str(exc),
                        "affected_windows": [
                            {**self._pending_scope(target[key], entry.get("strategy", STRATEGY_REOPEN)),
                             "publish_status": STATUS_FAILED}
                            for key in sorted(fresh) if fresh[key]
                        ],
                        "locked_versions": {},
                        "preserved_versions": original_versions,
                        "original_preserved": True,
                        "updated_at": time.time(),
                    })
                return {
                    "ok": False, "error": str(exc), "original_preserved": True,
                    "recompute": failed,
                    "affected_windows": self._entry_windows(failed),
                    "version_created": False,
                }

    def apply_late_recompute(
        self,
        events: list[dict[str, Any]],
        *,
        source: str,
        policy: str | None = None,
        strategy: str = STRATEGY_REOPEN,
        allowed_lateness: float = 10.0,
        watermark_after: float | None = None,
    ) -> dict[str, Any]:
        """登记并执行迟到补算：迟到处理 / 水位追踪 / 窗口配置共用同一处理结果。"""
        planned = self.plan_late_recompute(
            events, source=source, policy=policy, strategy=strategy,
            allowed_lateness=allowed_lateness, watermark_after=watermark_after,
        )
        if not planned.get("ok") or planned.get("duplicate") or not planned.get("planned"):
            return planned
        return self.execute_recompute(planned["recompute"]["recompute_id"])

    # ------------------------------------------------------------------
    # 滑动指标入口：读取窗口指标数据重算指定分组
    # ------------------------------------------------------------------

    def recompute_groups(
        self,
        *,
        index_name: str,
        group_keys: list[str],
        agg_funcs: Iterable[str] | None = None,
        group_filter: dict[str, str] | None = None,
        window_keys: list[str] | None = None,
        policy: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        """滑动指标按指定分组读取窗口指标数据并重算，登记受影响窗口与发布状态。"""
        keys = [key.strip() for key in group_keys if key and key.strip()]
        if not keys:
            return {"ok": False, "error": "缺少分组维度", "affected_windows": [], "version_created": False}
        funcs = list(agg_funcs or AGG_FUNCS)
        filters = {str(k): str(v) for k, v in (group_filter or {}).items()}
        group_label = ",".join(keys) + (
            "[" + ",".join(f"{k}={v}" for k, v in sorted(filters.items())) + "]"
            if filters else ""
        )
        with self._lock:
            rows = self.current_windows(policy=policy, source=source)
            if window_keys:
                wanted = {str(key) for key in window_keys}
                rows = [row for row in rows if str(row.get("window_key")) in wanted]
            if not rows:
                return {"ok": True, "recomputed": False, "reason": "无窗口指标数据",
                        "affected_windows": [], "version_created": False}

            # 幂等键基于窗口数据内容而非版本号：没有新数据时重复重算复用原结果。
            signature = "|".join(
                f"{row.get('window_key')}:{row.get('row_count')}:{row.get('late_row_count', 0)}"
                f":{hashlib.md5(','.join(row.get('applied_event_signatures', [])).encode()).hexdigest()[:8]}"
                for row in rows
            )
            key = hashlib.md5(
                f"group|{index_name}|{group_label}|{signature}".encode()
            ).hexdigest()[:16]
            existing = next(
                (entry for entry in store.rows(LATE_TABLE)
                 if entry.get("recompute_id") == key
                 and entry.get("status") not in (STATUS_FAILED, STATUS_HISTORY)),
                None,
            )
            if existing is not None:
                existing["duplicate_hits"] = int(existing.get("duplicate_hits", 0)) + 1
                return {
                    "ok": True, "duplicate": True, "recomputed": False,
                    "reason": "窗口指标版本未变化，复用已有重算结果",
                    "recompute": existing,
                    "affected_windows": self._entry_windows(existing),
                    "version_created": False,
                }

            snapshot = self._snapshot()
            entry = {
                "id": _next_id(LATE_TABLE),
                "recompute_id": key,
                "idempotency_key": key,
                "kind": "sliding_index_group",
                "index_name": index_name,
                "group_keys": keys,
                "group_filter": filters,
                "policy": policy or "",
                "source": source or "",
                "strategy": "group_recompute",
                "event_ids": [],
                "event_signatures": [],
                "events": [],
                "late_count": 0,
                "watermark_before": min(_as_float(row.get("watermark_before")) for row in rows),
                "watermark_after": max(_as_float(row.get("watermark_after")) for row in rows),
                "affected_windows": [],
                "locked_versions": {},
                "status": STATUS_RUNNING,
                "duplicate_hits": 0,
                "error": "",
                "created_at": time.time(),
                "updated_at": time.time(),
            }
            store.rows(LATE_TABLE).append(entry)
            try:
                affected: list[dict[str, Any]] = []
                outputs: list[dict[str, Any]] = []
                for row in rows:
                    new_row = self._write_group_version(
                        row, keys, funcs, filters, index_name, key
                    )
                    outputs.append({
                        "window_key": new_row["window_key"],
                        "version": new_row["version"],
                        "groups": new_row.get("index_aggregates", {}),
                        "recomputed_values": new_row.get("recomputed_values", {}),
                    })
                    affected.append(self._published_scope(row, new_row))
                entry["affected_windows"] = affected
                entry["locked_versions"] = {
                    str(item["window_key"]): int(item["locked_version"])
                    for item in affected
                }
                entry["status"] = STATUS_PUBLISHED
                entry["updated_at"] = time.time()
                return {
                    "ok": True, "duplicate": False, "recomputed": True,
                    "recompute": entry,
                    "affected_windows": affected,
                    "outputs": outputs,
                    "version_created": True,
                }
            except Exception as exc:  # noqa: BLE001
                self._restore(snapshot)
                failed = self.get_recompute(key)
                if failed is not None:
                    failed.update({
                        "status": STATUS_FAILED,
                        "error": str(exc),
                        "original_preserved": True,
                        "updated_at": time.time(),
                    })
                return {
                    "ok": False, "error": str(exc), "original_preserved": True,
                    "recompute": failed,
                    "affected_windows": [], "version_created": False,
                }

    # ------------------------------------------------------------------
    # 版本写入
    # ------------------------------------------------------------------

    def _write_window_version(
        self,
        old: dict[str, Any],
        *,
        late_events: list[dict[str, Any]],
        signatures: list[str],
        strategy: str,
        recompute_id: str,
        watermark_after: Any = None,
    ) -> dict[str, Any]:
        """写入新当前版本并退役旧版本；每个窗口始终只有一个 current。"""
        group_keys = list(old.get("group_keys") or DEFAULT_GROUP_KEYS)
        combined = copy.deepcopy(old.get("samples", {}))
        _, late_samples = aggregate_events(late_events, group_keys)
        for key, values in late_samples.items():
            combined.setdefault(key, []).extend(values)

        # 迟到样本单独累积，旁路增量与后续整段重算都能按同一份数据核对。
        late_sample_store = copy.deepcopy(old.get("late_samples", {}))
        for key, values in late_samples.items():
            late_sample_store.setdefault(key, []).extend(values)
        late_aggregates = aggregate_samples(late_sample_store)

        sample_dims = copy.deepcopy(old.get("sample_dims", {}))
        for event in late_events:
            sample_dims.setdefault(_sample_key(event, group_keys), _sample_dims(event, group_keys))

        # 两种策略都保留完整事件集，后续重算不会丢掉已到达的迟到事件。
        merged_events = old.get("events", []) + late_events
        row_count = len(merged_events)
        late_row_count = int(old.get("late_row_count", 0)) + len(late_events)

        if strategy == STRATEGY_BYPASS:
            # 只追加迟到旁路指标：主口径保持已发布结果，迟到增量单独可核对。
            aggregates = copy.deepcopy(old.get("aggregates", {}))
        else:
            # 整段窗口重开重算：用原始事件 + 迟到事件重算完整口径。
            aggregates = aggregate_samples(combined)

        before = _as_float(old.get("watermark_after"))
        latest_event = max((_as_float(e.get("event_time")) for e in late_events), default=0.0)
        after = max(
            before,
            latest_event + _as_float(old.get("allowed_lateness"), 10.0),
            _as_float(watermark_after, 0.0) if watermark_after is not None else 0.0,
        )

        self._retire(old)
        row = {
            "id": _next_id(WINDOW_TABLE),
            "window_key": old.get("window_key"),
            "policy": old.get("policy"),
            "source": old.get("source"),
            "batch_id": old.get("batch_id", ""),
            "window_start": old.get("window_start"),
            "window_end": old.get("window_end"),
            "watermark_before": before,
            "watermark_after": after,
            "allowed_lateness": old.get("allowed_lateness", 10.0),
            "group_keys": group_keys,
            "aggregates": aggregates,
            "late_aggregates": late_aggregates,
            "samples": combined,
            "late_samples": late_sample_store,
            "sample_dims": sample_dims,
            "row_count": row_count,
            "late_row_count": late_row_count,
            "version": int(old.get("version", 1)) + 1,
            "is_current": True,
            "locked": True,
            "publish_status": STATUS_PUBLISHED,
            "origin": strategy,
            "process_path": PROCESS_PATH,
            "recompute_id": recompute_id,
            "applied_event_signatures": sorted(
                set(old.get("applied_event_signatures", [])) | set(signatures)
            ),
            "events": copy.deepcopy(merged_events),
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        store.rows(WINDOW_TABLE).append(row)
        return row

    def _write_group_version(
        self,
        old: dict[str, Any],
        group_keys: list[str],
        agg_funcs: list[str],
        group_filter: dict[str, str],
        index_name: str,
        recompute_id: str,
    ) -> dict[str, Any]:
        """滑动指标分组重算：按指标的分组维度重新归并窗口事件。

        窗口自身聚合保持不变；指标口径落在 index_aggregates / recomputed_values，
        清单里记录受影响窗口与发布状态，与迟到补算共用同一套版本锁定。
        """
        samples = copy.deepcopy(old.get("samples", {}))
        sample_dims = copy.deepcopy(old.get("sample_dims", {}))
        index_samples: dict[str, list[float]] = {}
        for sample_key, values in samples.items():
            dims = sample_dims.get(sample_key, {})
            index_key = "|".join(str(dims.get(name, "")) for name in group_keys) or "__all__"
            if group_filter and not all(
                str(dims.get(name, "")) == value for name, value in group_filter.items()
            ):
                continue
            index_samples.setdefault(index_key, []).extend(values)

        recomputed: dict[str, float] = {}
        index_groups: dict[str, dict[str, float]] = {}
        for index_key, values in index_samples.items():
            index_groups[index_key] = {}
            for func in agg_funcs:
                value = _aggregate_value(func, values)
                index_groups[index_key][func] = value
                recomputed[f"{index_key}_{func}"] = value

        self._retire(old)
        row = {
            "id": _next_id(WINDOW_TABLE),
            "window_key": old.get("window_key"),
            "policy": old.get("policy"),
            "source": old.get("source"),
            "batch_id": old.get("batch_id", ""),
            "window_start": old.get("window_start"),
            "window_end": old.get("window_end"),
            "watermark_before": _as_float(old.get("watermark_after")),
            "watermark_after": _as_float(old.get("watermark_after")),
            "allowed_lateness": old.get("allowed_lateness", 10.0),
            "group_keys": list(old.get("group_keys") or group_keys),
            "aggregates": copy.deepcopy(old.get("aggregates", {})),
            "late_aggregates": copy.deepcopy(old.get("late_aggregates", {})),
            "index_aggregates": index_groups,
            "samples": samples,
            "late_samples": copy.deepcopy(old.get("late_samples", {})),
            "sample_dims": sample_dims,
            "row_count": old.get("row_count", 0),
            "late_row_count": old.get("late_row_count", 0),
            "version": int(old.get("version", 1)) + 1,
            "is_current": True,
            "locked": True,
            "publish_status": STATUS_PUBLISHED,
            "origin": "sliding_index_group",
            "process_path": PROCESS_PATH,
            "recompute_id": recompute_id,
            "recompute_index": index_name,
            "recomputed_group": ",".join(group_keys),
            "recomputed_values": recomputed,
            "applied_event_signatures": list(old.get("applied_event_signatures", [])),
            "events": copy.deepcopy(old.get("events", [])),
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        store.rows(WINDOW_TABLE).append(row)
        return row

    @staticmethod
    def _retire(row: dict[str, Any]) -> None:
        """退役旧版本：不再 current、不再锁定，历史版本可追溯但不可消费。"""
        row["is_current"] = False
        row["locked"] = False
        row["publish_status"] = STATUS_HISTORY
        row["updated_at"] = time.time()

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _locate_targets(
        self,
        events: list[dict[str, Any]],
        *,
        policy: str | None,
        source: str | None,
    ) -> dict[tuple[str, str, str], dict[str, Any]]:
        target: dict[tuple[str, str, str], dict[str, Any]] = {}
        for event in events:
            for row in self.locate_affected_windows(
                _as_float(event.get("event_time")), policy=policy, source=source
            ):
                target[_row_identity(row)] = row
        return target

    @staticmethod
    def _covers(row: dict[str, Any], event: dict[str, Any]) -> bool:
        moment = _as_float(event.get("event_time"))
        return (
            _as_float(row.get("window_start")) <= moment < _as_float(row.get("window_end"))
        )

    def _find_entry(
        self,
        source: str,
        policy: str | None,
        signatures: list[str],
        *,
        window_keys: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """按源端/策略/事件签名定位既有清单；窗口集合不一致时不算覆盖。"""
        wanted = set(signatures)
        for entry in store.rows(LATE_TABLE):
            if entry.get("source") != source:
                continue
            entry_policy = entry.get("policy") or ""
            if policy is not None and entry_policy and entry_policy != policy:
                continue
            if entry.get("kind") != "late_recompute":
                continue
            if not wanted.issubset(set(entry.get("event_signatures", []))):
                continue
            if window_keys is not None:
                known = {str(item.get("window_key")) for item in entry.get("affected_windows", [])}
                if known != set(window_keys):
                    continue
            return entry
        return None

    @staticmethod
    def _idempotency_key(source: str, policy: str | None, signatures: list[str]) -> str:
        raw = f"{source}|{policy or '*'}|{'|'.join(sorted(set(signatures)))}"
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    @staticmethod
    def _pending_scope(row: dict[str, Any], strategy: str) -> dict[str, Any]:
        return {
            "window_key": row.get("window_key"),
            "policy": row.get("policy"),
            "source": row.get("source"),
            "previous_version": row.get("version"),
            "locked_version": None,
            "current_version": row.get("version"),
            "watermark_before": row.get("watermark_before"),
            "watermark_after": row.get("watermark_after"),
            "publish_status": STATUS_PENDING,
            "origin": strategy,
        }

    @staticmethod
    def _published_scope(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
        return {
            "window_key": new.get("window_key"),
            "policy": new.get("policy"),
            "source": new.get("source"),
            "previous_version": old.get("version"),
            "locked_version": new.get("version"),
            "current_version": new.get("version"),
            "watermark_before": new.get("watermark_before"),
            "watermark_after": new.get("watermark_after"),
            "publish_status": new.get("publish_status", STATUS_PUBLISHED),
            "origin": new.get("origin"),
            "row_count_before": old.get("row_count"),
            "row_count_after": new.get("row_count"),
            "late_row_count": new.get("late_row_count", 0),
            "aggregates_before": copy.deepcopy(old.get("aggregates", {})),
            "aggregates_after": copy.deepcopy(new.get("aggregates", {})),
        }

    def _entry_windows(self, entry: dict[str, Any] | None) -> list[dict[str, Any]]:
        if not entry:
            return []
        items = []
        for item in entry.get("affected_windows", []):
            current = self.current_windows(
                policy=item.get("policy") or None,
                source=item.get("source") or None,
                window_key=item.get("window_key"),
            )
            current_version = current[0].get("version") if current else None
            items.append({
                **copy.deepcopy(item),
                "current_version": current_version,
                "is_latest": current_version == item.get("locked_version"),
                "is_consumable": bool(
                    current and current[0].get("locked")
                    and current[0].get("publish_status") == STATUS_PUBLISHED
                ),
            })
        return items

    def _snapshot(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return (
            copy.deepcopy(store.rows(WINDOW_TABLE)),
            copy.deepcopy(store.rows(LATE_TABLE)),
        )

    @staticmethod
    def _restore(snapshot: tuple[list[dict[str, Any]], list[dict[str, Any]]]) -> None:
        window_rows, late_rows = snapshot
        store._tables[WINDOW_TABLE] = window_rows
        store._tables[LATE_TABLE] = late_rows

    # ------------------------------------------------------------------
    # 演示数据
    # ------------------------------------------------------------------

    def bootstrap_demo(self) -> None:
        """初始化一组可巡检的窗口指标数据与补算清单。

        幂等：重复调用不会追加记录。只写入内部账本表，不改动任何既有模块行。
        """
        with self._lock:
            if store.rows(WINDOW_TABLE) or store.rows(LATE_TABLE):
                return
            base = 1_780_000_000.0
            source = "流式源端样例2"
            policy = "窗口指标样例策略"
            events = [
                {
                    "event_id": f"demo_{index}",
                    "source": source,
                    "event_time": base + offset,
                    "payload": {
                        "user_id": f"u{index % 3}",
                        "region": f"r{index % 2}",
                        "amount": 12.5 + index * 3.0,
                    },
                }
                for index, offset in enumerate(
                    [5, 15, 25, 35, 45, 55, 65, 75, 85, 95, 105, 115]
                )
            ]
            self.materialize_windows(
                policy_name=policy,
                source_name=source,
                windows=[
                    {"window_start": base, "window_end": base + 60},
                    {"window_start": base + 30, "window_end": base + 90},
                    {"window_start": base + 60, "window_end": base + 120},
                ],
                events=events,
                batch_id="demo_batch_v1",
                allowed_lateness=10.0,
            )
            # 已发布示例：迟到事件同时跨越前两个已发布窗口，整段重开重算。
            self.apply_late_recompute(
                [{
                    "event_id": "demo_late_published",
                    "source": source,
                    "event_time": base + 35,
                    "payload": {"user_id": "u1", "region": "r0", "amount": 48.0},
                }],
                source=source,
                policy=policy,
                strategy=STRATEGY_REOPEN,
                allowed_lateness=10.0,
                watermark_after=base + 130,
            )
            # 待补算示例：另一条迟到事件跨越后两个窗口，登记后等待人工执行。
            self.plan_late_recompute(
                [{
                    "event_id": "demo_late_pending",
                    "source": source,
                    "event_time": base + 75,
                    "payload": {"user_id": "u2", "region": "r1", "amount": 36.0},
                }],
                source=source,
                policy=policy,
                strategy=STRATEGY_BYPASS,
                allowed_lateness=10.0,
                watermark_after=base + 130,
            )


window_metric_ledger = WindowMetricLedger()
