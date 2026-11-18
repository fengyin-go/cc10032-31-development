"""窗口配置领域服务：定义流式计算的时间窗口策略。

数据流角色: stream_source → window_config（消费者+生产者）→ stream_job（消费者）
增强能力: 窗口参数校验、重叠检测、策略绑定、水位推算、消费上游事件批次
序号31: 窗口定义生成后同步物化为窗口指标数据，迟到事件按统一水位判定触发补算，
        窗口页面据此展示受影响范围，不再只停留在窗口定义。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.window_metric_ledger import (
    STRATEGY_REOPEN,
    classify_events,
    window_metric_ledger,
)
from app.store import store


class WindowConfigService(BaseModuleService):
    MODULE = "window_config"
    ENTITY = "窗口策略"
    LIST_FIELDS = ["策略名称", "窗口类型", "窗口大小", "滑动步长", "水位延迟", "触发策略", "延迟处理"]
    REQUIRED_FIELDS = ["策略名称", "窗口类型", "窗口大小"]
    STATUS_ORDER = ["草稿", "已生效", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已生效"}
    NEGATIVE_ACTIONS = ["停用策略"]

    VALID_WINDOW_TYPES = {"tumbling", "sliding", "session", "global"}

    # ---- 跨模块数据消费与生产 ----

    def validate_window_params(
        self, window_type: str, window_size: str, slide_step: str | None = None,
    ) -> dict[str, Any]:
        """校验窗口参数合法性：类型、大小范围、步长≤窗口大小。"""
        issues = []
        wtype = window_type.lower()
        if wtype not in self.VALID_WINDOW_TYPES:
            issues.append(f"窗口类型「{window_type}」无效，支持: {self.VALID_WINDOW_TYPES}")
        try:
            wsize_sec = float(window_size)
            if wsize_sec <= 0:
                issues.append("窗口大小必须 > 0")
            if wsize_sec > 86400:
                issues.append("窗口大小不能超过 24 小时")
        except (ValueError, TypeError):
            issues.append("窗口大小必须为数字")
        if slide_step is not None and wtype in ("sliding",):
            try:
                step_sec = float(slide_step)
                if step_sec <= 0:
                    issues.append("滑动步长必须 > 0")
                elif step_sec > float(window_size):
                    issues.append("滑动步长不能超过窗口大小")
            except (ValueError, TypeError):
                issues.append("滑动步长必须为数字")
        return {"ok": len(issues) == 0, "type": window_type, "size": window_size, "issues": issues}

    def detect_overlapping_policies(
        self, new_policy: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """检测新窗口策略是否与已有策略重叠或冲突。"""
        new_type = new_policy.get("窗口类型", "")
        new_size = new_policy.get("窗口大小", "")
        overlaps = []
        for r in store.rows(self.MODULE):
            if r.get("id", 0) == new_policy.get("id"):
                continue
            if r.get("status") == "已停用":
                continue
            if r.get("窗口类型") == new_type and r.get("窗口大小") == new_size:
                overlaps.append({
                    "conflicting_policy": r.get("策略名称", ""),
                    "conflict_id": r.get("id"),
                    "reason": f"相同类型({new_type})和大{new_size}",
                })
        return overlaps

    def consume_source_batch(
        self, source_name: str, policy_name: str,
    ) -> dict[str, Any]:
        """消费上游 stream_source 产生的数据批次，按窗口策略生成窗口定义。

        读取 store 中 stream_source 的生产批次记录，按策略参数计算窗口边界和
        触发时间，将窗口信息写入 store 供 stream_job 消费。
        """
        # 1. 确认策略已生效
        policy = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                policy = r
                break
        if policy is None:
            return {"ok": False, "error": f"窗口策略「{policy_name}」不存在"}
        if policy.get("status") != "已生效":
            return {"ok": False, "error": f"策略「{policy_name}」未生效"}

        # 2. 查找上游源端批次
        batches = [
            r for r in store.rows("stream_source")
            if isinstance(r.get("batch_id"), str) and r.get("source") == source_name
        ]
        if not batches:
            return {"ok": False, "error": f"未找到源端「{source_name}」的生产批次"}

        latest = max(batches, key=lambda b: b.get("produced_at", 0))
        events = latest.get("events", [])

        # 3. 按策略计算窗口
        try:
            wsize = float(policy.get("窗口大小", "60"))
            try:
                slide = float(policy.get("滑动步长", str(wsize)))
            except (ValueError, TypeError):
                slide = wsize
        except (ValueError, TypeError):
            wsize, slide = 60.0, 30.0

        wtype = policy.get("窗口类型", "sliding")
        try:
            allowed_late = float(policy.get("水位延迟", "10"))
        except (ValueError, TypeError):
            allowed_late = 10.0
        windows = []
        if events:
            min_ts = min(e.get("event_time", time.time()) for e in events)
            if wtype == "tumbling":
                step = wsize
            else:
                step = min(slide, wsize)
            ws = min_ts
            while ws <= min_ts + max(wsize * 2, 60):
                we = ws + wsize
                win_events = [e for e in events if ws <= e.get("event_time", 0) < we]
                windows.append({
                    "window_key": f"w_{int(ws)}_{int(we)}",
                    "window_start": ws,
                    "window_end": we,
                    "watermark_before": we - allowed_late,
                    "watermark_after": we - allowed_late,
                    "event_count": len(win_events),
                    "policy": policy_name,
                    "source": source_name,
                })
                ws += step

        # 3.1 同步物化窗口指标数据：窗口配置不再只产出窗口定义。
        materialized = window_metric_ledger.materialize_windows(
            policy_name=policy_name,
            source_name=source_name,
            windows=windows,
            events=events,
            batch_id=str(latest.get("batch_id", "")),
            allowed_lateness=allowed_late,
        )

        # 4. 写入窗口定义记录
        window_def = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
            ) + 1,
            "策略名称": policy_name,
            "窗口类型": wtype,
            "窗口大小": str(wsize),
            "滑动步长": str(slide),
            "水位延迟": policy.get("水位延迟", "10"),
            "触发策略": policy.get("触发策略", "watermark"),
            "延迟处理": policy.get("延迟处理", "bypass"),
            "status": "已生效",
            "pending": True,
            "abnormal": False,
            "batch_id": latest.get("batch_id", ""),
            "window_definitions": windows,
            "metric_batch": materialized.get("window_keys", []),
            "produced_at": time.time(),
        }
        store.rows(self.MODULE).append(window_def)
        return {
            "ok": True,
            "policy": policy_name,
            "source": source_name,
            "events_consumed": len(events),
            "windows_generated": len(windows),
            "metric_windows": materialized.get("materialized", 0),
            "metric_duplicate": materialized.get("duplicate", False),
            "batch_id": latest.get("batch_id", ""),
        }

    def calculate_watermark(
        self, policy_name: str, events_processed: int = 0, event_time_max: float = 0,
    ) -> dict[str, Any]:
        """根据策略计算当前水位线。"""
        policy = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                policy = r
                break
        try:
            allowed_late = float((policy or {}).get("水位延迟", "10"))
        except (ValueError, TypeError):
            allowed_late = 10.0
        watermarks = []
        # 收集窗口计算结果中的水位
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                windows = r.get("window_definitions", [])
                if windows:
                    last_end = max(w.get("window_end", 0) for w in windows)
                    watermarks.append({
                        "policy": policy_name,
                        "watermark": last_end - allowed_late,
                        "window_end": last_end,
                        "allowed_lateness": allowed_late,
                    })
        return {
            "ok": True,
            "policy": policy_name,
            "watermarks": watermarks,
        }

    def apply_late_events(
        self,
        policy_name: str,
        events: list[dict[str, Any]],
        *,
        watermark: float | None = None,
        strategy: str = STRATEGY_REOPEN,
    ) -> dict[str, Any]:
        """按迟到水位更新受影响窗口（窗口配置入口）。

        与迟到处理入口共用同一份判定和同一处理结果；重复到达的迟到事件复用
        已有版本，不再次扩大补算范围。
        """
        policy = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                policy = r
                break
        ledger_windows = window_metric_ledger.current_windows(policy=policy_name)
        if policy is None and not ledger_windows:
            return {"ok": False, "error": f"窗口策略「{policy_name}」不存在",
                    "affected_windows": [], "version_created": False}
        try:
            allowed_late = float((policy or {}).get("水位延迟", "10"))
        except (ValueError, TypeError):
            allowed_late = 10.0
        source = str((policy or {}).get("source") or "")
        if not source:
            batches = [
                r for r in store.rows(self.MODULE)
                if r.get("策略名称") == policy_name and r.get("source")
            ]
            source = str((batches[-1] if batches else {}).get("source", ""))
        if not source:
            current = ledger_windows or window_metric_ledger.current_windows(policy=policy_name)
            source = str(current[0].get("source", "")) if current else ""
        if watermark is None:
            latest_event = max(
                (float(event.get("event_time", 0)) for event in events), default=0.0
            )
            watermark = latest_event + allowed_late

        classified = classify_events(events, watermark, allowed_late)
        result = window_metric_ledger.apply_late_recompute(
            classified["late"],
            source=source,
            policy=policy_name,
            strategy=strategy,
            allowed_lateness=allowed_late,
            watermark_after=watermark,
        )
        result["late_count"] = classified["late_count"]
        result["watermark"] = watermark
        result["cutoff"] = classified["cutoff"]
        result["late_distribution"] = classified["late_distribution"]
        return result

    def affected_scope(self, policy_name: str | None = None) -> dict[str, Any]:
        """窗口页面的受影响范围：当前指标版本、水位边界和发布/待补状态。"""
        return window_metric_ledger.affected_scope(policy=policy_name)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active_policies"] = sum(1 for r in rows if r.get("status") == "已生效")
        base["total_window_defs"] = sum(
            1 for r in rows if isinstance(r.get("window_definitions"), list)
        )
        scope = window_metric_ledger.affected_scope()
        base["metric_windows"] = scope["total"]
        base["pending_recomputes"] = scope["pending_count"]
        return base