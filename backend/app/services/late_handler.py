"""迟到处理领域服务：定义流式计算中迟到数据的处理策略。

增强能力: 迟到计数统计、阈值报警、旁路输出调度、迟到数据标记、与补数联动触发
序号31: 迟到判定复用窗口指标账本的统一口径；迟到事件除旁路输出外，同步更新
        受影响窗口的指标数据并登记迟到补算清单。
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


class LateHandlerService(BaseModuleService):
    MODULE = "late_handler"
    ENTITY = "处理策略"
    LIST_FIELDS = ["策略名称", "允许延迟", "迟到动作", "旁路输出", "计数方式", "阈值百分比", "标记字段"]
    REQUIRED_FIELDS = ["策略名称", "允许延迟", "迟到动作"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已启用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def count_late_events(
        self, policy_name: str, events: list[dict[str, Any]], watermark: float,
    ) -> dict[str, Any]:
        """统计迟到事件数量并计算迟到率（判定复用窗口指标账本）。"""
        policy = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                policy = r
                break
        try:
            allowed_sec = float((policy or {}).get("允许延迟", "10"))
        except (ValueError, TypeError):
            allowed_sec = 10.0

        classified = classify_events(events, watermark, allowed_sec)
        return {
            "ok": True,
            "policy": policy_name,
            "total_events": len(events),
            "on_time": classified["on_time_count"],
            "late_count": classified["late_count"],
            "late_ratio": classified["late_ratio"],
            "allowed_lateness": allowed_sec,
            "watermark": watermark,
            "late_distribution": classified["late_distribution"],
        }

    def check_threshold_trigger(
        self, policy_name: str, late_ratio: float,
    ) -> dict[str, Any]:
        """检查迟到率是否超过阈值，触发旁路输出。"""
        policy = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                policy = r
                break
        try:
            threshold = float((policy or {}).get("阈值百分比", "5")) / 100
        except (ValueError, TypeError):
            threshold = 0.05

        triggered = late_ratio > threshold
        action_taken = "none"
        if triggered:
            bypass_action = (policy or {}).get("迟到动作", "bypass")
            if bypass_action == "bypass":
                action_taken = "bypass_to_side_output"
            elif bypass_action == "drop":
                action_taken = "drop_late"
            elif bypass_action == "retry":
                action_taken = "retry_with_adjusted_watermark"

        return {
            "ok": True,
            "policy": policy_name,
            "late_ratio": late_ratio,
            "threshold": threshold,
            "triggered": triggered,
            "action": (policy or {}).get("迟到动作", "bypass"),
            "action_taken": action_taken,
        }

    def handle_late_events(
        self,
        policy_name: str,
        events: list[dict[str, Any]],
        *,
        watermark: float | None = None,
        strategy: str = STRATEGY_REOPEN,
        index_name: str = "",
        source: str = "",
    ) -> dict[str, Any]:
        """迟到处理入口：旁路输出 + 窗口指标补算，形成可核对的处理结果。

        与窗口配置入口调用同一个账本方法，重复到达只复用处理结果。
        """
        policy = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                policy = r
                break
        try:
            allowed_sec = float((policy or {}).get("允许延迟", "10"))
        except (ValueError, TypeError):
            allowed_sec = 10.0
        if watermark is None:
            watermark = max(
                (float(event.get("event_time", 0)) for event in events), default=0.0
            ) + allowed_sec
        classified = classify_events(events, watermark, allowed_sec)
        late = classified["late"]

        if not source:
            source = str((policy or {}).get("数据源") or "")
        if not source:
            # 处理策略没有绑定源端时，用迟到事件自带的源端，再退回窗口指标数据。
            source = str(next((e.get("source") for e in late if e.get("source")), ""))
        if not source:
            scoped = window_metric_ledger.current_windows()
            source = str(scoped[0].get("source", "")) if scoped else ""

        result = window_metric_ledger.apply_late_recompute(
            late,
            source=source,
            policy=None,
            strategy=strategy,
            allowed_lateness=allowed_sec,
            watermark_after=watermark,
        )
        if result.get("ok") and late:
            mark_field = str((policy or {}).get("标记字段", "_late_mark"))
            for event in late:
                event[mark_field] = True
            side_output = {
                "policy": policy_name,
                "index": index_name,
                "source": source,
                "late_count": len(late),
                "watermark": watermark,
                "cutoff": classified["cutoff"],
                "recompute_id": (result.get("recompute") or {}).get("recompute_id", ""),
                "publish_status": (result.get("recompute") or {}).get("status", ""),
                "affected_windows": result.get("affected_windows", []),
                "timestamp": time.time(),
            }
            for row in store.rows(self.MODULE):
                if row is policy:
                    linked = row.setdefault("_late_bypass", [])
                    linked.append(side_output)
                    break
            result["side_output"] = side_output
        result["late_count"] = classified["late_count"]
        result["late_ratio"] = classified["late_ratio"]
        result["watermark"] = watermark
        result["cutoff"] = classified["cutoff"]
        result["late_distribution"] = classified["late_distribution"]
        return result

    def list_recompute_entries(
        self,
        *,
        status: str | None = None,
        policy: str | None = None,
        source: str | None = None,
    ) -> list[dict[str, Any]]:
        """迟到补算清单：受影响窗口、水位边界与发布状态。"""
        return window_metric_ledger.list_recomputes(
            status=status, policy=policy, source=source
        )

    def affected_scope(self, policy: str | None = None) -> dict[str, Any]:
        return window_metric_ledger.affected_scope(policy=policy)

    def trigger_backfill_for_late(
        self, policy_name: str, late_count: int, source_name: str,
    ) -> dict[str, Any]:
        """当迟到数据超过阈值时自动触发补数任务。"""
        if late_count == 0:
            return {"ok": True, "triggered": False, "reason": "无迟到数据"}

        # 写入旁路输出标记
        mark_field = "迟到了"
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                mark_field = r.get("标记字段", "_late_mark")
                break

        # 创建补数任务
        job = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows("backfill_job")), default=0
            ) + 1,
            "作业名称": f"late_backfill_{source_name}_{int(time.time())}",
            "目标指标": f"late_data_{source_name}",
            "补数范围": f"0-{late_count}",
            "并行度": str(min(late_count, 8)),
            "起始位点": "0",
            "截止位点": str(late_count),
            "进度比例": "0%",
            "status": "待执行",
            "pending": True,
            "abnormal": False,
            "_triggered_by": policy_name,
            "_late_count": late_count,
        }
        store.rows("backfill_job").append(job)

        return {
            "ok": True,
            "triggered": True,
            "policy": policy_name,
            "late_count": late_count,
            "backfill_job_id": job["id"],
            "backfill_job_name": job["作业名称"],
        }

    def list_late_statistics(self) -> list[dict[str, Any]]:
        """列出所有迟到处理策略的统计信息。"""
        result = []
        for r in store.rows(self.MODULE):
            if r.get("id", 0) < 100:
                name = r.get("策略名称", "")
                linked_jobs = sum(
                    1 for b in store.rows("backfill_job")
                    if name in str(b.get("_triggered_by", ""))
                )
                result.append({
                    "policy": name,
                    "action": r.get("迟到动作", ""),
                    "threshold": r.get("阈值百分比", ""),
                    "status": r.get("status", ""),
                    "linked_backfills": linked_jobs,
                })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active"] = sum(1 for r in rows if r.get("status") == "已启用")
        scope = window_metric_ledger.affected_scope()
        base["pending_recomputes"] = scope["pending_count"]
        base["published_recomputes"] = scope["published_count"]
        return base