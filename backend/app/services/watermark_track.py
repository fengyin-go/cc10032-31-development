"""水位追踪领域服务：追踪各流式数据源的事件时间水位。

增强能力: 水位趋势分析、延迟预测、溢出告警触发、水位快照持久化、对比检查
序号31: 水位边界计算与迟到判定统一走窗口指标账本，水位变化可按受影响窗口核对。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.window_metric_ledger import classify_events, window_metric_ledger
from app.store import store


class WatermarkTrackService(BaseModuleService):
    MODULE = "watermark_track"
    ENTITY = "水位记录"
    LIST_FIELDS = ["水位名称", "数据源", "当前水位", "事件时间", "延迟阈值", "空转时长", "溢出状态"]
    REQUIRED_FIELDS = ["水位名称", "数据源", "当前水位"]
    STATUS_ORDER = ["正常", "延迟", "溢出", "空转"]
    ACTION_RULES = {"重置水位": "延迟", "跳过溢出": "溢出"}
    NEGATIVE_ACTIONS = []

    def analyze_watermark_trend(
        self, source_name: str, lookback_points: int = 10,
    ) -> dict[str, Any]:
        """分析水位变化趋势，检测加速/减速/停滞模式。"""
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("数据源") == source_name
        ]
        records.sort(key=lambda r: r.get("事件时间", ""), reverse=True)
        recent = records[:lookback_points]

        if len(recent) < 3:
            return {"ok": False, "error": "样本不足，至少需要3个点"}

        deltas = []
        for i in range(1, len(recent)):
            try:
                curr = float(recent[i - 1].get("当前水位", "0"))
                prev = float(recent[i].get("当前水位", "0"))
                deltas.append(curr - prev)
            except (ValueError, TypeError):
                continue

        avg_delta = sum(deltas) / len(deltas) if deltas else 0

        if avg_delta > 0.1:
            trend = "加速"
        elif avg_delta < -0.1:
            trend = "减速"
        else:
            trend = "平稳"

        # 预测溢出时间
        prediction = None
        try:
            current = float(records[0].get("当前水位", "0"))
            threshold_str = records[0].get("延迟阈值", "100")
            threshold_val = float(threshold_str) if threshold_str and threshold_str != records[0].get("数据源", "") else 100
            if avg_delta > 0:
                overflow_in = max(0, (threshold_val - current) / avg_delta)
                prediction = f"约 {overflow_in:.1f} 秒后可能溢出"
        except (ValueError, TypeError):
            pass

        return {
            "ok": True,
            "source": source_name,
            "trend": trend,
            "samples": len(recent),
            "avg_delta": round(avg_delta, 4),
            "deltas": deltas[:5],
            "prediction": prediction,
        }

    def compare_watermark_across_sources(self) -> dict[str, Any]:
        """跨源对比水位：找出延迟最严重和最低的源端。"""
        sources: dict[str, list[float]] = {}
        for r in store.rows(self.MODULE):
            src = r.get("数据源", "")
            try:
                wm = float(r.get("当前水位", "0"))
                sources.setdefault(src, []).append(wm)
            except (ValueError, TypeError):
                pass

        summary = {}
        for src, wms in sources.items():
            summary[src] = {
                "latest": max(wms),
                "min": min(wms),
                "avg": sum(wms) / len(wms),
                "count": len(wms),
            }

        ranking = sorted(
            summary.items(),
            key=lambda x: x[1]["latest"], reverse=True,
        )
        return {
            "ok": True,
            "source_count": len(sources),
            "ranking": [
                {"source": s, "watermark": d["latest"], "avg": round(d["avg"], 2)}
                for s, d in ranking
            ],
        }

    def snapshot_to_state_store(self, watermark_name: str) -> dict[str, Any]:
        """将水位快照持久化到 state_store 作为历史状态。"""
        record = None
        for r in store.rows(self.MODULE):
            if r.get("水位名称") == watermark_name:
                record = r
                break
        if record is None:
            return {"ok": False, "error": f"水位记录「{watermark_name}」不存在"}

        state_entry = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows("state_store")), default=0
            ) + 1,
            "配置名称": f"watermark_{watermark_name}_{int(time.time())}",
            "后端类型": "memory",
            "状态键模式": f"watermark://{watermark_name}",
            "TTL时长": "86400",
            "检查点间隔": "60",
            "增量模式": "append",
            "清理策略": "ttl_expire",
            "status": "已生效",
            "pending": True,
            "abnormal": False,
            "_watermark_snapshot": dict(record),
            "_timestamp": time.time(),
        }
        store.rows("state_store").append(state_entry)
        return {
            "ok": True,
            "watermark": watermark_name,
            "state_id": state_entry["id"],
        }

    def advance_watermark(
        self,
        watermark_name: str,
        events: list[dict[str, Any]],
        watermark: float,
        *,
        strategy: str = "reopen_segment",
    ) -> dict[str, Any]:
        """按迟到水位推进并更新受影响窗口（水位追踪入口）。

        与窗口配置、迟到处理入口复用同一份判定和处理结果；重复推进水位不会
        重复扩大补算范围。
        """
        record = None
        for r in store.rows(self.MODULE):
            if r.get("水位名称") == watermark_name:
                record = r
                break
        if record is None:
            return {"ok": False, "error": f"水位记录「{watermark_name}」不存在",
                    "affected_windows": [], "version_created": False}
        try:
            allowed = float(record.get("延迟阈值", "10"))
        except (ValueError, TypeError):
            allowed = 10.0
        source = str(record.get("数据源", ""))
        classified = classify_events(events, watermark, allowed)
        result = window_metric_ledger.apply_late_recompute(
            classified["late"],
            source=source,
            policy=None,
            strategy=strategy,
            allowed_lateness=allowed,
            watermark_after=watermark,
        )
        record["当前水位"] = str(watermark)
        record["事件时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        if classified["late_count"]:
            record["status"] = "延迟"
            record["pending"] = True
        result["late_count"] = classified["late_count"]
        result["late_ratio"] = classified["late_ratio"]
        result["cutoff"] = classified["cutoff"]
        return result

    def affected_scope(self, source: str | None = None) -> dict[str, Any]:
        """窗口页面的受影响范围：按源端筛选当前指标版本和水位边界。"""
        return window_metric_ledger.affected_scope(source=source)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["normal_count"] = sum(1 for r in rows if r.get("status") == "正常")
        base["delayed_count"] = sum(1 for r in rows if r.get("status") == "延迟")
        base["overflow_count"] = sum(1 for r in rows if r.get("status") == "溢出")
        scope = window_metric_ledger.affected_scope()
        base["metric_windows"] = scope["total"]
        base["pending_windows"] = len(scope["pending_windows"])
        return base