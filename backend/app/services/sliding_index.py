"""滑动指标领域服务：定义基于滑动窗口的实时指标计算规则。

增强能力: 条件过滤、快照频率校验、过期清理、指标聚合输出、跨模块指标查询
序号31: 读取窗口指标数据重算指定分组，受影响窗口与发布状态写入迟到补算清单；
        指标详情与窗口页面共用同一份窗口指标视图，不再各写一遍。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.window_metric_ledger import window_metric_ledger
from app.store import store


def split_dimensions(raw: Any, default: list[str] | None = None) -> list[str]:
    """统一的维度/分组解析：逗号分隔，去空，保持顺序。"""
    if isinstance(raw, (list, tuple)):
        parts = [str(item).strip() for item in raw]
    else:
        parts = [part.strip() for part in str(raw or "").split(",")]
    parts = [part for part in parts if part]
    return parts or list(default or [])


def parse_group_filter(raw: Any) -> dict[str, str]:
    """统一的过滤条件解析（键=值;键=值），未知项忽略，不抛异常。"""
    filters: dict[str, str] = {}
    for part in str(raw or "").split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key, value = key.strip(), value.strip()
        if key:
            filters[key] = value
    return filters


class SlidingIndexService(BaseModuleService):
    MODULE = "sliding_index"
    ENTITY = "指标计算"
    LIST_FIELDS = ["指标名称", "计算窗口", "聚合函数", "分组维度", "过滤条件", "快照频率", "保留时长"]
    REQUIRED_FIELDS = ["指标名称", "计算窗口", "聚合函数"]
    STATUS_ORDER = ["草稿", "计算中", "已生效", "已过期"]
    ACTION_RULES = {"启用指标": "草稿", "停用指标": "计算中"}
    NEGATIVE_ACTIONS = ["停用指标"]

    def evaluate_index_condition(
        self, index_name: str, data: dict[str, Any],
    ) -> dict[str, Any]:
        """评估指标的过滤条件，判断数据是否满足计算条件。"""
        index_def = None
        for r in store.rows(self.MODULE):
            if r.get("指标名称") == index_name:
                index_def = r
                break
        if index_def is None:
            return {"ok": False, "error": f"指标「{index_name}」不存在"}

        filter_cond = index_def.get("过滤条件", "")
        parsed = parse_group_filter(filter_cond)
        matched = True
        details = {}
        for key, expected in parsed.items():
            actual = str(data.get(key, ""))
            hit = actual == expected
            matched = matched and hit
            details[key] = {"expected": expected, "actual": actual, "match": hit}
        for part in str(filter_cond or "").split(";"):
            part = part.strip()
            if ">" not in part or "=" in part:
                continue
            key, value = part.split(">", 1)
            try:
                hit = float(data.get(key.strip(), 0)) > float(value.strip())
            except (ValueError, TypeError):
                hit = False
            matched = matched and hit
            details[key.strip()] = {"expected": f">{value.strip()}", "match": hit}

        return {
            "ok": True,
            "index": index_name,
            "matched": matched,
            "filter": filter_cond,
            "details": details,
        }

    def expire_old_indexes(self) -> dict[str, Any]:
        """清理过期的指标定义，按保留时长计算。"""
        now = time.time()
        expired = 0
        for r in store.rows(self.MODULE):
            try:
                retention_hours = int(r.get("保留时长", "720"))
                retention_sec = retention_hours * 3600
                ts = r.get("_computed_at", 0)
                if isinstance(ts, (int, float)) and ts > 0 and now - ts > retention_sec:
                    r["status"] = "已过期"
                    expired += 1
            except (ValueError, TypeError):
                pass
        return {"ok": True, "expired": expired, "timestamp": now}

    def compute_and_output_to_sink(
        self, index_name: str, data_series: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """执行指标计算并将结果输出到 sink_output。"""
        index_def = None
        for r in store.rows(self.MODULE):
            if r.get("指标名称") == index_name:
                index_def = r
                break
        if index_def is None:
            return {"ok": False, "error": f"指标「{index_name}」不存在"}

        group_dims = split_dimensions(index_def.get("分组维度", ""))
        agg_func = split_dimensions(index_def.get("聚合函数", "count"), default=["count"])

        # 分组聚合
        groups: dict[str, list[float]] = {}
        for item in data_series:
            payload = item.get("payload", item)
            gk_parts = [str(payload.get(d, "")) for d in group_dims]
            gk = "|".join(gk_parts) or "__all__"
            val = payload.get("value", 0)
            if not isinstance(val, (int, float)):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    val = 0.0
            groups.setdefault(gk, []).append(val)

        # 计算并写入 sink_output
        output_count = 0
        for gk, vals in groups.items():
            result = self.aggregate_group(vals, agg_func[0])

            sink_entry = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("sink_output")), default=0
                ) + 1,
                "配置名称": f"index_{index_name}_{gk}",
                "输出类型": "index_result",
                "目标地址": f"index://{index_name}",
                "写入模式": "append",
                "分区策略": "round_robin",
                "格式转换": "json",
                "status": "已生效",
                "sink": f"index_{index_name}",
                "write_status": "success",
                "partition": f"pt_{gk}",
                "data": {"index": index_name, "group": gk, "value": result},
                "written_at": time.time(),
            }
            store.rows("sink_output").append(sink_entry)
            output_count += 1

        index_def["_computed_at"] = time.time()
        return {
            "ok": True,
            "index": index_name,
            "groups": len(groups),
            "outputs_written": output_count,
        }

    @staticmethod
    def aggregate_group(values: list[float], func: str) -> float:
        """统一聚合写法：滑动指标与窗口指标账本使用同一套口径。"""
        func = (func or "count").lower()
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
        return float(sum(values))

    def recompute_from_window_metrics(
        self,
        index_name: str,
        *,
        group_keys: list[str] | None = None,
        agg_funcs: list[str] | None = None,
        group_filter: dict[str, str] | None = None,
        window_keys: list[str] | None = None,
        policy: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        """读取窗口指标数据重算指定分组（滑动指标入口）。

        分组维度缺省取指标定义的「分组维度」，聚合函数缺省取「聚合函数」，
        命中过滤条件的值取自指标定义的「过滤条件」。
        """
        index_def = None
        for r in store.rows(self.MODULE):
            if r.get("指标名称") == index_name:
                index_def = r
                break
        if index_def is None:
            return {"ok": False, "error": f"指标「{index_name}」不存在",
                    "affected_windows": [], "version_created": False}

        keys = group_keys or split_dimensions(index_def.get("分组维度", ""))
        funcs = agg_funcs or split_dimensions(index_def.get("聚合函数", ""), default=["count"])
        filters = group_filter or parse_group_filter(index_def.get("过滤条件", ""))
        result = window_metric_ledger.recompute_groups(
            index_name=index_name,
            group_keys=keys,
            agg_funcs=funcs,
            group_filter=filters,
            window_keys=window_keys,
            policy=policy,
            source=source,
        )
        index_def["_recomputed_at"] = time.time()
        index_def["_recompute_id"] = (result.get("recompute") or {}).get("recompute_id", "")
        return result

    def window_metric_detail(
        self,
        index_name: str,
        *,
        window_key: str | None = None,
        policy: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        """指标详情页的窗口指标数据视图：统一调用账本，不再单独拼装。"""
        view = window_metric_ledger.metric_view(
            policy=policy, source=source, window_key=window_key
        )
        index_def = next(
            (r for r in store.rows(self.MODULE) if r.get("指标名称") == index_name), None
        )
        view["index"] = index_name
        view["group_keys"] = split_dimensions((index_def or {}).get("分组维度", ""))
        view["recompute_entries"] = [
            entry for entry in window_metric_ledger.list_recomputes(index_name=index_name)
        ]
        return view

    def list_affected_scope(
        self,
        *,
        policy: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        """指标页面的受影响范围与迟到补算清单。"""
        return window_metric_ledger.affected_scope(policy=policy, source=source)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active"] = sum(1 for r in rows if r.get("status") == "计算中")
        base["effective"] = sum(1 for r in rows if r.get("status") == "已生效")
        base["expired"] = sum(1 for r in rows if r.get("status") == "已过期")
        scope = window_metric_ledger.affected_scope()
        base["metric_windows"] = scope["total"]
        base["recompute_entries"] = scope["recompute_count"]
        return base