"""窗口指标账本接口：窗口指标数据 / 迟到补算清单 / 按水位重算（序号31）。

路由层只做参数校验和分派，窗口定位、版本锁定、幂等判定都留在
app.services.window_metric_ledger 里。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services.window_metric_ledger import (
    STRATEGY_REOPEN,
    VALID_STRATEGIES,
    window_metric_ledger,
)

router = APIRouter(prefix="/api", tags=["窗口指标账本"])


class LateEvent(BaseModel):
    event_id: str | None = None
    event_time: float
    payload: dict[str, Any] = Field(default_factory=dict)
    source: str | None = None


class LateRecomputePayload(BaseModel):
    source: str
    policy: str | None = None
    events: list[LateEvent] = Field(default_factory=list)
    strategy: str = STRATEGY_REOPEN
    watermark: float | None = None
    allowed_lateness: float = 10.0


class GroupRecomputePayload(BaseModel):
    index_name: str
    group_keys: list[str] | None = None
    agg_funcs: list[str] | None = None
    group_filter: dict[str, str] | None = None
    window_keys: list[str] | None = None
    policy: str | None = None
    source: str | None = None


@router.get("/window-metrics/recompute-list")
def list_recompute_entries(
    status: str | None = Query(default=None),
    policy: str | None = Query(default=None),
    source: str | None = Query(default=None),
    index_name: str | None = Query(default=None),
) -> dict[str, Any]:
    entries = window_metric_ledger.list_recomputes(
        status=status, policy=policy, source=source, index_name=index_name
    )
    return {
        "items": entries,
        "total": len(entries),
        "pending": sum(1 for e in entries if e.get("status") == "待补算"),
        "published": sum(1 for e in entries if e.get("status") == "已发布"),
        "failed": sum(1 for e in entries if e.get("status") == "已失败"),
    }


@router.get("/window-metrics/scope")
def affected_scope(
    policy: str | None = Query(default=None),
    source: str | None = Query(default=None),
) -> dict[str, Any]:
    """窗口页面：受影响范围（当前版本、水位边界、发布/待补状态）。"""
    return window_metric_ledger.affected_scope(policy=policy, source=source)


@router.get("/window-metrics")
def metric_view(
    policy: str | None = Query(default=None),
    source: str | None = Query(default=None),
    window_key: str | None = Query(default=None),
) -> dict[str, Any]:
    """指标页面：当前可消费的窗口指标版本。"""
    return window_metric_ledger.metric_view(policy=policy, source=source, window_key=window_key)


@router.get("/window-metrics/{window_key}/versions")
def window_versions(
    window_key: str,
    policy: str | None = Query(default=None),
    source: str | None = Query(default=None),
) -> dict[str, Any]:
    versions = window_metric_ledger.window_versions(
        window_key, policy=policy, source=source
    )
    return {"window_key": window_key, "versions": versions, "total": len(versions)}


@router.get("/window-metrics/consistency")
def consistency() -> dict[str, Any]:
    """巡检：确认每个受影响窗口只有一版当前指标。"""
    return window_metric_ledger.verify_single_current()


@router.post("/window-metrics/late-recompute")
def late_recompute(payload: LateRecomputePayload) -> dict[str, Any]:
    """登记并执行迟到补算，返回受影响窗口与锁定版本。"""
    if payload.strategy not in VALID_STRATEGIES:
        return {"ok": False, "error": f"未知重算策略「{payload.strategy}」"}
    events = [event.model_dump() for event in payload.events]
    return window_metric_ledger.apply_late_recompute(
        events,
        source=payload.source,
        policy=payload.policy,
        strategy=payload.strategy,
        allowed_lateness=payload.allowed_lateness,
        watermark_after=payload.watermark,
    )


@router.post("/window-metrics/{recompute_id}/execute")
def execute_recompute(recompute_id: str) -> dict[str, Any]:
    """执行一条待补算清单；已发布时复用当前版本。"""
    return window_metric_ledger.execute_recompute(recompute_id)


@router.post("/window-metrics/group-recompute")
def group_recompute(payload: GroupRecomputePayload) -> dict[str, Any]:
    """滑动指标读取窗口指标数据重算指定分组。"""
    return window_metric_ledger.recompute_groups(
        index_name=payload.index_name,
        group_keys=payload.group_keys or [],
        agg_funcs=payload.agg_funcs,
        group_filter=payload.group_filter,
        window_keys=payload.window_keys,
        policy=payload.policy,
        source=payload.source,
    )
