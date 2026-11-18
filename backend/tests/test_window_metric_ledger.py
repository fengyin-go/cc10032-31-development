"""序号31 窗口指标数据 + 迟到补算清单：数据链、幂等与并发一致性测试。

覆盖：
  * 迟到事件按水位更新受影响窗口，指标页可看到重算后的当前版本
  * 每个受影响窗口只有一个当前指标版本
  * 补算清单覆盖重算前后的水位边界，并记录发布状态
  * 同一迟到事件重复到达 / 重复执行 / 并发执行不会再次扩大补算范围
  * 迟到事件跨越多个已发布窗口：整段重开重算 / 只追加旁路指标两种策略
  * 滑动指标读取窗口指标数据重算指定分组
  * 事务失败保留原口径
  * 三个入口（窗口配置 / 迟到处理 / 水位追踪 / 管道）共用同一份判定
"""
from __future__ import annotations

import threading

import pytest

from app.pipeline import apply_window_aggregation, check_watermark_delay, run_end_to_end_pipeline
from app.services.late_handler import LateHandlerService
from app.services.sliding_index import SlidingIndexService
from app.services.watermark_track import WatermarkTrackService
from app.services.window_config import WindowConfigService
from app.services.window_metric_ledger import (
    LATE_TABLE,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_PUBLISHED,
    STRATEGY_BYPASS,
    STRATEGY_REOPEN,
    WINDOW_TABLE,
    classify_events,
    window_metric_ledger,
)
from app.store import Store, store


BASE = 1_790_000_000.0
SOURCE = "测试源端"
POLICY = "测试窗口策略"


@pytest.fixture()
def ledger():
    """每个用例使用干净的账本，避免示例数据干扰。"""
    original = (
        list(store.rows(WINDOW_TABLE)),
        list(store.rows(LATE_TABLE)),
    )
    store._tables[WINDOW_TABLE] = []
    store._tables[LATE_TABLE] = []
    yield window_metric_ledger
    store._tables[WINDOW_TABLE] = original[0]
    store._tables[LATE_TABLE] = original[1]


def _events(offsets, base=BASE, start=0, source=SOURCE):
    return [
        {
            "event_id": f"ev_{index}",
            "source": source,
            "event_time": base + offset,
            "payload": {
                "user_id": f"u{index % 3}",
                "region": f"r{index % 2}",
                "amount": 10.0 + index * 2.0,
            },
        }
        for index, offset in enumerate(offsets, start=start)
    ]


def _seed(ledger, windows=((0, 60), (30, 90), (60, 120))):
    events = _events([10, 20, 40, 50, 70, 80, 100, 110])
    ledger.materialize_windows(
        policy_name=POLICY,
        source_name=SOURCE,
        windows=[
            {"window_start": BASE + start, "window_end": BASE + end}
            for start, end in windows
        ],
        events=events,
        batch_id="batch_seed",
        allowed_lateness=10.0,
    )
    return events


# ---------------------------------------------------------------------------
# 受影响窗口与版本
# ---------------------------------------------------------------------------

class TestAffectedWindowRecompute:
    def test_late_event_creates_new_current_version(self, ledger):
        _seed(ledger)
        late = _events([35], start=100)
        result = ledger.apply_late_recompute(
            late, source=SOURCE, policy=POLICY, strategy=STRATEGY_REOPEN,
            allowed_lateness=10.0, watermark_after=BASE + 130,
        )
        assert result["ok"] is True
        assert result["version_created"] is True
        # 事件时间 35s 同时落在 [0,60) 和 [30,90) 两个已发布窗口
        affected = {item["window_key"] for item in result["affected_windows"]}
        assert affected == {f"w_{int(BASE)}_{int(BASE + 60)}", f"w_{int(BASE + 30)}_{int(BASE + 90)}"}
        for item in result["affected_windows"]:
            assert item["previous_version"] == 1
            assert item["locked_version"] == 2
            assert item["publish_status"] == STATUS_PUBLISHED

    def test_metric_view_reflects_recompute(self, ledger):
        _seed(ledger)
        before = {
            item["window_key"]: item["aggregates"]
            for item in ledger.metric_view(policy=POLICY)["items"]
        }
        ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        after = {
            item["window_key"]: item["aggregates"]
            for item in ledger.metric_view(policy=POLICY)["items"]
        }
        assert after != before
        # 指标页面只展示当前版本，不混入历史版本
        assert len(after) == len(before) == 3

    def test_single_current_version_per_window(self, ledger):
        _seed(ledger)
        for offset in (35, 45, 75):
            ledger.apply_late_recompute(
                _events([offset], start=int(offset)), source=SOURCE, policy=POLICY,
                watermark_after=BASE + 140,
            )
        check = ledger.verify_single_current()
        assert check["ok"], check["duplicates"]
        for row in ledger.current_windows(policy=POLICY):
            versions = ledger.window_versions(row["window_key"], policy=POLICY)
            assert sum(1 for v in versions if v["is_current"]) == 1
            assert len(versions) >= 2

    def test_affected_scope_marks_reshuffled_and_pending(self, ledger):
        _seed(ledger)
        ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        ledger.plan_late_recompute(
            _events([75], start=101), source=SOURCE, policy=POLICY,
            strategy=STRATEGY_BYPASS, watermark_after=BASE + 130,
        )
        scope = ledger.affected_scope(policy=POLICY)
        by_key = {item["window_key"]: item for item in scope["windows"]}
        recomputed = by_key[f"w_{int(BASE)}_{int(BASE + 60)}"]
        assert recomputed["recomputed"] is True
        assert recomputed["publish_status"] == STATUS_PUBLISHED
        assert recomputed["consumable"] is True
        pending = by_key[f"w_{int(BASE + 30)}_{int(BASE + 90)}"]
        assert pending["pending_recompute"] is True
        assert scope["pending_windows"]
        assert scope["published_count"] == 1


# ---------------------------------------------------------------------------
# 水位边界
# ---------------------------------------------------------------------------

class TestWatermarkBoundary:
    def test_plan_records_before_and_after_watermark(self, ledger):
        _seed(ledger)
        planned = ledger.plan_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        entry = planned["recompute"]
        assert entry["status"] == STATUS_PENDING
        assert entry["watermark_before"] == BASE + 50  # min(60, 90) - 10
        assert entry["watermark_after"] == BASE + 130
        assert entry["watermark_after"] > entry["watermark_before"]

    def test_execute_moves_window_watermark_forward(self, ledger):
        _seed(ledger)
        result = ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        for item in result["affected_windows"]:
            assert item["watermark_after"] == BASE + 130
            assert item["watermark_before"] < item["watermark_after"]

    def test_rewatermark_covers_new_late_event(self, ledger):
        _seed(ledger)
        first = ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        second = ledger.apply_late_recompute(
            _events([45], start=200), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 140,
        )
        assert first["version_created"] is True
        assert second["version_created"] is True
        assert first["recompute"]["recompute_id"] != second["recompute"]["recompute_id"]
        assert second["recompute"]["watermark_before"] >= BASE + 130


# ---------------------------------------------------------------------------
# 幂等与并发
# ---------------------------------------------------------------------------

class TestIdempotencyAndConcurrency:
    def test_duplicate_late_event_reuses_version(self, ledger):
        _seed(ledger)
        event = _events([35], start=100)
        first = ledger.apply_late_recompute(
            event, source=SOURCE, policy=POLICY, watermark_after=BASE + 130,
        )
        second = ledger.apply_late_recompute(
            event, source=SOURCE, policy=POLICY, watermark_after=BASE + 130,
        )
        assert first["version_created"] is True
        assert second["version_created"] is False
        assert second["duplicate"] is True
        assert second["recompute"]["recompute_id"] == first["recompute"]["recompute_id"]
        assert ledger.verify_single_current()["ok"]
        assert len(ledger.list_recomputes(source=SOURCE)) == 1

    def test_duplicate_does_not_expand_scope(self, ledger):
        _seed(ledger)
        ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        before = len(ledger.list_recomputes(source=SOURCE)[0]["affected_windows"])
        ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 200,
        )
        assert len(ledger.list_recomputes(source=SOURCE)[0]["affected_windows"]) == before

    def test_execute_twice_only_one_version(self, ledger):
        _seed(ledger)
        planned = ledger.plan_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        first = ledger.execute_recompute(planned["recompute"]["recompute_id"])
        second = ledger.execute_recompute(planned["recompute"]["recompute_id"])
        assert first["version_created"] is True
        assert second["version_created"] is False
        assert second["duplicate"] is True
        assert len(ledger.window_versions(f"w_{int(BASE)}_{int(BASE + 60)}")) == 2

    def test_concurrent_recompute_single_version(self, ledger):
        _seed(ledger)
        event = _events([35], start=100)
        results: list[dict] = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            results.append(ledger.apply_late_recompute(
                event, source=SOURCE, policy=POLICY, watermark_after=BASE + 130,
            ))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert all(result["ok"] for result in results)
        assert sum(1 for result in results if result.get("version_created")) == 1
        assert len(ledger.list_recomputes(source=SOURCE)) == 1
        assert ledger.verify_single_current()["ok"]
        # 受影响窗口为 2 版（初始 + 重算），未受影响窗口保持 1 版。
        versions = {
            row["window_key"]: row["version"]
            for row in ledger.current_windows(policy=POLICY)
        }
        assert versions[f"w_{int(BASE)}_{int(BASE + 60)}"] == 2
        assert versions[f"w_{int(BASE + 60)}_{int(BASE + 120)}"] == 1
        assert all(
            len(ledger.window_versions(key, policy=POLICY)) <= 2 for key in versions
        )

    def test_concurrent_plan_single_entry(self, ledger):
        _seed(ledger)
        event = _events([75], start=100)
        barrier = threading.Barrier(6)
        ids: list[str] = []

        def worker():
            barrier.wait()
            result = ledger.plan_late_recompute(event, source=SOURCE, policy=POLICY)
            ids.append(result["recompute"]["recompute_id"])

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert len(set(ids)) == 1
        assert len(ledger.list_recomputes(source=SOURCE)) == 1


# ---------------------------------------------------------------------------
# 跨窗口策略
# ---------------------------------------------------------------------------

class TestCrossWindowStrategies:
    def test_reopen_segment_recomputes_both_windows(self, ledger):
        _seed(ledger)
        result = ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            strategy=STRATEGY_REOPEN, watermark_after=BASE + 130,
        )
        assert len(result["affected_windows"]) == 2
        for item in result["affected_windows"]:
            assert item["row_count_after"] == item["row_count_before"] + 1
            assert item["aggregates_after"] != item["aggregates_before"]

    def test_bypass_only_appends_late_aggregates(self, ledger):
        _seed(ledger)
        result = ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            strategy=STRATEGY_BYPASS, watermark_after=BASE + 130,
        )
        for item in result["affected_windows"]:
            assert item["aggregates_after"] == item["aggregates_before"]
            assert item["late_row_count"] == 1
        row = ledger.current_windows(window_key=f"w_{int(BASE)}_{int(BASE + 60)}")[0]
        assert row["late_aggregates"]
        assert row["origin"] == STRATEGY_BYPASS

    def test_each_strategy_locks_one_consumable_version(self, ledger):
        _seed(ledger)
        for strategy, offset in ((STRATEGY_REOPEN, 35), (STRATEGY_BYPASS, 45), (STRATEGY_REOPEN, 75)):
            ledger.apply_late_recompute(
                _events([offset], start=int(offset)), source=SOURCE, policy=POLICY,
                strategy=strategy, watermark_after=BASE + 140,
            )
        assert ledger.verify_single_current()["ok"]
        for item in ledger.metric_view(policy=POLICY)["items"]:
            assert item["locked"] is True
            assert item["consumable"] is True
            assert item["publish_status"] == STATUS_PUBLISHED


# ---------------------------------------------------------------------------
# 滑动指标分组重算
# ---------------------------------------------------------------------------

class TestSlidingIndexGroupRecompute:
    def test_recompute_specific_group_only(self, ledger):
        _seed(ledger)
        window = ledger.current_windows(policy=POLICY, window_key=f"w_{int(BASE)}_{int(BASE + 60)}")[0]
        before = dict(window["aggregates"])
        result = ledger.recompute_groups(
            index_name="测试指标",
            group_keys=["user_id"],
            agg_funcs=["count", "sum"],
            group_filter={"region": "r0"},
            policy=POLICY,
        )
        assert result["ok"] is True
        assert result["recomputed"] is True
        assert result["version_created"] is True
        updated = ledger.current_windows(policy=POLICY, window_key=window["window_key"])[0]
        # 窗口自身聚合口径不变，指标分组结果按指标维度重新归并
        assert updated["aggregates"] == before
        groups = updated["index_aggregates"]
        assert groups, "指定分组应被重算"
        assert all(key.startswith("u") for key in groups)
        assert all("|" not in key for key in groups)
        first = result["outputs"][0]
        assert set(first["recomputed_values"]) == {
            f"{key}_{func}" for key in groups for func in ("count", "sum")
        }
        assert updated["origin"] == "sliding_index_group"
        assert updated["recomputed_group"] == "user_id"

    def test_group_recompute_is_idempotent(self, ledger):
        _seed(ledger)
        first = ledger.recompute_groups(
            index_name="测试指标", group_keys=["user_id"], policy=POLICY,
        )
        second = ledger.recompute_groups(
            index_name="测试指标", group_keys=["user_id"], policy=POLICY,
        )
        assert first["version_created"] is True
        assert second["version_created"] is False
        assert second["duplicate"] is True
        assert ledger.verify_single_current()["ok"]

    def test_late_recompute_then_group_recompute(self, ledger):
        _seed(ledger)
        ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        result = ledger.recompute_groups(
            index_name="测试指标", group_keys=["user_id"], policy=POLICY,
        )
        assert result["ok"] is True
        assert ledger.verify_single_current()["ok"]
        assert len(ledger.list_recomputes(policy=POLICY)) == 2

    def test_service_entry_reads_window_metrics(self, ledger):
        _seed(ledger)
        service = SlidingIndexService()
        result = service.recompute_from_window_metrics(
            "滑动指标样例2", policy=POLICY,
        )
        # 指标存在即可执行；分组维度取自指标定义
        assert "affected_windows" in result


# ---------------------------------------------------------------------------
# 事务失败保留原口径
# ---------------------------------------------------------------------------

class TestTransactionFailure:
    def test_failure_restores_original_versions(self, ledger, monkeypatch):
        _seed(ledger)
        before = {
            row["window_key"]: (row["version"], row["aggregates"])
            for row in ledger.current_windows(policy=POLICY)
        }
        original_write = ledger._write_window_version

        def broken_write(*args, **kwargs):
            original_write(*args, **kwargs)
            raise RuntimeError("模拟落库失败")

        monkeypatch.setattr(ledger, "_write_window_version", broken_write)
        result = ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        assert result["ok"] is False
        assert result["original_preserved"] is True
        assert result["version_created"] is False
        after = {
            row["window_key"]: (row["version"], row["aggregates"])
            for row in ledger.current_windows(policy=POLICY)
        }
        assert after == before
        entry = ledger.list_recomputes(source=SOURCE)[0]
        assert entry["status"] == STATUS_FAILED
        assert entry["preserved_versions"]

    def test_retry_after_failure_reuses_entry(self, ledger, monkeypatch):
        _seed(ledger)
        original_write = ledger._write_window_version
        calls = {"count": 0}

        def flaky_write(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("首次失败")
            return original_write(*args, **kwargs)

        monkeypatch.setattr(ledger, "_write_window_version", flaky_write)
        first = ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        assert first["ok"] is False
        second = ledger.apply_late_recompute(
            _events([35], start=100), source=SOURCE, policy=POLICY,
            watermark_after=BASE + 130,
        )
        assert second["ok"] is True
        assert second["version_created"] is True
        assert len(ledger.list_recomputes(source=SOURCE)) == 1
        assert ledger.verify_single_current()["ok"]


# ---------------------------------------------------------------------------
# 入口复用 / 口径一致
# ---------------------------------------------------------------------------

class TestEntryReuse:
    def test_classify_events_matches_pipeline(self):
        events = _events([10, 20, 40, 50, 70])
        watermark = BASE + 65
        entries, ratio = check_watermark_delay(SOURCE, watermark, events, allowed_lateness_sec=10.0)
        classified = classify_events(events, watermark, 10.0)
        assert [entry.original["event_id"] for entry in entries] == [
            event["event_id"] for event in classified["late"]
        ]
        assert ratio == classified["late_ratio"]

    def test_late_handler_and_window_config_share_result(self, ledger):
        _seed(ledger)
        event = _events([35], start=100)
        window_service = WindowConfigService()
        late_service = LateHandlerService()
        late_result = late_service.handle_late_events(
            "迟到处理样例2", event, watermark=BASE + 130,
        )
        window_result = window_service.apply_late_events(
            POLICY, event, watermark=BASE + 130,
        )
        # 两个入口看到的是同一条补算清单/同一版本
        assert late_result["version_created"] is True
        assert window_result["version_created"] is False
        assert window_result["duplicate"] is True
        assert window_result["recompute"]["id"] == late_result["recompute"]["id"]
        assert [w["window_key"] for w in window_result["affected_windows"]] == [
            w["window_key"] for w in late_result["affected_windows"]
        ]
        assert ledger.verify_single_current()["ok"]

    def test_watermark_entry_updates_scope(self, ledger):
        _seed(ledger)
        service = WatermarkTrackService()
        rows = store.rows("watermark_track")
        rows.append({
            "id": 999,
            "水位名称": "测试水位",
            "数据源": SOURCE,
            "当前水位": str(BASE),
            "事件时间": "",
            "延迟阈值": "10",
            "status": "正常",
        })
        try:
            result = service.advance_watermark("测试水位", _events([35], start=100), BASE + 130)
            assert result["version_created"] is True
            assert service.affected_scope(SOURCE)["total"] == 3
        finally:
            rows.remove(next(r for r in rows if r.get("id") == 999))

    def test_aggregate_consistency_with_pipeline(self, ledger):
        events = _events([5, 12, 25, 40])
        results = apply_window_aggregation(
            events, "tumbling", 30.0, 30.0,
            group_keys=["user_id", "region"], agg_funcs=["count", "sum", "avg"],
        )
        ledger.materialize_windows(
            policy_name=POLICY,
            source_name=SOURCE,
            windows=[{"window_start": r.start_ts, "window_end": r.end_ts} for r in results],
            events=events,
            batch_id="batch_consistency",
        )
        for result in results:
            row = ledger.current_windows(window_key=result.window_key)[0]
            # 账本额外保留 max/min，管道口径的 count/sum/avg 必须逐项一致
            shared = {
                key: value for key, value in row["aggregates"].items()
                if key.rsplit("_", 1)[-1] in ("count", "sum", "avg")
            }
            assert shared == pytest.approx(result.aggregates)

    def test_store_keeps_internal_tables_hidden(self):
        fresh = Store()
        assert WINDOW_TABLE not in fresh.module_names()
        assert LATE_TABLE not in fresh.module_names()
        assert fresh.rows(WINDOW_TABLE) == []
        assert fresh.rows(LATE_TABLE) == []

    def test_end_to_end_pipeline_records_metric_windows(self, ledger):
        report = run_end_to_end_pipeline("管道测试源端")
        chain = report["chain2_watermark_backfill"]
        assert chain["metric_windows"]
        assert "recompute" in chain
        assert ledger.verify_single_current()["ok"]
