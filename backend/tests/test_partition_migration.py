"""分区再平衡迁移评估测试：分布均匀、单区热点、迁移中断恢复三种场景对比。"""
from app.fixtures import (
    PARTITION_STATS, PARTITION_CAPACITY, PARTITION_KEY_RANGES,
    PARTITION_INFLIGHT, PARTITION_DELIVERED, PARTITION_INCOMING,
)
from app.services.partition_migration import evaluate_migration_plan, reconcile_inflight
from app.services.pipeline_orchestrator import run_partition_backpressure_pipeline
from app.store import store


def _fixtures_for(key: str, **overrides) -> dict:
    """按分区键取 fixtures 评估输入，允许逐字段覆盖。"""
    kwargs = dict(
        stat=next(s for s in PARTITION_STATS if s["分区键"] == key),
        capacity=next(c for c in PARTITION_CAPACITY if c["分区键"] == key),
        key_ranges=PARTITION_KEY_RANGES,
        inflight=PARTITION_INFLIGHT,
        delivered=PARTITION_DELIVERED,
        incoming=PARTITION_INCOMING,
        fetch_call=f"FETCH-{key}-TEST",
    )
    kwargs.update(overrides)
    return kwargs


class TestUniformDistribution:
    """分布均匀：不触发迁移，不发放额度，回执仍然对账平衡。"""

    def setup_method(self):
        self.plan = evaluate_migration_plan(
            stat={"分区键": "地区", "分区数量": 3, "分区0行数": 3000,
                  "分区1行数": 3100, "分区2行数": 2900, "热点分区": "", "再平衡建议": False},
            capacity={"分区键": "地区", "分区上限": 5000, "当次迁移阈值": 1000},
            key_ranges=[], inflight=[], delivered=[], incoming=[],
            fetch_call="FETCH-地区-0001",
        )

    def test_not_triggered(self):
        assert self.plan["歪斜度"] < 1.0
        assert self.plan["迁移触发"] is False
        assert self.plan["迁移结论"] == "分布均匀无需迁移"

    def test_no_pending_range_no_quota(self):
        assert self.plan["待迁移行范围"]["行数"] == 0
        assert self.plan["额度"]["状态"] == "无需发放"
        assert self.plan["额度"]["额度行数"] == 0
        assert self.plan["容量回写"] == []

    def test_receipt_still_reconciled(self):
        receipt = self.plan["回执"]
        assert receipt["输入对账"]["对账平衡"] is True
        assert receipt["旧区可交付行数"] + receipt["新区可交付行数"] == 3100


class TestSingleHotspot:
    """单区热点：待迁移行范围按分区上限与分区键范围评估，确认后发放额度。"""

    def setup_method(self):
        self.plan = evaluate_migration_plan(**_fixtures_for("用户ID"))

    def test_pending_range_computed(self):
        # 热点分区2 需迁出 4500-3150=1350；新区可接纳 min(2900, 阈值1500)=1500
        assert self.plan["热点分区"] == "分区2"
        assert self.plan["迁入分区"] == "分区3"
        assert self.plan["新区可接纳行数"] == 1500
        pending = self.plan["待迁移行范围"]
        assert pending["行数"] == 1350
        # 待迁移范围接在已交付(10202)与在途(10203)位点之后
        assert pending["起始位点"] == 10204
        assert pending["结束位点"] == 10204 + 1350 - 1

    def test_move_range_follows_existing_key_ranges(self):
        moved = self.plan["迁出范围"]
        assert len(moved) == 1
        assert moved[0]["范围编号"] == "UID-R3"  # 已有分区键范围中键起最高的热点段
        assert moved[0]["迁出行数"] == 1350

    def test_quota_granted_after_confirmation(self):
        quota = self.plan["额度"]
        assert quota["状态"] == "已发放"
        assert quota["额度行数"] == 1350
        # 按取数调用定位歪斜结果进入额度判断的位置
        assert quota["依据取数调用"] == self.plan["取数调用"]
        assert quota["歪斜结论"]["歪斜度"] == self.plan["歪斜度"]
        assert quota["歪斜结论"]["热点分区"] == "分区2"

    def test_receipt_ownership_and_reconciliation(self):
        receipt = self.plan["回执"]
        ownership = {o["范围编号"]: o for o in receipt["范围归属"]}
        assert ownership["UID-R3"]["当前归属"] == "分区3"
        assert ownership["UID-R3"]["迁出行数"] == 1350
        assert ownership["UID-R2"]["当前归属"] == "分区2"
        # 历史分区键范围也出现在归属说明里
        assert ownership["UID-L0"]["历史键"] is True
        # 旧区与新区可交付行数与输入对账：2600 + 1900 = 4500
        assert receipt["旧区可交付行数"] == 2600
        assert receipt["新区可交付行数"] == 1900
        assert receipt["输入对账"]["热点输入行数"] == 4500
        assert receipt["输入对账"]["对账平衡"] is True

    def test_hotspot_remaining_explained_by_ownership(self):
        receipt = self.plan["回执"]
        # 4500 = 旧区剩余 2600（未迁出 2300 + 已交付旧区位点 300）+ 迁出 1900
        assert receipt["热点剩余行数"] == 2600
        assert "已交付旧区位点 300" in receipt["热点剩余解释"]

    def test_capacity_updates_written_back(self):
        updates = {(u["分区键"], u["分区"]): u for u in self.plan["容量回写"]}
        assert updates[("用户ID", "分区2")]["已用行数"] == 2600
        assert updates[("用户ID", "分区3")]["已用行数"] == 3750  # 2100 + 在途300 + 待迁移1350
        assert updates[("用户ID", "分区1")]["已用行数"] == 3050  # 2800 + 在途250

    def test_new_partition_acceptance_bounded_by_threshold(self):
        # 当次迁移阈值收紧到 1000 时，新区能接的行数与待迁移范围都按当次阈值核验
        capacity = {"分区键": "用户ID", "分区上限": 5000, "当次迁移阈值": 1000}
        plan = evaluate_migration_plan(**_fixtures_for("用户ID", capacity=capacity))
        assert plan["新区可接纳行数"] == 1000
        assert plan["待迁移行范围"]["行数"] == 1000
        assert plan["额度"]["额度行数"] == 1000

    def test_historical_key_ranges_join_migration(self):
        # 历史分区键范围同样参与迁出选择（键起 Z 段排在最后迁出序列的最前）
        plan = evaluate_migration_plan(
            stat={"分区键": "旧键", "分区数量": 2, "分区0行数": 4000, "分区1行数": 1000,
                  "热点分区": "分区0", "再平衡建议": True},
            capacity={"分区键": "旧键", "分区上限": 6000, "当次迁移阈值": 2000},
            key_ranges=[
                {"分区键": "旧键", "分区": "分区0", "范围编号": "NEW-R0",
                 "键起": "N0000", "键止": "N9999", "行数": 2500, "历史键": False},
                {"分区键": "旧键", "分区": "分区0", "范围编号": "LEG-R0",
                 "键起": "Z0000", "键止": "Z9999", "行数": 1500, "历史键": True},
            ],
            inflight=[], delivered=[], incoming=[], fetch_call="FETCH-旧键-0001",
        )
        moved = plan["迁出范围"]
        assert moved[0]["范围编号"] == "LEG-R0"
        assert moved[0]["历史键"] is True
        assert moved[0]["迁出行数"] == 1500


class TestInterruptedMigrationRecovery:
    """迁移中断恢复：按原位置次序核对在途记录，源位点只被接纳一次。"""

    def test_inflight_reconciled_in_position_order(self):
        recon = reconcile_inflight(
            inflight=[
                {"分区键": "订单", "源分区": "分区0", "目标分区": "分区1", "源位点": 503, "行数": 100},
                {"分区键": "订单", "源分区": "分区0", "目标分区": "分区1", "源位点": 501, "行数": 100},
                {"分区键": "订单", "源分区": "分区0", "目标分区": "分区2", "源位点": 502, "行数": 100},
                {"分区键": "订单", "源分区": "分区0", "目标分区": "分区2", "源位点": 502, "行数": 100},
            ],
            delivered=[{"分区键": "订单", "分区": "分区0", "源位点": 501, "行数": 100}],
            partition_key="订单",
        )
        # 依原位置次序核对
        assert recon["核对位点次序"] == [501, 502, 502, 503]
        # 501 已交付不再接纳；502 只接纳一次，第二个 502 剔除；503 接纳
        assert [r["源位点"] for r in recon["接纳"]] == [502, 503]
        assert recon["接纳行数"] == 200
        reasons = {r["源位点"]: r["剔除原因"] for r in recon["重复剔除"]}
        assert reasons[501] == "位点已交付"
        assert reasons[502] == "在途位点重复"

    def test_source_position_accepted_once_across_partitions(self):
        # fixtures 中源位点 10202 已交付分区2，在途记录里它又指向分区3：
        # 恢复核对时该位点只算分区2 已处理，不能同时在两个分区算作已处理
        recon = reconcile_inflight(PARTITION_INFLIGHT, PARTITION_DELIVERED, "用户ID")
        accepted_pos = [r["源位点"] for r in recon["接纳"]]
        assert accepted_pos == [10201, 10203]
        assert 10202 not in accepted_pos
        dup = next(r for r in recon["重复剔除"] if r["源位点"] == 10202)
        assert dup["剔除原因"] == "位点已交付"

    def test_recovery_plan_reconciled_and_granted(self):
        plan = evaluate_migration_plan(
            stat={"分区键": "订单", "分区数量": 2, "分区0行数": 4000, "分区1行数": 2000,
                  "热点分区": "分区0", "再平衡建议": True},
            capacity={"分区键": "订单", "分区上限": 5000, "当次迁移阈值": 1000},
            key_ranges=[
                {"分区键": "订单", "分区": "分区0", "范围编号": "ORD-R0",
                 "键起": "A0000", "键止": "A4999", "行数": 2500, "历史键": False},
                {"分区键": "订单", "分区": "分区0", "范围编号": "ORD-R1",
                 "键起": "A5000", "键止": "A9999", "行数": 1500, "历史键": False},
            ],
            inflight=[
                {"分区键": "订单", "源分区": "分区0", "目标分区": "分区1", "源位点": 501, "行数": 100},
                {"分区键": "订单", "源分区": "分区0", "目标分区": "分区1", "源位点": 502, "行数": 100},
                {"分区键": "订单", "源分区": "分区0", "目标分区": "分区1", "源位点": 503, "行数": 100},
            ],
            delivered=[{"分区键": "订单", "分区": "分区0", "源位点": 501, "行数": 100}],
            incoming=[], fetch_call="FETCH-订单-0001",
        )
        # 中断恢复后：在途 300 中 100 已交付剔除，待迁移 1000 接在位点 503 之后
        assert plan["在途核对"]["接纳行数"] == 200
        assert plan["在途核对"]["剔除行数"] == 100
        assert plan["待迁移行范围"] == {"起始位点": 504, "结束位点": 1503, "行数": 1000}
        # 回执对账：旧区 2800 + 新区 1200 = 输入 4000
        receipt = plan["回执"]
        assert receipt["旧区可交付行数"] == 2800
        assert receipt["新区可交付行数"] == 1200
        assert receipt["输入对账"]["对账平衡"] is True
        # 迁移确认之后才发放额度
        assert plan["额度"]["状态"] == "已发放"
        assert plan["额度"]["额度行数"] == 1000

    def test_timing_conflict_holds_quota(self):
        # 渠道：热点分区1 仍有在途 500 行，新段 1300 行使 7000+1300=8300 超过上限 8000
        plan = evaluate_migration_plan(**_fixtures_for("渠道"))
        assert plan["时机冲突"] is True
        assert plan["额度"]["状态"] == "挂起"
        assert plan["额度"]["额度行数"] == 0
        assert "时机冲突" in plan["额度"]["挂起原因"]
        # 冲突未确认，容量清单不回写
        assert plan["容量回写"] == []

    def test_no_conflict_when_segment_fits(self):
        # 用户ID：在途 850 行，新段 300 到达后 4800 仍在上限 5000 内，不构成冲突
        plan = evaluate_migration_plan(**_fixtures_for("用户ID"))
        assert plan["时机冲突"] is False


class TestPartitionBackpressurePipeline:
    """管道级：背压控制消费待迁移行范围，迁移结论回写容量清单。"""

    def test_migration_plans_present(self):
        r = run_partition_backpressure_pipeline()
        assert r["partition_count"] == 2
        assert r["alert_count"] == 2
        plans = {p["分区键"]: p for p in r["migration_plans"]}
        assert plans["用户ID"]["额度"]["状态"] == "已发放"
        assert plans["用户ID"]["额度"]["额度行数"] == 1350
        assert plans["渠道"]["额度"]["状态"] == "挂起"
        assert plans["渠道"]["时机冲突"] is True
        assert r["pending_migration_rows"] == 1350 + 700
        assert r["quota_granted_rows"] == 1350

    def test_capacity_manifest_written_only_after_confirmation(self):
        r = run_partition_backpressure_pipeline()
        manifest = {(m["分区键"], m["分区"]): m for m in r["capacity_manifest"]}
        # 用户ID 迁移确认：旧区与新区都回写
        assert manifest[("用户ID", "分区2")]["已用行数"] == 2600
        assert manifest[("用户ID", "分区3")]["已用行数"] == 3750
        assert manifest[("用户ID", "分区2")]["依据取数调用"] == "FETCH-用户ID-0001"
        # 渠道时机冲突未确认：不回写容量清单
        assert not any(key == "渠道" for key, _ in manifest)
        # 容量清单落在 store，供背压控制后续消费
        assert store.rows("_capacity_manifest")

    def test_fetch_call_locates_skew_verdict_in_quota(self):
        r = run_partition_backpressure_pipeline()
        for plan in r["migration_plans"]:
            quota = plan["额度"]
            assert quota["依据取数调用"] == plan["取数调用"]
            assert quota["歪斜结论"]["歪斜度"] == plan["歪斜度"]
