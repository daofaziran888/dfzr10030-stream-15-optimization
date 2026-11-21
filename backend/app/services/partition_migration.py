"""分区再平衡迁移评估：待迁移行范围、在途核对、回执对账与额度判断。

grp_partition -> backpressure 管道的迁移评估内核：
- 迁移按分区上限与已有分区键范围评估，算出背压控制能实际使用的待迁移行范围
- 中断恢复时按原位置次序核对在途记录，同一源位点只被接纳一次，
  不能同时在旧区与新区算作已处理
- 回执按范围说明当前归属，旧区与新区的可交付行数都能和输入对账
- 热点分区仍有在途行且新一段输入超过容量时，判定为迁移时机冲突
- 迁移确认之后才请求发放额度，迁移结论由背压控制回写容量清单
"""
from __future__ import annotations

import statistics
from typing import Any

SKEW_LIMIT = 1.0  # 歪斜度超过该值即触发再平衡评估


def _to_int(v: Any) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def partition_rows(stat: dict) -> dict[str, int]:
    """把分区统计行展开成 {分区i: 行数}，缺失的分区列按 0 计。"""
    n = _to_int(stat.get("分区数量"))
    return {f"分区{i}": _to_int(stat.get(f"分区{i}行数")) for i in range(n)}


def skew_ratio(rows: dict[str, int]) -> float:
    """(最大区 - 最小区) / 均值，分布越均匀越接近 0。"""
    vals = list(rows.values())
    if not vals:
        return 0.0
    mean = statistics.mean(vals)
    if mean <= 0:
        return 0.0
    return round((max(vals) - min(vals)) / mean, 2)


def reconcile_inflight(
    inflight: list[dict], delivered: list[dict], partition_key: str
) -> dict[str, Any]:
    """中断恢复：按原位置次序核对在途记录。

    已交付位点不再接纳；同一源位点在整个分区键下只被接纳一次，
    不能同时在两个分区算作已处理。位点统一按数值比较，
    兼容历史分区键与已交付位点的写法差异。
    """
    delivered_pos = {
        _to_int(d.get("源位点")) for d in delivered if d.get("分区键") == partition_key
    }
    records = sorted(
        (r for r in inflight if r.get("分区键") == partition_key),
        key=lambda r: _to_int(r.get("源位点")),
    )
    seen: set[int] = set()
    accepted: list[dict] = []
    duplicates: list[dict] = []
    for rec in records:
        pos = _to_int(rec.get("源位点"))
        if pos in delivered_pos:
            duplicates.append({**rec, "剔除原因": "位点已交付"})
            continue
        if pos in seen:
            duplicates.append({**rec, "剔除原因": "在途位点重复"})
            continue
        seen.add(pos)
        accepted.append(rec)
    return {
        "核对位点次序": [_to_int(r.get("源位点")) for r in records],
        "接纳": accepted,
        "重复剔除": duplicates,
        "接纳行数": sum(_to_int(r.get("行数")) for r in accepted),
        "剔除行数": sum(_to_int(r.get("行数")) for r in duplicates),
    }


def evaluate_migration_plan(
    *,
    stat: dict,
    capacity: dict,
    key_ranges: list[dict],
    inflight: list[dict],
    delivered: list[dict],
    incoming: list[dict],
    fetch_call: str,
) -> dict[str, Any]:
    """评估单个分区键的再平衡迁移计划。

    fetch_call 是本次取数调用编号：歪斜结论带着它进入额度判断，
    从额度结果可以定位回歪斜评估发生的取数位置。
    """
    key = str(stat.get("分区键", ""))
    rows = partition_rows(stat)
    total = sum(rows.values())
    n = len(rows)
    mean = total / n if n else 0.0
    skew = skew_ratio(rows)
    cap = _to_int(capacity.get("分区上限"))
    threshold = _to_int(capacity.get("当次迁移阈值"))

    hotspot = str(stat.get("热点分区") or "") or (max(rows, key=lambda p: rows[p]) if rows else "")
    hotspot_rows = rows.get(hotspot, 0)

    # 1) 在途核对：中断恢复也走这里，按原位置次序、源位点只接纳一次
    recon = reconcile_inflight(inflight, delivered, key)
    accepted_rows = sum(
        _to_int(r.get("行数")) for r in recon["接纳"] if r.get("源分区") == hotspot
    )
    dup_rows = sum(
        _to_int(r.get("行数")) for r in recon["重复剔除"] if r.get("源分区") == hotspot
    )
    hotspot_inflight_rows = accepted_rows + dup_rows

    # 2) 时机冲突：热点仍有在途行，新一段输入到达后超过分区上限
    seg_rows = sum(
        _to_int(s.get("行数")) for s in incoming
        if s.get("分区键") == key and s.get("分区") == hotspot
    )
    overflow = cap > 0 and hotspot_rows + seg_rows > cap
    timing_conflict = hotspot_inflight_rows > 0 and overflow

    # 3) 待迁移行范围：按分区上限与已有分区键范围评估
    triggered = (
        bool(stat.get("再平衡建议"))
        or skew > SKEW_LIMIT
        or (cap > 0 and hotspot_rows > cap)
    )
    need_move = max(0, round(hotspot_rows - mean)) if triggered else 0

    # 迁入分区：行数最少且仍有剩余容量的分区
    candidates = sorted((p for p in rows if p != hotspot), key=lambda p: rows[p])
    target = next(
        (p for p in candidates if cap <= 0 or rows[p] < cap),
        candidates[0] if candidates else "",
    )
    target_free = max(0, cap - rows.get(target, 0)) if cap else 0
    # 新区能接的行数按当次迁移阈值核验
    accept_rows = min(target_free, threshold) if threshold else target_free
    planned = min(need_move, accept_rows)

    # 按已有分区键范围选择迁出段（历史键范围同样参与），从键起最高的范围迁起
    ranges = [r for r in key_ranges if r.get("分区键") == key and r.get("分区") == hotspot]
    move_ranges: list[dict] = []
    acc = 0
    for r in sorted(ranges, key=lambda r: str(r.get("键起", "")), reverse=True):
        if acc >= planned:
            break
        take = min(_to_int(r.get("行数")), planned - acc)
        if take <= 0:
            continue
        move_ranges.append({
            "范围编号": r.get("范围编号"),
            "键起": r.get("键起"),
            "键止": r.get("键止"),
            "迁出行数": take,
            "历史键": bool(r.get("历史键")),
        })
        acc += take
    planned = acc

    # 待迁移行范围接在已交付与在途位点之后，背压控制可直接消费
    delivered_max = max(
        (_to_int(d.get("源位点")) for d in delivered if d.get("分区键") == key), default=0
    )
    inflight_max = max(
        (_to_int(r.get("源位点")) for r in inflight if r.get("分区键") == key), default=0
    )
    base = max(delivered_max, inflight_max)
    pending_range = {
        "起始位点": base + 1 if planned else base,
        "结束位点": base + planned,
        "行数": planned,
    }

    # 4) 回执：按范围说明当前归属，旧区与新区可交付行数与输入对账
    remaining_old = hotspot_rows - hotspot_inflight_rows - planned
    moved_ids = {m["范围编号"] for m in move_ranges}
    ownership = [
        {
            "范围编号": r.get("范围编号"),
            "当前归属": target if (r.get("分区") == hotspot and r.get("范围编号") in moved_ids) else r.get("分区"),
            "迁出行数": next((m["迁出行数"] for m in move_ranges if m["范围编号"] == r.get("范围编号")), 0),
            "历史键": bool(r.get("历史键")),
        }
        for r in key_ranges
        if r.get("分区键") == key
    ]
    old_deliverable = remaining_old + dup_rows    # 重复位点早已交付旧区，仍归旧区
    new_deliverable = accepted_rows + planned     # 在途接纳与待迁移都归新区
    reconciled = old_deliverable + new_deliverable == hotspot_rows
    receipt = {
        "旧区": hotspot,
        "新区": target,
        "范围归属": ownership,
        "旧区可交付行数": old_deliverable,
        "新区可交付行数": new_deliverable,
        "热点剩余行数": old_deliverable,
        "热点剩余解释": (
            f"{hotspot_rows} = 旧区剩余 {old_deliverable}"
            f"（未迁出 {remaining_old} + 已交付旧区位点 {dup_rows}）"
            f" + 迁出 {accepted_rows + planned}（在途 {accepted_rows} + 待迁移 {planned}）"
        ),
        "输入对账": {"热点输入行数": hotspot_rows, "对账平衡": reconciled},
    }

    # 5) 额度判断：迁移确认之后才请求发放额度
    if not triggered:
        quota_status, hold_reason, conclusion = "无需发放", "", "分布均匀无需迁移"
    elif timing_conflict:
        quota_status = "挂起"
        hold_reason = "时机冲突：热点分区仍有在途行且新一段输入已超过分区上限"
        conclusion = "时机冲突暂缓迁移"
    elif planned <= 0:
        quota_status = "挂起"
        hold_reason = "新区剩余容量或当次迁移阈值不足"
        conclusion = "容量不足暂缓迁移"
    elif not reconciled:
        quota_status = "挂起"
        hold_reason = "回执与输入对账不平"
        conclusion = "对账异常暂缓迁移"
    else:
        quota_status, hold_reason = "已发放", ""
        conclusion = f"迁移确认：{hotspot} 向 {target} 迁出 {accepted_rows + planned} 行"
    quota = {
        "状态": quota_status,
        "额度行数": planned if quota_status == "已发放" else 0,
        "挂起原因": hold_reason,
        "依据取数调用": fetch_call,
        "歪斜结论": {"歪斜度": skew, "热点分区": hotspot, "迁移触发": triggered},
    }

    # 6) 容量清单回写内容：只在迁移确认后由背压控制落账
    capacity_updates: list[dict] = []
    if quota_status == "已发放":
        capacity_updates.append({
            "分区键": key,
            "分区": hotspot,
            "已用行数": old_deliverable,
            "分区上限": cap,
            "最近迁移结论": conclusion,
            "依据取数调用": fetch_call,
        })
        accepted_by_target: dict[str, int] = {}
        for r in recon["接纳"]:
            if r.get("源分区") == hotspot:
                t = str(r.get("目标分区"))
                accepted_by_target[t] = accepted_by_target.get(t, 0) + _to_int(r.get("行数"))
        if planned:
            accepted_by_target[target] = accepted_by_target.get(target, 0) + planned
        for t, added in sorted(accepted_by_target.items()):
            capacity_updates.append({
                "分区键": key,
                "分区": t,
                "已用行数": rows.get(t, 0) + added,
                "分区上限": cap,
                "最近迁移结论": f"接纳来自 {hotspot} 的 {added} 行",
                "依据取数调用": fetch_call,
            })

    return {
        "分区键": key,
        "取数调用": fetch_call,
        "分区行数": rows,
        "总行数": total,
        "歪斜度": skew,
        "热点分区": hotspot,
        "迁移触发": triggered,
        "在途核对": recon,
        "新输入段行数": seg_rows,
        "时机冲突": timing_conflict,
        "迁出分区": hotspot,
        "迁入分区": target,
        "迁出范围": move_ranges,
        "待迁移行范围": pending_range,
        "新区可接纳行数": accept_rows,
        "回执": receipt,
        "额度": quota,
        "迁移结论": conclusion,
        "容量回写": capacity_updates,
    }
