# -*- coding: utf-8 -*-
"""
条件触发提醒：把分析报告里的"等待XX再评估"变成系统自动盯。
分析留档时已把程序操作参考（方向/观察区/止损/目标）存进快照，
盘后（信号扫描之后，日线已更新）对照最新收盘价与最新信号：
- 回踩进入观察/买入区
- 跌破止损纪律位
- 到达第一目标位
- 信号转多/转空（用最新三周期数据重算方向结论，与快照时对比）
每个条件对每份快照只提醒一次（monitor_event 按 dedup_key 去重）。
"""

import json
from typing import Dict, List, Optional

from storage.sqlite.stock_storage import get_db
from utils.logger import logger


class ConditionWatcher:
    def __init__(self, notifier):
        self.db = get_db()
        self.notifier = notifier

    # ---------- 数据 ----------

    def _latest_rows(self, code: str):
        """从库里取三周期最新指标行（不重新联网，信号扫描已更新过日线）"""
        from tools.stock_tools import _ensure_indicators

        def _row(df, freq):
            if df is None or df.empty:
                return None
            return _ensure_indicators(df, freq).iloc[0].to_dict()

        daily_df = self.db.get_all_daily_data(code)
        if daily_df is None or daily_df.empty:
            return None, None, None, None
        daily = _ensure_indicators(daily_df, "daily")
        return (daily, daily.iloc[0].to_dict(),
                _row(self.db.get_all_weekly_data(code), "week"),
                _row(self.db.get_all_month_data(code), "month"))

    def _current_direction(self, daily_df, daily_row, weekly_row, monthly_row) -> Optional[Dict]:
        """用最新数据重算程序方向结论（与快照时对比判断信号翻转）"""
        try:
            from tools.trade_plan import build_trade_plan
            recent_low20 = float(daily_df.head(20)["low"].min()) if "low" in daily_df.columns else None
            recent_high60 = float(daily_df.head(60)["high"].max()) if "high" in daily_df.columns else None
            return build_trade_plan(daily_row, weekly_row, monthly_row, recent_low20, recent_high60)
        except Exception as e:
            logger.warning(f"[条件盯盘] 重算方向结论失败: {e}")
            return None

    # ---------- 条件判定（纯代码） ----------

    @staticmethod
    def price_drifted(daily_df, snap_created, snap_price,
                      tolerance_pct: float = 2.0) -> Optional[bool]:
        """
        除权漂移检测：分红/送股后前复权序列整体平移，快照里的价位条件会全部错位。
        对比快照当日（或其前最近交易日）在当前序列中的收盘价与快照记录的价格：
        偏差超过 tolerance_pct 判定漂移。无法核对（缺数据）返回 None。
        """
        try:
            from utils.common import parse_row_date
            if snap_price is None or daily_df is None or daily_df.empty:
                return None
            snap_date = parse_row_date(str(snap_created)[:10])
            if snap_date is None:
                return None
            for _, row in daily_df.iterrows():  # 降序：从最新往回找快照日或其前最近交易日
                d = parse_row_date(row.get("date"))
                if d is None or d > snap_date:
                    continue
                close_then = row.get("close")
                if close_then is None:
                    return None
                return abs(float(close_then) / float(snap_price) - 1) * 100 > tolerance_pct
            return None
        except Exception:
            return None

    @staticmethod
    def check_conditions(plan: Dict, close: float,
                         new_direction: Optional[str] = None) -> List[Dict[str, str]]:
        """
        对照旧操作参考与最新收盘价/最新方向，返回触发的条件列表。
        纯函数，便于测试。每项 {"key": 去重后缀, "text": 推送文案}
        """
        hits = []
        zone = plan.get("entry_zone")
        stop = plan.get("stop_loss")
        targets = plan.get("targets") or []

        if zone and zone[0] <= close <= zone[1]:
            hits.append({"key": "entry", "text": f"回踩进入观察/买入区 {zone[0]}~{zone[1]}（现价{close}）"})
        if stop is not None and close <= stop:
            hits.append({"key": "stop", "text": f"跌破止损纪律位 {stop}（现价{close}），按纪律应离场/放弃该结构"})
        if targets and close >= targets[0]:
            hits.append({"key": "target", "text": f"到达第一目标位 {targets[0]}（现价{close}），可评估分批了结"})

        old_dir = plan.get("direction")
        if new_direction and old_dir and new_direction != old_dir:
            if new_direction == "可考虑介入":
                hits.append({"key": f"flip:{new_direction}",
                             "text": f"程序信号由「{old_dir}」转为「可考虑介入」（多周期共振出现），建议重新分析确认"})
            elif old_dir == "可考虑介入":
                hits.append({"key": f"flip:{new_direction}",
                             "text": f"程序信号由「可考虑介入」转为「{new_direction}」（信号走弱）"})
        return hits

    # ---------- 主流程 ----------

    def scan(self) -> int:
        """扫描监控清单内有快照留档的个股，返回推送条数"""
        targets = [t for t in self.db.get_watch_targets()
                   if t["target_type"] == "company" and t.get("code")]
        if not targets:
            return 0

        pushed = 0
        for t in targets:
            try:
                pushed += self._scan_one(t["code"], t["name"])
            except Exception as e:
                logger.error(f"[条件盯盘] {t['name']} 失败: {e}")
        if pushed:
            logger.info(f"[条件盯盘] 推送 {pushed} 条条件触发提醒")
        return pushed

    def _scan_one(self, code: str, name: str) -> int:
        snap = self.db.get_latest_snapshot(code)
        if not snap or not snap.get("trade_plan"):
            return 0
        try:
            plan = json.loads(snap["trade_plan"])
        except (json.JSONDecodeError, TypeError):
            return 0

        daily_df, daily_row, weekly_row, monthly_row = self._latest_rows(code)
        if daily_row is None:
            return 0
        close = daily_row.get("close")
        try:
            close = round(float(close), 2)
        except (TypeError, ValueError):
            return 0

        new_plan = self._current_direction(daily_df, daily_row, weekly_row, monthly_row)
        new_direction = new_plan.get("direction") if new_plan else None

        # 除权漂移：旧价位条件全部作废（只提醒一次），方向翻转判断不受影响
        drifted = self.price_drifted(daily_df, snap.get("created_at"), snap.get("price_at_analysis"))
        if drifted:
            key = f"cond:{code}:{snap['id']}:drift"
            if self.db.save_monitor_event(target=name, event_type="condition", dedup_key=key,
                                          title="检测到除权/复权调整", importance="高", pushed=True):
                self.notifier.send(
                    f"⚠️ 价位条件失效 | {name}({code})\n"
                    f"检测到除权/复权调整，{str(snap.get('created_at'))[:10]} 分析留档的"
                    f"买卖/止损价位已不可比，建议重新发送「分析 {name}」刷新操作参考")
            hits = self.check_conditions({"direction": plan.get("direction")}, close,
                                         new_direction=new_direction)  # 只保留方向翻转判断
        else:
            hits = self.check_conditions(plan, close, new_direction=new_direction)
        count = 0
        for h in hits:
            dedup_key = f"cond:{code}:{snap['id']}:{h['key']}"
            saved = self.db.save_monitor_event(
                target=name, event_type="condition", dedup_key=dedup_key,
                title=h["text"], importance="高", pushed=True,
            )
            if saved:
                snap_date = str(snap.get("created_at"))[:10]
                self.notifier.send(
                    f"🎯 条件触发 | {name}({code})\n{h['text']}\n"
                    f"（对照 {snap_date} 分析留档的操作参考；价位条件仅提醒一次）")
                count += 1
        return count
