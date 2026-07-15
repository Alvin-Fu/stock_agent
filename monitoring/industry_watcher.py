# -*- coding: utf-8 -*-
"""
产业链候选池监控器（Industry Pool Watcher）
===============================================
监控最近产业链分析生成的候选池快照，定期检查：
1. 候选股价格相对快照时点的涨跌幅（+/-10% 触发）
2. 候选股 PE/PB 估值分位变化
3. 候选股北向持仓变化
4. 淘汰股是否出现反转信号

运行频率：由 scheduler 按日触发（交易日）
"""

from datetime import date, timedelta
from typing import Dict, Any, List, Optional
import traceback
import json

from utils.logger import logger


# ===== 阈值配置 =====
_THRESHOLDS = {
    # 候选股相对快照时价格涨跌幅超过此值触发
    "price_change_pct": 10.0,
    # 北向持仓周变动（百分点）
    "northbound_weekly_pct": 1.0,
    # 快照有效天数（超过此天数不再监控）
    "snapshot_max_age_days": 30,
}


def _get_db():
    """懒加载数据库实例"""
    from storage.sqlite.stock_storage import DatabaseManager
    return DatabaseManager()


def _get_tushare_fetcher():
    """懒加载 TushareFetcher 实例"""
    from tools.stock.tushare_fetcher import TushareFetcher
    return TushareFetcher()


def _push_alert(title: str, content: str, target: str = ""):
    """推送预警到飞书"""
    try:
        from monitoring.notifier import FeishuNotifier
        notifier = FeishuNotifier()
        msg = f"⚠️ **{title}**\n{content}"
        notifier.send(msg)
        logger.info(f"产业链预警已推送: {title}")
    except Exception as e:
        logger.warning(f"产业链预警推送失败: {e}")

    # 记录监控事件到 DB
    try:
        db = _get_db()
        dedup_key = f"industry_watch_{target}_{date.today().isoformat()}_{title[:20]}"
        db.save_monitor_event(
            target=target or "industry_pool",
            event_type="industry_alert",
            dedup_key=dedup_key,
            title=title[:100],
            content=content[:500],
            importance="high",
        )
    except Exception as e:
        logger.warning(f"监控事件记录失败: {e}")


def _get_current_price(code: str) -> Optional[float]:
    """获取个股当前价格"""
    try:
        db = _get_db()
        daily = db.get_latest_daily_basic_data(code, days=1)
        if daily is not None and not daily.empty:
            col = "close" if "close" in daily.columns else daily.columns[-1]
            return float(daily.iloc[0][col])
        # DB 无数据时尝试 Tushare
        from tools.stock.tushare_fetcher import TushareFetcher
        fetcher = TushareFetcher()
        df = fetcher.stock_daily_basic(
            date.today().isoformat().replace("-", ""),
            date.today().isoformat().replace("-", ""),
            stock_code=code
        )
        if df is not None and not df.empty:
            return float(df.iloc[0].get("close", 0))
    except Exception as e:
        logger.warning(f"获取价格失败 {code}: {e}")
    return None


def _get_valuation_percentile(code: str) -> Optional[Dict[str, float]]:
    """获取当前 PE/PB 预估分位（基于 daily_basic 近期数据近似计算）"""
    try:
        db = _get_db()
        import numpy as np
        daily_list = db.get_latest_daily_basic_data(code, days=500)
        if daily_list is None or daily_list.empty:
            return None
        pe_vals = daily_list["pe_ttm"].dropna().values.astype(float) if "pe_ttm" in daily_list.columns else np.array([])
        pb_vals = daily_list["pb"].dropna().values.astype(float) if "pb" in daily_list.columns else np.array([])
        if len(pe_vals) < 20:
            return None
        current_pe = float(pe_vals[-1])
        current_pb = float(pb_vals[-1]) if len(pb_vals) > 0 else None
        pe_pct = (pe_vals < current_pe).mean() * 100
        pb_pct = (pb_vals < current_pb).mean() * 100 if current_pb else None
        return {"pe_percentile": round(pe_pct, 1), "pb_percentile": round(pb_pct, 1) if pb_pct else None}
    except Exception:
        pass
    return None


def _get_company_name(code: str) -> str:
    """获取公司中文名"""
    try:
        db = _get_db()
        basic = db.get_stock_basic(code)
        if basic:
            return basic.name
    except Exception:
        pass
    return code


def scan_industry_pools() -> List[str]:
    """
    扫描所有最近产业链快照，返回触发的预警列表
    """
    all_alerts = []
    db = _get_db()

    try:
        # 获取最近 30 天的快照
        from sqlalchemy import text
        with db.get_session() as session:
            cutoff = (date.today() - timedelta(days=_THRESHOLDS["snapshot_max_age_days"])).isoformat()
            rows = session.execute(
                text("SELECT id, industry_name, created_at, candidates, top_pick, valuation, excluded "
                     "FROM industry_snapshot WHERE created_at >= :cutoff ORDER BY created_at DESC LIMIT 20"),
                {"cutoff": cutoff}
            ).fetchall()
    except Exception as e:
        logger.warning(f"查询产业链快照失败: {e}")
        return []

    if not rows:
        logger.info("产业链监控：最近无待检快照")
        return []

    for row in rows:
        snap_id = row[0]
        industry = row[1]
        snap_date = str(row[2])[:10] if row[2] else "未知"
        candidates_raw = row[3]
        top_pick = row[4] or ""
        valuation_raw = row[5]
        excluded_raw = row[6]

        # 解析候选列表
        candidates = []
        if candidates_raw:
            try:
                candidates = json.loads(candidates_raw) if isinstance(candidates_raw, str) else candidates_raw
            except Exception:
                candidates = []

        # 解析估值
        archived_pe_pct = None
        if valuation_raw:
            try:
                v = json.loads(valuation_raw) if isinstance(valuation_raw, str) else valuation_raw
                archived_pe_pct = v.get("pe_percentile")
            except Exception:
                pass

        # 解析淘汰名单
        excluded_items = []
        if excluded_raw:
            try:
                excluded_items = json.loads(excluded_raw) if isinstance(excluded_raw, str) else excluded_raw
            except Exception:
                excluded_items = []

        # ---- 检查前 3 名候选 ----
        for cand in candidates[:3]:
            code = cand.get("code", "")
            name = cand.get("name", code)
            archived_rank = cand.get("rank", 0)
            archived_composite = cand.get("composite", 0)
            if not code:
                continue

            # 1) 价格涨跌幅检查
            current_price = _get_current_price(code)
            if current_price is not None:
                # 快照时没有存价格，用当日收盘价近似
                archived_price = None
                try:
                    # 用快照日期附近的价格
                    from tools.stock.tushare_fetcher import TushareFetcher
                    fetcher = TushareFetcher()
                    snap_date_compact = snap_date.replace("-", "")
                    df, _ = fetcher.daily_basic(code, trade_date=snap_date_compact)
                    if df is not None and not df.empty:
                        archived_price = float(df.iloc[0].get("close",
                                     float(df.iloc[0].get("trade_date", df.iloc[-1].get("close", 0)))))
                except Exception:
                    pass

                if archived_price and archived_price > 0:
                    pct = (current_price - archived_price) / archived_price * 100
                    if abs(pct) >= _THRESHOLDS["price_change_pct"]:
                        direction = "📈 大涨" if pct > 0 else "📉 大跌"
                        alert = (
                            f"**{industry}** 候选 **{name}({code})** "
                            f"{direction} {pct:+.1f}%（快照日 {snap_date}：{archived_price:.2f} → 当前：{current_price:.2f}）"
                        )
                        all_alerts.append(alert)

            # 2) 估值变化
            if archived_pe_pct is not None:
                current_val = _get_valuation_percentile(code)
                if current_val:
                    current_pe = current_val.get("pe_percentile_median")
                    if current_pe is not None:
                        pe_diff = current_pe - archived_pe_pct
                        if abs(pe_diff) > 15:
                            direction = "估值大幅扩张" if pe_diff > 0 else "估值大幅收缩"
                            alert = (
                                f"**{industry}** 候选 **{name}({code})** "
                                f"{direction}（PE分位：{archived_pe_pct:.0f}% → {current_pe:.0f}%）"
                            )
                            all_alerts.append(alert)

        # ---- 检查淘汰股是否有反转信号 ----
        for exc in excluded_items:
            code = exc.get("code", "")
            reason = exc.get("reason", "")
            if not code:
                continue
            name = exc.get("name", code)

            current_price = _get_current_price(code)
            if current_price is None:
                continue

            # 拿快照日的价格
            archived_price = None
            try:
                from tools.stock.tushare_fetcher import TushareFetcher
                fetcher = TushareFetcher()
                snap_date_compact = snap_date.replace("-", "")
                df, _ = fetcher.daily_basic(code, trade_date=snap_date_compact)
                if df is not None and not df.empty:
                    archived_price = float(df.iloc[0].get("close",
                                 float(df.iloc[0].get("trade_date", df.iloc[-1].get("close", 0)))))
            except Exception:
                pass

            if archived_price and archived_price > 0:
                pct = (current_price - archived_price) / archived_price * 100
                if pct >= _THRESHOLDS["price_change_pct"]:
                    direction = "📈 淘汰后大幅上涨（可重新评估）" if pct > 0 else "📉 淘汰后持续下跌（验证正确）"
                    alert = (
                        f"**{industry}** 淘汰股 **{name}({code})** "
                        f"{direction} {pct:+.1f}%（快照日 → 当前）\n"
                        f"> 淘汰原因：{reason[:100]}"
                    )
                    all_alerts.append(alert)

    return all_alerts


def scan_all() -> List[str]:
    """
    主入口：扫描所有产业链快照，返回触发的预警列表
    由 scheduler 的 _run_signal_scan 调用
    """
    try:
        alerts = scan_industry_pools()
        if alerts:
            combined = "\n\n".join(alerts)
            summary = (
                f"📊 **产业链候选池监控（{date.today().isoformat()}）**\n\n"
                f"共 {len(alerts)} 条触发:\n\n{combined}"
            )
            _push_alert("产业链快照扫描", summary, "industry_pool")
            logger.info(f"产业链预警: 触发 {len(alerts)} 条")
        return alerts
    except Exception as e:
        logger.error(f"产业链监控异常: {e}\n{traceback.format_exc()}")
        return []


if __name__ == "__main__":
    alerts = scan_all()
    for a in alerts:
        print(a)
        print()
