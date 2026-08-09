# -*- coding: utf-8 -*-
"""
业务指标预警监控器（Business Metrics Watcher）
=============================================
监控三类业务指标变化，当触发阈值时通过 Feishu 推送预警：
1. 北向资金持仓变动：单周/单月持仓比例变动超过阈值
2. 存货周转改善/恶化：最新季度存货周转天数同比变化超过阈值
3. 行业渗透率变化：新能源车月度渗透率变化超过阈值

运行频率：由 scheduler 按日触发（交易日）
"""

from datetime import date, timedelta
from typing import Dict, Any, List, Optional
import traceback
import pandas as pd

from utils.logger import logger


# ===== 阈值配置 =====
_THRESHOLDS = {
    # 北向持仓：单周变动超过此比例（百分点）触发
    "northbound_weekly_pct": 1.0,
    # 北向持仓：单月变动超过此比例（百分点）触发
    "northbound_monthly_pct": 2.0,
    # 存货周转天数：同比变化超过此天数触发
    "inv_turn_days_change": 15,
    # 新能源渗透率：月度变化超过此百分点触发
    "penetration_monthly_pct": 3.0,
}


def _get_db():
    """懒加载数据库实例"""
    from storage.sqlite.stock_storage import DatabaseManager
    return DatabaseManager()


def _push_alert(title: str, content: str, target: str = ""):
    """推送预警到飞书"""
    try:
        from monitoring.notifier import FeishuNotifier
        notifier = FeishuNotifier()
        msg = f"⚠️ **{title}**\n{content}"
        notifier.send(msg)
        logger.info(f"业务预警已推送: {title}")
    except Exception as e:
        logger.warning(f"业务预警推送失败: {e}")

    # 记录监控事件到 DB（去重）
    try:
        db = _get_db()
        dedup_key = f"biz_watch_{target}_{date.today().isoformat()}_{title[:20]}"
        db.save_monitor_event(
            target=target or "business",
            event_type="business_alert",
            dedup_key=dedup_key,
            title=title[:100],
            content=content[:500],
            importance="high",
        )
    except Exception as e:
        logger.debug(f"记录监控事件失败: {e}")


def _get_watch_codes() -> List[str]:
    """获取所有已启用的监控目标股票代码"""
    try:
        db = _get_db()
        targets = db.get_watch_targets()
        codes = []
        for t in targets:
            if isinstance(t, dict):
                code = t.get("code")
                if code:
                    codes.append(code)
                elif t.get("target_type") == "company":
                    name = t.get("name")
                    if name:
                        from tools.company_code_validator import find_stock_code
                        try:
                            code = find_stock_code(name)
                            if code:
                                codes.append(code)
                        except Exception:
                            pass
        return list(set(codes))
    except Exception as e:
        logger.warning(f"获取监控列表失败: {e}")
        return []


# ===== 1. 北向持仓监控 =====

def check_northbound_holdings() -> List[str]:
    """
    检查所有监控股票的北向持仓变化
    返回: 触发的预警消息列表
    """
    alerts = []
    codes = _get_watch_codes()
    if not codes:
        logger.debug("北向监控: 无监控目标")
        return alerts

    db = _get_db()
    today = date.today()

    for code in codes:
        try:
            df = db.get_stock_northbound_hold(code)
            if df is None or df.empty:
                continue

            # 确保有 trade_date 列
            if 'trade_date' not in df.columns:
                continue

            # 解析日期
            df = df.copy()
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df.sort_values('trade_date', ascending=False)

            if len(df) < 2:
                continue

            latest_hold = _num(df.iloc[0].get('total_share') or df.iloc[0].get('vol'))
            if latest_hold is None:
                continue

            latest_date = df.iloc[0]['trade_date'].date() if hasattr(df.iloc[0]['trade_date'], 'date') else df.iloc[0]['trade_date']

            # 找一周前的数据
            week_ago = latest_date - timedelta(days=7)
            week_data = df[df['trade_date'].dt.date <= week_ago]
            month_data = df[df['trade_date'].dt.date <= (latest_date - timedelta(days=25))]

            # 周变动
            if not week_data.empty:
                week_hold = _num(week_data.iloc[0].get('total_share') or week_data.iloc[0].get('vol'))
                if week_hold and week_hold > 0:
                    week_change = (latest_hold - week_hold) / week_hold * 100
                    if abs(week_change) >= _THRESHOLDS["northbound_weekly_pct"]:
                        direction = "增持" if week_change > 0 else "减持"
                        alerts.append(
                            f"🔵 [{code}] 北向资金{direction}预警\n"
                            f"  近一周北向持仓变动: {week_change:+.2f}%\n"
                            f"  当前持仓量: {latest_hold:.0f} 股"
                        )

            # 月变动
            if not month_data.empty:
                month_hold = _num(month_data.iloc[0].get('total_share') or month_data.iloc[0].get('vol'))
                if month_hold and month_hold > 0:
                    month_change = (latest_hold - month_hold) / month_hold * 100
                    if abs(month_change) >= _THRESHOLDS["northbound_monthly_pct"]:
                        direction = "增持" if month_change > 0 else "减持"
                        alert_text = (
                            f"🔵 [{code}] 北向资金{direction}预警（月度）\n"
                            f"  近一月北向持仓变动: {month_change:+.2f}%\n"
                            f"  当前持仓量: {latest_hold:.0f} 股"
                        )
                        # 避免与周预警重复
                        if alert_text not in alerts:
                            alerts.append(alert_text)

        except Exception as e:
            logger.debug(f"北向监控 [{code}] 失败: {e}")
            continue

    return alerts


# ===== 2. 存货周转监控 =====

def check_inventory_turnover() -> List[str]:
    """
    检查所有监控股票的存货周转天数同比变化
    需要 fina_indicator 表中有 inv_turn 数据
    """
    alerts = []
    codes = _get_watch_codes()
    if not codes:
        return alerts

    db = _get_db()
    today = date.today()

    for code in codes:
        try:
            df = db.get_stock_fina_indicator(code)
            if df is None or df.empty:
                continue

            # 需要 inv_turn 列和 report_date 列
            if 'inv_turn' not in df.columns or 'report_date' not in df.columns:
                continue

            df = df.copy()
            df['report_date'] = pd.to_datetime(df['report_date'])
            df = df.sort_values('report_date', ascending=False)

            if len(df) < 2:
                continue

            latest = df.iloc[0]
            prev = df.iloc[1]

            latest_inv = _num(latest.get('inv_turn'))
            prev_inv = _num(prev.get('inv_turn'))

            if latest_inv and prev_inv and latest_inv > 0 and prev_inv > 0:
                latest_days = 365 / latest_inv
                prev_days = 365 / prev_inv
                days_change = latest_days - prev_days

                if abs(days_change) >= _THRESHOLDS["inv_turn_days_change"]:
                    direction = "恶化（周转变慢）" if days_change > 0 else "改善（周转加快）"
                    alerts.append(
                        f"🟡 [{code}] 存货周转{direction}\n"
                        f"  存货周转天数: {prev_days:.0f} → {latest_days:.0f} 天"
                        f"（变动 {days_change:+.0f} 天）\n"
                        f"  周转率: {prev_inv:.2f} → {latest_inv:.2f} 次/年"
                    )

        except Exception as e:
            logger.debug(f"存货周转监控 [{code}] 失败: {e}")
            continue

    return alerts


# ===== 3. 行业渗透率监控 =====

def check_industry_penetration() -> List[str]:
    """
    检查新能源车月度渗透率变化
    需要 new_energy_penetration 表中有数据
    """
    alerts = []
    try:
        db = _get_db()
        df = db.get_new_energy_penetration()
        if df is None or df.empty:
            return alerts

        if 'penetration_rate' not in df.columns or 'month' not in df.columns:
            return alerts

        df = df.copy()
        df['month'] = pd.to_datetime(df['month'])
        df = df.sort_values('month', ascending=False)

        if len(df) < 2:
            return alerts

        latest_rate = _num(df.iloc[0].get('penetration_rate'))
        prev_rate = _num(df.iloc[1].get('penetration_rate'))

        if latest_rate is not None and prev_rate is not None:
            change = latest_rate - prev_rate
            if abs(change) >= _THRESHOLDS["penetration_monthly_pct"]:
                direction = "加速提升" if change > 0 else "回落"
                alerts.append(
                    f"🟢 **新能源车渗透率{direction}**\n"
                    f"  渗透率: {df.iloc[1]['month'].strftime('%Y-%m')} {prev_rate:.1f}%"
                    f" → {df.iloc[0]['month'].strftime('%Y-%m')} {latest_rate:.1f}%\n"
                    f"  月度变动: {change:+.1f} 百分点\n"
                    f"  {'新能源替代加速，利好整车和锂电产业链' if change > 0 else '渗透率短期回调，关注是否趋势性放缓'}"
                )
    except Exception as e:
        logger.debug(f"渗透率监控失败: {e}")

    return alerts


def _num(v):
    """安全取数值"""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ===== 主入口 =====

def scan_all() -> List[str]:
    """
    扫描所有业务指标，返回触发的预警列表
    由 scheduler.py 按日调用
    """
    import pandas as pd
    all_alerts = []

    try:
        alerts = check_northbound_holdings()
        all_alerts.extend(alerts)
    except Exception as e:
        logger.error(f"北向持仓监控异常: {e}\n{traceback.format_exc()}")

    try:
        alerts = check_inventory_turnover()
        all_alerts.extend(alerts)
    except Exception as e:
        logger.error(f"存货周转监控异常: {e}\n{traceback.format_exc()}")

    try:
        alerts = check_industry_penetration()
        all_alerts.extend(alerts)
    except Exception as e:
        logger.error(f"渗透率监控异常: {e}\n{traceback.format_exc()}")

    # 推送
    if all_alerts:
        combined = "\n\n".join(all_alerts)
        summary = f"📊 **业务指标预警（{date.today().isoformat()}）**\n\n共 {len(all_alerts)} 条触发:\n\n{combined}"
        _push_alert("业务指标扫描", summary, "business_metrics")
        logger.info(f"业务指标预警: 触发 {len(all_alerts)} 条")

    return all_alerts


if __name__ == "__main__":
    alerts = scan_all()
    for a in alerts:
        print(a)
        print()
