# -*- coding: utf-8 -*-
"""
盘后信号扫描：对监控清单里的个股跑日线管线，收集程序判定的技术信号并推送。
信号来源就是数据层的信号列（ma_cross/vol_signal/gap_signal/macd_signal），零额外计算。
"""

from datetime import date
from typing import List, Dict, Any

from storage.sqlite.stock_storage import get_db
from utils.common import parse_row_date
from utils.config import load_config
from utils.logger import logger


class SignalScanner:
    def __init__(self, notifier):
        self.db = get_db()
        self.notifier = notifier
        monitor_cfg = load_config().get("monitor", {}) or {}
        self.price_change_threshold = float(monitor_cfg.get("price_change_threshold", 5.0))

    def _collect_signals(self, code: str, name: str) -> List[str]:
        """拉最新日线，收集最新交易日的信号（含涨跌幅阈值）"""
        from tools.stock_tools import stock_tool_instance, _ensure_indicators

        df = stock_tool_instance.fetch_and_save_stock_daily_data(code)
        if df is None or df.empty:
            logger.warning(f"[监控] {name}({code}) 无日线数据，跳过")
            return []
        df = _ensure_indicators(df, "daily")
        latest = df.iloc[0]
        latest_date = parse_row_date(latest.get("date"))

        parts = []
        pct = latest.get("pct_chg")
        try:
            if pct is not None and abs(float(pct)) >= self.price_change_threshold:
                parts.append(f"{'涨' if float(pct) > 0 else '跌'}幅 {float(pct):.2f}%")
        except (TypeError, ValueError):
            pass
        if latest.get("macd_signal") == 1:
            parts.append("MACD金叉")
        elif latest.get("macd_signal") == -1:
            parts.append("MACD死叉")
        for col in ("ma_cross", "vol_signal", "gap_signal"):
            v = latest.get(col)
            if v and isinstance(v, str) and v.strip():
                parts.append(v.strip())

        if not parts:
            return []
        return [f"{name}({code}) {latest_date}: {'、'.join(parts)}"]

    def scan(self) -> None:
        """扫描全部监控个股，汇总成一条消息推送（去重后）"""
        targets = [t for t in self.db.get_watch_targets() if t["target_type"] == "company" and t.get("code")]
        if not targets:
            logger.info("[监控] 监控清单为空，跳过盘后信号扫描")
            return

        logger.info(f"[监控] 盘后信号扫描开始，共 {len(targets)} 只")
        lines: List[str] = []
        for t in targets:
            try:
                for line in self._collect_signals(t["code"], t["name"]):
                    dedup_key = f"signal:{t['code']}:{date.today()}"
                    if self.db.monitor_event_exists(dedup_key):
                        continue
                    if self.db.save_monitor_event(
                            target=t["name"], event_type="signal", dedup_key=dedup_key,
                            title=line, importance="中", pushed=True):
                        lines.append(line)
            except Exception as e:
                logger.error(f"[监控] 扫描 {t['name']} 信号失败: {e}")

        if lines:
            text = "📊 盘后技术信号提醒\n" + "\n".join(f"· {ln}" for ln in lines)
            self.notifier.send(text)
            logger.info(f"[监控] 盘后信号推送 {len(lines)} 条")
        else:
            logger.info("[监控] 今日无新信号")
