# -*- coding: utf-8 -*-
"""
盘后信号扫描：对监控清单里的个股跑日线管线，收集程序判定的技术信号并推送。
信号来源就是数据层的信号列（ma_cross/vol_signal/gap_signal/macd_signal），零额外计算。
另增加资金维度信号：北向资金当日净买入额、主力资金净流入（moneyflow）。
增加基本面维度信号：业绩预告类型（预增/预减/扭亏/续亏）、财报即将披露提醒、营收/利润增速大幅变动。
信号按维度分类标注 [技术]/[资金]/[基本面]，按类型设置 importance（资金大量流入=高、基本面重大=高、技术信号=中）。
"""

from datetime import date, timedelta
from typing import List, Dict

import pandas as pd

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
        # 资金信号阈值（单位：元）；超阈值才触发 [资金] 信号
        self.north_bound_net_buy_threshold = float(monitor_cfg.get("north_bound_net_buy_threshold", 5e7))
        self.main_force_net_inflow_threshold = float(monitor_cfg.get("main_force_net_inflow_threshold", 5e7))

    def _collect_signals(self, code: str, name: str) -> List[Dict[str, str]]:
        """拉最新日线，收集最新交易日的技术信号与资金信号。
        返回 [{"text": 信号文本, "importance": 高/中, "type": 技术/资金}]，
        信号按 [技术]/[资金] 分类标注。"""
        from tools.stock_tools import stock_tool_instance, _ensure_indicators

        df = stock_tool_instance.fetch_and_save_stock_daily_data(code)
        if df is None or df.empty:
            logger.warning(f"[监控] {name}({code}) 无日线数据，跳过")
            return []
        df = _ensure_indicators(df, "daily")
        latest = df.iloc[0]
        latest_date = parse_row_date(latest.get("date"))

        # 最新收盘价（资金信号金额计算用）
        close = None
        try:
            close = float(latest.get("close", 0) or 0) or None
        except (TypeError, ValueError):
            close = None

        # ---- [技术] 信号 ----
        tech_parts = []
        pct = latest.get("pct_chg")
        try:
            if pct is not None and abs(float(pct)) >= self.price_change_threshold:
                tech_parts.append(f"{'涨' if float(pct) > 0 else '跌'}幅 {float(pct):.2f}%")
        except (TypeError, ValueError):
            pass
        if latest.get("macd_signal") == 1:
            tech_parts.append("MACD金叉")
        elif latest.get("macd_signal") == -1:
            tech_parts.append("MACD死叉")
        for col in ("ma_cross", "vol_signal", "gap_signal"):
            v = latest.get(col)
            if v and isinstance(v, str) and v.strip():
                tech_parts.append(v.strip())

        # ---- [资金] 信号 ----
        cap_parts = []
        cap_high = False  # 是否存在大量资金流入（决定 importance=高）

        # 资金信号1：主力资金净流入（moneyflow）
        # ★ net_mf_amount 字段不准确（与 buy/sell 列对不上，差约12倍），
        #   统一按 主力=大单+超大单 的口径重新计算（与 industry_metrics 一致）。
        #   DB 中大单/超大单金额单位为万元，转为元后与阈值（单位：元）比较。
        try:
            from tools.industry_metrics import compute_main_force_net
            mf_df = self.db.get_stock_moneyflow(code, limit=1)
            if mf_df is not None and not mf_df.empty:
                net_series = compute_main_force_net(mf_df)
                if net_series is not None:
                    # 万元 → 元，与 main_force_net_inflow_threshold（单位：元）对齐
                    net_mf = float(net_series.iloc[0]) * 1e4
                else:
                    net_mf = 0.0
                if net_mf >= self.main_force_net_inflow_threshold:
                    cap_parts.append(f"主力资金净流入{net_mf / 1e8:.2f}亿")
                    cap_high = True  # 主力大量流入 → 高
                elif net_mf <= -self.main_force_net_inflow_threshold:
                    cap_parts.append(f"主力资金净流出{abs(net_mf) / 1e8:.2f}亿")
        except Exception as e:
            logger.debug(f"[监控] {name}({code}) 主力资金流获取失败: {e}")

        # 资金信号2：北向资金当日净买入额（用 stock_capital_fetcher + 库内历史算日变动）
        try:
            from tools.stock_capital_fetcher import fetch_north_bound_holdings
            nb = fetch_north_bound_holdings([code])
            if nb and code in nb:
                latest_vol = float(nb[code].get("shares", 0) or 0)
                prev_vol = 0.0
                hist = self.db.get_stock_northbound_hold(code)
                if hist is not None and not hist.empty and "vol" in hist.columns:
                    hist = hist.sort_values("trade_date", ascending=False)
                    if len(hist) >= 2:
                        prev_vol = float(hist.iloc[1].get("vol", 0) or 0)
                if latest_vol and prev_vol and close:
                    net_buy_amount = (latest_vol - prev_vol) * close
                    if net_buy_amount >= self.north_bound_net_buy_threshold:
                        cap_parts.append(f"北向资金净买入{net_buy_amount / 1e8:.2f}亿")
                        cap_high = True  # 北向大量流入 → 高
                    elif net_buy_amount <= -self.north_bound_net_buy_threshold:
                        cap_parts.append(f"北向资金净卖出{abs(net_buy_amount) / 1e8:.2f}亿")
        except Exception as e:
            logger.debug(f"[监控] {name}({code}) 北向资金获取失败: {e}")

        # ---- [基本面] 信号 ----
        fund_parts = []
        fund_high = False  # 业绩预增/扭亏/增速大幅变动 → 高
        fund_parts, fund_high = self._collect_fundamental_signals(code, name, latest_date)

        # 组装信号（按维度分类标注 [技术]/[资金]/[基本面]）
        results: List[Dict[str, str]] = []
        if tech_parts:
            results.append({
                "text": f"{name}({code}) {latest_date} [技术]: {'、'.join(tech_parts)}",
                "importance": "中",
                "type": "技术",
            })
        if cap_parts:
            results.append({
                "text": f"{name}({code}) {latest_date} [资金]: {'、'.join(cap_parts)}",
                "importance": "高" if cap_high else "中",
                "type": "资金",
            })
        if fund_parts:
            results.append({
                "text": f"{name}({code}) {latest_date} [基本面]: {'、'.join(fund_parts)}",
                "importance": "高" if fund_high else "中",
                "type": "基本面",
            })
        return results

    def _collect_fundamental_signals(self, code: str, name: str, signal_date) -> tuple:
        """收集基本面维度信号：业绩预告/财报披露/增速大幅变动。
        返回 (parts: List[str], is_high: bool)。"""
        parts = []
        is_high = False
        today = date.today()

        # 1) 业绩预告（StockFinaAudit）：检查最近7天内的业绩预告
        try:
            audit_df = self.db.get_stock_fina_audit(code, limit=5)
            if audit_df is not None and not audit_df.empty:
                for _, row in audit_df.iterrows():
                    end_date = str(row.get("end_date", ""))[:10]
                    forecast_type = str(row.get("forecast_type", "") or row.get("type", "") or "").strip()
                    profit_range = str(row.get("profit_range", "") or row.get("forecast_content", "") or "").strip()
                    # 判断预告类型的重要性
                    if forecast_type:
                        high_keywords = ("预增", "扭亏", "续盈")
                        low_keywords = ("预减", "续亏", "首亏")
                        if any(kw in forecast_type for kw in high_keywords):
                            desc = f"业绩预告{forecast_type}（报告期{end_date}）"
                            if profit_range:
                                desc += f" 净利润区间:{profit_range[:40]}"
                            parts.append(desc)
                            is_high = True
                        elif any(kw in forecast_type for kw in low_keywords):
                            desc = f"业绩预告{forecast_type}（报告期{end_date}）"
                            if profit_range:
                                desc += f" 净利润区间:{profit_range[:40]}"
                            parts.append(desc)
                            is_high = True
                        break  # 只取最近一条
        except Exception as e:
            logger.debug(f"[监控] {name}({code}) 业绩预告获取失败: {e}")

        # 2) 财报即将披露提醒：检查未来7天内是否有预约披露
        try:
            disc_df = self.db.get_stock_disclosure_date(code, limit=10)
            if disc_df is not None and not disc_df.empty:
                for _, row in disc_df.iterrows():
                    actual_diss = row.get("actual_diss_date")
                    if actual_diss is None:
                        continue
                    try:
                        diss_date = pd.Timestamp(actual_diss).date() if not isinstance(actual_diss, date) else actual_diss
                    except Exception:
                        continue
                    end_date = str(row.get("end_date", ""))[:10]
                    # 未来7天内即将披露
                    days_until = (diss_date - today).days
                    if 0 <= days_until <= 7:
                        parts.append(f"财报将于{diss_date}披露（报告期{end_date}，距今{days_until}天）")
                        is_high = True
                        break
        except Exception as e:
            logger.debug(f"[监控] {name}({code}) 财报披露日期获取失败: {e}")

        # 3) 营收/利润增速大幅变动：对比最近两期财务指标
        try:
            fina_df = self.db.get_stock_fina_indicator(code)
            if fina_df is not None and not fina_df.empty and len(fina_df) >= 2:
                latest_fina = fina_df.iloc[0]
                prev_fina = fina_df.iloc[1]
                # 营收增速变动
                rev_growth = latest_fina.get("revenue_growth")
                prev_rev_growth = prev_fina.get("revenue_growth")
                if rev_growth is not None and prev_rev_growth is not None:
                    try:
                        rev_chg = float(rev_growth) - float(prev_rev_growth)
                        if abs(rev_chg) >= 20:
                            direction = "加速" if rev_chg > 0 else "放缓"
                            parts.append(f"营收增速{direction}{abs(rev_chg):.1f}pct"
                                         f"（{float(prev_rev_growth):.1f}%→{float(rev_growth):.1f}%）")
                            if rev_chg > 0:
                                is_high = True
                    except (TypeError, ValueError):
                        pass
                # 净利润增速变动
                np_growth = latest_fina.get("netprofit_growth")
                prev_np_growth = prev_fina.get("netprofit_growth")
                if np_growth is not None and prev_np_growth is not None:
                    try:
                        np_chg = float(np_growth) - float(prev_np_growth)
                        if abs(np_chg) >= 30:
                            direction = "加速" if np_chg > 0 else "放缓"
                            parts.append(f"净利增速{direction}{abs(np_chg):.1f}pct"
                                         f"（{float(prev_np_growth):.1f}%→{float(np_growth):.1f}%）")
                            if np_chg > 0:
                                is_high = True
                    except (TypeError, ValueError):
                        pass
        except Exception as e:
            logger.debug(f"[监控] {name}({code}) 财务指标获取失败: {e}")

        return parts, is_high

    def scan(self) -> None:
        """扫描全部监控个股，汇总成一条消息推送（去重后）"""
        targets = [t for t in self.db.get_watch_targets() if t["target_type"] == "company" and t.get("code")]
        if not targets:
            logger.info("[监控] 监控清单为空，跳过盘后信号扫描")
            return

        logger.info(f"[监控] 盘后信号扫描开始，共 {len(targets)} 只")

        # 1) 并行收集所有股票的信号（每只股票的 _collect_signals 相互独立，可完全并行）
        import concurrent.futures
        all_signals: List[tuple] = []  # [(code, name, sig), ...]

        def _collect_one(target):
            try:
                return self._collect_signals(target["code"], target["name"])
            except Exception as e:
                logger.error(f"[监控] 扫描 {target['name']} 信号失败: {e}")
                return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_target = {executor.submit(_collect_one, t): t for t in targets}
            for future in concurrent.futures.as_completed(future_to_target):
                t = future_to_target[future]
                try:
                    for sig in future.result():
                        all_signals.append((t["code"], t["name"], sig))
                except Exception as e:
                    logger.error(f"[监控] 扫描 {t['name']} 信号失败: {e}")

        # 2) 串行去重检查和保存（SQLite 同一连接非线程安全，DB 写操作不能并行）
        lines: List[str] = []
        for code, name, sig in all_signals:
            # 按标的+日期+信号类型去重（技术/资金分别记录）
            dedup_key = f"signal:{code}:{date.today()}:{sig['type']}"
            if self.db.monitor_event_exists(dedup_key):
                continue
            if self.db.save_monitor_event(
                    target=name, event_type="signal", dedup_key=dedup_key,
                    title=sig["text"], importance=sig["importance"], pushed=True):
                lines.append(sig["text"])

        # 3) 汇总推送
        if lines:
            text = "📊 盘后信号提醒\n" + "\n".join(f"· {ln}" for ln in lines)
            self.notifier.send(text)
            logger.info(f"[监控] 盘后信号推送 {len(lines)} 条")
        else:
            logger.info("[监控] 今日无新信号")
