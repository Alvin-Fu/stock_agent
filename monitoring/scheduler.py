# -*- coding: utf-8 -*-
"""
监控调度器：独立 daemon 线程跑 schedule 循环，与主对话/分析流程互不阻塞。
- 盘后信号扫描：每个交易日固定时间（默认 15:10）
- 新闻/政策扫描：固定间隔（默认 30 分钟），仅在白天时段（8:00-22:00）执行
"""

import threading
import time
from datetime import date, datetime

import schedule

from utils.config import load_config
from utils.logger import logger

from .notifier import FeishuNotifier
from .signal_scanner import SignalScanner
from .news_monitor import NewsMonitor
from .condition_watcher import ConditionWatcher
from .review import ReviewRunner


def _is_weekday() -> bool:
    return date.today().weekday() < 5


class MonitorScheduler:
    def __init__(self, notifier: FeishuNotifier = None):
        cfg = load_config().get("monitor", {}) or {}
        self.enabled = bool(cfg.get("enabled", True))
        self.signal_scan_time = str(cfg.get("signal_scan_time", "15:10"))
        self.news_interval = int(cfg.get("news_interval_minutes", 30))

        self.notifier = notifier or FeishuNotifier()
        self.signal_scanner = SignalScanner(self.notifier)
        self.news_monitor = NewsMonitor(self.notifier)
        self.condition_watcher = ConditionWatcher(self.notifier)
        self.review_runner = ReviewRunner(self.notifier)
        self.review_after_days = int(cfg.get("review_after_days", 5))
        self.industry_review_after_days = int(cfg.get("industry_review_after_days", 10))
        self.review_time = str(cfg.get("review_time", "15:40"))
        self.pre_market_time = str(cfg.get("pre_market_time", "09:00"))
        self.post_market_time = str(cfg.get("post_market_time", "19:30"))
        self.golden_enabled = bool(cfg.get("golden_enabled", True))
        self.golden_day = str(cfg.get("golden_day", "saturday")).lower()
        self.golden_time = str(cfg.get("golden_time", "09:00"))
        self._golden_running = False

        # 周六早期信号巡逻
        self.scout_enabled = bool(cfg.get("scout_enabled", True))
        self.scout_day = str(cfg.get("scout_day", "saturday")).lower()
        self.scout_time = str(cfg.get("scout_time", "08:00"))
        self._scout_running = False

        self._running = False
        self._thread = None
        # 用独立的 Scheduler 实例，避免与 tasks/scheduled_analyzer 的全局 schedule 相互干扰
        self._schedule = schedule.Scheduler()

    # ---------- 任务包装（异常不打断调度循环） ----------

    def _run_signal_scan(self):
        if not _is_weekday():
            return
        try:
            self.signal_scanner.scan()
        except Exception as e:
            logger.error(f"[监控] 盘后信号扫描异常: {e}")
        # 条件触发盯盘：紧随信号扫描（日线刚更新完），对照快照里的操作参考
        try:
            self.condition_watcher.scan()
        except Exception as e:
            logger.error(f"[监控] 条件触发盯盘异常: {e}")
        # 业务指标预警扫描（北向持仓/存货周转/渗透率）
        try:
            from monitoring.business_watcher import scan_all as scan_business
            biz_alerts = scan_business()
            if biz_alerts:
                logger.info(f"业务指标扫描完成，{len(biz_alerts)} 条预警")
        except Exception as e:
            logger.error(f"业务指标扫描失败: {e}")

        # 产业链候选池监控
        try:
            from monitoring.industry_watcher import scan_all as scan_industry
            industry_alerts = scan_industry()
            if industry_alerts:
                logger.info(f"产业链监控扫描完成，{len(industry_alerts)} 条预警")
        except Exception as e:
            logger.error(f"产业链监控扫描失败: {e}")

        # 低位价值发现扫描
        try:
            from tools.stock_tools import call_fetch_value_discovery
            result = call_fetch_value_discovery()
            if result and "❌" not in result:
                # 推送简短摘要到飞书
                lines = result.strip().split("\n")
                summary_lines = [l for l in lines if l.startswith("|")]
                summary = "\n".join(summary_lines[:15]) if summary_lines else result[:500]
                from monitoring.notifier import FeishuNotifier
                notifier = FeishuNotifier()
                notifier.send(f"📊 **低位价值发现扫描**\n\n{summary[:2000]}")
                logger.info("低位价值发现扫描完成")
        except Exception as e:
            logger.error(f"低位价值发现扫描失败: {e}")
        # 大盘估值快照
        try:
            val_text = self._fetch_market_valuation()
            if val_text:
                self.notifier.send(f"📈 **大盘估值快照**\n\n{val_text}")
        except Exception as e:
            logger.debug(f"大盘估值快照跳过: {e}")

    def _run_news_scan(self):
        hour = datetime.now().hour
        if not (8 <= hour <= 22):
            return
        try:
            self.news_monitor.scan()
        except Exception as e:
            logger.error(f"[监控] 新闻扫描异常: {e}")

    def _run_reviews(self):
        if not _is_weekday():
            return
        try:
            self.review_runner.run_due_reviews(self.review_after_days, self.industry_review_after_days)
        except Exception as e:
            logger.error(f"[复盘] 定时复盘异常: {e}")

    def _run_macro_analysis(self, session: str = "pre"):
        """拉取大盘宏观数据快照并推送飞书"""
        if not _is_weekday():
            return
        label = "开盘前" if session == "pre" else "收盘后"
        try:
            from .macro_watcher import fetch_macro_snapshot
            text = fetch_macro_snapshot(session=session)
            self.notifier.send(f"🏛 **大盘宏观数据快照（{label}）**\n\n{text[:6000]}")
            logger.info(f"[宏观] {label}宏观分析推送完成")
        except Exception as e:
            logger.error(f"[宏观] {label}宏观分析异常: {e}")

    @staticmethod
    def _fetch_market_valuation() -> str:
        """获取大盘估值快照文本（上证50/中证500 PE/PB + 沪深300 K线均线），用于盘后推送"""
        from tools.market_context import _fetch_valuation_text, _fetch_index_kline_text
        parts = []
        val = _fetch_valuation_text()
        if val:
            parts.append(val)
        kline = _fetch_index_kline_text()
        if kline:
            parts.append(f"沪深300K线:\n{kline}")
        return "\n\n".join(parts) if parts else ""

    def _run_early_scout(self):
        """周六早期信号巡逻：扫描热门行业 → 选低位企稳的 → 自动深度分析 → 保存触发条件"""
        if self._scout_running:
            logger.warning("[EarlyScout] 上一轮巡逻尚未结束，本次跳过")
            return

        def job():
            self._scout_running = True
            try:
                logger.info("[EarlyScout] 周六早期信号巡逻开始...")
                from .early_scout import run_early_scout
                report = run_early_scout()
                logger.info(f"[EarlyScout] 巡逻完成，报告长度 {len(report)} 字符")
            except Exception as e:
                logger.error(f"[EarlyScout] 巡逻异常: {e}\n{traceback.format_exc()}")
                self.notifier.send(f"【早期信号巡逻】运行失败: {e}")
            finally:
                self._scout_running = False

        threading.Thread(target=job, name="early-scout", daemon=True).start()

    def _run_golden(self):
        """周度 golden 回归：改 prompt/规则后的裸奔检测例行化。
        跑一轮要几小时且不能阻塞调度循环 → 独立线程；防重入"""
        if self._golden_running:
            logger.warning("[Golden] 上一轮回归尚未结束，本次跳过")
            return

        def job():
            self._golden_running = True
            try:
                logger.info("[Golden] 周度回归开始（预计数十分钟到数小时）")
                from eval.golden_run import run as golden_run
                summary = golden_run()
                self.notifier.send("【Golden 周回归】\n" + summary[:3500])
            except Exception as e:
                logger.error(f"[Golden] 周度回归失败: {e}")
                self.notifier.send(f"【Golden 周回归】运行失败: {e}")
            finally:
                self._golden_running = False

        threading.Thread(target=job, name="golden-weekly", daemon=True).start()

    def _run_backup(self):
        """每日备份主库（快照/复盘/监控历史都在里面），保留最近 7 份"""
        try:
            import glob
            import os
            import sqlite3
            from utils.config import load_config
            raw = str((load_config().get("database") or {}).get("sqlite_path", "./data/sqlite/stock.db"))
            path = raw.replace("sqlite:///", "")
            if not os.path.exists(path):
                return
            bdir = os.path.join(os.path.dirname(path) or ".", "backup")
            os.makedirs(bdir, exist_ok=True)
            dest = os.path.join(bdir, f"stock-{date.today().strftime('%Y%m%d')}.db")
            src = sqlite3.connect(path)
            dst = sqlite3.connect(dest)
            with dst:
                src.backup(dst)  # sqlite 在线备份 API，写入中也能安全拷贝
            src.close()
            dst.close()
            for old in sorted(glob.glob(os.path.join(bdir, "stock-*.db")))[:-7]:
                os.remove(old)
            logger.info(f"[备份] 数据库已备份: {dest}（保留最近7份）")
        except Exception as e:
            logger.error(f"[备份] 数据库备份失败: {e}")

    # ---------- 生命周期 ----------

    def start(self):
        if not self.enabled:
            logger.info("[监控] monitor.enabled=false，监控调度未启动")
            return
        if self._running:
            return
        self._schedule.every().day.at(self.signal_scan_time).do(self._run_signal_scan)
        self._schedule.every(self.news_interval).minutes.do(self._run_news_scan)
        self._schedule.every().day.at(self.review_time).do(self._run_reviews)
        self._schedule.every().day.at(self.pre_market_time).do(self._run_macro_analysis, session="pre")
        self._schedule.every().day.at(self.post_market_time).do(self._run_macro_analysis, session="post")
        self._schedule.every().day.at("22:30").do(self._run_backup)
        if self.golden_enabled:
            day_job = getattr(self._schedule.every(), self.golden_day, None)
            if day_job is None:
                logger.warning(f"[Golden] golden_day 配置无效（{self.golden_day}），回退 saturday")
                day_job = self._schedule.every().saturday
            day_job.at(self.golden_time).do(self._run_golden)

        if self.scout_enabled:
            day_job = getattr(self._schedule.every(), self.scout_day, None)
            if day_job is None:
                logger.warning(f"[Scout] scout_day 配置无效（{self.scout_day}），回退 saturday")
                day_job = self._schedule.every().saturday
            day_job.at(self.scout_time).do(self._run_early_scout)

        self._running = True
        self._thread = threading.Thread(target=self._loop, name="monitor-scheduler", daemon=True)
        self._thread.start()
        logger.info(f"[监控] 调度已启动：盘后信号 {self.signal_scan_time}，新闻每 {self.news_interval} 分钟，"
                    f"复盘 {self.review_time}（分析满 {self.review_after_days} 天）"
                    + (f"，golden 回归每周 {self.golden_day} {self.golden_time}" if self.golden_enabled else "")
                    + (f"，早期信号巡逻每周 {self.scout_day} {self.scout_time}" if self.scout_enabled else ""))

    def _loop(self):
        while self._running:
            try:
                self._schedule.run_pending()
            except Exception as e:
                logger.error(f"[监控] 调度循环异常: {e}")
            time.sleep(5)

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("[监控] 调度已停止")

    # ---------- 手动触发（对话命令/调试用） ----------

    def set_analysis_runner(self, runner) -> None:
        """注入完整分析的执行器（callable(question)），财报发布触发自动重分析用"""
        self.news_monitor.analysis_runner = runner

    def run_once_now(self) -> str:
        """立即跑一轮信号+新闻扫描（同步），返回摘要"""
        self._run_signal_scan()
        self._run_news_scan()
        return "已完成一轮监控扫描（盘后信号+条件盯盘+新闻）"
