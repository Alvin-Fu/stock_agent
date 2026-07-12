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
        self._schedule.every().day.at("22:30").do(self._run_backup)

        self._running = True
        self._thread = threading.Thread(target=self._loop, name="monitor-scheduler", daemon=True)
        self._thread.start()
        logger.info(f"[监控] 调度已启动：盘后信号 {self.signal_scan_time}，新闻每 {self.news_interval} 分钟，"
                    f"复盘 {self.review_time}（分析满 {self.review_after_days} 天）")

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
