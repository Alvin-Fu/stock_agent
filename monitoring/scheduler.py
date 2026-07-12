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

    def _run_news_scan(self):
        hour = datetime.now().hour
        if not (8 <= hour <= 22):
            return
        try:
            self.news_monitor.scan()
        except Exception as e:
            logger.error(f"[监控] 新闻扫描异常: {e}")

    # ---------- 生命周期 ----------

    def start(self):
        if not self.enabled:
            logger.info("[监控] monitor.enabled=false，监控调度未启动")
            return
        if self._running:
            return
        self._schedule.every().day.at(self.signal_scan_time).do(self._run_signal_scan)
        self._schedule.every(self.news_interval).minutes.do(self._run_news_scan)

        self._running = True
        self._thread = threading.Thread(target=self._loop, name="monitor-scheduler", daemon=True)
        self._thread.start()
        logger.info(f"[监控] 调度已启动：盘后信号 {self.signal_scan_time}，新闻每 {self.news_interval} 分钟")

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

    def run_once_now(self) -> str:
        """立即跑一轮信号+新闻扫描（同步），返回摘要"""
        self._run_signal_scan()
        self._run_news_scan()
        return "已完成一轮监控扫描（盘后信号+新闻）"
