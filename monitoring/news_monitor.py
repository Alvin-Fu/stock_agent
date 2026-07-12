# -*- coding: utf-8 -*-
"""
新闻/政策监控：
- 个股（company）：akshare 东财个股新闻接口增量拉取（结构化、带时间戳，比搜索引擎稳）
- 行业（industry）：联网搜索"行业+政策/新闻"，覆盖宏观政策面
新增条目先按 dedup_key（链接/标题哈希）去重，再交 LLM 批量评估重要性，
「中」以上推送，受每日推送上限约束。
"""

import hashlib
import json
import re
from datetime import date
from typing import List, Dict, Any

from storage.sqlite.stock_storage import get_db
from utils.config import load_config
from utils.logger import logger


class NewsMonitor:
    def __init__(self, notifier):
        self.db = get_db()
        self.notifier = notifier
        monitor_cfg = load_config().get("monitor", {}) or {}
        self.daily_push_limit = int(monitor_cfg.get("daily_push_limit", 20))
        self.importance_order = {"高": 3, "中": 2, "低": 1}
        self.min_importance = self.importance_order.get(
            str(monitor_cfg.get("news_importance_min", "中")), 2)
        self._llm = None

    def _get_llm(self):
        if self._llm is None:
            from core.llm import get_default_llm
            self._llm = get_default_llm()
        return self._llm

    # ---------- 数据源（统一走 tools.info_sources） ----------

    @staticmethod
    def _fetch_company_news(code: str, name: str) -> List[Dict[str, str]]:
        """东财个股新闻（结构化主源）；失败/为空时退财联社快讯按公司名过滤"""
        from tools.info_sources import fetch_stock_news, fetch_cls_telegraph
        items = fetch_stock_news(code, limit=20)
        if items:
            return items
        if not name:
            return []
        logger.info(f"[监控] {name}({code}) 东财新闻无数据，改用财联社快讯按名称过滤")
        # 快讯没有链接，去重键回落到标题+时间（_dedup_key 已处理）
        return [{**it, "url": ""} for it in fetch_cls_telegraph(keywords=[name], limit=10)]

    @staticmethod
    def _fetch_industry_news(name: str, keywords: str) -> List[Dict[str, str]]:
        """行业/宏观政策：财联社快讯按关键词过滤（主源）；无命中时联网搜索兜底"""
        from tools.info_sources import fetch_cls_telegraph
        kws = [name] + [k.strip() for k in (keywords or "").split() if k.strip()]
        items = fetch_cls_telegraph(keywords=kws, limit=10)
        if items:
            # 快讯没有链接，用标题+时间做去重键（fetch 层已带 time/title）
            return [{**it, "url": ""} for it in items]

        # 兜底：联网搜索（整体作为一条候选，按天去重）
        from agents.researcher.web_search_tool import web_search
        query = f"{name} {keywords or ''} 政策 新闻 最新 {date.today().strftime('%Y年%m月')}".strip()
        result = web_search.invoke({"query": query})
        text = str(result)
        if not text or text.startswith("搜索失败"):
            return []
        return [{
            "title": f"{name} 行业动态/政策（{date.today()}）",
            "content": text[:1500],
            "url": "",
            "time": str(date.today()),
        }]

    # ---------- 去重 + LLM 评估 ----------

    @staticmethod
    def _dedup_key(target: str, item: Dict[str, str]) -> str:
        raw = item.get("url") or (item.get("title", "") + item.get("time", ""))
        return "news:" + hashlib.md5(f"{target}|{raw}".encode("utf-8")).hexdigest()

    def _evaluate_batch(self, target_name: str, items: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """LLM 批量评估：重要性 + 一句话摘要。解析失败时保守处理（全部按低，不推送）"""
        numbered = "\n".join(
            f"{i+1}. [{it.get('time','')}] {it.get('title','')}\n   {it.get('content','')[:200]}"
            for i, it in enumerate(items)
        )
        prompt = f"""你是财经新闻筛选助手。以下是关于「{target_name}」的新增新闻，请逐条评估。

重要性标准：
- 高：重大公告（业绩预告/重组/停复牌/监管处罚/实控人变更）、直接影响业务的政策、重大订单或事故
- 中：经营数据、行业政策、机构动向、产品/技术进展
- 低：日常波动解读、重复报道、与该标的关系弱的泛行业稿、营销软文

只输出JSON数组（不要markdown包裹）：
[{{"index": 1, "importance": "高/中/低", "summary": "一句话摘要（30字内）"}}, ...]

新闻列表：
{numbered}"""
        try:
            response = self._get_llm().invoke(prompt)
            raw = response.content if hasattr(response, "content") else str(response)
            match = re.search(r"\[.*\]", raw, re.DOTALL)
            if match:
                evaluated = json.loads(match.group(0))
                results = []
                for ev in evaluated:
                    idx = int(ev.get("index", 0)) - 1
                    if 0 <= idx < len(items):
                        results.append({
                            **items[idx],
                            "importance": str(ev.get("importance", "低")),
                            "summary": str(ev.get("summary", ""))[:60],
                        })
                return results
        except Exception as e:
            logger.error(f"[监控] LLM 新闻评估失败: {e}")
        # 保守：评估失败不推送（避免噪音轰炸），只记录
        return [{**it, "importance": "低", "summary": ""} for it in items]

    # ---------- 主流程 ----------

    def scan(self) -> None:
        targets = self.db.get_watch_targets()
        if not targets:
            return
        logger.info(f"[监控] 新闻扫描开始，共 {len(targets)} 个标的")

        pushed_before = self.db.count_events_pushed_today()
        for t in targets:
            try:
                self._scan_one(t)
            except Exception as e:
                logger.error(f"[监控] {t['name']} 新闻扫描失败: {e}")
        logger.info(f"[监控] 新闻扫描完成：{len(targets)} 个标的，"
                    f"本轮推送 {self.db.count_events_pushed_today() - pushed_before} 条")

    def _scan_one(self, target: Dict[str, Any]) -> None:
        name = target["name"]
        if target["target_type"] == "company" and target.get("code"):
            items = self._fetch_company_news(target["code"], name)
        else:
            items = self._fetch_industry_news(name, target.get("keywords") or "")

        # 先去重，只评估新增
        fresh = []
        for it in items:
            if not it.get("title"):
                continue
            if not self.db.monitor_event_exists(self._dedup_key(name, it)):
                fresh.append(it)
        if not fresh:
            return
        logger.info(f"[监控] {name} 新增 {len(fresh)} 条新闻，进入评估")

        evaluated = self._evaluate_batch(name, fresh)
        for ev in evaluated:
            key = self._dedup_key(name, ev)
            score = self.importance_order.get(ev.get("importance", "低"), 1)
            should_push = score >= self.min_importance \
                and self.db.count_events_pushed_today() < self.daily_push_limit
            saved = self.db.save_monitor_event(
                target=name, event_type="news", dedup_key=key,
                title=ev.get("title"), content=ev.get("summary") or ev.get("content", "")[:300],
                importance=ev.get("importance"), pushed=should_push,
            )
            if saved and should_push:
                icon = "🔴" if ev.get("importance") == "高" else "🟡"
                text = (f"{icon} 监控提醒 | {name}\n"
                        f"{ev.get('summary') or ev.get('title')}\n"
                        f"标题：{ev.get('title')}\n"
                        f"时间：{ev.get('time')}"
                        + (f"\n链接：{ev.get('url')}" if ev.get("url") else ""))
                self.notifier.send(text)
