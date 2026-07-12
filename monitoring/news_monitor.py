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
        # 财报发布自动触发：由入口（feishu_bot）注入 callable(question)，未注入则只推提醒不重分析
        self.analysis_runner = None

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

    def _check_reeval_triggers(self, industry: str, items: List[Dict[str, str]]) -> None:
        """
        对照行业新闻核对 news 型重估触发条件（valuation 型由 condition_watcher 程序判定）。
        命中 → 标记 hit + 推送提醒 +（注入了 analysis_runner 时）自动触发产业链重分析。
        """
        triggers = [t for t in self.db.get_active_industry_triggers(industry)
                    if t.get("trigger_type") == "news"]
        if not triggers or not items:
            return

        trig_lines = "\n".join(f"{t['id']}. {t['description']}" for t in triggers)
        news_lines = "\n".join(
            f"{i+1}. [{it.get('time','')}] {it.get('title','')}\n   {it.get('content','')[:200]}"
            for i, it in enumerate(items[:15])
        )
        prompt = f"""以下是行业「{industry}」此前分析留下的重估触发条件，和该行业的最新新闻。
请逐条判断触发条件是否被新闻**明确证实发生**（仅传闻/计划/预期不算命中）。

触发条件（编号=条件ID）：
{trig_lines}

最新新闻：
{news_lines}

只输出JSON数组（不要markdown包裹），只包含命中的条件：
[{{"trigger_id": 条件ID, "evidence": "命中依据（引用新闻标题，40字内）"}}, ...]
没有命中输出 []。宁可漏判不可误判。"""
        try:
            response = self._get_llm().invoke(prompt)
            raw = response.content if hasattr(response, "content") else str(response)
            match = re.search(r"\[.*\]", raw, re.DOTALL)
            hits = json.loads(match.group(0)) if match else []
        except Exception as e:
            logger.error(f"[监控] {industry} 触发条件 LLM 判定失败: {e}")
            return

        valid_ids = {t["id"]: t for t in triggers}
        for h in hits or []:
            try:
                tid = int(h.get("trigger_id"))
            except (TypeError, ValueError):
                continue
            trig = valid_ids.get(tid)
            if not trig:
                continue
            evidence = str(h.get("evidence") or "")[:200]
            # mark 内部只对 active 生效，天然防重复推送
            if not self.db.mark_industry_trigger_hit(tid, evidence):
                continue
            self.db.save_monitor_event(
                target=industry, event_type="reeval",
                dedup_key=f"reeval:{tid}",
                title=f"重估触发：{trig['description']}",
                content=evidence, importance="高", pushed=True,
            )
            self.notifier.send(f"🔁 行业重估触发 | {industry}\n"
                               f"条件：{trig['description']}\n依据：{evidence}\n"
                               + ("已自动触发产业链重新分析" if self.analysis_runner
                                  else f"可发送「分析{industry}产业链」重新评估"))
            logger.info(f"[监控] {industry} 重估触发条件命中 #{tid}: {evidence}")
            if self.analysis_runner:
                try:
                    self.analysis_runner(f"分析{industry}产业链上下游，筛选出所有关键公司，"
                                         f"对比技术面和基本面，选出最值得投资的股票（重估触发：{trig['description']}）")
                except Exception as e:
                    logger.error(f"[监控] {industry} 重估触发重分析失败: {e}")

    # 定期报告/业绩类公告标题特征（排除摘要与取消类）
    _REPORT_TITLE = re.compile(r"(年度报告|半年度报告|季度报告|业绩快报|业绩预告)")
    _REPORT_EXCLUDE = re.compile(r"(摘要|英文|已取消|提示性公告|披露时间)")

    def _check_report_release(self, code: str, name: str) -> None:
        """监控标的发布定期报告/业绩公告 → 推送提醒 + 自动触发完整重分析"""
        from tools.info_sources import fetch_stock_announcements
        for it in fetch_stock_announcements(code, days=5, limit=10):
            title = it.get("title", "")
            if not self._REPORT_TITLE.search(title) or self._REPORT_EXCLUDE.search(title):
                continue
            key = "report:" + hashlib.md5(f"{code}|{title}".encode("utf-8")).hexdigest()
            saved = self.db.save_monitor_event(
                target=name, event_type="report", dedup_key=key,
                title=title, importance="高", pushed=True,
            )
            if not saved:
                continue
            self.notifier.send(f"📢 财报发布 | {name}({code})\n{title}（{it.get('time', '')}）\n"
                               + ("已自动触发重新分析，完成后推送报告" if self.analysis_runner
                                  else "可发送「分析 " + name + "」获取最新解读"))
            if self.analysis_runner:
                try:
                    self.analysis_runner(f"分析{name}（{code}）最新财报发布后的基本面与技术面变化")
                except Exception as e:
                    logger.error(f"[监控] 财报触发重分析失败 {name}: {e}")

    def _scan_one(self, target: Dict[str, Any]) -> None:
        name = target["name"]
        if target["target_type"] == "company" and target.get("code"):
            self._check_report_release(target["code"], name)
            items = self._fetch_company_news(target["code"], name)
        else:
            items = self._fetch_industry_news(name, target.get("keywords") or "")
            # 行业标的顺带核对重估触发条件（产业链分析留下的"若发生XX则重估"钩子）
            try:
                self._check_reeval_triggers(name, items)
            except Exception as e:
                logger.error(f"[监控] {name} 重估触发条件核对失败: {e}")

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
