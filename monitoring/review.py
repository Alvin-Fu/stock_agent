# -*- coding: utf-8 -*-
"""
分析复盘闭环：
1. snapshot_analysis：分析完成后从最终报告抽取「可检验判断」留档（异步调用，不阻塞回复）
2. ReviewRunner：到期快照与实际走势对账（方向/支撑压力由代码判定），LLM 生成复盘卡片并推送
复盘原则：检验的是"当时的推理质量"，prompt 强制区分「当时可知」与「事后才知道」，禁止马后炮。
"""

import json
import re
import threading
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, List

from storage.sqlite.stock_storage import get_db
from utils.common import parse_row_date
from utils.logger import logger

# 方向对账的涨跌幅阈值（%）：|涨跌| 小于该值视为波动太小、判断未验证
DIRECTION_THRESHOLD = 1.0

_EXTRACT_PROMPT = """从以下股票分析报告中抽取可检验的核心判断，用于日后复盘对账。

只输出JSON（不要markdown包裹）：
{{
  "short_term_view": "偏多/中性/偏空",
  "mid_term_view": "偏多/中性/偏空",
  "support": [支撑位价格数字, ...],
  "resistance": [压力位价格数字, ...],
  "key_reasons": ["核心理由1", "核心理由2", "核心理由3"]
}}

规则：报告没有明确给出的项用 null（数组给 []）；support/resistance 只要具体价格数字；
key_reasons 最多3条、每条20字内，只选支撑最终结论的关键依据。

分析报告：
{report}"""


def _parse_judgement(raw: str) -> Optional[Dict[str, Any]]:
    """解析 LLM 抽取结果，失败返回 None"""
    try:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return None
        data = json.loads(match.group(0))
        def _view(v):
            return v if v in ("偏多", "中性", "偏空") else None
        def _prices(v):
            if not isinstance(v, list):
                return []
            out = []
            for x in v:
                try:
                    out.append(round(float(x), 2))
                except (TypeError, ValueError):
                    continue
            return out[:4]
        return {
            "short_term_view": _view(data.get("short_term_view")),
            "mid_term_view": _view(data.get("mid_term_view")),
            "support": _prices(data.get("support")),
            "resistance": _prices(data.get("resistance")),
            "key_reasons": [str(r)[:40] for r in (data.get("key_reasons") or [])[:3]],
        }
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def snapshot_analysis(stock_code: str, question: str, final_answer: str) -> Optional[int]:
    """
    分析完成后留档（同步实现，调用方用线程异步跑）。
    仅对单只股票的分析留档；返回快照 id。
    """
    if not stock_code or "," in stock_code or not final_answer:
        return None
    try:
        db = get_db()
        from core.llm import get_default_llm
        response = get_default_llm().invoke(_EXTRACT_PROMPT.format(report=final_answer[:6000]))
        raw = response.content if hasattr(response, "content") else str(response)
        judgement = _parse_judgement(raw)
        if judgement is None:
            logger.warning(f"[复盘] {stock_code} 判断抽取失败，跳过留档")
            return None

        # 当时价格与关键指标：取库内最新日线
        price, indicators = None, {}
        daily = db.get_all_daily_data(stock_code)
        if daily is not None and not daily.empty:
            latest = daily.iloc[0]
            price = float(latest.get("close")) if latest.get("close") is not None else None
            for col in ("ma_pattern", "rsi6", "pos_52w", "volume_ratio"):
                v = latest.get(col)
                if v is not None:
                    indicators[col] = v if isinstance(v, str) else round(float(v), 2)

        name = None
        try:
            from tools.company_code_validator import find_company_name
            name = find_company_name(stock_code)
        except Exception:
            pass

        snapshot_id = db.save_analysis_snapshot(
            code=stock_code, name=name, question=(question or "")[:500],
            price_at_analysis=price,
            short_term_view=judgement["short_term_view"],
            mid_term_view=judgement["mid_term_view"],
            support=json.dumps(judgement["support"], ensure_ascii=False),
            resistance=json.dumps(judgement["resistance"], ensure_ascii=False),
            key_reasons=json.dumps(judgement["key_reasons"], ensure_ascii=False),
            indicators=json.dumps(indicators, ensure_ascii=False, default=str),
        )
        logger.info(f"[复盘] {stock_code} 分析快照已留档 #{snapshot_id}"
                    f"（短期{judgement['short_term_view']}，价 {price}）")
        return snapshot_id
    except Exception as e:
        logger.error(f"[复盘] 留档失败 {stock_code}: {e}\n{traceback.format_exc()}")
        return None


def snapshot_analysis_async(stock_code: str, question: str, final_answer: str) -> None:
    """fire-and-forget 异步留档，不阻塞对话回复"""
    threading.Thread(
        target=snapshot_analysis, args=(stock_code, question, final_answer),
        name="analysis-snapshot", daemon=True,
    ).start()


def _judge_direction(view: str, pct_change: float) -> str:
    """方向对账（代码判定，不交给 LLM）"""
    if view not in ("偏多", "偏空") or pct_change is None:
        return "未验证"
    if abs(pct_change) <= DIRECTION_THRESHOLD:
        return "未验证"
    if view == "偏多":
        return "正确" if pct_change > 0 else "错误"
    return "正确" if pct_change < 0 else "错误"


def _check_levels(levels: List[float], interval_low: float, interval_high: float, kind: str) -> List[str]:
    """支撑/压力位触及检验（代码判定）"""
    notes = []
    for lv in levels or []:
        touched = interval_low <= lv <= interval_high
        if kind == "support":
            notes.append(f"支撑{lv}：{'被触及' if touched else '未触及'}"
                         + ("，且区间最低价未跌破" if touched and interval_low >= lv * 0.99 else ""))
        else:
            notes.append(f"压力{lv}：{'被触及' if touched else '未触及'}")
    return notes


def calc_industry_verdicts(performance: List[Dict[str, Any]], benchmark_then: Optional[float],
                           benchmark_now: Optional[float], top_pick: Optional[str],
                           industry_view: Optional[str]) -> Dict[str, Any]:
    """
    产业链四维对账（纯代码判定）：
    组合超额（vs 沪深300）、排名区分度（前半 vs 后半平均收益）、首选实际名次、行业方向
    """
    pcts = [p["pct"] for p in performance]
    portfolio_return = round(sum(pcts) / len(pcts), 2)

    benchmark_return, excess, portfolio_verdict = None, None, "无基准"
    if benchmark_then and benchmark_now:
        benchmark_return = round((benchmark_now / benchmark_then - 1) * 100, 2)
        excess = round(portfolio_return - benchmark_return, 2)
        if excess > 0.5:
            portfolio_verdict = "跑赢"
        elif excess < -0.5:
            portfolio_verdict = "跑输"
        else:
            portfolio_verdict = "持平"

    # 排名区分度：按当时综合排名切前后半（无排名的排最后）
    ranked = sorted(performance, key=lambda x: (x.get("rank") is None, x.get("rank") or 999))
    half = len(ranked) // 2
    top_half = [p["pct"] for p in ranked[:half]] if half else []
    bottom_half = [p["pct"] for p in ranked[half:]]
    top_half_avg = round(sum(top_half) / len(top_half), 2) if top_half else None
    bottom_half_avg = round(sum(bottom_half) / len(bottom_half), 2) if bottom_half else None
    if top_half_avg is None or bottom_half_avg is None:
        rank_effective = "无法判定"
    elif top_half_avg - bottom_half_avg > 1.0:
        rank_effective = "有效"
    elif top_half_avg - bottom_half_avg < -1.0:
        rank_effective = "反向"
    else:
        rank_effective = "无区分"

    # 首选实际名次（按实际涨幅降序）
    by_pct = sorted(performance, key=lambda x: x["pct"], reverse=True)
    top_pick_rank = "未记录"
    if top_pick:
        for i, p in enumerate(by_pct, 1):
            if p["code"] == top_pick:
                top_pick_rank = f"{i}/{len(by_pct)}"
                break

    direction_verdict = _judge_direction(industry_view, portfolio_return)

    return {
        "portfolio_return": portfolio_return,
        "benchmark_return": benchmark_return,
        "excess_return": excess,
        "portfolio_verdict": portfolio_verdict,
        "top_half_avg": top_half_avg,
        "bottom_half_avg": bottom_half_avg,
        "rank_effective": rank_effective,
        "top_pick_rank": top_pick_rank,
        "direction_verdict": direction_verdict,
    }


_INDUSTRY_EXTRACT_PROMPT = """从以下产业链分析报告中抽取可检验的核心判断，用于日后复盘对账。

候选股票代码（以此为准）：{codes}

只输出JSON（不要markdown包裹）：
{{
  "ranking": ["按综合排名从高到低排列的股票代码"],
  "top_pick": "技术面最强的1只股票代码（报告未明确则 null）",
  "industry_view": "行业整体判断：偏多/中性/偏空（未明确则 null）"
}}

规则：ranking 只能使用候选代码列表里的代码；报告里综合排名表是排名依据。

分析报告（节选）：
{report}"""


def snapshot_industry_analysis(industry_name: str, question: str, final_answer: str,
                               candidate_codes: List[str]) -> Optional[int]:
    """产业链分析完成后留档（同步实现，调用方用线程异步跑）"""
    if not industry_name or not candidate_codes:
        return None
    try:
        db = get_db()
        from core.llm import get_default_llm
        response = get_default_llm().invoke(_INDUSTRY_EXTRACT_PROMPT.format(
            codes=",".join(candidate_codes), report=final_answer[:6000]))
        raw = response.content if hasattr(response, "content") else str(response)

        ranking, top_pick, industry_view = [], None, None
        try:
            match = re.search(r"\{.*\}", raw, re.DOTALL)
            if match:
                data = json.loads(match.group(0))
                ranking = [c for c in (data.get("ranking") or []) if c in candidate_codes]
                tp = data.get("top_pick")
                top_pick = tp if tp in candidate_codes else None
                iv = data.get("industry_view")
                industry_view = iv if iv in ("偏多", "中性", "偏空") else None
        except (json.JSONDecodeError, TypeError):
            pass
        # 排名兜底：没抽到的候选按原顺序补在后面
        rank_order = ranking + [c for c in candidate_codes if c not in ranking]

        # 每只候选的当时收盘价（分析刚拉过K线，库内是新鲜的）
        from tools.company_code_validator import find_company_name
        candidates = []
        for i, code in enumerate(rank_order, 1):
            price, name = None, None
            try:
                daily = db.get_all_daily_data(code)
                if daily is not None and not daily.empty:
                    price = float(daily.iloc[0].get("close"))
            except Exception:
                pass
            try:
                name = find_company_name(code)
            except Exception:
                pass
            candidates.append({"code": code, "name": name or code, "price": price, "rank": i})

        # 基准点位
        benchmark_price = None
        try:
            from tools.stock_tools import _get_hs300_close
            series = _get_hs300_close()
            if series is not None and len(series):
                benchmark_price = float(series.iloc[-1])
        except Exception:
            pass

        snapshot_id = db.save_industry_snapshot(
            industry_name=industry_name[:100], question=(question or "")[:500],
            candidates=json.dumps(candidates, ensure_ascii=False),
            top_pick=top_pick, industry_view=industry_view,
            benchmark_price=benchmark_price,
        )
        logger.info(f"[复盘] 产业链快照已留档 #{snapshot_id}：{industry_name}，"
                    f"候选 {len(candidates)} 只，首选 {top_pick}，行业判断 {industry_view}")
        return snapshot_id
    except Exception as e:
        logger.error(f"[复盘] 产业链留档失败 {industry_name}: {e}\n{traceback.format_exc()}")
        return None


def snapshot_industry_analysis_async(industry_name: str, question: str, final_answer: str,
                                     candidate_codes: List[str]) -> None:
    threading.Thread(
        target=snapshot_industry_analysis,
        args=(industry_name, question, final_answer, candidate_codes),
        name="industry-snapshot", daemon=True,
    ).start()


class ReviewRunner:
    def __init__(self, notifier=None):
        self.db = get_db()
        self.notifier = notifier
        self._llm = None

    def _get_llm(self):
        if self._llm is None:
            from core.llm import get_default_llm
            self._llm = get_default_llm()
        return self._llm

    def review_snapshot(self, snap: Dict[str, Any], push: bool = True) -> Optional[str]:
        """对单个快照复盘：拉最新走势 → 代码对账 → LLM 生成复盘卡片 → 存档/推送"""
        code = snap["code"]
        try:
            from tools.stock_tools import stock_tool_instance
            df = stock_tool_instance.fetch_and_save_stock_daily_data(code)
            if df is None or df.empty:
                logger.warning(f"[复盘] {code} 无行情数据，跳过")
                return None

            snap_date = snap["created_at"].date() if isinstance(snap["created_at"], datetime) \
                else parse_row_date(str(snap["created_at"])[:10])
            interval = df[df["date"].apply(lambda d: parse_row_date(d) > snap_date)]
            if interval.empty:
                logger.info(f"[复盘] {code} 分析后暂无新交易日，跳过")
                return None

            price_now = float(interval.iloc[0]["close"])
            interval_high = float(interval["high"].max())
            interval_low = float(interval["low"].min())
            price_then = snap.get("price_at_analysis")
            pct_change = round((price_now / price_then - 1) * 100, 2) if price_then else None
            days_elapsed = (datetime.now().date() - snap_date).days

            verdict = _judge_direction(snap.get("short_term_view"), pct_change)
            support = json.loads(snap.get("support") or "[]")
            resistance = json.loads(snap.get("resistance") or "[]")
            level_notes = _check_levels(support, interval_low, interval_high, "support") \
                + _check_levels(resistance, interval_low, interval_high, "resistance")
            key_reasons = json.loads(snap.get("key_reasons") or "[]")

            # 期间该标的的监控新闻（如有），供归因参考
            news_titles = []
            try:
                from storage.sqlite.stock_storage import MonitorEvent
                with self.db.get_session() as session:
                    from sqlalchemy import select, and_
                    events = session.execute(
                        select(MonitorEvent).where(and_(
                            MonitorEvent.target.in_([snap.get("name") or code, code]),
                            MonitorEvent.event_type == "news",
                            MonitorEvent.created_at >= snap["created_at"] if isinstance(snap["created_at"], datetime) else True,
                        )).limit(5)
                    ).scalars().all()
                    news_titles = [e.title for e in events if e.title]
            except Exception:
                pass

            prompt = f"""你是复盘助手。对之前的一次股票分析做简短复盘，检验**当时推理的质量**。

【硬性规则】
- 严格区分「当时可知」与「事后才知道」：判断错误只有在当时已有信息足以避免时才算误判，
  由事后新信息导致的走势变化应标注「新信息，非误判」
- 禁止马后炮（"早该看出会涨/跌"）；禁止改写当时的判断
- 方向对账结论已由程序判定，直接引用，不要重新判断
- 输出精简（250字内），按模板来

【当时的分析】{snap_date} 收盘价 {price_then}
- 短期判断：{snap.get('short_term_view') or '未明确'}；中期判断：{snap.get('mid_term_view') or '未明确'}
- 核心理由：{'；'.join(key_reasons) if key_reasons else '未记录'}

【{days_elapsed} 天后的实际走势】
- 现价 {price_now}，区间涨跌 {pct_change}%，区间最高 {interval_high} / 最低 {interval_low}
- 方向对账（程序判定）：{verdict}
- 关键价位检验（程序判定）：{'；'.join(level_notes) if level_notes else '当时未给出具体价位'}
- 期间相关新闻：{'；'.join(news_titles) if news_titles else '无记录'}

【输出模板】
判断结果：一句话（引用程序判定）
理由核验：当时的核心理由逐条标注 兑现✅/未兑现❌/无新信息⏸
教训：一两句（没有就写"无明显误判"；区分误判与新信息）"""

            response = self._get_llm().invoke(prompt)
            content = response.content if hasattr(response, "content") else str(response)

            header = (f"📋 复盘 | {snap.get('name') or code}({code})\n"
                      f"{snap_date} 分析（当时 {price_then}）→ {days_elapsed} 天后 {price_now}"
                      f"（{pct_change:+.2f}%）方向判断：{verdict}\n")
            card = header + content.strip()

            self.db.save_analysis_review(
                snapshot_id=snap["id"], code=code, days_elapsed=days_elapsed,
                price_now=price_now, pct_change=pct_change,
                direction_verdict=verdict, review_content=card,
            )
            self.db.mark_snapshot_reviewed(snap["id"])
            if push and self.notifier:
                self.notifier.send(card)
            logger.info(f"[复盘] {code} 完成：{verdict}（{pct_change}%）")
            return card
        except Exception as e:
            logger.error(f"[复盘] {code} 复盘失败: {e}\n{traceback.format_exc()}")
            return None

    def run_due_reviews(self, after_days: int = 5, industry_after_days: int = 10) -> int:
        """复盘全部到期快照（个股+产业链），返回完成数"""
        done = 0

        due = self.db.get_snapshots_due_review(after_days)
        if due:
            logger.info(f"[复盘] {len(due)} 个个股快照到期，开始复盘")
            for snap in due:
                if self.review_snapshot(snap):
                    done += 1

        due_ind = self.db.get_industry_snapshots_due_review(industry_after_days)
        if due_ind:
            logger.info(f"[复盘] {len(due_ind)} 个产业链快照到期，开始复盘")
            for snap in due_ind:
                if self.review_industry_snapshot(snap):
                    done += 1

        if not due and not due_ind:
            logger.info("[复盘] 无到期快照")
            return 0

        # 附上系统成绩单
        if done and self.notifier:
            acc = self.db.get_direction_accuracy()
            if acc.get("accuracy") is not None:
                self.notifier.send(
                    f"📈 系统成绩单：近 {acc['total']} 次个股复盘中，可验证的方向判断 {acc['judged']} 次，"
                    f"命中 {acc['correct']} 次（命中率 {acc['accuracy']}%）")
            track = self.db.get_industry_track_record()
            if track.get("total"):
                self.notifier.send(
                    f"📈 产业链选股成绩单：近 {track['total']} 次复盘，组合跑赢基准 {track['outperform']} 次，"
                    f"排名区分度有效 {track['rank_effective']} 次")
        return done

    # ================== 产业链复盘 ==================

    def review_industry_snapshot(self, snap: Dict[str, Any], push: bool = True) -> Optional[str]:
        """产业链快照复盘：组合超额/排名区分度/首选命中/方向 四维代码对账 + LLM 卡片"""
        industry = snap["industry_name"]
        try:
            candidates = json.loads(snap.get("candidates") or "[]")
            if not candidates:
                logger.warning(f"[复盘] {industry} 快照无候选清单，跳过")
                self.db.mark_industry_snapshot_reviewed(snap["id"])
                return None

            snap_date = snap["created_at"].date() if isinstance(snap["created_at"], datetime) \
                else parse_row_date(str(snap["created_at"])[:10])
            days_elapsed = (datetime.now().date() - snap_date).days

            # 逐只取现价、算区间收益
            from tools.stock_tools import stock_tool_instance
            performance = []  # [{code,name,rank,pct}]
            for cand in candidates:
                code = cand.get("code")
                price_then = cand.get("price")
                if not code or not price_then:
                    continue
                try:
                    df = stock_tool_instance.fetch_and_save_stock_daily_data(code)
                    if df is None or df.empty:
                        continue
                    price_now = float(df.iloc[0]["close"])
                    performance.append({
                        "code": code, "name": cand.get("name") or code,
                        "rank": cand.get("rank"),
                        "pct": round((price_now / float(price_then) - 1) * 100, 2),
                    })
                except Exception as e:
                    logger.warning(f"[复盘] {industry} 候选 {code} 取价失败: {e}")
            if len(performance) < 2:
                logger.warning(f"[复盘] {industry} 有效候选不足 2 只，跳过")
                return None

            verdicts = calc_industry_verdicts(
                performance, snap.get("benchmark_price"), self._current_benchmark_price(),
                snap.get("top_pick"), snap.get("industry_view"))

            perf_sorted = sorted(performance, key=lambda x: x["pct"], reverse=True)
            perf_lines = "\n".join(
                f"  综合排名{p.get('rank', '?')} {p['name']}({p['code']}): {p['pct']:+.2f}%"
                + ("（当时技术面首选）" if p["code"] == snap.get("top_pick") else "")
                for p in perf_sorted)

            prompt = f"""你是复盘助手。对之前一次产业链选股分析做简短复盘，检验筛选与排名逻辑的质量。

【硬性规则】
- 四项对账结论已由程序判定，直接引用，不要重新判断
- 区分「筛选/排名误判」与「事后新信息驱动」；禁止马后炮
- 输出精简（250字内）

【当时的分析】{snap_date}，行业：{industry}，行业判断：{snap.get('industry_view') or '未明确'}
【{days_elapsed} 天后的实际表现】
- 候选组合等权收益 {verdicts['portfolio_return']}% vs 沪深300 {verdicts['benchmark_return']}%，
  超额 {verdicts['excess_return']}% → 程序判定：{verdicts['portfolio_verdict']}
- 排名区分度：排名前半平均 {verdicts['top_half_avg']}%，后半平均 {verdicts['bottom_half_avg']}%
  → 程序判定：{verdicts['rank_effective']}
- 技术面首选实际涨幅名次：{verdicts['top_pick_rank']}
- 行业方向对账：{verdicts['direction_verdict']}
【逐只表现】
{perf_lines}

【输出模板】
组合结论：一句话
排名与首选核验：一两句（谁被高估/低估，标注 误判/新信息）
教训：一两句（没有就写"无明显误判"）"""

            response = self._get_llm().invoke(prompt)
            content = response.content if hasattr(response, "content") else str(response)

            header = (f"📋 产业链复盘 | {industry}\n"
                      f"{snap_date} 分析 → {days_elapsed} 天后：组合 {verdicts['portfolio_return']:+.2f}% "
                      f"vs 基准 {verdicts['benchmark_return']:+.2f}%（{verdicts['portfolio_verdict']}）｜"
                      f"排名{verdicts['rank_effective']}｜首选名次 {verdicts['top_pick_rank']}\n")
            card = header + content.strip()

            self.db.save_industry_review(
                snapshot_id=snap["id"], industry_name=industry, days_elapsed=days_elapsed,
                portfolio_return=verdicts["portfolio_return"],
                benchmark_return=verdicts["benchmark_return"],
                excess_return=verdicts["excess_return"],
                portfolio_verdict=verdicts["portfolio_verdict"],
                rank_effective=verdicts["rank_effective"],
                top_pick_rank=verdicts["top_pick_rank"],
                direction_verdict=verdicts["direction_verdict"],
                review_content=card,
            )
            self.db.mark_industry_snapshot_reviewed(snap["id"])
            if push and self.notifier:
                self.notifier.send(card)
            logger.info(f"[复盘] 产业链 {industry} 完成：{verdicts['portfolio_verdict']}")
            return card
        except Exception as e:
            logger.error(f"[复盘] 产业链 {industry} 复盘失败: {e}\n{traceback.format_exc()}")
            return None

    @staticmethod
    def _current_benchmark_price() -> Optional[float]:
        """沪深300 最新收盘点位"""
        try:
            from tools.stock_tools import _get_hs300_close
            series = _get_hs300_close()
            if series is not None and len(series):
                return float(series.iloc[-1])
        except Exception as e:
            logger.warning(f"[复盘] 获取基准点位失败: {e}")
        return None
