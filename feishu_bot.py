#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书机器人入口（长连接模式，无需公网地址）：
- 双向对话：在飞书私聊/群里发问题 → 多 Agent 工作流分析 → 回复结果
- 内置命令：监控 XX / 取消监控 XX / 监控列表 / 立即扫描 / 帮助
- 同进程启动监控调度（盘后信号 + 新闻/政策扫描 + 推送）

启动前配置（local.yaml）：
feishu:
  app_id: "cli_xxx"        # 开放平台自建应用
  app_secret: "xxx"
  push_open_id: ""         # 主动推送目标（你的 open_id，可先留空：给机器人发一句话，日志里会打印你的 open_id）
  webhook_url: ""          # 群自定义机器人（可选兜底）

运行：python feishu_bot.py
仅配置 webhook_url 不配应用凭据时，进入"纯监控推送模式"（无法对话）。
"""

import json
import logging
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

from monitoring.notifier import FeishuNotifier
from monitoring.scheduler import MonitorScheduler
from storage.sqlite.stock_storage import get_db
from utils.config import load_config
from utils.logger import logger

def _convert_to_feishu_markdown(text: str) -> str:
    """
    将标准 Markdown 转换为飞书友好格式：
    1. 表格：确保每行列数一致，补充缺失的列，添加表头分隔行
    2. 加粗：确保 **text** 格式正确
    3. 标题：## 转成飞书支持的格式
    4. 列表：优化嵌套列表显示
    5. 代码块：添加语言标记
    """
    lines = text.split("\n")
    result = []
    i = 0
    while i < len(lines):
        line = lines[i]
        
        if line.startswith("|") and line.endswith("|"):
            table_lines = [line]
            i += 1
            while i < len(lines) and (lines[i].startswith("|") or lines[i].strip().startswith(":--")):
                table_lines.append(lines[i])
                i += 1
            
            header = table_lines[0]
            header_cells = [c.strip() for c in header.split("|") if c.strip()]
            col_count = len(header_cells)
            
            has_separator = False
            for tl in table_lines[1:]:
                if tl.strip().startswith(":--") or tl.strip().startswith("---"):
                    has_separator = True
                    break
            
            if not has_separator and len(table_lines) > 1:
                separator = "| " + " | ".join(["---"] * col_count) + " |"
                table_lines.insert(1, separator)
            
            for j, tl in enumerate(table_lines):
                cells = [c.strip() for c in tl.split("|") if c.strip()]
                if len(cells) < col_count:
                    cells += ["-"] * (col_count - len(cells))
                elif len(cells) > col_count:
                    cells = cells[:col_count]
                table_lines[j] = "| " + " | ".join(cells) + " |"
            
            result.extend(table_lines)
            if i < len(lines):
                result.append("")
            continue
        
        if line.startswith("## "):
            result.append(line)
            result.append("---")
            i += 1
            continue
        
        if line.startswith("### "):
            result.append(line)
            i += 1
            continue
        
        if line.startswith("- **"):
            result.append(line.replace("- **", "• **"))
            i += 1
            continue
        
        if line.startswith("**") and ":" in line:
            result.append(line)
            i += 1
            continue
        
        if "|" in line and not line.startswith("|") and not line.startswith("```"):
            parts = line.split("|")
            if len(parts) >= 3:
                padded = []
                for p in parts:
                    padded.append(p.ljust(12)[:12])
                result.append("  ".join(padded))
                i += 1
                continue
        
        if line.startswith("```"):
            code_block = [line]
            i += 1
            while i < len(lines) and not lines[i].startswith("```"):
                code_block.append(lines[i])
                i += 1
            if i < len(lines):
                code_block.append(lines[i])
                i += 1
            if len(code_block) > 2 and not code_block[0].strip() == "```":
                code_block[0] = "```text"
            result.extend(code_block)
            continue
        
        result.append(line)
        i += 1
    
    return "\n".join(result)


# ============================================================
# 交互式卡片（interactive card）转换
# ============================================================

_CARD_ICONS = {
    "投资结论": "📌", "公司概况": "🏢", "业务拆解": "🔧", "财务分析": "📊",
    "护城河": "🛡️", "核心竞争力": "🛡️", "资金筹码": "💰", "技术分析": "📈",
    "估值分析": "💎", "投资建议": "🎯", "风险提示": "⚠️", "同业对比": "📊",
    "同行对比": "📊",
}

_MAX_CARD_ELEMENTS = 40  # 单张卡片最大元素数

def _parse_markdown_table(lines: list) -> dict:
    """
    解析 markdown 表格文本为飞书 CardKit v2 table 组件。
    优先使用 table 组件实现真正表格渲染，降级方案为 lark_md 文本排版。
    """
    header_cells = [c.strip() for c in lines[0].split("|") if c.strip()]
    data_rows = []
    for line in lines[2:]:  # 跳过表头和分隔行
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.split("|") if c.strip()]
        if not cells:
            continue
        if len(cells) < len(header_cells):
            cells += [""] * (len(header_cells) - len(cells))
        else:
            cells = cells[:len(header_cells)]
        data_rows.append(cells)
    if not data_rows:
        return {}

    n_cols = len(header_cells)
    # CardKit v2 table 组件列数限制
    if n_cols > 10 or len(data_rows) > 50:
        return _build_text_table(header_cells, data_rows)

    # 构建 CardKit v2 table 组件
    # 列定义：col_0, col_1, ...
    columns = []
    for i, h in enumerate(header_cells):
        col_name = f"col_{i}"
        columns.append({
            "name": col_name,
            "display_name": h,
            "data_type": "text",
            "width": "auto",
        })

    # 行数据：用列名作 key
    rows = []
    for cells in data_rows:
        row = {}
        for i, c in enumerate(cells):
            row[f"col_{i}"] = c if c else "-"
        rows.append(row)

    return {
        "tag": "table",
        "columns": columns,
        "rows": rows,
        "header_style": {"bold": True},
    }


def _build_text_table(header_cells: list, data_rows: list) -> dict:
    """降级方案：用 lark_md 文本排版模拟表格（列数超过 10 时使用）"""
    lines_text = ["| " + " | ".join(f"**{h}**" for h in header_cells) + " |"]
    for cells in data_rows:
        lines_text.append("| " + " | ".join(cells) + " |")
    return {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines_text)}}

def _markdown_to_card_elements(text: str) -> list:
    """
    将 markdown 报告文本转换为飞书卡片的 elements 列表。
    支持 ## 标题、| 表格、- 列表、加粗、分隔线。"""
    # 预处理1：在粗体子标题（**Xxx**：/：）前插入 \n\n
    # 匹配不在行首的 **子标题**：，拆成独立段落
    # 注意不要误拆引文标记如 **[财报]**。（后面跟 . 不是 ：）
    text = re.sub(r'(?<!\n)(\*\*[^*]{1,10}?\*\*[:：])', r'\n\n\1', text)

    # 预处理2：移除列表项开头的加粗关键词（如 **核心证据**：→ 核心证据：）
    # 列表项关键词只做语义区分，不加粗，避免视觉上字号偏大、层级混乱
    # （飞书 lark_md 中加粗文字视觉上比普通文字"重"，密集加粗会显大）
    text = re.sub(r'^(\s*)(?:[•\-]|\d+\.)\s*\*\*([^*]+)\*\*([：:])',
                  lambda m: f"{m.group(1)}{'• ' if m.group(0).strip()[0] in '-•' else m.group(0).split('.')[0]+'. '}{m.group(2)}{m.group(3)}",
                  text, flags=re.MULTILINE)
    text = re.sub(r'^([\s•\-\d\.]+)\*\*([^*]+)\*\*([：:])',
                  r'\1\2\3', text, flags=re.MULTILINE)

    elements = []
    lines = text.strip().split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]

        # ## 标题 → div with heading text_size
        if line.startswith("## "):
            title = line[3:].strip()
            elements.append({"tag": "hr"})
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**{title}**"},
                "text_size": "heading-4"
            })
            i += 1
            # 收集标题后的内容直到下一个标题
            body_lines = []
            while i < len(lines) and not lines[i].startswith("## ") and not lines[i].startswith("```"):
                stripped = lines[i].strip()
                if stripped.startswith("|"):
                    break
                body_lines.append(lines[i])
                i += 1
            if body_lines:
                body_text = "\n".join(body_lines).strip()
                if body_text:
                    paragraphs = [p.strip() for p in body_text.split("\n\n") if p.strip()]
                    for p in paragraphs:
                        elements.append({
                            "tag": "div",
                            "text": {"tag": "lark_md", "content": p}
                        })
            continue

        # 表格（支持缩进的表格，如嵌在列表中的表格）
        if line.strip().startswith("|"):
            table_lines = [line.strip()]
            i += 1
            while i < len(lines):
                stripped = lines[i].strip()
                if stripped.startswith("|") or stripped.startswith(":--") or stripped.startswith("---"):
                    table_lines.append(stripped)
                    i += 1
                else:
                    break
            # 跳过纯分隔行，取有效行
            data_lines = [l for l in table_lines if not l.startswith(":--") and not l.startswith("---")]
            if len(data_lines) >= 2:
                element = _parse_markdown_table(data_lines)
                if element:
                    elements.append(element)
            continue

        # 代码块跳过（卡片不支持代码块，转 text）
        if line.startswith("```"):
            i += 1
            while i < len(lines) and not lines[i].startswith("```"):
                i += 1
            i += 1
            continue

        # 普通行
        body_lines = []
        while i < len(lines):
            li = lines[i]
            if li.startswith("## ") or li.startswith("```"):
                break
            stripped = li.strip()
            if stripped.startswith("|"):
                break
            if stripped:
                # 列表优化
                if stripped.startswith("-"):
                    stripped = "• " + stripped[1:].strip()
                body_lines.append(stripped)
            i += 1
        if body_lines:
            body_text = "\n".join(body_lines)
            # 按双换行切分段落
            paragraphs = [p.strip() for p in body_text.split("\n\n") if p.strip()]
            for p in paragraphs:
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": p}
                })
            continue

        i += 1

    # 移除开头的 hr（多余的无意义）
    while elements and elements[0].get("tag") == "hr":
        elements.pop(0)
    return elements


def _add_color_tags(text: str) -> str:
    """给关键数字添加红绿色标签：负数为红，正数为绿。
    注意：只给非加粗的数字加颜色；已在 **...** 中的数字只保留加粗，不再叠加颜色
    （避免加粗+颜色双重放大导致字号不协调）。"""
    import re
    def _colorize(m):
        num = m.group(0)
        if num.startswith("-"):
            return f"<font color='red'>{num}</font>"
        else:
            return f"<font color='green'>{num}</font>"

    # 先把 **...** 加粗块保护起来，避免里面的数字被加颜色
    bold_parts = []
    def _save_bold(m):
        bold_parts.append(m.group(0))
        return f"\x00BOLD{len(bold_parts)-1}\x00"

    text = re.sub(r'\*\*[^*]+\*\*', _save_bold, text)

    # 给非加粗区域的数字加颜色
    text = re.sub(r"[+-]?\d+\.?\d*%", _colorize, text)
    text = re.sub(r"(?<=[（(])\+?\d+\.?\d*亿", _colorize, text)

    # 还原加粗块
    for i, bp in enumerate(bold_parts):
        text = text.replace(f"\x00BOLD{i}\x00", bp)

    return text


def _report_to_cards(report_text: str, stock_label: str = "") -> list:
    """
    将完整 markdown 报告转换为多张交互式卡片消息 JSON。
    返回 list[dict]，每张 dict 可直接作为 card_content 发送。
    stock_label 用于卡片标题，如 "比亚迪(002594)"。"""
    if not report_text or not report_text.strip():
        return []

    # 解析 elements
    raw_elements = _markdown_to_card_elements(report_text)
    if not raw_elements:
        return []

    # 将 lark_md 文本中的数字加上颜色标签
    for el in raw_elements:
        if el.get("tag") == "div":
            el["text"]["content"] = _add_color_tags(el["text"]["content"])

    # 添加结尾免责声明（CardKit v2 不支持 note 标签，改用 div）
    has_note = any(e.get("tag") in ("note", "div") and "数据来源" in str(e) for e in raw_elements)
    if not has_note:
        raw_elements.append({
            "tag": "div",
            "text": {"tag": "lark_md",
                     "content": "_数据来源：财报、程序监测 | 仅供参考，不构成投资建议_"}
        })

    # 按 _MAX_CARD_ELEMENTS 分卡
    # 使用 CardKit v2 格式（schema: 2.0 + body.elements）
    title_base = f"📊 {stock_label} 投研报告" if stock_label else "📊 投研报告"
    cards = []
    chunk_size = _MAX_CARD_ELEMENTS
    for idx in range(0, len(raw_elements), chunk_size):
        chunk = raw_elements[idx:idx + chunk_size]
        total_cards = (len(raw_elements) + chunk_size - 1) // chunk_size
        title = f"{title_base} ({idx // chunk_size + 1}/{total_cards})" if total_cards > 1 else title_base
        card = {
            "schema": "2.0",
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "blue"
            },
            "body": {
                "elements": chunk,
            },
        }
        cards.append(card)

    return cards


def split_report(text: str, limit: int = 4000) -> list:
    """
    长报告切分：优先在 ##/### 标题边界断段，段内超限再按行边界硬切，
    绝不在表格行/句子中间截断。返回切好的段列表。
    输出前先转换为飞书友好格式。
    """
    text = _convert_to_feishu_markdown(text)
    
    if len(text) <= limit:
        return [text]
    parts, buf = [], ""
    sections = re.split(r"(?=\n#{2,3} )", "\n" + text)
    for sec in sections:
        if not sec.strip():
            continue
        if len(buf) + len(sec) <= limit:
            buf += sec
            continue
        if buf.strip():
            parts.append(buf.strip())
        while len(sec) > limit:
            cut = sec.rfind("\n", limit // 2, limit)
            cut = cut if cut > 0 else limit
            parts.append(sec[:cut].strip())
            sec = sec[cut:]
        buf = sec
    if buf.strip():
        parts.append(buf.strip())
    return parts or [text[:limit]]


HELP_TEXT = """# 🤖 股票分析助手使用说明

## 基本用法
· 直接提问：如「分析比亚迪」「600519 技术面怎么样」「白酒产业链有哪些机会」
· 一条消息可带多个对象：如「分析比亚迪和宁德时代」「看看比亚迪，再看看半导体产业链」
  （自动拆解，逐个分析，最后附组合速览）

## 监控管理
· 监控 比亚迪 —— 加入监控清单（公司名或6位代码；识别不到时按行业监控）
· 取消监控 比亚迪
· 监控列表

## 运维指令
· 立即扫描 —— 马上跑一轮信号+新闻扫描
· 复盘 比亚迪 —— 对最近一次分析做复盘（对照实际走势）

## 纠错机制
· 纠错 比亚迪 销量应该是38万不是41万 —— 指出报告错误，系统记住，下次分析不再重犯
· 纠错列表 —— 查看已记录的纠错

## 报告格式
分析报告将以**交互式卡片**推送，支持表格渲染、段落分段和关键数据颜色标注。
· 帮助 —— 显示本说明

**监控提醒：** 盘后技术信号 + 个股新闻/行业政策，自动推送到这里"""


class FeishuBot:
    def __init__(self, config_section: str = "feishu"):
        self.config_section = config_section
        cfg = load_config().get(config_section, {}) or {}
        self.app_id = (cfg.get("app_id") or "").strip()
        self.app_secret = (cfg.get("app_secret") or "").strip()
        self.push_open_id = (cfg.get("push_open_id") or "").strip()
        self.db = get_db()
        self.notifier = FeishuNotifier(config_section=config_section)
        self.monitor = MonitorScheduler(self.notifier)
        # 分析耗时较长，有限线程池防止并发打爆 LLM/数据源
        self.pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="feishu-worker")
        self._executor = None  # WorkflowExecutor 延迟创建（首条消息时）

        # WebSocket 健康监控
        self._last_message_time = time.time()
        self._ws_watchdog_running = False

        # 已知会话文件
        self._known_chats_path = Path("data/known_chats.json")

    @staticmethod
    def _save_known_chat(chat_id: str, chat_type: str):
        """自动记录群/会话 ID，重启后仍可用"""
        path = Path("data/known_chats.json")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            known = {}
            if path.exists():
                known = json.loads(path.read_text())
            if chat_id not in known:
                known[chat_id] = {"type": chat_type, "first_seen": time.strftime("%Y-%m-%d %H:%M:%S")}
                path.write_text(json.dumps(known, ensure_ascii=False, indent=2))
                logger.info(f"[自动获取] 已记录 {chat_type} chat_id: {chat_id} → data/known_chats.json")
        except Exception as e:
            logger.debug(f"[自动获取] 记录失败: {e}")

    # ---------- 工作流 ----------

    def _get_workflow(self):
        if self._executor is None:
            from orchestration.workflow import WorkflowExecutor
            self._executor = WorkflowExecutor(enable_memory=True)
        return self._executor

    @staticmethod
    def _send_answer(answer: str, reply):
        """超长报告按章节边界切段发送（不在表格/句子中间截断），多段带序号"""
        chunks = split_report(answer)
        total = len(chunks)
        for i, chunk in enumerate(chunks, 1):
            reply(f"({i}/{total})\n{chunk}" if total > 1 else chunk)

    def _send_card_answer(self, answer: str, stock_label: str, receive_id: str,
                          receive_id_type: str = "open_id"):
        """
        交互式卡片发送回答。
        将 markdown 报告转换为 interactive card JSON 后发送。
        转换失败时降级为普通文本分段发送。"""
        if not answer or not answer.strip():
            return
        try:
            cards = _report_to_cards(answer, stock_label)
            if cards:
                self.notifier.send_card_chunked(cards, receive_id, receive_id_type)
                return
        except Exception as e:
            logger.warning(f"[卡片] 报告转卡片失败，降级为文本: {e}")
        # 降级：通过 text/post 兜底
        chunks = split_report(answer)
        for i, chunk in enumerate(chunks, 1):
            label = f"({i}/{len(chunks)})\n" if len(chunks) > 1 else ""
            self.notifier.send_to(receive_id, f"{label}{chunk}", receive_id_type)

    @staticmethod
    def _extract_summary(report_text: str) -> str:
        """从完整报告中提取结论摘要（## 结论 段），找不到时取前 15 行"""
        import re
        match = re.search(r'## 结论\n(.*?)(?=\n## |\Z)', report_text, re.DOTALL)
        if match and match.group(1).strip():
            return "## 📋 结论摘要\n\n" + match.group(1).strip()
        lines = report_text.strip().split('\n')
        return '\n'.join(lines[:15])

    def _send_summary_with_doc(self, answer: str, label: str, receive_id: str,
                               receive_id_type: str, reply):
        """
        统一入口：完整报告写入飞书文档，卡片只推结论摘要 + 文档链接。
        文档或卡片创建失败时降级为全文文本发送。"""
        if not answer or not answer.strip():
            return
        try:
            doc_url = self.notifier.create_feishu_doc(f"{label} 完整报告", answer)
            summary = self._extract_summary(answer)
            card_text = (f"{summary}\n\n---\n📄 **[查看完整报告]({doc_url})**"
                         if doc_url else answer)
            cards = _report_to_cards(card_text, label)
            if cards:
                self.notifier.send_card_chunked(cards, receive_id, receive_id_type)
                return
        except Exception as e:
            logger.warning(f"[摘要] 文档/卡片创建失败，降级文本: {e}")
        self._send_answer(answer, reply)

    def _run_analysis(self, question: str, thread_id: str, reply, receive_id_type: str = "open_id"):
        try:
            executor = self._get_workflow()

            # 入口任务拆解：一条消息里含多个公司/行业时逐个分析（拆解失败自动回落单任务）
            from orchestration.task_splitter import split_tasks
            tasks = split_tasks(question)

            if len(tasks) <= 1:
                state = executor.run_sync(question, thread_id=thread_id)
                answer = executor.get_final_answer(state)
                # 提取股票代码用于卡片标题；产业链模式用行业名
                stock_code = state.get("stock_code", "")
                industry_name = state.get("industry_name", "")
                if industry_name and "," in (stock_code or ""):
                    label = f"【{industry_name}】产业链分析"
                else:
                    stock_name = self._resolve_name(stock_code) if stock_code else ""
                    stock_label = f"{stock_name}({stock_code})" if stock_name else stock_code
                    label = stock_label or "分析报告"
                self._send_summary_with_doc(answer, label, thread_id, receive_id_type, reply)
                return

            names = "、".join(t["target"] or f"对象{i}" for i, t in enumerate(tasks, 1))
            reply(f"🧩 识别到 {len(tasks)} 个分析对象：{names}\n将逐个完整分析（每个约3-10分钟），完成一个回一个")

            plans = []
            for i, t in enumerate(tasks, 1):
                label = t["target"] or f"对象{i}"
                reply(f"▶ ({i}/{len(tasks)}) 开始分析 {label} …")
                try:
                    # 每个对象独立会话，互不污染对话记忆
                    state = executor.run_sync(t["question"], thread_id=f"{thread_id}:{label}")
                    answer = executor.get_final_answer(state)
                    # 统一走摘要+文档格式
                    self._send_summary_with_doc(answer, label, thread_id, receive_id_type, reply)
                    plan = (state.get("technical_result") or {}).get("trade_plan")
                    if plan:
                        plans.append((label, plan))
                except Exception as e:
                    logger.error(f"[飞书] 子任务 {label} 分析失败: {e}\n{traceback.format_exc()}")
                    reply(f"❌ {label} 分析失败：{e}（继续下一个）")

            # 组合速览卡：程序数字直接拼，不经 LLM
            if len(plans) >= 2:
                lines = ["📋 组合速览（程序操作参考对比）"]
                for label, p in plans:
                    seg = f"· {label}：{p.get('direction')} | 现价{p.get('close')} | 仓位{p.get('position')}成"
                    if p.get("risk_reward") is not None:
                        seg += f" | 盈亏比{p.get('risk_reward')}"
                    lines.append(seg)
                lines.append("（详情见各标的报告；仅罗列程序数字，非组合配置建议）")
                reply("\n".join(lines))
        except Exception as e:
            logger.error(f"[飞书] 分析失败: {e}\n{traceback.format_exc()}")
            reply(f"❌ 分析出错了：{e}")

    # ---------- 命令处理 ----------

    def _handle_command(self, text: str) -> str:
        """内置命令；返回 None 表示不是命令（走分析流程）"""
        text = text.strip()
        if text in ("帮助", "help", "菜单"):
            return "__HELP__"

        if text == "监控列表":
            targets = self.db.get_watch_targets()
            if not targets:
                return "监控清单为空。发送「监控 比亚迪」添加。"
            lines = []
            for t in targets:
                tag = f"{t['name']}({t['code']})" if t.get("code") else f"{t['name']}【行业】"
                lines.append(f"· {tag}")
            return "📋 当前监控清单：\n" + "\n".join(lines)

        m = re.match(r"^取消监控\s*(.+)$", text)
        if m:
            name = m.group(1).strip()
            ok = self.db.remove_watch_target(name)
            return f"✅ 已取消监控：{name}" if ok else f"未找到监控标的：{name}"

        m = re.match(r"^复盘\s*(.+)$", text)
        if m:
            return ("__REVIEW__", m.group(1).strip())

        if text == "纠错列表":
            records = self.db.list_recent_feedback()
            if not records:
                return "还没有纠错记录。发送「纠错 比亚迪 哪里错了」记录一条。"
            lines = ["📝 纠错记录（新→旧）："]
            for r in records:
                day = str(r["created_at"])[:10]
                lines.append(f"· [{day}] {r['target_name']}：{r['content'][:60]}")
            return "\n".join(lines)

        m = re.match(r"^纠错\s+(\S+)\s+(.+)$", text, re.DOTALL)
        if m:
            return self._record_feedback(m.group(1).strip(), m.group(2).strip())
        if re.match(r"^纠错\s*\S*$", text):
            return "用法：纠错 对象 错误内容\n例：纠错 比亚迪 销量应该是38万不是41万"

        m = re.match(r"^监控\s*(.+)$", text)
        if m:
            return self._add_watch(m.group(1).strip())

        if text in ("立即扫描", "扫描"):
            # 扫描可能要几分钟，丢线程池异步跑
            return "__SCAN__"

        return None

    def _run_review(self, raw: str, reply):
        """手动复盘：先按个股解析，解析不到按行业找快照（不推送全局通道，直接回复）"""
        from tools.company_code_validator import find_stock_code
        from monitoring.review import ReviewRunner

        code = raw if re.match(r"^\d{6}$", raw) else None
        if code is None:
            try:
                code = find_stock_code(raw)
            except Exception:
                code = None

        runner = ReviewRunner(self.notifier)
        if code:
            snap = self.db.get_latest_snapshot(code)
            if not snap:
                reply(f"没有 {raw} 的分析记录，先发送「分析 {raw}」，隔几天再来复盘")
                return
            card = runner.review_snapshot(snap, push=False)
            reply(card or "复盘失败（可能分析后还没有新交易日）")
            return

        # 按行业复盘
        ind_snap = self.db.get_latest_industry_snapshot(raw)
        if ind_snap:
            card = runner.review_industry_snapshot(ind_snap, push=False)
            reply(card or "产业链复盘失败（可能候选行情数据不足）")
            return
        reply(f"未找到「{raw}」的个股或产业链分析记录")

    def _record_feedback(self, target: str, content: str) -> str:
        """记录用户纠错：解析代码、关联最近快照，下次分析该标的时自动注入"""
        from tools.company_code_validator import find_stock_code, find_company_name

        code, name = None, target
        try:
            if re.match(r"^\d{6}$", target):
                code, name = target, find_company_name(target) or target
            else:
                found = find_stock_code(target)
                if found:
                    code, name = found, find_company_name(found) or target
        except Exception as e:
            logger.warning(f"[飞书] 纠错解析代码失败（按行业名记录）: {e}")

        snapshot_id = None
        if code:
            snap = self.db.get_latest_snapshot(code)
            snapshot_id = snap["id"] if snap else None

        self.db.save_user_feedback(target_name=name, content=content,
                                   code=code, snapshot_id=snapshot_id)
        tag = f"{name}({code})" if code else f"{name}【按行业记录】"
        return (f"✅ 已记录纠错：{tag}\n「{content[:100]}」\n"
                f"下次分析该对象时会注入这条纠错，要求不得再犯；复盘时也会对账是否复发")

    def _add_watch(self, raw: str) -> str:
        """添加监控：优先按公司解析代码，解析不到按行业"""
        from tools.company_code_validator import find_stock_code, find_company_name

        code, name = None, raw
        try:
            if re.match(r"^\d{6}$", raw):
                real = find_company_name(raw)
                if real:
                    code, name = raw, real
            else:
                found = find_stock_code(raw)
                if found:
                    code, name = found, find_company_name(found) or raw
        except Exception as e:
            logger.warning(f"[飞书] 解析监控标的失败: {e}")

        if code:
            self.db.add_watch_target(name=name, target_type="company", code=code)
            return f"✅ 已加入监控：{name}({code})\n将监控：盘后技术信号 + 个股新闻"
        self.db.add_watch_target(name=raw, target_type="industry")
        return f"✅ 已按行业加入监控：{raw}\n将监控：行业新闻与政策动态"

    # ---------- 消息入口 ----------

    def on_message(self, data) -> None:
        try:
            message = data.event.message
            sender_open_id = data.event.sender.sender_id.open_id
            chat_type = message.chat_type  # p2p / group

            # 只处理文本消息
            if message.message_type != "text":
                return
            try:
                text = json.loads(message.content).get("text", "")
            except (json.JSONDecodeError, TypeError):
                return

            # 群聊中：检查 mentions，判断是 @股票分析助手 还是 @股票技术面
            is_tech_mention = False
            if chat_type == "group":
                mentions = message.mentions if hasattr(message, 'mentions') else []
                if not mentions:
                    raw_content = json.loads(message.content)
                    mentions = raw_content.get("mentions", []) if isinstance(raw_content, dict) else []

                mention_names = set()
                for m in mentions:
                    name = m.name if hasattr(m, 'name') else (m.get("name", "") if isinstance(m, dict) else "")
                    mention_names.add(name)

                if "股票技术面" in mention_names:
                    is_tech_mention = True
                elif "股票分析助手" in mention_names:
                    is_tech_mention = False
                elif mention_names:
                    # 有 mention 但名字不匹配（Feishu 偶发字段缺失/别名/字符编码不统一）
                    # 不直接 return（避免静默丢失消息），打警告后按 @股票分析助手 继续处理
                    logger.warning(f"[飞书] 群聊 mention 名未匹配 [{mention_names}]，"
                                   f"按 @股票分析助手 继续")

            # 去掉群聊里的 @机器人 占位符
            text = re.sub(r"@_user_\d+\s*", "", text).strip()
            if not text:
                return

            # 回复通道：私聊回给个人，群聊回到群
            if chat_type == "p2p":
                receive_id, receive_type = sender_open_id, "open_id"
            else:
                receive_id, receive_type = message.chat_id, "chat_id"
                # 自动记录群 chat_id，用于定时任务推送到群
                self._save_known_chat(message.chat_id, "group")

            def reply(msg: str):
                self.notifier.send_to(receive_id, msg, receive_type)

            # 记录消息时间（watchdog 检测连接用）
            now = time.time()
            gap = now - self._last_message_time
            self._last_message_time = now
            if gap > 120:
                logger.warning(f"[飞书] 消息间隔 {gap:.0f} 秒，WebSocket 可能曾断开")
            logger.info(f"[飞书] 收到消息 [{chat_type}] {sender_open_id}: {text[:80]}"
                        f"（push_open_id 未配置时可将此 open_id 填入 local.yaml）")

            # 线程池排队提醒
            queue_size = self.pool._work_queue.qsize()
            if queue_size > 0:
                reply(f"⏳ 当前排队 {queue_size} 个任务，前序分析完成后立即处理…")

            # 快速技术位查询（纯支撑/压力位，不走完整工作流）
            # @股票技术面 → 总是走快速技术位；@股票分析助手 → 不走快速路径
            # p2p 私聊也不走快速路径（用户直接和"股票分析助手"对话，应跑完整分析）
            if is_tech_mention:
                quick = self._quick_technical_query(text)
                if quick:
                    reply(quick)
                    return

            command_result = self._handle_command(text)
            if command_result == "__SCAN__":
                reply("🔍 开始扫描，完成后推送结果…")
                self.pool.submit(self._safe_scan, reply)
                return
            if command_result == "__HELP__":
                self.notifier.send_card_text(HELP_TEXT, "股票分析助手",
                                             receive_id, receive_type)
                return
            if isinstance(command_result, tuple) and command_result[0] == "__REVIEW__":
                reply("📋 复盘中…")
                self.pool.submit(self._run_review, command_result[1], reply)
                return
            if command_result is not None:
                reply(command_result)
                return

            # 走完整分析流程
            reply("📊 收到，分析中（个股约3-6分钟，产业链10分钟以上），完成后回复…")
            thread_id = sender_open_id if chat_type == "p2p" else message.chat_id
            receive_type = "open_id" if chat_type == "p2p" else "chat_id"
            self.pool.submit(self._run_analysis, text, thread_id, reply, receive_type)

        except Exception as e:
            logger.error(f"[飞书] 处理消息异常: {e}\n{traceback.format_exc()}")

    def _ws_watchdog(self):
        """WebSocket 健康看门狗：每 2 分钟检查，断连超过 8 分钟主动触发重连"""
        self._ws_watchdog_running = True
        while self._ws_watchdog_running:
            time.sleep(120)
            gap = time.time() - self._last_message_time
            if gap > 300:
                logger.warning(f"[飞书WS] 已 {gap:.0f} 秒未收到消息，连接可能已断开")

            # 断连超过 8 分钟 → 主动触发重连
            if gap > 480:
                logger.warning(f"[飞书WS] 断连超过 8 分钟，强制重启 WebSocket")
                try:
                    self._ws_client.stop()
                except Exception:
                    pass

    def _ws_connect_loop(self):
        """WebSocket 连接主循环：断连后自动重连（每隔 30 秒重试一次）"""
        import lark_oapi as lark
        # 抑制 lark SDK 自身的 ERROR 日志（"no close frame" 等底层断连日志由我们自行处理）
        logging.getLogger("lark_oapi").setLevel(logging.WARNING)
        while True:
            try:
                handler = lark.EventDispatcherHandler.builder("", "") \
                    .register_p2_im_message_receive_v1(self.on_message) \
                    .build()
                self._ws_client = lark.ws.Client(
                    self.app_id, self.app_secret,
                    event_handler=handler, log_level=lark.LogLevel.INFO,
                )
                logger.info("🚀 飞书 WebSocket 连接已建立")
                self._last_message_time = time.time()  # 重置时间戳，防止看门狗立即误判断连
                self._ws_client.start()  # 阻塞，断开时返回
            except Exception as e:
                logger.error(f"[飞书WS] 连接异常: {e}")
            # 无论是异常退出还是看门狗触发 stop，都等待后重连
            logger.info("[飞书WS] 连接断开，30 秒后重连…")
            time.sleep(30)

    def _start_watchdog(self):
        import threading
        t = threading.Thread(target=self._ws_watchdog, name="feishu-ws-watchdog", daemon=True)
        t.start()
        logger.info("[飞书WS] 健康看门狗已启动（每5分钟检查一次）")

    def _resolve_name(self, code: str) -> str:
        """反查公司名称"""
        if not code:
            return ""
        try:
            from tools.company_code_validator import find_company_name
            return find_company_name(code) or ""
        except Exception:
            return ""

    @staticmethod
    def _quick_technical_query(text: str) -> Optional[str]:
        """
        检测纯技术位查询，直接返回结果，不走完整工作流。

        触发条件（任一）：
        1. 包含技术关键词（支撑/压力/止损/止盈/技术位/关键位）
        2. 文本仅为股票名称/代码（无基本面分析意图），视为快速技术查询

        返回 str 时直接回复；返回 None 时走完整分析。
        """
        # ---------- 第一步：尝试提取股票代码 ----------
        # 优先找 6 位数字代码
        code_match = re.search(r'(\d{6})', text)
        code = code_match.group(1) if code_match else None
        name = None

        # 去掉可能的占位/无意义词，方便匹配公司名
        _noise = re.compile(r'(支撑|压力|止损|止盈|技术位|关键位|技术面|[的\s,，。！？、])', re.I)
        cleaned_text = _noise.sub('', text).strip()

        if not code and cleaned_text:
            from tools.company_code_validator import find_stock_code
            try:
                code = find_stock_code(cleaned_text)
            except Exception:
                code = None
        if not code:
            return None

        # ---------- 第二步：判断是否应作为技术位返回 ----------
        # 情形A：文本包含技术关键词 → 直接返回技术位
        has_tech_kw = bool(re.search(r'(支撑|压力|止损|止盈|技术位|关键位)', text, re.I))
        # 情形B：文本中除了公司名/代码外无分析意图内容 → 默认为技术查询
        has_fund_kw = bool(re.search(
            r'(财报|利润|营收|估值|PE|PB|ROE|护城河|竞争力|同业|对比|产业链|行业|分析|报告|基本面)',
            text, re.I
        ))

        # 清理后只剩公司名 → 视为技术查询
        is_pure_stock = cleaned_text and not re.search(
            r'(财报|利润|营收|估值|分析|报告|监控|复盘|扫描|帮助)',
            cleaned_text, re.I
        )

        if not has_tech_kw and has_fund_kw:
            return None  # 明显的分析意图，走完整工作流
        if not has_tech_kw and not is_pure_stock:
            return None  # 既无技术关键词也不像纯股票名，走完整工作流

        # ---------- 第三步：查询技术位 ----------
        from tools.company_code_validator import find_company_name
        name = find_company_name(code) or code

        db = get_db()
        df = db.get_all_daily_data(code)
        if df is None or df.empty:
            return None

        from tools.support_resistance import compute_sr_levels, format_sr_levels
        sr = compute_sr_levels(df)
        sr_text = format_sr_levels(sr)
        if not sr_text:
            return None

        close = sr.get("close", "N/A")
        lines = [
            f"⚡ {name}({code}) 技术关键位",
            f"**现价**：{close}",
            "",
            sr_text,
        ]
        return "\n".join(lines)

    def _safe_scan(self, reply):
        try:
            result = self.monitor.run_once_now()
            reply(f"✅ {result}")
        except Exception as e:
            reply(f"❌ 扫描失败：{e}")

    # ---------- 启动 ----------

    def _push_startup_help(self):
        """启动时推送使用说明。10分钟内重复启动不重发——launchd 崩溃拉起时防止刷屏，
        同时该提示缺席本身就是'进程在反复重启'的信号（正常重启一定会收到）"""
        bot_name = "股票分析助手"
        marker = Path(f"./data/.last_startup_push_{self.config_section}")
        try:
            if marker.exists() and time.time() - marker.stat().st_mtime < 600:
                logger.info(f"[飞书/{self.config_section}] 10分钟内已推送过启动说明，跳过（崩溃拉起防刷屏）")
                return
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()
            self.notifier.send_card_text(HELP_TEXT, bot_name)
        except Exception as e:
            logger.warning(f"[飞书/{self.config_section}] 启动说明推送失败（不影响运行）: {e}")

    def start(self):
        # 启动监控调度（盘后信号 + 新闻/政策扫描 + 推送）
        self.monitor.set_analysis_runner(
            lambda question: self.pool.submit(
                self._run_analysis, question, "report-trigger", self.notifier.send))
        self.monitor.start()

        # 启动即推送使用说明
        self._push_startup_help()

        if not (self.app_id and self.app_secret):
            logger.warning("[飞书] 未配置 feishu.app_id/app_secret，无法连接")
            return

        # 启动 WebSocket 健康看门狗 + 可自动重连的长连主循环
        self._start_watchdog()
        self._ws_connect_loop()


if __name__ == "__main__":
    from utils.config import ensure_runtime_config
    ensure_runtime_config()
    FeishuBot(config_section="feishu").start()
