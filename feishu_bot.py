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
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from monitoring.notifier import FeishuNotifier
from monitoring.scheduler import MonitorScheduler
from storage.sqlite.stock_storage import get_db
from utils.config import load_config
from utils.logger import logger

def split_report(text: str, limit: int = 4000) -> list:
    """
    长报告切分：优先在 ##/### 标题边界断段，段内超限再按行边界硬切，
    绝不在表格行/句子中间截断。返回切好的段列表。
    """
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


HELP_TEXT = """🤖 股票分析助手使用说明
· 直接提问：如「分析比亚迪」「600519 技术面怎么样」「白酒产业链有哪些机会」
· 一条消息可以带多个对象：如「分析比亚迪和宁德时代」「看看比亚迪，再看看半导体产业链」
  （自动拆开逐个分析，最多4个，最后附组合速览）
· 监控 比亚迪 —— 加入监控清单（公司名或6位代码；识别不到代码时按行业监控）
· 取消监控 比亚迪
· 监控列表
· 立即扫描 —— 马上跑一轮信号+新闻扫描
· 复盘 比亚迪 —— 对最近一次分析做复盘（对照实际走势）
· 纠错 比亚迪 销量应该是38万不是41万 —— 指出报告里的错误，系统记住，下次分析严禁再犯
· 纠错列表 —— 查看已记录的纠错
· 帮助 —— 显示本说明
监控提醒：盘后技术信号 + 个股新闻/行业政策，自动推送到这里"""


class FeishuBot:
    def __init__(self):
        cfg = load_config().get("feishu", {}) or {}
        self.app_id = (cfg.get("app_id") or "").strip()
        self.app_secret = (cfg.get("app_secret") or "").strip()
        self.db = get_db()
        self.notifier = FeishuNotifier()
        self.monitor = MonitorScheduler(self.notifier)
        # 分析耗时较长，串行池防止并发打爆 LLM/数据源
        self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="feishu-worker")
        self._executor = None  # WorkflowExecutor 延迟创建（首条消息时）

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

    def _run_analysis(self, question: str, thread_id: str, reply):
        try:
            executor = self._get_workflow()

            # 入口任务拆解：一条消息里含多个公司/行业时逐个分析（拆解失败自动回落单任务）
            from orchestration.task_splitter import split_tasks
            tasks = split_tasks(question)

            if len(tasks) <= 1:
                state = executor.run_sync(question, thread_id=thread_id)
                self._send_answer(executor.get_final_answer(state), reply)
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
                    self._send_answer(f"【{label}】\n{answer}", reply)
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
            return HELP_TEXT

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
            # 去掉群聊里的 @机器人 占位符
            text = re.sub(r"@_user_\d+\s*", "", text).strip()
            if not text:
                return

            # 回复通道：私聊回给个人，群聊回到群
            if chat_type == "p2p":
                receive_id, receive_type = sender_open_id, "open_id"
            else:
                receive_id, receive_type = message.chat_id, "chat_id"

            def reply(msg: str):
                self.notifier.send_to(receive_id, msg, receive_type)

            logger.info(f"[飞书] 收到消息 [{chat_type}] {sender_open_id}: {text[:80]}"
                        f"（push_open_id 未配置时可将此 open_id 填入 local.yaml）")

            command_result = self._handle_command(text)
            if command_result == "__SCAN__":
                reply("🔍 开始扫描，完成后推送结果…")
                self.pool.submit(self._safe_scan, reply)
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
            self.pool.submit(self._run_analysis, text, thread_id, reply)

        except Exception as e:
            logger.error(f"[飞书] 处理消息异常: {e}\n{traceback.format_exc()}")

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
        marker = Path("./data/.last_startup_push")
        try:
            if marker.exists() and time.time() - marker.stat().st_mtime < 600:
                logger.info("[飞书] 10分钟内已推送过启动说明，跳过（崩溃拉起防刷屏）")
                return
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()
            self.notifier.send("🚀 股票分析助手已启动，监控调度运行中\n\n" + HELP_TEXT)
        except Exception as e:
            logger.warning(f"[飞书] 启动说明推送失败（不影响运行）: {e}")

    def start(self):
        # 财报发布触发的自动重分析：丢进工作线程池跑，结果推到默认通道
        self.monitor.set_analysis_runner(
            lambda question: self.pool.submit(
                self._run_analysis, question, "report-trigger", self.notifier.send))

        # 监控调度始终启动（有 watchlist 且配了任一推送通道即可工作）
        self.monitor.start()

        # 启动即推送使用说明（纯推送模式也发：webhook 通道同样能收到）
        self._push_startup_help()

        if not (self.app_id and self.app_secret):
            logger.warning("[飞书] 未配置 feishu.app_id/app_secret，进入纯监控推送模式（无法对话）")
            try:
                while True:
                    time.sleep(60)
            except KeyboardInterrupt:
                self.monitor.stop()
            return

        import lark_oapi as lark

        handler = lark.EventDispatcherHandler.builder("", "") \
            .register_p2_im_message_receive_v1(self.on_message) \
            .build()

        ws_client = lark.ws.Client(
            self.app_id,
            self.app_secret,
            event_handler=handler,
            log_level=lark.LogLevel.INFO,
        )
        logger.info("🚀 飞书机器人启动（长连接模式），给机器人发「帮助」查看用法")
        ws_client.start()  # 阻塞运行


if __name__ == "__main__":
    from utils.config import ensure_runtime_config
    ensure_runtime_config()  # 关键配置缺失时启动即报错，不等跑到 LLM 调用才炸
    FeishuBot().start()
