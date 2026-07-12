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

from monitoring.notifier import FeishuNotifier
from monitoring.scheduler import MonitorScheduler
from storage.sqlite.stock_storage import get_db
from utils.config import load_config
from utils.logger import logger

HELP_TEXT = """🤖 股票分析助手使用说明
· 直接提问：如「分析比亚迪」「600519 技术面怎么样」「白酒产业链有哪些机会」
· 监控 比亚迪 —— 加入监控清单（公司名或6位代码；识别不到代码时按行业监控）
· 取消监控 比亚迪
· 监控列表
· 立即扫描 —— 马上跑一轮信号+新闻扫描
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

    def _run_analysis(self, question: str, thread_id: str, reply):
        try:
            executor = self._get_workflow()
            state = executor.run_sync(question, thread_id=thread_id)
            answer = executor.get_final_answer(state)
            # 飞书单条文本消息上限约 150KB，超长拆段（按 4000 字符）
            for i in range(0, len(answer), 4000):
                reply(answer[i:i + 4000])
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

        m = re.match(r"^监控\s*(.+)$", text)
        if m:
            return self._add_watch(m.group(1).strip())

        if text in ("立即扫描", "扫描"):
            # 扫描可能要几分钟，丢线程池异步跑
            return "__SCAN__"

        return None

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
            if command_result is not None:
                reply(command_result)
                return

            # 走完整分析流程
            reply("📊 收到，分析中（约2-5分钟，完成后回复）…")
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

    def start(self):
        # 监控调度始终启动（有 watchlist 且配了任一推送通道即可工作）
        self.monitor.start()

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
    FeishuBot().start()
