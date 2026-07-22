# -*- coding: utf-8 -*-
"""
飞书推送器
两种通道（按配置自动选择）：
1. 企业自建应用机器人（配了 feishu.app_id/app_secret + push_open_id）→ 私聊推送，与对话机器人同一应用
2. 群自定义机器人 webhook（配了 feishu.webhook_url）→ 群推送，10 分钟就能配好
两者都配时优先应用私聊；都没配则只记日志。
"""

import json
import threading

import requests

from utils.config import load_config
from utils.logger import logger


class FeishuNotifier:
    def __init__(self):
        cfg = load_config().get("feishu", {}) or {}
        self.app_id = (cfg.get("app_id") or "").strip()
        self.app_secret = (cfg.get("app_secret") or "").strip()
        self.push_open_id = (cfg.get("push_open_id") or "").strip()
        self.webhook_url = (cfg.get("webhook_url") or "").strip()
        self._lark_client = None
        self._lock = threading.Lock()

        if not self._app_ready() and not self.webhook_url:
            logger.warning("飞书推送未配置（feishu.app_id/app_secret+push_open_id 或 feishu.webhook_url），"
                           "监控事件只写日志不推送")

    def _app_ready(self) -> bool:
        return bool(self.app_id and self.app_secret and self.push_open_id)

    def _get_lark_client(self):
        """延迟创建 lark 客户端（未安装 lark-oapi 时优雅降级到 webhook）"""
        if self._lark_client is not None:
            return self._lark_client
        with self._lock:
            if self._lark_client is None:
                import lark_oapi as lark
                self._lark_client = lark.Client.builder() \
                    .app_id(self.app_id).app_secret(self.app_secret).build()
        return self._lark_client

    def send(self, text: str) -> bool:
        """发送一条文本消息，按配置选择通道；失败返回 False"""
        if self._app_ready():
            try:
                return self._send_via_app(self.push_open_id, text)
            except Exception as e:
                logger.error(f"飞书应用推送失败，尝试 webhook 兜底: {e}")
        if self.webhook_url:
            try:
                return self._send_via_webhook(text)
            except Exception as e:
                logger.error(f"飞书 webhook 推送失败: {e}")
                return False
        logger.info(f"[未配置飞书推送] {text[:200]}")
        return False

    def _build_post_content(self, text: str) -> dict:
        """将文本内容转换为飞书 post 格式（支持 Markdown 表格、加粗等）"""
        return {
            "post": {
                "zh_cn": {
                    "title": "报告",
                    "content": [[{"tag": "text", "text": text}]]
                }
            }
        }

    def send(self, text: str, msg_type: str = "auto") -> bool:
        """
        发送一条消息，按配置选择通道。
        msg_type: "auto"=根据内容长度判断, "text"=纯文本, "post"=富文本
        """
        if msg_type == "auto":
            has_table = "|" in text and "---" in text
            msg_type = "post" if (len(text) > 100 or has_table) else "text"
        if self._app_ready():
            for try_type in (msg_type, "text" if msg_type == "post" else None):
                if try_type is None:
                    break
                try:
                    if self._send_via_app(self.push_open_id, text, msg_type=try_type):
                        return True
                except Exception as e:
                    logger.error(f"飞书应用推送失败 ({try_type}): {e}")
                if try_type == "post":
                    logger.warning("post 格式失败，尝试 webhook 兜底")
                    break  # 走 webhook 兜底
        if self.webhook_url:
            try:
                return self._send_via_webhook(text, msg_type=msg_type)
            except Exception as e:
                logger.error(f"飞书 webhook 推送失败: {e}")
                return False
        logger.info(f"[未配置飞书推送] {text[:200]}")
        return False

    def send_to(self, receive_id: str, text: str,
                receive_id_type: str = "open_id", msg_type: str = "auto") -> bool:
        """对话机器人回复用：向指定用户/群发送消息"""
        if msg_type == "auto":
            has_table = "|" in text and "---" in text
            msg_type = "post" if (len(text) > 100 or has_table) else "text"
        if not (self.app_id and self.app_secret):
            logger.error("未配置 feishu.app_id/app_secret，无法发送应用消息")
            return False
        for try_type in (msg_type, "text" if msg_type == "post" else None):
            if try_type is None:
                break
            try:
                if self._send_via_app(receive_id, text, receive_id_type, msg_type=try_type):
                    return True
            except Exception as e:
                logger.error(f"飞书发送消息失败 ({try_type}): {e}")
            if try_type == "post":
                logger.warning("post 格式失败，降级为 text 重试")
        return False

    def send_card(self, receive_id: str, card_content: dict,
                  receive_id_type: str = "open_id") -> bool:
        """
        发送交互式卡片消息（interactive）。
        card_content 为卡片完整 JSON dict（含 config/header/elements）。
        仅支持应用 API 通道（webhook 不支持 interactive）。
        """
        if not (self.app_id and self.app_secret):
            logger.error("未配置 feishu.app_id/app_secret，无法发送卡片消息")
            return False
        import lark_oapi as lark
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody
        client = self._get_lark_client()
        content = json.dumps(card_content, ensure_ascii=False)
        req = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(receive_id)
                .msg_type("interactive")
                .content(content)
                .build()
            ).build()
        resp = client.im.v1.message.create(req)
        if not resp.success():
            logger.error(f"飞书卡片发送失败: code={resp.code}, msg={resp.msg}")
            return False
        return True

    def send_card_chunked(self, cards: list, receive_id: str,
                          receive_id_type: str = "open_id") -> bool:
        """
        发送多张卡片（适用于超长报告分多卡）。cards 为 list[card_dict]。
        每张卡独立发送，如某张失败继续下一张。
        """
        ok = True
        for i, card in enumerate(cards):
            try:
                self.send_card(receive_id, card, receive_id_type)
            except Exception as e:
                logger.error(f"[卡片] 第{i+1}/{len(cards)}张发送失败: {e}")
                ok = False
        return ok

    def _send_via_webhook(self, text: str, msg_type: str = "text") -> bool:
        payload = {"msg_type": msg_type}
        if msg_type == "post":
            payload["content"] = self._build_post_content(text)
        else:
            payload["content"] = {"text": text}
        resp = requests.post(
            self.webhook_url,
            json=payload,
            timeout=10,
        )
        data = resp.json()
        ok = data.get("code") == 0 or data.get("StatusCode") == 0
        if not ok:
            logger.error(f"飞书 webhook 返回异常: {data}")
        return ok

    def _build_app_post_content(self, text: str) -> dict:
        """构建应用 API 的 post 消息 content（无外层 {"post":...} 包裹）"""
        return {
            "zh_cn": {
                "title": "报告",
                "content": [[{"tag": "text", "text": text}]]
            }
        }

    def _send_via_app(self, receive_id: str, text: str,
                      receive_id_type: str = "open_id",
                      msg_type: str = "text") -> bool:
        import lark_oapi as lark
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

        client = self._get_lark_client()
        if msg_type == "post":
            content = json.dumps(self._build_app_post_content(text), ensure_ascii=False)
        else:
            content = json.dumps({"text": text}, ensure_ascii=False)
        req = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(receive_id)
                .msg_type(msg_type)
                .content(content)
                .build()
            ).build()
        resp = client.im.v1.message.create(req)
        if not resp.success():
            logger.error(f"飞书应用发消息失败: code={resp.code}, msg={resp.msg}")
            return False
        return True
