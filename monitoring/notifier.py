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

    def _send_via_webhook(self, text: str) -> bool:
        resp = requests.post(
            self.webhook_url,
            json={"msg_type": "text", "content": {"text": text}},
            timeout=10,
        )
        data = resp.json()
        ok = data.get("code") == 0 or data.get("StatusCode") == 0
        if not ok:
            logger.error(f"飞书 webhook 返回异常: {data}")
        return ok

    def _send_via_app(self, receive_id: str, text: str, receive_id_type: str = "open_id") -> bool:
        import lark_oapi as lark
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

        client = self._get_lark_client()
        req = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(receive_id)
                .msg_type("text")
                .content(json.dumps({"text": text}, ensure_ascii=False))
                .build()
            ).build()
        resp = client.im.v1.message.create(req)
        if not resp.success():
            logger.error(f"飞书应用发消息失败: code={resp.code}, msg={resp.msg}")
            return False
        return True

    def send_to(self, receive_id: str, text: str, receive_id_type: str = "open_id") -> bool:
        """对话机器人回复用：向指定用户/群发送消息（需要应用凭据）"""
        if not (self.app_id and self.app_secret):
            logger.error("未配置 feishu.app_id/app_secret，无法发送应用消息")
            return False
        try:
            return self._send_via_app(receive_id, text, receive_id_type)
        except Exception as e:
            logger.error(f"飞书发送消息失败: {e}")
            return False
