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
import re

import requests

from utils.config import load_config
from utils.logger import logger


class FeishuNotifier:
    def __init__(self, config_section: str = "feishu"):
        cfg = load_config().get(config_section, {}) or {}
        self.app_id = (cfg.get("app_id") or "").strip()
        self.app_secret = (cfg.get("app_secret") or "").strip()
        raw = (cfg.get("push_open_id") or "").strip()
        # 支持多人：逗号/分号分隔，单个也兼容
        self.push_open_ids = [x.strip() for x in raw.replace("\n", ",").split(",") if x.strip()]
        self.push_open_id = self.push_open_ids[0] if self.push_open_ids else ""
        self.webhook_url = (cfg.get("webhook_url") or "").strip()
        self._lark_client = None
        self._lock = threading.Lock()
        self.config_section = config_section

        if not self._app_ready() and not self.webhook_url:
            logger.warning(f"飞书推送未配置（{config_section}.app_id/app_secret+push_open_id 或 {config_section}.webhook_url），"
                           "监控事件只写日志不推送")

    def _app_ready(self) -> bool:
        return bool(self.app_id and self.app_secret and self.push_open_ids)

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

    # ========== 通用发送 ==========

    def send(self, text: str, msg_type: str = "auto") -> bool:
        """
        发送一条消息到默认订阅者（push_open_ids）。
        msg_type: "auto"=根据内容判断, "text"=纯文本, "post"=富文本
        """
        if msg_type == "auto":
            has_table = "|" in text and "---" in text
            msg_type = "post" if (len(text) > 100 or has_table) else "text"
        if self._app_ready():
            ok = False
            for oid in self.push_open_ids:
                oid_ok = False
                for try_type in (msg_type, "text" if msg_type == "post" else None):
                    if try_type is None:
                        break
                    try:
                        if self._send_via_app(oid, text, msg_type=try_type):
                            oid_ok = True
                            break
                    except Exception as e:
                        logger.error(f"飞书推送失败({oid[:16]}..., {try_type}): {e}")
                if oid_ok:
                    ok = True
            if ok:
                return True
            logger.warning(f"全部 {len(self.push_open_ids)} 个 open_id 推送失败，尝试 webhook 兜底")
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
        """发送交互式卡片消息。"""
        if not (self.app_id and self.app_secret):
            logger.error("未配置 feishu.app_id/app_secret，无法发送卡片消息")
            return False
        import lark_oapi as lark
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody
        import time as _time
        client = self._get_lark_client()
        content = json.dumps(card_content, ensure_ascii=False)
        for retry in range(3):
            try:
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
                    raise RuntimeError(f"飞书卡片发送失败: code={resp.code}, msg={resp.msg}")
                return True
            except Exception as e:
                if retry < 2:
                    wait = 2 ** (retry + 1)
                    logger.warning(f"卡片推送失败（第{retry+1}次），{wait}秒后重试: {e}")
                    _time.sleep(wait)
                else:
                    logger.error(f"卡片推送3次重试均失败: {e}")
                    raise
        return False  # 不可达，保持类型安全

    def send_card_text(self, text: str, title: str = "定时报告",
                       receive_id: str = "", receive_id_type: str = "open_id",
                       task_id: str = "") -> bool:
        """
        将 markdown 文本转换为交互式卡片发送。
        receive_id 为空时使用 task_id 查 DB 获取订阅者，
        task_id 也为空时使用 push_open_id（默认推送目标）。
        """
        if not text or not text.strip():
            return False
        import lark_oapi as lark
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

        # 确定接收者
        if receive_id:
            target_ids = [receive_id]
            target_type = receive_id_type
        elif task_id:
            from storage.sqlite.stock_storage import get_db
            db = get_db()
            target_ids = db.get_task_subscribers(task_id)
            target_type = "open_id"
            if not target_ids:
                logger.info(f"[任务订阅] task_id={task_id} 无订阅者，退回到 push_open_ids")
                target_ids = self.push_open_ids
                target_type = "open_id"
        else:
            target_ids = self.push_open_ids
            target_type = "open_id"

        ok = True
        for rid in target_ids:
            if not rid:
                continue
            if not (self.app_id and self.app_secret):
                if not self.send(text):
                    ok = False
                continue
            try:
                from feishu_bot import _report_to_cards
                cards = _report_to_cards(text, title)
                if cards:
                    self.send_card_chunked(cards, rid, target_type)
                else:
                    self.send_to(rid, text, target_type)
            except Exception as e:
                logger.debug(f"[卡片] send_card_text 转卡/发送失败({rid[:16]}...): {e}")
                try:
                    self.send_to(rid, text, target_type)
                except Exception as e2:
                    logger.error(f"[卡片] 降级文本也失败({rid[:16]}...): {e2}")
                    ok = False
        return ok

    def send_card_chunked(self, cards: list, receive_id: str,
                          receive_id_type: str = "open_id") -> bool:
        """发送多张卡片（适用于超长报告分多卡）。"""
        import time as _time
        ok = True
        failed_cards = []
        for i, card in enumerate(cards):
            sent = False
            for retry in range(2):
                try:
                    self.send_card(receive_id, card, receive_id_type)
                    sent = True
                    break
                except Exception as e:
                    if retry < 1:
                        logger.warning(f"第{i+1}张卡片发送失败，重试: {e}")
                        _time.sleep(3)
                    else:
                        logger.error(f"第{i+1}张卡片2次重试均失败，跳过: {e}")
            if not sent:
                failed_cards.append(i + 1)
                ok = False
        if failed_cards:
            logger.error(f"[卡片] 以下卡片发送失败: {failed_cards}")
        return ok

    # ========== 定时任务订阅管理 ==========

    def send_to_task(self, task_id: str, text: str, title: str = "定时报告") -> bool:
        """向指定定时任务的所有订阅者推送消息"""
        return self.send_card_text(text, title=title, task_id=task_id)

    def subscribe(self, task_id: str, open_id: str) -> bool:
        """订阅定时任务"""
        from storage.sqlite.stock_storage import get_db
        db = get_db()
        return db.subscribe_task(task_id, open_id)

    def unsubscribe(self, task_id: str, open_id: str) -> bool:
        """取消订阅定时任务"""
        from storage.sqlite.stock_storage import get_db
        db = get_db()
        return db.unsubscribe_task(task_id, open_id)

    def get_subscriptions(self, open_id: str) -> list:
        """查询某个用户订阅了哪些任务"""
        from storage.sqlite.stock_storage import get_db
        db = get_db()
        return db.get_all_subscriptions(open_id)

    def get_task_subscribers(self, task_id: str) -> list:
        """查询某个任务的订阅者列表"""
        from storage.sqlite.stock_storage import get_db
        db = get_db()
        return db.get_task_subscribers(task_id)

    # ========== 飞书文档 ==========

    def create_feishu_doc(self, title: str, markdown_text: str) -> str:
        """创建飞书文档并写入内容；返回文档 URL；失败返回空串。"""
        if not (self.app_id and self.app_secret):
            logger.error("未配置 feishu.app_id/app_secret，无法创建文档")
            return ""

        import lark_oapi as lark
        client = self._get_lark_client()

        try:
            doc_req = lark.api.docx.v1.model.CreateDocumentRequest.builder() \
                .request_body(
                    lark.api.docx.v1.model.CreateDocumentRequestBody.builder()
                    .title(title)
                    .build()
                ).build()
            doc_resp = client.docx.v1.document.create(doc_req)
            if not doc_resp.success():
                logger.error(f"飞书文档创建失败: code={doc_resp.code}, msg={doc_resp.msg}")
                return ""
            document_id = doc_resp.data.document.document_id
            logger.info(f"[飞书文档] 创建成功: {document_id}")
        except Exception as e:
            logger.error(f"飞书文档创建异常: {e}")
            return ""

        # 给用户添加编辑权限
        try:
            if self.push_open_id:
                from lark_oapi.core.token.manager import TokenManager
                lark_token = TokenManager.get_self_tenant_token(client.config)
                perm_resp = requests.post(
                    f"https://open.feishu.cn/open-apis/drive/v1/permissions/{document_id}/members?type=docx&need_notification=false",
                    headers={"Authorization": f"Bearer {lark_token}",
                             "Content-Type": "application/json; charset=utf-8"},
                    json={
                        "member_type": "openid",
                        "member_id": self.push_open_id,
                        "perm": "full_access"
                    }
                )
                pr = perm_resp.json()
                if pr.get("code") == 0:
                    logger.info(f"[飞书文档] 已授予编辑权限: {self.push_open_id}")
                else:
                    logger.warning(f"[飞书文档] 权限授予失败: code={pr.get('code')}, msg={pr.get('msg')}")
        except Exception as e:
            logger.warning(f"[飞书文档] 权限授予异常（不影响写入）: {e}")

        content_items = self._md_to_docx_items(markdown_text)
        logger.info(f"[飞书文档] 解析得到 {len(content_items)} 个内容项（{sum(1 for x in content_items if x['type']=='block')} 普通块 + {sum(1 for x in content_items if x['type']=='table')} 表格）")
        if len(markdown_text) < 200:
            logger.info(f"[飞书文档] 内容预览（全文）: {markdown_text[:200]}")
        else:
            logger.info(f"[飞书文档] 内容前200字: {markdown_text[:200]}")
        import requests as http_req
        from lark_oapi.core.token.manager import TokenManager
        lark_token = TokenManager.get_self_tenant_token(client.config)

        flat_buffer = []
        table_idx = 0
        flat_ok = 0
        table_ok = 0

        def _flush_flat():
            nonlocal flat_ok
            if not flat_buffer:
                return
            batch_size = 50
            for i in range(0, len(flat_buffer), batch_size):
                batch = flat_buffer[i:i + batch_size]
                try:
                    body_builder = lark.api.docx.v1.model.CreateDocumentBlockChildrenRequestBody.builder()
                    body_builder.children(batch)
                    body_builder.index(-1)
                    req = lark.api.docx.v1.model.CreateDocumentBlockChildrenRequest.builder() \
                        .document_id(document_id) \
                        .block_id(document_id) \
                        .request_body(body_builder.build()) \
                        .build()
                    resp = client.docx.v1.document_block_children.create(req)
                    if resp.success():
                        flat_ok += len(batch)
                    else:
                        logger.error(f"[飞书文档] 普通块写入失败: code={resp.code}, msg={resp.msg}")
                except Exception as e:
                    logger.error(f"[飞书文档] 普通块写入异常: {e}")
            flat_buffer.clear()

        for item in content_items:
            if item["type"] == "block":
                flat_buffer.append(item["data"])
            elif item["type"] == "table":
                _flush_flat()
                tbl = item["data"]
                try:
                    n_rows, n_cols = tbl["n_rows"], tbl["n_cols"]
                    rows = tbl["rows"]
                    tid = table_idx
                    table_idx += 1
                    tbl_id = f"tb_{tid}"
                    cell_ids = [f"tb_{tid}_c_{ri}_{ci}" for ri in range(n_rows) for ci in range(n_cols)]

                    descendants = [{
                        "block_id": tbl_id,
                        "block_type": 31,
                        "table": {"property": {"row_size": n_rows, "column_size": n_cols}},
                        "children": cell_ids
                    }]
                    for ri in range(n_rows):
                        for ci in range(n_cols):
                            cell_id = f"tb_{tid}_c_{ri}_{ci}"
                            text_id = f"tb_{tid}_t_{ri}_{ci}"
                            ct = rows[ri][ci] if ci < len(rows[ri]) else ""
                            el = (self._parse_inline_elements(f"**{ct}**")
                                  if ri == 0 else self._parse_inline_elements(ct))
                            descendants.append({
                                "block_id": cell_id, "block_type": 32,
                                "table_cell": {}, "children": [text_id]
                            })
                            descendants.append({
                                "block_id": text_id, "block_type": 2,
                                "text": {"elements": el}, "children": []
                            })
                    d_resp = http_req.post(
                        f"https://open.feishu.cn/open-apis/docx/v1/documents/{document_id}/blocks/{document_id}/descendant",
                        headers={"Authorization": f"Bearer {lark_token}",
                                 "Content-Type": "application/json; charset=utf-8"},
                        json={"index": -1, "children_id": [tbl_id], "descendants": descendants}
                    )
                    if d_resp.json().get("code") == 0:
                        table_ok += 1
                except Exception as e:
                    logger.error(f"[飞书文档] 表格创建异常: {e}")

        _flush_flat()
        logger.info(f"[飞书文档] 写入完成: {flat_ok} 普通块 + {table_ok} 表格")
        return f"https://www.feishu.cn/docx/{document_id}"

    # ========== Markdown 解析 ==========

    @staticmethod
    def _parse_inline_elements(text: str) -> list:
        """将行内文本中的 **加粗** 和 <font color='...'>...</font> 解析为飞书元素列表。"""
        _COLOR_MAP = {
            "red": {"red": 230, "green": 50, "blue": 50},
            "green": {"red": 0, "green": 170, "blue": 0},
        }
        pattern = re.compile(
            r'\*\*([^*]+)\*\*'
            r'|<font color=\'([^\']+)\'>([^<]+)</font>'
        )
        elements = []
        pos = 0
        for m in pattern.finditer(text):
            if m.start() > pos:
                elements.append({
                    "text_run": {"content": text[pos:m.start()], "text_element_style": {}}
                })
            if m.group(1) is not None:
                elements.append({
                    "text_run": {"content": m.group(1), "text_element_style": {"bold": True}}
                })
            else:
                rgb = _COLOR_MAP.get(m.group(2), {"red": 0, "green": 0, "blue": 0})
                elements.append({
                    "text_run": {"content": m.group(3), "text_element_style": {"text_color": rgb}}
                })
            pos = m.end()
        if pos < len(text):
            elements.append({
                "text_run": {"content": text[pos:], "text_element_style": {}}
            })
        return elements if elements else [{"text_run": {"content": text, "text_element_style": {}}}]

    @staticmethod
    def _md_to_docx_items(markdown_text: str) -> list:
        """将 markdown 文本解析为有序内容项列表（block/table）。"""
        parse = FeishuNotifier._parse_inline_elements
        items = []
        lines = markdown_text.strip().split("\n")
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            if line.startswith("## ") and not line.startswith("### "):
                items.append({"type": "block", "data": {
                    "block_type": 4,
                    "heading2": {"elements": parse(line[3:].strip())}
                }})
                i += 1
                continue
            if line.startswith("### "):
                items.append({"type": "block", "data": {
                    "block_type": 5,
                    "heading3": {"elements": parse(line[4:].strip())}
                }})
                i += 1
                continue
            if re.match(r"^---+\s*$", line):
                items.append({"type": "block", "data": {"block_type": 22, "divider": {}}})
                i += 1
                continue
            sep = '│' if '│' in line else None
            is_pipe_table = line.startswith("|")
            if sep or is_pipe_table:
                table_lines = []
                while i < len(lines):
                    sl = lines[i].strip()
                    if (sep and '│' in sl) or (is_pipe_table and sl.startswith("|")):
                        table_lines.append(sl)
                        i += 1
                    elif sl.strip().startswith(":--") or sl.strip().startswith("---"):
                        i += 1
                    else:
                        break
                sep_char = sep if sep else '|'
                raw_rows = []
                for row in table_lines:
                    cells = [c.strip() for c in row.split(sep_char) if c.strip()]
                    if all(re.match(r'^:?-{2,}:?$', c) for c in cells):
                        continue
                    if cells:
                        raw_rows.append(cells)
                if len(raw_rows) >= 2:
                    n_cols = max(len(r) for r in raw_rows)
                    padded = [r + [''] * (n_cols - len(r)) for r in raw_rows]
                    items.append({"type": "table", "data": {
                        "n_rows": len(padded), "n_cols": n_cols, "rows": padded
                    }})
                else:
                    for row in raw_rows:
                        items.append({"type": "block", "data": {
                            "block_type": 2,
                            "text": {"elements": parse(" │ ".join(row))}
                        }})
                continue
            if line.startswith("- ") or line.startswith("• "):
                bline = line[2:].strip() if line.startswith("- ") else line[1:].strip()
                raw_line = lines[i]
                level = (len(raw_line) - len(raw_line.lstrip())) // 2
                items.append({"type": "block", "data": {
                    "block_type": 12,
                    "bullet": {"elements": parse(bline), "style": {"level": level}}
                }})
                i += 1
                continue
            paragraph = line
            i += 1
            while i < len(lines):
                next_line = lines[i].strip()
                if not next_line or next_line.startswith("## ") \
                        or next_line.startswith("### ") \
                        or next_line.startswith("- ") or next_line.startswith("• ") \
                        or next_line.startswith("|") or '│' in next_line \
                        or re.match(r"^---+", next_line):
                    break
                paragraph += "\n" + next_line
                i += 1
            if paragraph.strip():
                items.append({"type": "block", "data": {
                    "block_type": 2,
                    "text": {"elements": parse(paragraph)}
                }})
        return items

    # ========== 底层 API 调用 ==========

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
