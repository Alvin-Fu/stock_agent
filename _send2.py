#!/usr/bin/env python3
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from monitoring.notifier import FeishuNotifier
from feishu_bot import _report_to_cards

notifier = FeishuNotifier()
oid = notifier.push_open_id
with open("data/reports/20260722_141930_002594.md") as f:
    text = f.read()
cards = _report_to_cards(text, "比亚迪(002594)")
print(f"卡片数: {len(cards)}")
for i, c in enumerate(cards):
    print(f"  卡{i+1}: {len(c['elements'])} 元素")
ok = notifier.send_card_chunked(cards, oid)
print(f"{'✅' if ok else '❌'} 发送完成")
