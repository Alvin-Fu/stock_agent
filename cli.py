#!/usr/bin/env python
"""
命令行交互界面
用法: python -m app.cli
"""

import sys
from pathlib import Path

from orchestration.workflow import WorkflowExecutor
from utils.logger import logger



def print_banner():
    print("\n" + "=" * 50)
    print("   📈 多 Agent 财经知识库问答系统")
    print("=" * 50)
    print("输入您的问题，输入 'exit' 或 'quit' 退出\n")


def main():
    print_banner()
    executor = WorkflowExecutor(enable_memory=False)  # CLI 可关闭记忆

    while True:
        try:
            question = input("💬 用户: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n👋 再见！")
            break

        if question.lower() in ("exit", "quit", "q"):
            print("👋 再见！")
            break

        if not question:
            continue

        print("🤔 思考中...", end="", flush=True)
        state = executor.run_sync(question)
        answer = executor.get_final_answer(state)

        print("\r✅ 回答:")
        print(f"   {answer}\n")
        print("-" * 50)


if __name__ == "__main__":
    main()