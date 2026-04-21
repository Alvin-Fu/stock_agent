"""
Streamlit Web 界面
用法: streamlit run app/web_ui.py
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import streamlit as st
from orchestration.workflow import WorkflowExecutor

st.set_page_config(
    page_title="财经知识库助手",
    page_icon="📈",
    layout="centered"
)

st.title("📈 多 Agent 财经知识库")
st.caption("基于 LangGraph 的专业财务分析助手")

# 初始化会话状态
if "executor" not in st.session_state:
    st.session_state.executor = WorkflowExecutor(enable_memory=True)
if "messages" not in st.session_state:
    st.session_state.messages = []
if "thread_id" not in st.session_state:
    import uuid
    st.session_state.thread_id = str(uuid.uuid4())

# 显示历史消息
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# 输入框
if prompt := st.chat_input("请输入您的财经问题..."):
    # 显示用户消息
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 处理回答
    with st.chat_message("assistant"):
        with st.spinner("分析中，请稍候..."):
            executor = st.session_state.executor
            state = executor.run_sync(prompt, thread_id=st.session_state.thread_id)
            answer = executor.get_final_answer(state)

            st.markdown(answer)

            # 可展开查看详情
            with st.expander("🔍 查看分析详情"):
                st.json({
                    "intent": state.get("intent"),
                    "next_agent": state.get("next_agent"),
                    "steps": [str(step) for step in state.get("intermediate_steps", [])],
                    "documents_count": len(state.get("documents", []))
                })

    st.session_state.messages.append({"role": "assistant", "content": answer})