langchain_multi_agent_kb/
├── README.md               # 项目说明、部署/使用指南
├── requirements.txt         # 固定依赖版本
├── config.yaml             # 🌟 核心配置中心（所有知识库/Agent/模型配置）
├── main.py                 # 项目主入口（统一调度）
# ===================== 核心通用层（所有组件共享，无业务耦合）=====================
├── core/
│   ├── __init__.py
│   ├── llm.py               # 统一封装本地大模型（Ollama）
│   ├── embeddings.py        # 统一封装向量嵌入模型
│   ├── base_knowledge.py    # 🌟 知识库抽象基类（所有知识库继承）
│   ├── base_agent.py        # 🌟 Agent抽象基类（所有Agent继承）
│   └── vector_store.py      # Chroma向量库通用封装（持久化/检索）
# ===================== 多知识库层（每个文件夹 = 1个独立私有知识库）=====================
├── knowledge_bases/
│   ├── __init__.py
│   ├── kb_product/          # 知识库1：产品文档知识库
│   ├── kb_technical/        # 知识库2：技术文档知识库
│   ├── kb_operation/        # 知识库3：运营手册知识库（后续新增）
│   └── registry.py          # 🌟 知识库注册中心（统一管理所有知识库）
# ===================== 多Agent层（每个文件夹 = 1个独立智能Agent）=====================
├── agents/
│   ├── __init__.py
│   ├── qa_agent/            # Agent1：知识库问答Agent
│   ├── summary_agent/       # Agent2：文档总结Agent
│   ├── router_agent/        # Agent3：路由Agent（自动分配问题到对应知识库）
│   └── registry.py          # 🌟 Agent注册中心（统一管理所有Agent）
# ===================== 工具层 =====================
├── utils/
│   ├── __init__.py
│   ├── logger.py            # 日志系统
│   ├── config.py            # 配置加载器
│   └── file_tools.py        # 文档加载/切分通用工具
# ===================== 数据与存储 =====================
├── data/                    # 原始文档（按知识库分类）
│   ├── kb_product/
│   └── kb_technical/
├── storage/                 # 本地持久化存储
│   ├── chroma/              # 🌟 每个知识库独立Chroma向量库
│   │   ├── kb_product/
│   │   └── kb_technical/
│   └── cache/               # 模型缓存
└── logs/                    # 运行日志