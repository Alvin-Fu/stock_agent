#from langchain_community.document_loaders import DirectoryLoader, TextLoader, PyPDFLoader, UnstructuredWordDocumentLoader
import os

from langchain_community.document_loaders import DirectoryLoader, PyPDFLoader  # 核心：导入 PyPDFLoader
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

from .logger import logger


def load_documents_from_dir(dir_path: str, glob: str = "**/*.pdf") -> list[Document]:
    """
    Plan B版：使用轻量级 PyPDFLoader 从文件夹加载所有 PDF
    """
    if not os.path.exists(dir_path):
        logger.error(f"文档目录不存在：{dir_path}")
        raise FileNotFoundError(f"文档目录不存在：{dir_path}")

    # 定义加载参数
    # 注意：PyPDFLoader 本身比较轻量，DirectoryLoader 的多线程支持会让它更快
    loader_kwargs = {
        "show_progress": True,
        "use_multithreading": True,
        "loader_cls": PyPDFLoader, # 核心：强制指定使用 PyPDFLoader，避开 unstructured
        "silent_errors": True      # 核心：遇到个别坏文件跳过，不崩溃
    }

    # 加载文件夹内文档
    loader = DirectoryLoader(
        path=dir_path,
        glob=glob, # 建议默认传 "**/*.pdf"
        **loader_kwargs
    )

    try:
        documents = loader.load()
        logger.info(f"✅ 成功加载 {len(documents)} 个原始文档页 | 路径：{dir_path}")
        return documents
    except Exception as e:
        logger.error(f"❌ 文档加载失败：{str(e)}")
        raise

# split_documents 函数保持不变，原来的代码完全兼容


"""
    通用：从文件夹加载所有支持的文档
    :param dir_path: 文档文件夹路径
    :param glob: 文件匹配规则（默认所有文件）
    :return: 文档列表
"""
"""
def load_documents_from_dir(dir_path: str, glob: str = "*.*") -> list[Document]:
    
    if not os.path.exists(dir_path):
        logger.error(f"文档目录不存在：{dir_path}")
        raise FileNotFoundError(f"文档目录不存在：{dir_path}")

    # 支持的文件格式：TXT/PDF/MD/DOCX
    loader_kwargs = {
        "show_progress": True,
        "use_multithreading": True
    }

    # 加载文件夹内所有文档
    loader = DirectoryLoader(
        path=dir_path,
        glob=glob,
        **loader_kwargs
    )

    try:
        documents = loader.load()
        logger.info(f"✅ 成功加载 {len(documents)} 个原始文档 | 路径：{dir_path}")
        return documents
    except Exception as e:
        logger.error(f"❌ 文档加载失败：{str(e)}")
        raise
"""


def split_documents(
    documents: list[Document],
    chunk_size: int = 500,
    chunk_overlap: int = 50
) -> list[Document]:
    """
    通用：长文本切分（适配大模型上下文限制）
    :param documents: 原始文档列表
    :param chunk_size: 单块文本长度
    :param chunk_overlap: 文本块重叠长度（保证语义连贯）
    :return: 切分后的文本块列表
    """
    if not documents:
        logger.warning("⚠️ 无文档可切分")
        return []

    # 智能文本分割器（按段落/句子/单词分层切分）
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
        separators=["\n\n", "\n", "。", "！", "？", " "]
    )

    split_docs = splitter.split_documents(documents)
    logger.info(f"✅ 文档切分完成，共生成 {len(split_docs)} 个文本块")
    return split_docs