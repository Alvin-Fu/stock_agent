import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

# 自动创建日志文件夹
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logger(name: str = "langchain_kb_agent") -> logging.Logger:
    """
    项目通用日志配置（单例模式）
    :param name: 日志器名称
    :return: 配置好的日志器
    """
    logger = logging.getLogger(name)

    # 避免重复添加处理器（关键！防止日志重复打印）
    if logger.handlers:
        return logger

    # 日志级别：DEBUG/INFO/WARNING/ERROR/CRITICAL
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # 日志格式：时间 - 日志器名 - 级别 - 文件名:行号 - 信息
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s",
    )

    # 1. 控制台输出处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # 2. 文件输出处理器（每天生成一个新日志文件）
    log_file = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",  # 每日零点分割
        encoding="utf-8",
        backupCount=30  # 保留30天日志
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # 添加处理器
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# 全局默认日志器（全项目直接导入使用）
logger = setup_logger()