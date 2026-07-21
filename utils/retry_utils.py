import time
import random
from typing import Callable, Any, Tuple, Optional
from utils.logger import logger


def retry_with_backoff(
    func: Callable,
    max_retries: int = 3,
    initial_delay: float = 0.5,
    max_delay: float = 5.0,
    jitter: bool = True,
    retry_on: Tuple = (Exception,),
    fallback_func: Optional[Callable] = None,
    fallback_on: Tuple = (Exception,),
    *args,
    **kwargs
) -> Any:
    """
    带指数退避的重试装饰器。

    Args:
        func: 要执行的函数
        max_retries: 最大重试次数
        initial_delay: 初始延迟（秒）
        max_delay: 最大延迟（秒）
        jitter: 是否添加随机抖动
        retry_on: 哪些异常类型需要重试（默认所有异常）
        fallback_func: 备选函数（主函数失败时调用）
        fallback_on: 哪些异常类型触发备选函数
        *args, **kwargs: 传递给函数的参数

    Returns:
        函数执行结果，如果所有重试都失败则返回 None
    """
    last_error = None
    delay = initial_delay

    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except retry_on as e:
            last_error = e
            logger.warning(f"[重试] 第 {attempt + 1}/{max_retries} 次失败: {e}")

            if attempt < max_retries - 1:
                sleep_time = min(delay, max_delay)
                if jitter:
                    sleep_time = sleep_time * (0.5 + random.random())
                logger.info(f"[重试] 等待 {sleep_time:.2f}s 后重试...")
                time.sleep(sleep_time)
                delay *= 2

    logger.error(f"[重试] 已达最大重试次数 {max_retries}，最后错误: {last_error}")

    if fallback_func is not None:
        try:
            logger.info("[重试] 尝试备选方案...")
            return fallback_func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"[重试] 备选方案也失败: {e}")

    return None


def retry_with_multiple_sources(
    sources: list,
    source_retries: int = 3,
    retry_delay: float = 0.5,
    *args,
    **kwargs
) -> Any:
    """
    多数据源重试：每个源内部先重试多次（指数退避），再切下一个源。

    Args:
        sources: 数据源列表，每个元素为 (name, func) 元组
        source_retries: 每个源内部的重试次数
        retry_delay: 初始重试间隔（秒）
        *args, **kwargs: 传递给函数的参数

    Returns:
        第一个成功的数据源结果，如果全部失败则返回 None
    """
    last_error = None
    for name, func in sources:
        for attempt in range(source_retries):
            try:
                logger.info(f"[多源重试] 尝试数据源: {name}（第{attempt + 1}次）")
                result = func(*args, **kwargs)
                if result is not None and (not hasattr(result, '__len__') or len(result) > 0):
                    logger.info(f"[多源重试] 数据源 {name} 成功")
                    return result
                logger.warning(f"[多源重试] 数据源 {name} 返回空数据")
                last_error = f"{name} 返回空数据"
            except Exception as e:
                logger.warning(f"[多源重试] 数据源 {name} 第{attempt + 1}次失败: {e}")
                last_error = e
            if attempt < source_retries - 1:
                delay = retry_delay * (2 ** attempt) * (0.5 + random.random())
                logger.info(f"[多源重试] {name} 等待 {delay:.2f}s 后重试...")
                time.sleep(delay)

    logger.error(f"[多源重试] 所有数据源均失败，最后错误: {last_error}")
    return None