"""
数据监控与告警模块
提供数据源健康状态监控、性能统计和告警通知功能
"""

import time
import threading
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from collections import deque
from enum import Enum
from utils.logger import logger


class AlertLevel(Enum):
    """告警级别"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AlertType(Enum):
    """告警类型"""
    DATA_SOURCE_FAILURE = "data_source_failure"
    LOW_SUCCESS_RATE = "low_success_rate"
    HIGH_LATENCY = "high_latency"
    DATA_QUALITY_ISSUE = "data_quality_issue"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"


class Alert:
    """告警对象"""
    
    def __init__(
        self,
        alert_type: AlertType,
        level: AlertLevel,
        message: str,
        source_name: str = None,
        stock_code: str = None,
        metadata: Dict = None
    ):
        self.alert_type = alert_type
        self.level = level
        self.message = message
        self.source_name = source_name
        self.stock_code = stock_code
        self.metadata = metadata or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'type': self.alert_type.value,
            'level': self.level.value,
            'message': self.message,
            'source': self.source_name,
            'stock_code': self.stock_code,
            'timestamp': self.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'metadata': self.metadata
        }


class DataSourceMonitor:
    """
    数据源监控器
    
    监控指标：
    1. 请求成功率
    2. 响应时间
    3. 错误计数
    4. 数据质量
    """

    def __init__(self):
        # 性能指标
        self.metrics: Dict[str, Dict] = {}  # {source_name: metrics}
        
        # 告警队列
        self.alerts: List[Alert] = []
        self.max_alerts = 100  # 最大告警数
        
        # 告警阈值配置
        self.thresholds = {
            'success_rate': 0.6,      # 成功率低于60%告警
            'avg_latency': 30,        # 平均延迟超过30秒告警
            'error_rate': 0.3,        # 错误率超过30%告警
            'consecutive_failures': 5  # 连续失败5次告警
        }
        
        # 线程锁（可重入，保护 metrics 和 alerts 的并发访问）
        self._lock = threading.RLock()

    def record_request(
        self,
        source_name: str,
        stock_code: str,
        success: bool,
        response_time: float,
        data_type: str = None,
        error_message: str = None
    ):
        """
        记录请求结果
        
        Args:
            source_name: 数据源名称
            stock_code: 股票代码
            success: 是否成功
            response_time: 响应时间（秒）
            data_type: 数据类型
            error_message: 错误信息（失败时）
        """
        # 初始化数据源指标 + 更新指标（加锁保护）
        with self._lock:
            if source_name not in self.metrics:
                self.metrics[source_name] = {
                    'total_requests': 0,
                    'success_requests': 0,
                    'total_response_time': 0,
                    'latency_history': deque(maxlen=100),
                    'consecutive_failures': 0,
                    'error_counts': {},
                    'last_request_time': None
                }
            
            metrics = self.metrics[source_name]
            metrics['total_requests'] += 1
            metrics['last_request_time'] = datetime.now()
            
            if success:
                metrics['success_requests'] += 1
                metrics['consecutive_failures'] = 0
            else:
                metrics['consecutive_failures'] += 1
                # 记录错误类型
                error_type = self._classify_error(error_message)
                metrics['error_counts'][error_type] = metrics['error_counts'].get(error_type, 0) + 1
            
            # 记录延迟
            metrics['total_response_time'] += response_time
            metrics['latency_history'].append(response_time)
            
            # 检查告警条件
            self._check_alerts(source_name, stock_code, metrics)

    def _classify_error(self, error_message: str) -> str:
        """分类错误类型"""
        if error_message is None:
            return 'unknown'
        
        error_lower = error_message.lower()
        
        if any(keyword in error_lower for keyword in ['rate limit', 'quota', '频率']):
            return 'rate_limit'
        elif any(keyword in error_lower for keyword in ['banned', 'blocked', '封禁']):
            return 'banned'
        elif any(keyword in error_lower for keyword in ['timeout', 'time out']):
            return 'timeout'
        elif any(keyword in error_lower for keyword in ['connection', 'network']):
            return 'network'
        else:
            return 'other'

    def _check_alerts(self, source_name: str, stock_code: str, metrics: Dict):
        """检查是否需要触发告警"""
        alerts_to_add = []
        
        # 1. 检查成功率
        if metrics['total_requests'] >= 20:  # 至少20次请求后才检查
            success_rate = metrics['success_requests'] / metrics['total_requests']
            if success_rate < self.thresholds['success_rate']:
                alerts_to_add.append(Alert(
                    alert_type=AlertType.LOW_SUCCESS_RATE,
                    level=AlertLevel.WARNING,
                    message=f"数据源 {source_name} 成功率 {success_rate:.1%}，低于阈值 {self.thresholds['success_rate']:.0%}",
                    source_name=source_name,
                    metadata={'success_rate': success_rate}
                ))
        
        # 2. 检查连续失败
        if metrics['consecutive_failures'] >= self.thresholds['consecutive_failures']:
            alerts_to_add.append(Alert(
                alert_type=AlertType.DATA_SOURCE_FAILURE,
                level=AlertLevel.ERROR,
                message=f"数据源 {source_name} 连续失败 {metrics['consecutive_failures']} 次",
                source_name=source_name,
                stock_code=stock_code
            ))
        
        # 3. 检查平均延迟
        if len(metrics['latency_history']) >= 10:
            avg_latency = sum(metrics['latency_history']) / len(metrics['latency_history'])
            if avg_latency > self.thresholds['avg_latency']:
                alerts_to_add.append(Alert(
                    alert_type=AlertType.HIGH_LATENCY,
                    level=AlertLevel.WARNING,
                    message=f"数据源 {source_name} 平均延迟 {avg_latency:.1f}秒，超过阈值 {self.thresholds['avg_latency']}秒",
                    source_name=source_name,
                    metadata={'avg_latency': avg_latency}
                ))
        
        # 4. 检查错误率
        error_rate = 1 - (metrics['success_requests'] / metrics['total_requests']) if metrics['total_requests'] > 0 else 0
        if error_rate > self.thresholds['error_rate']:
            alerts_to_add.append(Alert(
                alert_type=AlertType.DATA_SOURCE_FAILURE,
                level=AlertLevel.WARNING,
                message=f"数据源 {source_name} 错误率 {error_rate:.1%}，超过阈值 {self.thresholds['error_rate']:.0%}",
                source_name=source_name,
                metadata={'error_rate': error_rate, 'error_counts': metrics['error_counts']}
            ))
        
        # 添加告警
        for alert in alerts_to_add:
            self.add_alert(alert)

    def add_alert(self, alert: Alert):
        """添加告警"""
        with self._lock:
            # 避免重复告警（同一类型同一数据源5分钟内不重复）
            recent_alerts = [
                a for a in self.alerts 
                if a.alert_type == alert.alert_type 
                and a.source_name == alert.source_name
                and (datetime.now() - a.timestamp) < timedelta(minutes=5)
            ]
            
            if not recent_alerts:
                self.alerts.append(alert)
                
                # 输出日志
                log_level = {
                    AlertLevel.INFO: logger.info,
                    AlertLevel.WARNING: logger.warning,
                    AlertLevel.ERROR: logger.error,
                    AlertLevel.CRITICAL: logger.critical
                }
                log_func = log_level.get(alert.level, logger.info)
                log_func(f"[告警] [{alert.level.value}] {alert.message}")
            
            # 限制告警数量
            if len(self.alerts) > self.max_alerts:
                self.alerts = self.alerts[-self.max_alerts:]

    def get_alerts(self, level: AlertLevel = None, limit: int = 20) -> List[Alert]:
        """
        获取告警列表
        
        Args:
            level: 告警级别过滤（可选）
            limit: 返回数量限制
        
        Returns:
            告警列表（按时间倒序）
        """
        alerts = self.alerts.copy()
        
        if level:
            alerts = [a for a in alerts if a.level == level]
        
        # 按时间倒序
        alerts.sort(key=lambda x: x.timestamp, reverse=True)
        
        return alerts[:limit]

    def get_source_health(self, source_name: str) -> Dict[str, Any]:
        """获取数据源健康状态"""
        if source_name not in self.metrics:
            return {
                'status': 'unknown',
                'message': '暂无数据'
            }
        
        metrics = self.metrics[source_name]
        
        if metrics['total_requests'] == 0:
            return {
                'status': 'idle',
                'message': '尚未有请求'
            }
        
        success_rate = metrics['success_requests'] / metrics['total_requests']
        avg_latency = metrics['total_response_time'] / metrics['total_requests']
        
        # 评估健康状态
        if success_rate >= 0.95 and avg_latency < 10:
            status = 'healthy'
            message = '运行正常'
        elif success_rate >= 0.8:
            status = 'degraded'
            message = '性能下降'
        else:
            status = 'unhealthy'
            message = '异常'
        
        return {
            'status': status,
            'message': message,
            'success_rate': f"{success_rate:.1%}",
            'total_requests': metrics['total_requests'],
            'avg_latency': f"{avg_latency:.1f}s",
            'consecutive_failures': metrics['consecutive_failures'],
            'last_request': metrics['last_request_time'].strftime('%Y-%m-%d %H:%M:%S') if metrics['last_request_time'] else None
        }

    def get_overall_health_summary(self) -> Dict[str, Any]:
        """获取整体健康状态摘要"""
        summary = {
            'sources': {},
            'overall_status': 'healthy',
            'total_requests': 0,
            'total_success': 0,
            'active_alerts': len(self.get_alerts())
        }
        
        for source_name, metrics in self.metrics.items():
            summary['sources'][source_name] = self.get_source_health(source_name)
            summary['total_requests'] += metrics['total_requests']
            summary['total_success'] += metrics['success_requests']
            
            # 检查是否有异常数据源
            if summary['sources'][source_name]['status'] == 'unhealthy':
                summary['overall_status'] = 'unhealthy'
            elif summary['sources'][source_name]['status'] == 'degraded' and summary['overall_status'] == 'healthy':
                summary['overall_status'] = 'degraded'
        
        if summary['total_requests'] > 0:
            summary['overall_success_rate'] = f"{summary['total_success'] / summary['total_requests']:.1%}"
        
        return summary

    def clear_alerts(self):
        """清空告警"""
        self.alerts.clear()
        logger.info("[监控] 告警已清空")


class PerformanceLogger:
    """
    性能日志记录器
    
    记录关键操作的执行时间，用于性能分析
    """

    def __init__(self):
        self.timers: Dict[str, List[float]] = {}  # {operation: [durations]}
    
    def start_timer(self, operation_name: str) -> float:
        """开始计时"""
        return time.time()
    
    def end_timer(self, operation_name: str, start_time: float):
        """结束计时并记录"""
        duration = time.time() - start_time
        
        if operation_name not in self.timers:
            self.timers[operation_name] = []
        
        self.timers[operation_name].append(duration)
        
        # 记录超过阈值的操作
        if duration > 10:  # 超过10秒记录警告
            logger.warning(f"[性能警告] {operation_name} 耗时 {duration:.1f}秒")
        
        # 定期输出统计
        if len(self.timers[operation_name]) >= 100:
            self._log_stats(operation_name)
    
    def _log_stats(self, operation_name: str):
        """输出统计信息"""
        durations = self.timers[operation_name]
        avg = sum(durations) / len(durations)
        max_duration = max(durations)
        min_duration = min(durations)
        
        logger.info(
            f"[性能统计] {operation_name}: "
            f"执行{len(durations)}次, "
            f"平均{avg:.2f}s, "
            f"最大{max_duration:.2f}s, "
            f"最小{min_duration:.2f}s"
        )
        
        # 重置计数器
        self.timers[operation_name] = []


# 全局实例
monitor = DataSourceMonitor()
performance_logger = PerformanceLogger()
