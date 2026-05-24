"""
新模块集成指南
展示如何在现有代码中集成和使用新增的功能模块
"""

from core.state_manager import (
    state_validator,
    StateValidator,
    StateTransitionManager,
    StateCache,
    AgentStatus,
    AgentExecutionRecord
)

from core.agent_coordination import (
    AgentMessage,
    MessageType,
    Priority,
    MessageBus,
    ParallelExecutor,
    AgentResultAggregator,
    message_bus,
    parallel_executor,
    result_aggregator
)

from core.business_analyzer import (
    valuation_analyzer,
    risk_analyzer,
    time_series_analyzer,
    ValuationAnalyzer,
    RiskAnalyzer,
    TimeSeriesAnalyzer,
    ValuationMetrics,
    ValuationLevel,
    RiskLevel
)

from core.data_processor import (
    data_validator,
    data_cleaner,
    data_comparator,
    DataValidator,
    DataCleaner,
    DataComparator,
    DataQuality,
    ValidationResult
)

from core.plugin_system import (
    plugin_registry,
    config_manager,
    agent_factory,
    ab_test_manager,
    PluginRegistry,
    ConfigManager,
    PluginType,
    PluginMetadata
)


def example_state_validation():
    """示例：状态验证"""
    state = {
        "messages": [],
        "question": "分析比亚迪",
        "stock_code": "002594",
        "intent": "financial_analysis"
    }

    is_valid, errors = StateValidator.validate_state(state)
    print(f"状态验证: {is_valid}, 错误: {errors}")

    safe_value = StateValidator.safe_get(state, "financial_data", default={})
    print(f"安全获取字段: {safe_value}")


def example_agent_coordination():
    """示例：Agent协作"""
    message = AgentMessage(
        msg_type=MessageType.REQUEST,
        sender="router",
        receiver="analyst",
        content={"task": "analyze_stock", "stock_code": "002594"},
        priority=Priority.HIGH
    )

    message_bus.publish(message)
    print(f"消息发布: {message.to_dict()}")


def example_parallel_execution():
    """示例：并行执行"""
    tasks = [
        {"agent": "retriever", "input": {"stock_code": "002594"}},
        {"agent": "researcher", "input": {"stock_code": "002594"}},
    ]

    def executor(task):
        return {"result": f"执行完成: {task['agent']}"}

    results = parallel_executor.execute_parallel(tasks, executor)
    print(f"并行执行结果: {results}")


def example_business_analysis():
    """示例：业务分析"""
    metrics = ValuationMetrics(
        pe_ratio=25.5,
        pb_ratio=3.2,
        dividend_yield=2.5,
        peg_ratio=0.8
    )

    valuation_result = ValuationAnalyzer.comprehensive_valuation(
        metrics,
        industry="制造业"
    )
    print(f"估值分析结果: {valuation_result}")

    business_data = {
        "revenue_distribution": {
            "汽车": 60,
            "手机部件": 25,
            "电池": 15
        },
        "customer_concentration": 35
    }

    risk_result = RiskAnalyzer.comprehensive_risk_assessment(
        business_data,
        financial_data={"debt_ratio": 65}
    )
    print(f"风险评估结果: {risk_result}")


def example_data_processing():
    """示例：数据处理"""
    result = DataValidator.validate_numeric(
        25.5,
        min_val=0,
        max_val=100
    )
    print(f"数据验证: {result.is_valid}, 质量: {result.quality.value}")

    comparison = DataComparator.compare_periods(
        {"revenue": 100, "profit": 20},
        {"revenue": 90, "profit": 18},
        ["revenue", "profit"]
    )
    print(f"数据对比: {comparison}")


def example_plugin_system():
    """示例：插件系统"""
    plugin_metadata = PluginMetadata(
        name="custom_agent",
        version="1.0.0",
        plugin_type=PluginType.AGENT,
        description="自定义Agent",
        author="user"
    )
    print(f"插件元数据: {plugin_metadata}")

    config_manager.register_default_config("agent:custom_agent", {
        "timeout": 30,
        "retry_count": 3
    })
    print(f"Agent配置: {config_manager.get_agent_config('custom_agent')}")


if __name__ == "__main__":
    print("=" * 50)
    print("状态验证示例")
    print("=" * 50)
    example_state_validation()

    print("\n" + "=" * 50)
    print("Agent协作示例")
    print("=" * 50)
    example_agent_coordination()

    print("\n" + "=" * 50)
    print("业务分析示例")
    print("=" * 50)
    example_business_analysis()

    print("\n" + "=" * 50)
    print("数据处理示例")
    print("=" * 50)
    example_data_processing()

    print("\n" + "=" * 50)
    print("插件系统示例")
    print("=" * 50)
    example_plugin_system()
