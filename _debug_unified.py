"""验证归一化后所有路径输出"""
import logging
logging.disable(logging.CRITICAL)

code = '002594'

print("=" * 60)
print("路径1: _format_fina_indicator（财务指标明细）")
print("=" * 60)
from tools.stock_tools import stock_tool_instance, _format_fina_indicator
df = stock_tool_instance.fetch_and_save_fina_indicator(code)
text = _format_fina_indicator(df, code)
for line in text.split('\n'):
    ls = line.strip()
    if any(kw in ls for kw in ['ROE', '毛利率', '净利率', '周转天数', '负债率', '营收增长', '净利润增长']):
        print(f"  {ls}")

print("\n" + "=" * 60)
print("路径2: call_fetch_financial_health_summary（深度财务健康度）")
print("=" * 60)
from tools.stock_tools import call_fetch_financial_health_summary
text2 = call_fetch_financial_health_summary(code)
for line in text2.split('\n'):
    ls = line.strip()
    if any(kw in ls for kw in ['ROE', '净利率', '周转', '权益乘数', '现金循环']):
        print(f"  {ls}")

print("\n" + "=" * 60)
print("路径3: call_fetch_cross_validation（数据交叉校验）")
print("=" * 60)
from tools.stock_tools import call_fetch_data_validator
text3 = call_fetch_data_validator(code)
for line in text3.split('\n'):
    ls = line.strip()
    if any(kw in ls for kw in ['毛利率', '偏差', '营收', 'ROE 异常']):
        print(f"  {ls}")
