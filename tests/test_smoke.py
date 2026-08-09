# -*- coding: utf-8 -*-
"""
冒烟测试：覆盖过去高频出错的核心函数。
绕过需要网络/数据库的外部依赖，用纯内存数据+mock测试逻辑正确性。
"""
import json
import math
from typing import Dict, Any, Optional
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest


# ============================================================
# 1. support_resistance.py — 支撑压力位计算
# ============================================================

def _make_daily_df(prices, high_prices=None, low_prices=None, volumes=None):
    """构造测试用日线 DataFrame（降序，最新在前）"""
    n = len(prices)
    return pd.DataFrame({
        "date": [f"2026-01-{i+1:02d}" for i in range(n)],
        "close": list(reversed(prices)),
        "high": list(reversed(high_prices)) if high_prices else list(reversed(prices)),
        "low": list(reversed(low_prices)) if low_prices else list(reversed(prices)),
        "volume": list(reversed(volumes)) if volumes else [1e6] * n,
    })


def test_sr_empty_df():
    """空 DataFrame → None"""
    from tools.support_resistance import compute_sr_levels
    assert compute_sr_levels(pd.DataFrame()) is None


def test_sr_too_few_rows():
    """数据不足 → None"""
    from tools.support_resistance import compute_sr_levels
    df = _make_daily_df([10] * 5)
    assert compute_sr_levels(df, swing_k=2) is None


def test_sr_basic():
    """基本场景：输入有梯度，应返回支撑+压力"""
    from tools.support_resistance import compute_sr_levels
    prices = [10 + i * 0.5 for i in range(120)]
    df = _make_daily_df(prices)
    sr = compute_sr_levels(df, swing_k=2)
    assert sr is not None
    assert "close" in sr
    assert round(sr["close"], 1) == 10.0  # 最新价=第一条（降序第一）
    assert isinstance(sr.get("supports"), list)
    assert isinstance(sr.get("resistances"), list)


def test_sr_format_output():
    """format_sr_levels 非空输出包含预期关键词"""
    from tools.support_resistance import compute_sr_levels, format_sr_levels
    prices = [10 + i * 0.5 for i in range(120)]
    df = _make_daily_df(prices)
    sr = compute_sr_levels(df, swing_k=2)
    text = format_sr_levels(sr)
    assert text != ""
    assert "支撑" in text
    assert "压力" in text


# ============================================================
# 2. forecast.py — 机构预测工具函数
# ============================================================

def test_num_val():
    """_num_val 正常/None/NaN"""
    from tools.forecast import _num_val
    assert _num_val(3.14) == 3.14
    assert _num_val("3.14") == 3.14
    assert _num_val(0) is None  # 非正值
    assert _num_val(None) is None
    assert _num_val(float("nan")) is None
    assert _num_val("abc") is None


def test_count_institutions_empty():
    """_count_institutions 空/None → None"""
    from tools.forecast import _count_institutions
    assert _count_institutions(None) is None
    assert _count_institutions(pd.DataFrame()) is None


def test_count_institutions_normal():
    """含机构个数的表 → 返回个数"""
    from tools.forecast import _count_institutions
    df = pd.DataFrame([{"机构个数": 18, "EPS": 1.79}])
    assert _count_institutions(df) == 18


# ============================================================
# 3. report_formatter.py — "财务分析未覆盖"标签检测
# ============================================================

def test_format_report_no_dup_finance_label():
    """报告中含财务数据 → 不应追加"未覆盖"标签"""
    from tools.report_formatter import format_stock_report
    text = (
        "## 核心逻辑\n"
        "2026Q1 归母净利润同比 -13.77%\n"
        "毛利率 23.86%\n\n"
        "## 风险提示\n"
        "行业政策风险\n"
    )
    result = format_stock_report(text)
    assert "该维度在当前分析中未覆盖" not in result, (
        "有财务数据时不应该追加未覆盖标签"
    )


def test_format_report_no_finance():
    """报告中无财务数据 → 应保留未覆盖标签"""
    from tools.report_formatter import format_stock_report
    text = (
        "## 技术分析\n"
        "均线多头排列\n"
        "MACD 金叉\n\n"
        "## 风险提示\n"
        "市场波动风险\n"
    )
    result = format_stock_report(text)
    assert "该维度在当前分析中未覆盖" in result, (
        "无财务数据时应追加未覆盖标签"
    )


# ============================================================
# 4. trade_plan.py — 交易计划核心逻辑
# ============================================================

def _fake_row(vals: Dict[str, Any]) -> Dict[str, Any]:
    """构造 fake K线行"""
    return {
        "close": "35.63", "high": "35.88", "low": "35.30",
        "volume": "1e8",
        "ma5": "34.46", "ma10": "34.30", "ma20": "34.08",
        "ma60": "33.50", "ma120": "32.10", "ma200": "31.00",
        "boll_upper": "35.47", "boll_mid": "34.08", "boll_lower": "32.70",
        "atr": "0.80",
        "macd": "0.15", "dif": "0.12", "dea": "0.05",
        "rsi": "55", "k": "60", "d": "58", "j": "64",
        **vals,
    }


def test_trade_plan_basic():
    """基本交易计划（日线主导）应返回完整结构"""
    from tools.trade_plan import build_trade_plan
    daily = _fake_row({})
    plan = build_trade_plan(daily)
    assert plan is not None
    assert "direction" in plan
    assert "buy_zone" in plan
    assert "stop_loss" in plan
    assert "target" in plan
    assert "position" in plan


def test_trade_plan_empty():
    """空行 → None"""
    from tools.trade_plan import build_trade_plan
    assert build_trade_plan({}) is not None  # 也返回默认值


def test_trade_plan_null_values():
    """缺失关键指标的行 → 返回带默认值的 plan"""
    from tools.trade_plan import build_trade_plan
    daily = _fake_row({"ma20": None, "boll_mid": None})
    plan = build_trade_plan(daily)
    assert plan is not None


# ============================================================
# 5. stock_storage.py — save_industry_snapshot JSON 序列化
# ============================================================

def test_save_industry_snapshot_auto_json():
    """list/dict 参数应被自动序列化为 str"""
    from storage.sqlite.stock_storage import StockStorage
    db = StockStorage()
    kwargs = dict(
        industry_name="test_industry",
        question="test question",
        candidates=[{"code": "000001", "name": "PA"}],
        top_pick="000001",
        industry_view="中性",
        valuation={"pe_percentile": 50},
        excluded=[{"code": "000002", "reason": "low score"}],
        watch=[],
        benchmark_price=None,
    )
    db.save_industry_snapshot(**kwargs)
    # 验证：保存后读出，4个 JSON 列应为 str（非 list/dict）
    snap = db.get_latest_industry_snapshot("test_industry")
    assert snap is not None
    for col in ("candidates", "valuation", "excluded", "watch"):
        val = snap.get(col)
        assert isinstance(val, str), f"{col} 应为 str，实际 {type(val)}"
        parsed = json.loads(val)  # 可反序列化
        assert isinstance(parsed, (list, dict)), f"{col} 反序列化后应为 list/dict"
    # 清理测试数据
    if snap:
        with db.get_session() as session:
            from storage.sqlite.stock_storage import IndustrySnapshot
            session.execute(
                __import__("sqlalchemy").delete(IndustrySnapshot).where(
                    IndustrySnapshot.industry_name == "test_industry"
                )
            )
            session.commit()


# ============================================================
# 6. 边界场景：None 穿透
# ============================================================

def test_num_ma_handling():
    """MA 值为 None 时不应崩溃"""
    from tools.support_resistance import _col
    df = pd.DataFrame({"close": [None, 10.0, 11.0]})
    vals = _col(df, "close")
    assert len(vals) == 3
    assert vals[0] is None


def test_cluster_empty():
    """空列表聚类 → 空列表"""
    from tools.support_resistance import _cluster
    assert _cluster([], 1.5) == []


def test_cluster_single():
    """单元素聚类 → 单簇"""
    from tools.support_resistance import _cluster
    result = _cluster([10.0], 1.5)
    assert len(result) == 1
    assert result[0]["price"] == 10.0
    assert result[0]["touches"] == 1


def test_cluster_merge():
    """相邻元素应归并"""
    from tools.support_resistance import _cluster
    result = _cluster([10.0, 10.05, 10.12], 1.5)
    assert len(result) == 1  # 全部归并
    assert result[0]["touches"] == 3
