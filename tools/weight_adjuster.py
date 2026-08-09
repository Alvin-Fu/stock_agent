# -*- coding: utf-8 -*-
"""
权重动态调整模块——复盘闭环的最后一公里。

复盘识别出误判类别后，自动反算权重偏差并写入 local.yaml，
下次分析加载 config 时自动使用调整后的权重，无需重启。

【映射规则】（error_pattern → 权重调整方向）
  技术面权重失衡  →  日线-0.05, 周线+0.05（中长期权重略提升）
  基本面利空高估  →  fundamental-0.05, momentum+0.05（边际变化权重提升）
  基本面利好高估  →  fundamental-0.05, moat+0.05（护城河权重提升）
  资金面驱动低估  →  momentum+0.05, moat-0.05（短期动量权重提升）
  技术面信号误读  →  不调权重（判读错误，非权重问题）
  信息时效性误判  →  不调权重（搜索策略问题，非权重问题）
  无明显误判/其他 →  不调权重

【保护机制】
  - 每步调整幅度 ≤ 0.05，累积偏移上限 ±0.15（防止单次复盘过度矫正）
  - 调整后总权重需归一化到 1.0 ± 0.01
"""

import copy
import os
from datetime import datetime
from typing import Dict, Any, Optional

import yaml

from utils.logger import logger

# ======================================================================
# 默认值（代码兜底，config 里没有 weights 段时用这些）
# ======================================================================

DEFAULT_STAGE_WEIGHTS: Dict[str, Dict[str, float]] = {
    "成熟期": {"business": 0.20, "fundamental": 0.30, "moat": 0.40, "momentum": 0.10},
    "成长期": {"business": 0.20, "fundamental": 0.25, "moat": 0.25, "momentum": 0.30},
    "导入期": {"business": 0.15, "fundamental": 0.20, "moat": 0.25, "momentum": 0.40},
}

DEFAULT_TECH_WEIGHTS: Dict[str, float] = {
    "daily": 0.5,
    "weekly": 0.3,
    "monthly": 0.2,
}

DEFAULT_PE_ADJUSTMENTS: Dict[str, float] = {
    "pe_gt_200": -1.5,   # 绝对PE > 200（极端泡沫）
    "pe_gt_100": -1.0,   # 绝对PE > 100（高PE）
    "pct_ge_80": -1.0,   # PE分位 >= 80
    "pct_ge_60": -0.5,   # PE分位 >= 60
    "pct_le_30": 0.5,    # PE分位 <= 30（低估）
    "dual_high": -1.5,   # 分位>=80 且 绝对PE>100（双高叠加扣分）
}

# 单次最大调整幅度
STEP_LIMIT = 0.05
# 累计偏移上限（相对默认值）
CUMULATIVE_LIMIT = 0.15

# 误判类别 → 调整规则
_ADJUSTMENT_RULES = {
    "技术面权重失衡": {
        "type": "tech",
        "changes": {"daily": -STEP_LIMIT, "weekly": +STEP_LIMIT},
        "clamp": {"daily": (0.35, 0.65), "weekly": (0.15, 0.45), "monthly": (0.10, 0.30)},
    },
    "基本面利空高估": {
        "type": "stage",
        "changes": {"fundamental": -STEP_LIMIT, "momentum": +STEP_LIMIT},
        "clamp": {"momentum": (0.05, 0.50), "fundamental": (0.10, 0.40)},
    },
    "基本面利好高估": {
        "type": "stage",
        "changes": {"fundamental": -STEP_LIMIT, "moat": +STEP_LIMIT},
        "clamp": {"moat": (0.10, 0.50), "fundamental": (0.10, 0.40)},
    },
    "资金面驱动低估": {
        "type": "stage",
        "changes": {"momentum": +STEP_LIMIT, "moat": -STEP_LIMIT},
        "clamp": {"momentum": (0.05, 0.50), "moat": (0.10, 0.50)},
    },
}

# ======================================================================
# 配置读写
# ======================================================================

def _get_config_path() -> str:
    """获取 local.yaml 路径（与 config.py 逻辑一致）"""
    from utils.config import PROJECT_ROOT
    p = os.path.join(PROJECT_ROOT, "local.yaml")
    return p if os.path.exists(p) else os.path.join(PROJECT_ROOT, "config.yaml")


def _load_weights_from_config() -> Dict[str, Any]:
    """从 config 的 weights 段加载权重，缺失段返回空 dict"""
    try:
        path = _get_config_path()
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        return (cfg.get("weights") or {}).copy()
    except Exception as e:
        logger.warning(f"[weight_adjuster] 从 config 加载权重失败: {e}")
        return {}


def _save_weights_to_config(weights_section: Dict[str, Any]) -> bool:
    """将 weights 段写回 local.yaml（只改 weights，不动其他段）

    使用 fcntl 文件锁防止复盘调权和阈值校准并发写入时相互覆盖。
    """
    try:
        import fcntl
        from utils.config import PROJECT_ROOT
        path = os.path.join(PROJECT_ROOT, "local.yaml")
        if not os.path.exists(path):
            logger.warning(f"[weight_adjuster] local.yaml 不存在，跳过写入: {path}")
            return False

        # 加排他锁：读-改-写整个流程原子化，防止并发进程相互覆盖
        with open(path, "r+", encoding="utf-8") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except (IOError, OSError):
                pass  # 锁失败不阻断写入（非关键路径），但记录警告
            try:
                cfg = yaml.safe_load(f) or {}
                cfg["weights"] = weights_section
                f.seek(0)
                f.truncate()
                yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            finally:
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                except (IOError, OSError):
                    pass

        logger.info("[weight_adjuster] 权重已写回 %s", path)
        return True
    except Exception as e:
        logger.error(f"[weight_adjuster] 写回 config 失败: {e}")
        return False


# ======================================================================
# 公开读取接口
# ======================================================================

def get_stage_weights() -> Dict[str, Dict[str, float]]:
    """
    获取行业综合评分权重（从 config → 兜底默认值）。
    返回完整副本，调用方自行修改不影响全局。
    """
    cfg = _load_weights_from_config()
    stored = cfg.get("stage_weights")
    if stored and isinstance(stored, dict):
        return copy.deepcopy(stored)
    return copy.deepcopy(DEFAULT_STAGE_WEIGHTS)


def get_tech_weights(code: str = None) -> Dict[str, float]:
    """获取技术多周期权重（daily/weekly/monthly）。

    支持按标的隔离：传 code 时优先读该标的专属权重（tech_weights_by_code[code]），
    无专属配置则回退全局 tech_weights，再回退默认值。
    """
    cfg = _load_weights_from_config()
    if code:
        by_code = cfg.get("tech_weights_by_code") or {}
        if isinstance(by_code, dict) and code in by_code and isinstance(by_code[code], dict):
            return copy.deepcopy(by_code[code])
    stored = cfg.get("tech_weights")
    if stored and isinstance(stored, dict):
        return copy.deepcopy(stored)
    return copy.deepcopy(DEFAULT_TECH_WEIGHTS)


def get_pe_adjustments() -> Dict[str, float]:
    """获取 PE 预期差调整阈值。

    以默认值为基准，叠加 config 中已配置的键：老 config 只存了 3 个分位键，
    新默认里的绝对 PE / 双高键（pe_gt_200/pe_gt_100/dual_high）自动补默认值，
    避免旧配置覆盖掉新增的键。
    """
    cfg = _load_weights_from_config()
    merged = copy.deepcopy(DEFAULT_PE_ADJUSTMENTS)
    stored = cfg.get("pe_adjustments")
    if stored and isinstance(stored, dict):
        for k, v in stored.items():
            if isinstance(v, (int, float)):
                merged[k] = float(v)
    return merged


# ======================================================================
# 自动调权
# ======================================================================

def apply_review_adjustment(
    error_pattern: str,
    source_info: str = "",
    code: str = None,
) -> bool:
    """
    复盘入口：根据误判类别自动微调权重，写回 config。
    返回 True=已调整, False=无需调整或调整失败。

    技术类误判支持按标的隔离：传 code 时只调整该股票的技术权重
    （存 tech_weights_by_code[code]，不影响其他股票），不传则调全局。
    阶段类误判仍调全局（产业链综合评分本就是全局口径）。

    外部只需调这一个函数，内部自动处理：
    读取当前权重 → 叠加 delta → 钳制边界 → 归一化 → 写回 + 记日志
    """
    rule = _ADJUSTMENT_RULES.get(error_pattern)
    if not rule:
        logger.info(f"[weight_adjuster] 误判类别「{error_pattern}」无需调整权重")
        return False

    wtype = rule["type"]
    changes = rule["changes"]
    clamp = rule.get("clamp", {})

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    scope = f"{source_info}（code={code}）" if code else source_info
    log_entry = f"[{now_str}] {scope} → {error_pattern} → {changes}"

    if wtype == "tech":
        return _adjust_tech_weights(changes, clamp, log_entry, code=code)
    elif wtype == "stage":
        return _adjust_stage_weights(changes, clamp, log_entry)

    return False


def _adjust_tech_weights(
    changes: Dict[str, float],
    clamp: Dict[str, tuple],
    log_entry: str,
    code: str = None,
) -> bool:
    """调整技术权重（daily/weekly/monthly）。

    code 传入时只调整该标的专属权重（tech_weights_by_code[code]），
    不传则调整全局 tech_weights。初始状态从 get_tech_weights(code) 取，
    保证 per-code 配置缺省时继承全局默认值再叠加 delta。
    """
    current = get_tech_weights(code=code)

    for key, delta in changes.items():
        if key in current:
            old = current[key]
            new_val = round(old + delta, 2)
            # 钳制
            if key in clamp:
                lo, hi = clamp[key]
                new_val = max(lo, min(hi, new_val))
                # 累计偏移上限检查（相对默认值）
                default_val = DEFAULT_TECH_WEIGHTS.get(key, old)
                if abs(new_val - default_val) > CUMULATIVE_LIMIT:
                    direction = 1 if delta > 0 else -1
                    new_val = round(default_val + direction * CUMULATIVE_LIMIT, 2)
            current[key] = new_val
            logger.info(f"[weight_adjuster] tech.{key}: {old} → {new_val}")

    # 加载完整 weights 段，按作用域写入（per-code 或全局）
    cfg = _load_weights_from_config()
    if code:
        by_code = cfg.get("tech_weights_by_code") or {}
        if not isinstance(by_code, dict):
            by_code = {}
        by_code[code] = current
        cfg["tech_weights_by_code"] = by_code
        logger.info(f"[weight_adjuster] 技术权重按标的隔离写入 tech_weights_by_code[{code}]")
    else:
        cfg["tech_weights"] = current
    # 调权日志
    log = cfg.get("adjustment_log") or []
    log.append(log_entry)
    cfg["adjustment_log"] = log
    cfg["_version"] = 2  # 结构版本号，便于后续迁移

    ok = _save_weights_to_config(cfg)
    if ok:
        logger.info(f"[weight_adjuster] 技术权重调整完成: {log_entry}")
    return ok


def _adjust_stage_weights(
    changes: Dict[str, float],
    clamp: Dict[str, tuple],
    log_entry: str,
) -> bool:
    """调整行业综合评分权重（对三个阶段的同一维度同时调）"""
    current = get_stage_weights()

    for stage_name, stage_weights in current.items():
        for key, delta in changes.items():
            if key in stage_weights:
                old = stage_weights[key]
                new_val = round(old + delta, 2)
                # 钳制
                if key in clamp:
                    lo, hi = clamp[key]
                    new_val = max(lo, min(hi, new_val))
                    default_val = DEFAULT_STAGE_WEIGHTS.get(stage_name, {}).get(key, old)
                    if abs(new_val - default_val) > CUMULATIVE_LIMIT:
                        direction = 1 if delta > 0 else -1
                        new_val = round(default_val + direction * CUMULATIVE_LIMIT, 2)
                stage_weights[key] = new_val
                logger.info(f"[weight_adjuster] stage.{stage_name}.{key}: {old} → {new_val}")

        # 归一化：调整后该阶段权重之和应 ≈ 1.0
        total = sum(stage_weights.values())
        if abs(total - 1.0) > 0.01:
            scale = 1.0 / total
            for k in stage_weights:
                stage_weights[k] = round(stage_weights[k] * scale, 2)
            # 修约误差补偿到最大维度
            diff = round(1.0 - sum(stage_weights.values()), 2)
            if abs(diff) > 0:
                max_key = max(stage_weights, key=stage_weights.get)
                stage_weights[max_key] = round(stage_weights[max_key] + diff, 2)

    # 写回
    cfg = _load_weights_from_config()
    cfg["stage_weights"] = current
    # PE 调整和 tech 权重如果未在 config 中，保留默认值
    if "tech_weights" not in cfg:
        cfg["tech_weights"] = copy.deepcopy(DEFAULT_TECH_WEIGHTS)
    if "pe_adjustments" not in cfg:
        cfg["pe_adjustments"] = copy.deepcopy(DEFAULT_PE_ADJUSTMENTS)

    log = cfg.get("adjustment_log") or []
    log.append(log_entry)
    cfg["adjustment_log"] = log
    cfg["_version"] = 2

    ok = _save_weights_to_config(cfg)
    if ok:
        logger.info(f"[weight_adjuster] 行业权重调整完成: {log_entry}")
    return ok
