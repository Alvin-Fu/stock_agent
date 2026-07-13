# Golden 基线（2026-07-13 人工合并）

5 个 case 分别取自 20260713-104805（byd/moutai/catl）、-111540（aerospace）、-120233（liquor，DDG 兜底跑）。
注意：aerospace 的 4 个问题全是「分位缺窗口」旧格式问题，修复已合入，下轮应清零；
liquor 为搜索降级（Tavily 配额耗尽）下的产出，候选覆盖偏低，字符数基线仅供参考。
