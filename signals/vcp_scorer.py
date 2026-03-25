"""
vcp_scorer.py — VCP (Volatility Contraction Pattern) 评分模块

基于 Minervini 体系：股价在上涨后进入整理，每次回调幅度递减，
成交量同步萎缩，最后在极低波动中突破。

VCP 触发条件（同时满足）：
  - volatility_contraction = True（三段波动逐步收缩）
  - drawdown_from_high < 30%
  - vcp_score >= 45
"""

import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.tiingo_client import get_history as tiingo_get_history

# ── VCP 触发阈值 ───────────────────────────────────────────────────────────────
VCP_MIN_SCORE         = 45
VCP_MAX_DRAWDOWN      = 30.0   # 最大允许回撤（%）
VCP_MIN_GAIN_60D      = 10.0   # 需要有涨幅基础

# ── 数据获取（Tiingo） ────────────────────────────────────────────────────────

def _get_history_stooq(ticker: str, days: int = 60) -> pd.DataFrame:
    """保留原函数名，内部改用 Tiingo。"""
    return tiingo_get_history(ticker, days=days)



# ── VCP 指标计算 ───────────────────────────────────────────────────────────────

def _compute_vcp_metrics(df: pd.DataFrame, stock: dict) -> dict:
    """
    基于60天日线 DataFrame 计算 VCP 相关指标。

    Returns:
        VCP 指标 dict
    """
    result: dict = {
        "seg1_vol": None, "seg2_vol": None, "seg3_vol": None,
        "vol_seg1": None, "vol_seg2": None, "vol_seg3": None,
        "volatility_contraction": False,
        "volume_contraction":     False,
        "drawdown_from_high":     None,
        "high_60d":               None,
        "min_low_20d":            None,
        "pivot_point":            None,
    }

    n = len(df)
    if n < 20:
        return result

    # 尽量分三段；数据不足60天时按实际行数均分
    seg_size = n // 3
    seg1 = df.iloc[:seg_size]
    seg2 = df.iloc[seg_size:seg_size * 2]
    seg3 = df.iloc[seg_size * 2:]

    def _volatility(seg: pd.DataFrame) -> Optional[float]:
        h, l = seg["high"].max(), seg["low"].min()
        if l > 0:
            return round((h - l) / l * 100, 2)
        return None

    s1_vol = _volatility(seg1)
    s2_vol = _volatility(seg2)
    s3_vol = _volatility(seg3)

    result["seg1_vol"] = s1_vol
    result["seg2_vol"] = s2_vol
    result["seg3_vol"] = s3_vol

    if s1_vol is not None and s2_vol is not None and s3_vol is not None:
        result["volatility_contraction"] = bool(s3_vol < s2_vol < s1_vol)

    # ── 成交量收缩 ────────────────────────────────────────────────────────────
    v1 = seg1["volume"].mean()
    v2 = seg2["volume"].mean()
    v3 = seg3["volume"].mean()
    result["vol_seg1"] = round(v1, 0)
    result["vol_seg2"] = round(v2, 0)
    result["vol_seg3"] = round(v3, 0)
    if v1 > 0:
        result["volume_contraction"] = bool(v3 < v1)

    # ── 距高点回撤 ────────────────────────────────────────────────────────────
    high_60d = df["high"].max()
    current  = df["close"].iloc[-1]
    result["high_60d"] = round(float(high_60d), 2)
    if high_60d > 0:
        result["drawdown_from_high"] = round((high_60d - current) / high_60d * 100, 2)

    # ── 最近20天低点 & pivot ──────────────────────────────────────────────────
    last20 = df.tail(20)
    result["min_low_20d"] = round(float(last20["low"].min()), 2)
    result["pivot_point"] = round(float(last20["high"].max()), 2)

    return result


def _vcp_score(metrics: dict, stock: dict) -> int:
    """计算 VCP 强度评分（0–100+）。"""
    score = 0

    if metrics["volatility_contraction"]:
        score += 35

    if metrics["volume_contraction"]:
        score += 25

    dd = metrics["drawdown_from_high"]
    if dd is not None:
        if 5 <= dd <= 15:
            score += 20
        elif 15 < dd <= 25:
            score += 10

    g60 = stock.get("gain_60d")
    if g60 is not None:
        if g60 > 30:
            score += 15
        elif g60 > VCP_MIN_GAIN_60D:
            score += 8

    if stock.get("consolidating"):
        score += 5

    return score


def _is_vcp(metrics: dict, vcp_scr: int, stock: dict) -> bool:
    """检查是否满足 VCP 触发条件。"""
    if not metrics["volatility_contraction"]:
        return False
    dd = metrics["drawdown_from_high"]
    if dd is None or dd >= VCP_MAX_DRAWDOWN:
        return False
    if vcp_scr < VCP_MIN_SCORE:
        return False
    return True


def calculate_cheat_entry(df: pd.DataFrame, current_price: float,
                          cheat_low: float, cheat_high: float) -> dict:
    """
    分析低吸策略的可行性。

    Args:
        df:            60天历史 OHLCV DataFrame（升序）
        current_price: 当前价格
        cheat_low:     低吸区间下限
        cheat_high:    低吸区间上限

    Returns:
        包含可行性指标的 dict
    """
    result = {
        "distance_to_cheat":      None,
        "slope_5d":               None,
        "vol_shrinking_3d":       False,
        "ma10_in_cheat_zone":     False,
        "ma20_in_cheat_zone":     False,
        "cheat_entry_score":      0,
        "cheat_entry_feasibility": "低",
        "vol_trend":              "N/A",
        "ma_support":             "无支撑",
    }

    n = len(df)
    if n < 5 or current_price <= 0 or cheat_low <= 0:
        return result

    close  = df["close"]
    volume = df["volume"]

    # a) 价格距低吸区的距离
    distance = (current_price - cheat_low) / current_price * 100
    result["distance_to_cheat"] = round(distance, 1)

    # b) 近5日价格趋势
    if n >= 5:
        p5 = float(close.iloc[-5])
        p1 = float(close.iloc[-1])
        slope_5d = (p1 - p5) / p5 * 100 if p5 > 0 else 0
        result["slope_5d"] = round(slope_5d, 2)

    # c) 近3日成交量是否收缩
    if n >= 3:
        vol_shrinking = float(volume.iloc[-1]) < float(volume.iloc[-3])
        result["vol_shrinking_3d"] = vol_shrinking
        result["vol_trend"] = "量缩" if vol_shrinking else "量增"

    # d) 均线支撑
    if n >= 20:
        ma20 = float(close.tail(20).mean())
        result["ma20_in_cheat_zone"] = cheat_low <= ma20 <= cheat_high
    if n >= 10:
        ma10 = float(close.tail(10).mean())
        result["ma10_in_cheat_zone"] = cheat_low <= ma10 <= cheat_high

    if result["ma20_in_cheat_zone"]:
        result["ma_support"] = "MA20强支撑"
    elif result["ma10_in_cheat_zone"]:
        result["ma_support"] = "MA10中等支撑"
    else:
        result["ma_support"] = "无均线支撑"

    # ── 综合评分 ──────────────────────────────────────────────────────────────
    score = 0

    d = result["distance_to_cheat"]
    if d is not None:
        if d < 5:
            score += 30
        elif d < 10:
            score += 20
        elif d < 20:
            score += 10

    s5 = result["slope_5d"]
    if s5 is not None:
        if s5 < -1:
            score += 25
        elif s5 < 0:
            score += 15

    if result["vol_shrinking_3d"]:
        score += 20

    if result["ma20_in_cheat_zone"]:
        score += 25
    elif result["ma10_in_cheat_zone"]:
        score += 15

    result["cheat_entry_score"] = score

    if score >= 60:
        result["cheat_entry_feasibility"] = "高"
    elif score >= 40:
        result["cheat_entry_feasibility"] = "中"
    else:
        result["cheat_entry_feasibility"] = "低"

    return result


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def score(candidates: list) -> list:
    """
    对技术面候选股计算 VCP 评分并返回触发信号的股票。

    Args:
        candidates: technical_filter.run() 返回的股票 dict 列表

    Returns:
        触发 VCP 信号的股票列表（附带 VCP 指标），按 vcp_score 降序
    """
    total   = len(candidates)
    signals = []
    skipped = 0
    stooq_requests = 0

    print(f"[vcp_scorer] 开始 VCP 评分，共 {total} 只候选")
    print(f"  触发条件: volatility_contraction=True & drawdown<{VCP_MAX_DRAWDOWN}% & score>={VCP_MIN_SCORE}")

    for stock in candidates:
        ticker    = stock.get("ticker", "")
        gain_60d  = stock.get("gain_60d")

        if not ticker:
            skipped += 1
            continue

        # 涨幅基础检查（快速剔除，不需要额外请求）
        if gain_60d is not None and gain_60d < VCP_MIN_GAIN_60D:
            continue

        # 尝试从 technical_filter 缓存数据重建 DataFrame
        df = _df_from_cache(stock)

        if df is None:
            # 缓存数据不足，从 Stooq 重新拉取
            df = _get_history_stooq(ticker, days=60)
            stooq_requests += 1
            if df.empty:
                skipped += 1
                continue

        metrics  = _compute_vcp_metrics(df, stock)
        vcp_scr  = _vcp_score(metrics, stock)

        if not _is_vcp(metrics, vcp_scr, stock):
            continue

        current_price = float(stock.get("last_close") or df["close"].iloc[-1])
        pivot         = metrics["pivot_point"]
        min_low_20d   = metrics["min_low_20d"]
        stop_price    = round(min_low_20d * 0.98, 2)

        # ── 止损幅度检查 & cheat entry 逻辑 ──────────────────────────────────
        stop_loss_pct = (current_price - stop_price) / current_price * 100 if current_price > 0 else 999

        if stop_loss_pct > 10:
            # 低吸入场点：当前价和整理低点的中间位
            cheat_low  = round((current_price + min_low_20d) / 2, 2)
            cheat_high = round(cheat_low * 1.03, 2)
            new_stop_pct = (cheat_low - stop_price) / cheat_low * 100 if cheat_low > 0 else 999

            if new_stop_pct > 12:
                continue  # 低吸后止损仍过大，跳过

            cheat_info = calculate_cheat_entry(df, current_price, cheat_low, cheat_high)

            signal = {
                **stock,
                "signal_type":            "VCP_CHEAT_ENTRY",
                "vcp_score":              vcp_scr,
                "volatility_contraction": metrics["volatility_contraction"],
                "volume_contraction":     metrics["volume_contraction"],
                "drawdown_from_high":     metrics["drawdown_from_high"],
                "seg1_vol":               metrics["seg1_vol"],
                "seg2_vol":               metrics["seg2_vol"],
                "seg3_vol":               metrics["seg3_vol"],
                "pivot_point":            pivot,
                "current_price":          current_price,
                "cheat_entry_low":        cheat_low,
                "cheat_entry_high":       cheat_high,
                "entry_zone":             f"{cheat_low:.2f}–{cheat_high:.2f}",
                "stop_loss":              f"{stop_price:.2f}",
                "stop_loss_pct":          round(new_stop_pct, 1),
                "cheat_entry":            True,
                "wait_for_pullback":      True,
                "original_breakout":      pivot,
                # 低吸可行性分析
                "distance_to_cheat":      cheat_info["distance_to_cheat"],
                "slope_5d":               cheat_info["slope_5d"],
                "vol_trend":              cheat_info["vol_trend"],
                "ma_support":             cheat_info["ma_support"],
                "cheat_entry_score":      cheat_info["cheat_entry_score"],
                "cheat_entry_feasibility": cheat_info["cheat_entry_feasibility"],
            }
        else:
            signal = {
                **stock,
                "signal_type":            "VCP",
                "vcp_score":              vcp_scr,
                "volatility_contraction": metrics["volatility_contraction"],
                "volume_contraction":     metrics["volume_contraction"],
                "drawdown_from_high":     metrics["drawdown_from_high"],
                "seg1_vol":               metrics["seg1_vol"],
                "seg2_vol":               metrics["seg2_vol"],
                "seg3_vol":               metrics["seg3_vol"],
                "pivot_point":            pivot,
                "entry_zone":             f"{current_price * 0.99:.2f}–{pivot:.2f}",
                "stop_loss":              f"{stop_price:.2f}",
                "stop_loss_pct":          round(stop_loss_pct, 1),
                "cheat_entry":            False,
                "wait_for_pullback":      False,
                "original_breakout":      None,
            }

        signals.append(signal)

    signals.sort(key=lambda x: x.get("vcp_score", 0), reverse=True)

    print(f"\n[vcp_scorer] 完成")
    print(f"  检测: {total} 只  VCP信号: {len(signals)} 个  跳过: {skipped} 只  "
          f"Stooq请求: {stooq_requests} 次")

    if signals:
        print()
        hdr = (f"{'#':>3} {'Ticker':<7} {'Sector':<20} {'VCPScore':>8} "
               f"{'DD%':>6} {'G60d':>6} {'S1v':>6} {'S2v':>6} {'S3v':>6} "
               f"{'VCont':>5} {'QCont':>5} {'Pivot':>8}")
        print(hdr)
        print("-" * 100)
        for rank, s in enumerate(signals, 1):
            vc  = "Y" if s.get("volatility_contraction") else "N"
            qc  = "Y" if s.get("volume_contraction")     else "N"
            dd  = f"{s['drawdown_from_high']:.1f}%"  if s.get("drawdown_from_high") is not None else "-"
            g60 = f"{s['gain_60d']:+.1f}%"           if s.get("gain_60d")           is not None else "-"
            s1  = f"{s['seg1_vol']:.1f}%"            if s.get("seg1_vol")           is not None else "-"
            s2  = f"{s['seg2_vol']:.1f}%"            if s.get("seg2_vol")           is not None else "-"
            s3  = f"{s['seg3_vol']:.1f}%"            if s.get("seg3_vol")           is not None else "-"
            print(
                f"{rank:>3} {s['ticker']:<7} {str(s.get('sector', ''))[:19]:<20} "
                f"{s['vcp_score']:>8} "
                f"{dd:>6} {g60:>6} {s1:>6} {s2:>6} {s3:>6} "
                f"{vc:>5} {qc:>5} {s.get('pivot_point', '-'):>8}"
            )

    return signals


def _df_from_cache(stock: dict) -> Optional[pd.DataFrame]:
    """
    尝试从 technical_filter 缓存字段重建单行 DataFrame。
    仅含最新一天数据，无法用于 VCP 三段分析，始终返回 None。
    保留此函数作为扩展预留（如未来缓存完整历史）。
    """
    return None


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from screener.fundamental_filter import run as fundamental_run
    from screener.technical_filter import run as technical_run

    print("=" * 60)
    print("Step 1: 基本面候选股")
    print("=" * 60)
    fund_candidates = fundamental_run()
    print(f"基本面候选: {len(fund_candidates)} 只\n")

    print("=" * 60)
    print("Step 2: 技术面过滤")
    print("=" * 60)
    tech_candidates = technical_run(fund_candidates)
    print(f"技术面候选: {len(tech_candidates)} 只\n")

    print("=" * 60)
    print("Step 3: VCP 评分")
    print("=" * 60)
    vcp_signals = score(tech_candidates)

    print()
    if not vcp_signals:
        print("今日无 VCP 信号")
    else:
        print(f"共发现 {len(vcp_signals)} 个 VCP 信号：")
        for s in vcp_signals:
            print(f"  {s['ticker']}: score={s['vcp_score']}  "
                  f"dd={s['drawdown_from_high']:.1f}%  "
                  f"g60={s.get('gain_60d', 0):+.1f}%  "
                  f"entry={s['entry_zone']}  stop={s['stop_loss']}  "
                  f"pivot={s['pivot_point']}")
