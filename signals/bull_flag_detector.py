"""
bull_flag_detector.py — Bull Flag（牛旗形态）信号检测模块

形态定义：
  强势旗杆（快速上涨）→ 旗面（量缩浅回调）→ 今日放量突破旗面高点

Bull Flag 触发条件（同时满足）：
  - 旗杆涨幅 > 15%，3-10天完成
  - 旗杆均量 > 20日均量 1.5x
  - 旗面回调 < 旗杆涨幅的 50%，且回调幅度 3%-15%
  - 旗面成交量 < 旗杆均量 70%
  - 今日收盘 > 旗面高点（突破）
  - 今日量 > 20日均量 1.5x
  - 今日收阳（close > open）
  - bf_score >= 50
"""

import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.tiingo_client import get_history as tiingo_get_history

HISTORY_DAYS = 60     # 拉取天数（保证有足够历史）
FLAG_DAYS    = 5      # 旗面识别天数（今日前 N 天）
POLE_WINDOW  = 20     # 旗杆搜索窗口（旗面之前 N 天）

# ── 触发阈值 ──────────────────────────────────────────────────────────────────
BF_MIN_SCORE         = 50
BF_MIN_POLE_GAIN     = 15.0    # 旗杆最小涨幅（%）
BF_MAX_POLE_DAYS     = 10      # 旗杆最长天数
BF_MIN_POLE_DAYS     = 3       # 旗杆最短天数
BF_MAX_PULLBACK_RATIO = 0.50   # 旗面回调 < 旗杆涨幅 * 50%
BF_MIN_PULLBACK      = 3.0     # 旗面最小回调（%，太浅不算旗面）
BF_MAX_PULLBACK      = 15.0    # 旗面最大回调（%）
BF_MIN_POLE_VOL_RATIO = 1.5    # 旗杆均量 / 20日均量
BF_MAX_FLAG_VOL_RATIO = 0.70   # 旗面均量 / 旗杆均量（必须收缩）
BF_BREAKOUT_VOL_RATIO = 1.5    # 突破日量 / 20日均量


# ── 数据获取（Tiingo） ────────────────────────────────────────────────────────

def _get_history_stooq(ticker: str, days: int = HISTORY_DAYS) -> pd.DataFrame:
    """保留原函数名，内部改用 Tiingo。"""
    return tiingo_get_history(ticker, days=days)


# ── Bull Flag 形态检测 ────────────────────────────────────────────────────────

def _compute_bull_flag_metrics(df: pd.DataFrame) -> dict:
    """
    基于日线 DataFrame 检测 Bull Flag 形态，返回指标 dict。
    df 必须升序排列，df.iloc[-1] 为今日。
    """
    INVALID = {"valid": False}
    n = len(df)
    min_needed = FLAG_DAYS + BF_MIN_POLE_DAYS + 5
    if n < min_needed:
        return INVALID

    # ── 20日均量 ──────────────────────────────────────────────────────────────
    vol_ma20 = df["volume"].tail(20).mean()
    if vol_ma20 <= 0:
        return INVALID

    # ── 今日（突破候选日）────────────────────────────────────────────────────
    today       = df.iloc[-1]
    today_open  = float(today["open"])
    today_close = float(today["close"])
    today_high  = float(today["high"])
    today_vol   = float(today["volume"])
    today_vol_ratio = today_vol / vol_ma20

    # ── 旗面：今日之前 FLAG_DAYS 天 ──────────────────────────────────────────
    flag = df.iloc[-(FLAG_DAYS + 1):-1]
    if len(flag) < 3:
        return INVALID

    flag_high        = float(flag["high"].max())
    flag_low         = float(flag["low"].min())
    flag_avg_volume  = float(flag["volume"].mean())

    # ── 旗杆搜索窗口：旗面之前 POLE_WINDOW 天 ────────────────────────────────
    pole_start_idx = max(0, n - FLAG_DAYS - 1 - POLE_WINDOW)
    pole_end_idx   = n - FLAG_DAYS - 1
    pole_window    = df.iloc[pole_start_idx:pole_end_idx].reset_index(drop=True)

    if len(pole_window) < BF_MIN_POLE_DAYS:
        return INVALID

    # 在窗口内找最低点，再从最低点向后找最高点（形成旗杆）
    pole_low_loc   = int(pole_window["low"].idxmin())
    after_low      = pole_window.iloc[pole_low_loc:].reset_index(drop=True)

    if len(after_low) < BF_MIN_POLE_DAYS:
        return INVALID

    pole_high_loc  = int(after_low["high"].idxmax())
    pole_low_price = float(pole_window.iloc[pole_low_loc]["low"])
    pole_high_price= float(after_low.iloc[pole_high_loc]["high"])
    pole_duration  = pole_high_loc   # 低点到高点的交易天数

    # 旗杆均量（低点到高点区间）
    pole_data       = after_low.iloc[:pole_high_loc + 1]
    pole_avg_volume = float(pole_data["volume"].mean()) if len(pole_data) > 0 else 0
    pole_vol_ratio  = pole_avg_volume / vol_ma20 if vol_ma20 > 0 else 0

    # ── 计算各项指标 ──────────────────────────────────────────────────────────
    if pole_low_price <= 0:
        return INVALID

    pole_gain_pct      = (pole_high_price - pole_low_price) / pole_low_price * 100
    flag_pullback_pct  = (pole_high_price - flag_low) / pole_high_price * 100
    vol_contraction_pct= flag_avg_volume / pole_avg_volume * 100 if pole_avg_volume > 0 else 100

    return {
        "valid":                True,
        "pole_gain_pct":        round(pole_gain_pct, 2),
        "pole_duration":        pole_duration,
        "pole_high_price":      round(pole_high_price, 2),
        "pole_low_price":       round(pole_low_price, 2),
        "pole_avg_volume":      round(pole_avg_volume, 0),
        "pole_vol_ratio":       round(pole_vol_ratio, 2),
        "flag_high":            round(flag_high, 2),
        "flag_low":             round(flag_low, 2),
        "flag_pullback_pct":    round(flag_pullback_pct, 2),
        "flag_avg_volume":      round(flag_avg_volume, 0),
        "vol_contraction_pct":  round(vol_contraction_pct, 2),
        "vol_ma20":             round(vol_ma20, 0),
        "today_open":           round(today_open, 2),
        "today_close":          round(today_close, 2),
        "today_high":           round(today_high, 2),
        "today_vol":            round(today_vol, 0),
        "today_vol_ratio":      round(today_vol_ratio, 2),
    }


# ── 评分 ──────────────────────────────────────────────────────────────────────

def _bf_score(m: dict) -> int:
    """计算 Bull Flag 强度评分（0-100）。"""
    score = 0

    # 旗杆涨幅
    if m["pole_gain_pct"] > 30:
        score += 25
    elif m["pole_gain_pct"] > 15:
        score += 15

    # 旗杆成交量放大
    if m["pole_vol_ratio"] > 2.0:
        score += 20
    elif m["pole_vol_ratio"] > 1.5:
        score += 10

    # 旗面回调幅度（越浅越好）
    if m["flag_pullback_pct"] < 10:
        score += 20
    elif m["flag_pullback_pct"] < 15:
        score += 10

    # 旗面成交量收缩
    if m["vol_contraction_pct"] < 50:
        score += 20

    # 突破日成交量
    if m["today_vol_ratio"] > 2.0:
        score += 15

    return score


# ── 触发条件检查 ──────────────────────────────────────────────────────────────

def _is_bull_flag(m: dict, score: int) -> bool:
    """检查是否满足 Bull Flag 所有触发条件。"""
    if not m.get("valid"):
        return False
    # 评分
    if score < BF_MIN_SCORE:
        return False
    # 旗杆涨幅
    if m["pole_gain_pct"] <= BF_MIN_POLE_GAIN:
        return False
    # 旗杆时间
    if not (BF_MIN_POLE_DAYS <= m["pole_duration"] <= BF_MAX_POLE_DAYS):
        return False
    # 旗杆量放大
    if m["pole_vol_ratio"] < BF_MIN_POLE_VOL_RATIO:
        return False
    # 旗面回调幅度
    max_pullback = m["pole_gain_pct"] * BF_MAX_PULLBACK_RATIO
    if not (BF_MIN_PULLBACK <= m["flag_pullback_pct"] <= min(BF_MAX_PULLBACK, max_pullback)):
        return False
    # 旗面量收缩
    if m["flag_avg_volume"] >= m["pole_avg_volume"] * BF_MAX_FLAG_VOL_RATIO:
        return False
    # 今日突破
    if m["today_close"] <= m["flag_high"]:
        return False
    # 突破量
    if m["today_vol_ratio"] < BF_BREAKOUT_VOL_RATIO:
        return False
    # 收阳
    if m["today_close"] <= m["today_open"]:
        return False
    return True


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def detect(candidates: list) -> list:
    """
    对技术面候选股检测 Bull Flag 信号。

    Args:
        candidates: technical_filter.run() 返回的股票 dict 列表

    Returns:
        触发 Bull Flag 的股票列表（附带形态指标），按 bf_score 降序
    """
    total          = len(candidates)
    signals        = []
    skipped        = 0
    stooq_requests = 0

    print(f"[bull_flag_detector] 开始 Bull Flag 检测，共 {total} 只候选")
    print(f"  触发条件: pole>{BF_MIN_POLE_GAIN}% & flag_pb<{int(BF_MAX_PULLBACK_RATIO*100)}%pole"
          f" & breakout & score>={BF_MIN_SCORE}")

    for stock in candidates:
        ticker = stock.get("ticker", "")
        if not ticker:
            skipped += 1
            continue

        # 快速剔除：60日涨幅不足旗杆基础的直接跳过
        gain_60d = stock.get("gain_60d")
        if gain_60d is not None and gain_60d < BF_MIN_POLE_GAIN:
            continue

        df = _get_history_stooq(ticker, days=HISTORY_DAYS)
        stooq_requests += 1

        if df.empty:
            skipped += 1
            continue

        m     = _compute_bull_flag_metrics(df)
        score = _bf_score(m) if m["valid"] else 0

        if not _is_bull_flag(m, score):
            continue

        entry_price = m["today_close"]
        stop_price  = round(m["flag_low"] * 0.98, 2)
        pole_gain   = m["pole_gain_pct"] / 100

        signal = {
            **stock,
            "signal_type":           "BULL_FLAG",
            "bf_score":              score,
            "pole_gain_pct":         m["pole_gain_pct"],
            "flag_pullback_pct":     m["flag_pullback_pct"],
            "pole_avg_volume":       m["pole_avg_volume"],
            "flag_avg_volume":       m["flag_avg_volume"],
            "volume_contraction_pct": m["vol_contraction_pct"],
            "pole_duration":         m["pole_duration"],
            "pole_vol_ratio":        m["pole_vol_ratio"],
            "today_vol_ratio":       m["today_vol_ratio"],
            "entry_zone":   f"{entry_price * 0.99:.2f}–{entry_price * 1.01:.2f}",
            "stop_loss":    f"{stop_price:.2f}",
            "target_1":     f"{entry_price * (1 + pole_gain * 0.5):.2f}",
            "target_2":     f"{entry_price * (1 + pole_gain):.2f}",
        }
        signals.append(signal)

    signals.sort(key=lambda x: x.get("bf_score", 0), reverse=True)

    print(f"\n[bull_flag_detector] 完成")
    print(f"  检测: {total} 只  Bull Flag信号: {len(signals)} 个  "
          f"跳过: {skipped} 只  Stooq请求: {stooq_requests} 次")

    if signals:
        print()
        hdr = (f"{'#':>3} {'Ticker':<7} {'Sector':<20} {'BFScore':>7} "
               f"{'Pole%':>7} {'PoleD':>5} {'PoleV':>6} "
               f"{'Flag%':>6} {'VolCon':>7} {'TodayV':>7} {'Stop':>8}")
        print(hdr)
        print("-" * 100)
        for rank, s in enumerate(signals, 1):
            print(
                f"{rank:>3} {s['ticker']:<7} {str(s.get('sector', ''))[:19]:<20} "
                f"{s['bf_score']:>7} "
                f"{s['pole_gain_pct']:>+6.1f}% "
                f"{s['pole_duration']:>5}d "
                f"{s['pole_vol_ratio']:>5.1f}x "
                f"{s['flag_pullback_pct']:>+5.1f}% "
                f"{s['volume_contraction_pct']:>6.0f}% "
                f"{s['today_vol_ratio']:>6.1f}x "
                f"{s['stop_loss']:>8}"
            )
        print()
        for s in signals:
            print(f"  {s['ticker']} — 旗杆 +{s['pole_gain_pct']:.1f}%（{s['pole_duration']}天）"
                  f"  旗面回调 -{s['flag_pullback_pct']:.1f}%"
                  f"  入场 {s['entry_zone']}  止损 {s['stop_loss']}"
                  f"  T1 {s['target_1']}  T2 {s['target_2']}")

    return signals


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
    print("Step 3: Bull Flag 信号检测")
    print("=" * 60)
    bf_signals = detect(tech_candidates)

    print()
    if not bf_signals:
        print("今日无 Bull Flag 信号")
    else:
        print(f"共发现 {len(bf_signals)} 个 Bull Flag 信号：")
        for s in bf_signals:
            print(f"\n  {'='*50}")
            print(f"  {s['ticker']} | score={s['bf_score']}")
            print(f"  旗杆: +{s['pole_gain_pct']:.1f}%  {s['pole_duration']}天  "
                  f"量放大 {s['pole_vol_ratio']:.1f}x")
            print(f"  旗面: 回调 -{s['flag_pullback_pct']:.1f}%  "
                  f"量收缩至 {s['volume_contraction_pct']:.0f}%")
            print(f"  突破量: {s['today_vol_ratio']:.1f}x")
            print(f"  入场 {s['entry_zone']}  止损 {s['stop_loss']}"
                  f"  T1 {s['target_1']}  T2 {s['target_2']}")
