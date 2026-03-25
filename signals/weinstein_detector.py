"""
weinstein_detector.py — Stan Weinstein Stage 分析模块

识别两种信号：
  A. WEINSTEIN_S2       — Stage 1→2 转换（刚刚突破30周线，最早期买点）
  B. WEINSTEIN_S2_PULLBACK — Stage 2 回调支撑买点（已在上涨中，回调到30周线附近）

触发条件（同时满足）：
  - ma30w_slope > 0（30周均线上翘）
  - close > ma30w（价格在30周线上方）
  - weinstein_score >= 45
"""

import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.tiingo_client import get_history as tiingo_get_history

HISTORY_DAYS = 200    # 约40周

# ── 均线参数 ──────────────────────────────────────────────────────────────────
MA30W_PERIOD  = 150   # 30周 ≈ 150交易日
MA10W_PERIOD  = 50    # 10周 ≈ 50交易日
SLOPE_WINDOW  = 20    # 用最近20天均线计算斜率

# ── 触发阈值 ──────────────────────────────────────────────────────────────────
WS_MIN_SCORE  = 45


# ── 数据获取（Tiingo） ────────────────────────────────────────────────────────

def _get_history_stooq(ticker: str, days: int = HISTORY_DAYS) -> pd.DataFrame:
    """保留原函数名，内部改用 Tiingo。"""
    return tiingo_get_history(ticker, days=days)


# ── 指标计算 ──────────────────────────────────────────────────────────────────

def _compute_weinstein_metrics(df: pd.DataFrame) -> dict:
    """
    计算 Weinstein Stage 相关指标。
    df 升序排列，df.iloc[-1] 为今日。
    """
    INVALID = {"valid": False}
    n = len(df)
    if n < MA30W_PERIOD:
        return INVALID

    close  = df["close"]
    volume = df["volume"]

    # ── 均线 ──────────────────────────────────────────────────────────────────
    ma30w = float(close.tail(MA30W_PERIOD).mean())
    ma10w = float(close.tail(MA10W_PERIOD).mean())

    if ma30w <= 0:
        return INVALID

    # ── 30周均线斜率（用最近 SLOPE_WINDOW 天均线的首尾差除以均值归一化）────────
    # 滚动计算每天的 MA30W，取最近 SLOPE_WINDOW 天的序列
    rolling_ma30w = close.rolling(MA30W_PERIOD).mean().dropna()
    if len(rolling_ma30w) < SLOPE_WINDOW:
        return INVALID

    slope_series = rolling_ma30w.tail(SLOPE_WINDOW)
    ma30w_slope  = float(
        (slope_series.iloc[-1] - slope_series.iloc[0]) / (slope_series.iloc[0] * SLOPE_WINDOW)
        * 100   # 单位：% / 天，方便阈值判断
    )

    # ── 今日价格 & 成交量 ──────────────────────────────────────────────────────
    today_close = float(close.iloc[-1])
    today_open  = float(df["open"].iloc[-1])
    today_vol   = float(volume.iloc[-1])
    vol_ma20    = float(volume.tail(20).mean())
    vol_ratio   = today_vol / vol_ma20 if vol_ma20 > 0 else 0

    price_vs_ma30w_pct = (today_close - ma30w) / ma30w * 100
    ma10w_vs_ma30w_pct = (ma10w - ma30w) / ma30w * 100

    # ── 涨幅 ──────────────────────────────────────────────────────────────────
    gain_20d = None
    gain_60d = None
    if n >= 20:
        price_20d_ago = float(close.iloc[-21])
        if price_20d_ago > 0:
            gain_20d = (today_close - price_20d_ago) / price_20d_ago * 100
    if n >= 60:
        price_60d_ago = float(close.iloc[-61])
        if price_60d_ago > 0:
            gain_60d = (today_close - price_60d_ago) / price_60d_ago * 100

    # ── 60天震荡幅度（Stage 1 特征）────────────────────────────────────────────
    last60 = df.tail(60)
    hi60   = float(last60["high"].max())
    lo60   = float(last60["low"].min())
    range60_pct = (hi60 - lo60) / lo60 * 100 if lo60 > 0 else 999

    # ── 历史成交量均值（与近期比较）────────────────────────────────────────────
    vol_hist_mean = float(volume.tail(MA30W_PERIOD).mean())
    vol_recent_ratio = float(volume.tail(20).mean()) / vol_hist_mean if vol_hist_mean > 0 else 1

    return {
        "valid":               True,
        "ma30w":               round(ma30w, 2),
        "ma10w":               round(ma10w, 2),
        "ma30w_slope":         round(ma30w_slope, 4),
        "today_close":         round(today_close, 2),
        "today_open":          round(today_open, 2),
        "today_vol":           round(today_vol, 0),
        "vol_ma20":            round(vol_ma20, 0),
        "vol_ratio":           round(vol_ratio, 2),
        "price_vs_ma30w_pct":  round(price_vs_ma30w_pct, 2),
        "ma10w_vs_ma30w_pct":  round(ma10w_vs_ma30w_pct, 2),
        "gain_20d":            round(gain_20d, 2) if gain_20d is not None else None,
        "gain_60d":            round(gain_60d, 2) if gain_60d is not None else None,
        "range60_pct":         round(range60_pct, 2),
        "vol_recent_ratio":    round(vol_recent_ratio, 2),
    }


# ── Stage 분류 ────────────────────────────────────────────────────────────────

def _classify_stage(m: dict) -> Optional[str]:
    """
    根据指标判断触发哪种 Weinstein 信号类型，或 None（不触发）。
    返回 "WEINSTEIN_S2" / "WEINSTEIN_S2_PULLBACK" / None
    """
    if not m["valid"]:
        return None

    pct   = m["price_vs_ma30w_pct"]
    slope = m["ma30w_slope"]
    g20   = m["gain_20d"]
    g60   = m["gain_60d"]

    # 基本门槛：均线向上 + 价格在均线上方
    if slope <= 0 or pct <= 0:
        return None

    # ── A. Stage 2 突破（刚刚突破，最早期买点）─────────────────────────────────
    # 价格刚站上 ma30w（2%-20% 上方），短均线 > 长均线，近20天有涨幅，今日放量
    if (2.0 <= pct <= 20.0
            and m["ma10w"] > m["ma30w"]
            and g20 is not None and g20 > 10.0
            and m["vol_ratio"] >= 1.5):
        return "WEINSTEIN_S2"

    # ── B. Stage 2 回调买点（已在上涨中，回调到30周线附近）────────────────────
    # 价格在 ma30w 的 0-8% 上方（回调到支撑），60天有涨幅基础
    if (0.0 < pct <= 8.0
            and slope > 0
            and g60 is not None and g60 > 20.0):
        return "WEINSTEIN_S2_PULLBACK"

    return None


# ── 评分 ──────────────────────────────────────────────────────────────────────

def _weinstein_score(m: dict, signal_subtype: str) -> int:
    """计算 Weinstein 强度评分（0-100）。"""
    score = 0

    # 基础分（信号类型）
    if signal_subtype == "WEINSTEIN_S2":
        score += 40
    elif signal_subtype == "WEINSTEIN_S2_PULLBACK":
        score += 30

    # 均线斜率强劲
    if m["ma30w_slope"] > 0.1:
        score += 20
    elif m["ma30w_slope"] > 0:
        score += 8

    # 成交量放大
    if m["vol_ratio"] > 2.0:
        score += 20
    elif m["vol_ratio"] > 1.5:
        score += 10

    # 60天涨幅基础
    g60 = m["gain_60d"]
    if g60 is not None:
        if g60 > 40:
            score += 15
        elif g60 > 20:
            score += 10

    # 短均线领先幅度
    if m["ma10w_vs_ma30w_pct"] > 5:
        score += 5

    return score


# ── Stage 描述 ────────────────────────────────────────────────────────────────

def _stage_description(m: dict, subtype: str) -> str:
    pct   = m["price_vs_ma30w_pct"]
    slope = m["ma30w_slope"]
    g60   = m["gain_60d"] or 0

    if subtype == "WEINSTEIN_S2":
        return (f"Stage 1→2 突破：价格站上30周线 +{pct:.1f}%，"
                f"30周均线上翘（斜率 {slope:+.3f}），"
                f"近20天涨幅 {m['gain_20d'] or 0:+.1f}%，"
                f"今日量 {m['vol_ratio']:.1f}x")
    else:
        return (f"Stage 2 回调支撑：价格回踩至30周线 +{pct:.1f}%，"
                f"60天涨幅 {g60:+.1f}%，"
                f"30周均线仍在上翘（斜率 {slope:+.3f}）")


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def detect(candidates: list) -> list:
    """
    对技术面候选股检测 Weinstein Stage 2 信号。

    Args:
        candidates: technical_filter.run() 返回的股票 dict 列表

    Returns:
        触发 Weinstein 信号的股票列表，按 weinstein_score 降序
    """
    total          = len(candidates)
    signals        = []
    skipped        = 0
    stooq_requests = 0

    print(f"[weinstein_detector] 开始 Weinstein Stage 分析，共 {total} 只候选")
    print(f"  信号类型: S2突破 & S2回调支撑  最低分: {WS_MIN_SCORE}")

    for stock in candidates:
        ticker = stock.get("ticker", "")
        if not ticker:
            skipped += 1
            continue

        df = _get_history_stooq(ticker, days=HISTORY_DAYS)
        stooq_requests += 1
        time.sleep(STOOQ_DELAY)

        if df.empty:
            skipped += 1
            continue

        m       = _compute_weinstein_metrics(df)
        subtype = _classify_stage(m)

        if subtype is None:
            continue

        score = _weinstein_score(m, subtype)
        if score < WS_MIN_SCORE:
            continue
        if m["ma30w_slope"] <= 0 or m["price_vs_ma30w_pct"] <= 0:
            continue

        entry_price = m["today_close"]
        stop_price  = round(m["ma30w"] * 0.97, 2)

        signal = {
            **stock,
            "signal_type":        subtype,
            "weinstein_score":    score,
            "ma30w":              m["ma30w"],
            "ma10w":              m["ma10w"],
            "ma30w_slope":        m["ma30w_slope"],
            "price_vs_ma30w_pct": m["price_vs_ma30w_pct"],
            "vol_ratio":          m["vol_ratio"],
            "gain_20d":           m["gain_20d"],
            "gain_60d":           m["gain_60d"],
            "stage_description":  _stage_description(m, subtype),
            "entry_zone":   f"{entry_price * 0.99:.2f}–{entry_price * 1.01:.2f}",
            "stop_loss":    f"{stop_price:.2f}",
            "target_1":     f"{entry_price * 1.20:.2f}",
            "target_2":     f"{entry_price * 1.35:.2f}",
        }
        signals.append(signal)

    signals.sort(key=lambda x: x.get("weinstein_score", 0), reverse=True)

    print(f"\n[weinstein_detector] 完成")
    print(f"  检测: {total} 只  Weinstein信号: {len(signals)} 个  "
          f"跳过: {skipped} 只  Stooq请求: {stooq_requests} 次")

    if signals:
        print()
        hdr = (f"{'#':>3} {'Ticker':<7} {'Sector':<20} {'WScore':>6} "
               f"{'Type':<22} {'vs30W':>6} {'Slope':>7} "
               f"{'VolR':>5} {'G60d':>6} {'Stop':>8}")
        print(hdr)
        print("-" * 105)
        for rank, s in enumerate(signals, 1):
            g60_str  = f"{s['gain_60d']:+.1f}%" if s["gain_60d"] is not None else "N/A"
            print(
                f"{rank:>3} {s['ticker']:<7} {str(s.get('sector', ''))[:19]:<20} "
                f"{s['weinstein_score']:>6} "
                f"{s['signal_type']:<22} "
                f"{s['price_vs_ma30w_pct']:>+5.1f}% "
                f"{s['ma30w_slope']:>+7.3f} "
                f"{s['vol_ratio']:>4.1f}x "
                f"{g60_str:>6} "
                f"{s['stop_loss']:>8}"
            )
        print()
        for s in signals:
            print(f"  {s['ticker']} — {s['stage_description']}")
            print(f"    入场 {s['entry_zone']}  止损 {s['stop_loss']}"
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
    print("Step 3: Weinstein Stage 分析")
    print("=" * 60)
    ws_signals = detect(tech_candidates)

    print()
    if not ws_signals:
        print("今日无 Weinstein 信号")
    else:
        print(f"共发现 {len(ws_signals)} 个 Weinstein 信号：")
        for s in ws_signals:
            print(f"\n  {'='*55}")
            print(f"  {s['ticker']} | {s['signal_type']} | score={s['weinstein_score']}")
            print(f"  {s['stage_description']}")
            print(f"  MA30W=${s['ma30w']:.2f}  MA10W=${s['ma10w']:.2f}"
                  f"  价格距MA30W {s['price_vs_ma30w_pct']:+.1f}%")
            print(f"  入场 {s['entry_zone']}  止损 {s['stop_loss']}"
                  f"  T1 {s['target_1']}  T2 {s['target_2']}")
