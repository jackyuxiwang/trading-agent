"""
ep_detector.py — Episodic Pivot (EP) 信号检测模块

基于 Qullamaggie 体系：重大催化剂驱动的跳空高开，当天放量，
价格站稳开盘区间高点。

EP 触发条件（同时满足）：
  - gap_pct > 5%（今日开盘 vs 昨日收盘）
  - volume_ratio > 2.0（今日量 / 20日均量）
  - today_close > today_open（收盘强于开盘）
"""

import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.tiingo_client import get_history as tiingo_get_history

# ── EP 触发阈值 ────────────────────────────────────────────────────────────────
EP_MIN_GAP_PCT      = 5.0   # 最小跳空幅度（%）
EP_MIN_VOLUME_RATIO = 2.0   # 最小相对成交量
# today_close > today_open（硬性条件，无阈值）


# ── 数据获取（Tiingo） ────────────────────────────────────────────────────────

def _get_recent_stooq(ticker: str, days: int = 5) -> pd.DataFrame:
    """保留原函数名，内部改用 Tiingo。"""
    return tiingo_get_history(ticker, days=days)

    df["open"]   = pd.to_numeric(df["open"],   errors="coerce")
    df["high"]   = pd.to_numeric(df["high"],   errors="coerce")
    df["low"]    = pd.to_numeric(df["low"],    errors="coerce")
    df["close"]  = pd.to_numeric(df["close"],  errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    df = df[df["close"] > 0].copy()

    if len(df) < 2:
        return pd.DataFrame()

    return df.sort_values("date").tail(days).reset_index(drop=True)


# ── EP 指标计算 ────────────────────────────────────────────────────────────────

def _compute_ep_metrics(df: pd.DataFrame, vol_ma20: Optional[float]) -> Optional[dict]:
    """
    基于最近日线数据计算 EP 指标。

    Args:
        df:       至少含最近2行的 OHLCV DataFrame（升序）
        vol_ma20: 20日均量（来自 technical_filter 结果）

    Returns:
        EP 指标 dict；无法计算时返回 None
    """
    if len(df) < 2:
        return None

    today     = df.iloc[-1]
    yesterday = df.iloc[-2]

    today_open  = today["open"]
    today_high  = today["high"]
    today_low   = today["low"]
    today_close = today["close"]
    today_vol   = today["volume"]
    yest_close  = yesterday["close"]

    if yest_close <= 0 or today_open <= 0:
        return None

    gap_pct       = (today_open - yest_close) / yest_close * 100
    close_vs_open = (today_close - today_open) / today_open * 100

    volume_ratio: Optional[float] = None
    if vol_ma20 and vol_ma20 > 0:
        volume_ratio = round(today_vol / vol_ma20, 2)

    return {
        "gap_pct":       round(gap_pct, 2),
        "close_vs_open": round(close_vs_open, 2),
        "volume_ratio":  volume_ratio,
        "today_open":    round(float(today_open), 2),
        "today_high":    round(float(today_high), 2),
        "today_low":     round(float(today_low), 2),
        "today_close":   round(float(today_close), 2),
    }


def _ep_score(metrics: dict, technical_score: int) -> int:
    """计算 EP 强度评分（0–100+）。"""
    score = 0

    gap = metrics["gap_pct"]
    if gap > 15:
        score += 40
    elif gap > 10:
        score += 25
    elif gap > 5:
        score += 15

    rv = metrics["volume_ratio"]
    if rv is not None:
        if rv > 5.0:
            score += 30
        elif rv > 3.0:
            score += 20
        elif rv > 2.0:
            score += 10

    cvo = metrics["close_vs_open"]
    if cvo > 2:
        score += 20
    elif cvo > 0:
        score += 10

    if technical_score > 60:
        score += 10

    return score


def _metrics_from_cache(stock: dict, vol_ma20: Optional[float]) -> Optional[dict]:
    """
    从 technical_filter 缓存的字段直接构建 EP 指标，无需额外 Stooq 请求。
    仅当 stock 包含 last_open/last_close/prev_close 时返回 dict，否则返回 None。
    """
    required_keys = {"last_open", "last_high", "last_low", "last_close", "prev_close"}
    if not required_keys.issubset(stock.keys()):
        return None

    today_open  = stock["last_open"]
    today_high  = stock["last_high"]
    today_low   = stock["last_low"]
    today_close = stock["last_close"]
    today_vol   = stock.get("last_volume", 0)
    yest_close  = stock["prev_close"]

    if yest_close <= 0 or today_open <= 0:
        return None

    gap_pct       = (today_open - yest_close) / yest_close * 100
    close_vs_open = (today_close - today_open) / today_open * 100

    volume_ratio: Optional[float] = None
    if vol_ma20 and vol_ma20 > 0:
        volume_ratio = round(today_vol / vol_ma20, 2)

    return {
        "gap_pct":       round(gap_pct, 2),
        "close_vs_open": round(close_vs_open, 2),
        "volume_ratio":  volume_ratio,
        "today_open":    round(float(today_open), 2),
        "today_high":    round(float(today_high), 2),
        "today_low":     round(float(today_low), 2),
        "today_close":   round(float(today_close), 2),
    }


def _is_ep(metrics: dict) -> bool:
    """检查是否满足 EP 触发条件。"""
    if metrics["gap_pct"] <= EP_MIN_GAP_PCT:
        return False
    if metrics["volume_ratio"] is None or metrics["volume_ratio"] <= EP_MIN_VOLUME_RATIO:
        return False
    if metrics["today_close"] <= metrics["today_open"]:
        return False
    return True


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def detect(candidates: list) -> list:
    """
    对技术面候选股检测 EP 信号。

    Args:
        candidates: technical_filter.run() 返回的股票 dict 列表

    Returns:
        触发 EP 信号的股票列表（附带 EP 指标），按 ep_score 降序
    """
    total   = len(candidates)
    signals = []
    skipped = 0

    print(f"[ep_detector] 开始 EP 检测，共 {total} 只候选")
    print(f"  触发条件: gap>{EP_MIN_GAP_PCT}% & vol_ratio>{EP_MIN_VOLUME_RATIO}x & close>open")

    for stock in candidates:
        ticker     = stock.get("ticker", "")
        vol_ma20   = stock.get("vol_ma20")
        tech_score = stock.get("technical_score", 0)

        if not ticker:
            skipped += 1
            continue

        # 优先使用 technical_filter 已缓存的最近2天数据，避免重复请求 Stooq
        metrics = _metrics_from_cache(stock, vol_ma20)
        if metrics is None:
            df = _get_recent_stooq(ticker, days=5)
            if df.empty:
                skipped += 1
                continue
            metrics = _compute_ep_metrics(df, vol_ma20)

        if metrics is None:
            skipped += 1
            continue

        if not _is_ep(metrics):
            continue

        ep_scr = _ep_score(metrics, tech_score)

        signal = {
            **stock,
            "signal_type":   "EP",
            "gap_pct":       metrics["gap_pct"],
            "volume_ratio":  metrics["volume_ratio"],
            "close_vs_open": metrics["close_vs_open"],
            "ep_score":      ep_scr,
            "entry_zone":    f"{metrics['today_open']:.2f}–{metrics['today_high']:.2f}",
            "stop_loss":     f"{metrics['today_low'] * 0.99:.2f}",
        }
        signals.append(signal)

    signals.sort(key=lambda x: x.get("ep_score", 0), reverse=True)

    print(f"\n[ep_detector] 完成")
    print(f"  检测: {total} 只  EP信号: {len(signals)} 个  跳过: {skipped} 只")

    if signals:
        print()
        hdr = (f"{'#':>3} {'Ticker':<7} {'Sector':<20} {'EPScore':>7} "
               f"{'Gap':>7} {'VolRatio':>8} {'C/O':>6} "
               f"{'EntryZone':<18} {'Stop':>8}")
        print(hdr)
        print("-" * 90)
        for rank, s in enumerate(signals, 1):
            print(
                f"{rank:>3} {s['ticker']:<7} {str(s.get('sector', ''))[:19]:<20} "
                f"{s['ep_score']:>7} "
                f"{s['gap_pct']:>+6.1f}% {s['volume_ratio']:>7.2f}x "
                f"{s['close_vs_open']:>+5.1f}% "
                f"{s['entry_zone']:<18} {s['stop_loss']:>8}"
            )

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
    print("Step 3: EP 信号检测")
    print("=" * 60)
    ep_signals = detect(tech_candidates)

    print()
    if not ep_signals:
        print("今日无 EP 信号")
    else:
        print(f"共发现 {len(ep_signals)} 个 EP 信号：")
        for s in ep_signals:
            print(f"  {s['ticker']}: gap={s['gap_pct']:+.1f}%  "
                  f"vol={s['volume_ratio']:.1f}x  "
                  f"ep_score={s['ep_score']}  "
                  f"entry={s['entry_zone']}  stop={s['stop_loss']}")
