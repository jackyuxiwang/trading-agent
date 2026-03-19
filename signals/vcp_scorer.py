"""
vcp_scorer.py — VCP (Volatility Contraction Pattern) 评分模块

基于 Minervini 体系：股价在上涨后进入整理，每次回调幅度递减，
成交量同步萎缩，最后在极低波动中突破。

VCP 触发条件（同时满足）：
  - volatility_contraction = True（三段波动逐步收缩）
  - drawdown_from_high < 30%
  - vcp_score >= 45
"""

import io
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

STOOQ_DELAY = 0.05  # 秒，请求间隔

# ── VCP 触发阈值 ───────────────────────────────────────────────────────────────
VCP_MIN_SCORE         = 45
VCP_MAX_DRAWDOWN      = 30.0   # 最大允许回撤（%）
VCP_MIN_GAIN_60D      = 10.0   # 需要有涨幅基础

# ── Stooq 数据获取 ────────────────────────────────────────────────────────────

def _get_history_stooq(ticker: str, days: int = 60) -> pd.DataFrame:
    """
    从 Stooq 获取最近 N 根日线。

    Returns:
        DataFrame，列: date, open, high, low, close, volume（升序）
        失败时返回空 DataFrame
    """
    url = f"https://stooq.com/q/d/l/?s={ticker.lower()}.us&i=d"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        if "Exceeded the daily hits limit" in resp.text:
            print(f"  [warn] Stooq 达到每日请求上限，请明天重新运行")
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(resp.text))
    except Exception:
        return pd.DataFrame()

    if df.empty or len(df) < 20:
        return pd.DataFrame()

    df.columns = [c.lower() for c in df.columns]
    required = {"date", "open", "high", "low", "close", "volume"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame()

    for col in ("open", "high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    df = df[df["close"] > 0].copy()

    if len(df) < 20:
        return pd.DataFrame()

    return df.sort_values("date").tail(days).reset_index(drop=True)


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
            time.sleep(STOOQ_DELAY)
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

        signal = {
            **stock,
            "signal_type":           "VCP",
            "vcp_score":             vcp_scr,
            "volatility_contraction": metrics["volatility_contraction"],
            "volume_contraction":     metrics["volume_contraction"],
            "drawdown_from_high":     metrics["drawdown_from_high"],
            "seg1_vol":              metrics["seg1_vol"],
            "seg2_vol":              metrics["seg2_vol"],
            "seg3_vol":              metrics["seg3_vol"],
            "pivot_point":           pivot,
            "entry_zone":            f"{current_price * 0.99:.2f}–{pivot:.2f}",
            "stop_loss":             f"{min_low_20d * 0.98:.2f}",
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
