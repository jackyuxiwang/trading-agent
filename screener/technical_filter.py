"""
technical_filter.py — 技术面筛选模块（Tiingo 数据源）

对基本面候选股做技术面二次过滤，保留处于均线多头排列、
有近期动量且波动收缩的股票。

数据源: Tiingo EOD API（via data/tiingo_client.py）

硬性条件: stage2_check（close > MA20 且 close > MA50）
评分条件: 动量 + 波动收缩 + 成交量放大，总分 >= 35 进入最终候选
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.tiingo_client import get_history as tiingo_get_history

CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"

# ── 技术面阈值 ────────────────────────────────────────────────────────────────
CONSOLIDATION_RATIO = 0.85   # atr_10 / atr_30 < 此值视为收缩
MIN_TECH_SCORE      = 35     # 最低技术得分

# ── 评分权重 ──────────────────────────────────────────────────────────────────
SCORE_STAGE2          = 25
SCORE_GAIN20_HIGH     = 20   # gain_20d > 10%
SCORE_GAIN20_MID      = 10   # gain_20d > 5%
SCORE_CONSOLIDATING   = 25
SCORE_RVOL_HIGH       = 20   # relative_volume > 2.0
SCORE_RVOL_MID        = 10   # relative_volume > 1.5
SCORE_GAIN60_BONUS    = 10   # gain_60d > 30%


# ── 缓存工具 ──────────────────────────────────────────────────────────────────

def _cache_path(name: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{name}.json"


def _load_cache(name: str) -> Optional[list]:
    path = _cache_path(name)
    if path.exists():
        print(f"  [cache] 命中缓存: {path.name}")
        return json.loads(path.read_text(encoding="utf-8"))
    return None


class _NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        import numpy as np
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        return super().default(obj)


def _save_cache(name: str, data: list) -> None:
    path = _cache_path(name)
    path.write_text(json.dumps(data, ensure_ascii=False, cls=_NumpyEncoder), encoding="utf-8")
    print(f"  [cache] 已写入缓存: {path.name}")


# ── 数据获取（Tiingo） ────────────────────────────────────────────────────────

def get_history_stooq(ticker: str, days: int = 60) -> pd.DataFrame:
    """保留原函数名以兼容外部调用，内部改用 Tiingo。"""
    return tiingo_get_history(ticker, days=days)


# ── 技术指标计算 ──────────────────────────────────────────────────────────────

def _compute_technicals(df: pd.DataFrame) -> dict:
    """
    对单只股票的日线 DataFrame 计算全部技术指标。

    Returns:
        技术指标 dict；计算失败的字段为 None
    """
    result: dict = {
        "ma20": None, "ma50": None,
        "atr_10": None, "atr_30": None,
        "vol_ma20": None, "relative_volume": None,
        "gain_20d": None, "gain_60d": None,
        "consolidating": None,
        "stage2_check": False,
        "technical_score": 0,
    }

    if df.empty or len(df) < 5:
        return result

    closes  = df["close"]
    highs   = df["high"]
    lows    = df["low"]
    volumes = df["volume"]
    latest  = closes.iloc[-1]

    # ── 均线 ─────────────────────────────────────────────────────────────────
    ma20 = closes.iloc[-20:].mean() if len(closes) >= 20 else closes.mean()
    ma50 = closes.iloc[-50:].mean() if len(closes) >= 50 else closes.mean()
    result["ma20"] = round(ma20, 2)
    result["ma50"] = round(ma50, 2)

    # ── Stage 2（均线多头）────────────────────────────────────────────────────
    result["stage2_check"] = bool(latest > ma20 and latest > ma50)

    # ── 涨幅 ─────────────────────────────────────────────────────────────────
    if len(closes) >= 2:
        base_60 = closes.iloc[0]
        if base_60 > 0:
            result["gain_60d"] = round((latest - base_60) / base_60 * 100, 2)

    if len(closes) >= 20:
        base_20 = closes.iloc[-20]
        if base_20 > 0:
            result["gain_20d"] = round((latest - base_20) / base_20 * 100, 2)

    # ── ATR（使用 high - low 简化版，不需要前收） ──────────────────────────────
    tr = (highs - lows).abs()
    if len(tr) >= 10:
        result["atr_10"] = round(tr.iloc[-10:].mean(), 4)
    if len(tr) >= 30:
        result["atr_30"] = round(tr.iloc[-30:].mean(), 4)
    elif len(tr) >= 10:
        result["atr_30"] = round(tr.mean(), 4)   # 不足30天用全部数据

    # ── 波动收缩 ──────────────────────────────────────────────────────────────
    if result["atr_10"] is not None and result["atr_30"] is not None and result["atr_30"] > 0:
        result["consolidating"] = bool(result["atr_10"] < result["atr_30"] * CONSOLIDATION_RATIO)

    # ── 相对成交量 ────────────────────────────────────────────────────────────
    if len(volumes) >= 21:
        vol_ma20 = volumes.iloc[-21:-1].mean()   # 前20日均量（排除今日）
        result["vol_ma20"] = round(vol_ma20, 0)
        today_vol = volumes.iloc[-1]
        if vol_ma20 > 0:
            result["relative_volume"] = round(today_vol / vol_ma20, 2)

    # ── 技术得分 ──────────────────────────────────────────────────────────────
    score = 0
    if result["stage2_check"]:
        score += SCORE_STAGE2

    g20 = result["gain_20d"]
    if g20 is not None:
        if g20 > 10:
            score += SCORE_GAIN20_HIGH
        elif g20 > 5:
            score += SCORE_GAIN20_MID

    if result["consolidating"]:
        score += SCORE_CONSOLIDATING

    rv = result["relative_volume"]
    if rv is not None:
        if rv > 2.0:
            score += SCORE_RVOL_HIGH
        elif rv > 1.5:
            score += SCORE_RVOL_MID

    if result["gain_60d"] is not None and result["gain_60d"] > 30:
        score += SCORE_GAIN60_BONUS

    result["technical_score"] = score
    return result


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def run(candidates: list, date: Optional[str] = None) -> list:
    """
    对基本面候选股执行技术面过滤。

    Args:
        candidates: fundamental_filter.run() 返回的股票 dict 列表
        date:       指定日期 "YYYY-MM-DD"（回测用），默认 None 表示今天

    Returns:
        通过技术面过滤的股票列表（附带技术指标），按 technical_score 降序
    """
    today = datetime.today().strftime("%Y-%m-%d")
    cache_key = f"technical_candidates_{date or today}"
    cached = _load_cache(cache_key)
    if cached is not None:
        print(f"[technical_filter] 从缓存加载 {len(cached)} 只技术面候选")
        return cached

    total   = len(candidates)
    passed  = []
    skipped = 0
    t_start = time.time()

    print(f"[technical_filter] 开始技术面过滤，共 {total} 只候选")
    print(f"  数据源: Tiingo EOD")
    print(f"  硬性条件: Stage2（close > MA20 且 close > MA50）")
    print(f"  最低得分: {MIN_TECH_SCORE}")

    for i, stock in enumerate(candidates, 1):
        ticker = stock.get("ticker", "")

        if i % 50 == 0 or i == total:
            elapsed = time.time() - t_start
            rate    = i / elapsed if elapsed > 0 else 1
            eta     = (total - i) / rate
            print(f"  [进度] {i:4d}/{total}  通过 {len(passed):3d} 只  "
                  f"跳过 {skipped:3d} 只  已用 {elapsed:.0f}s  ETA {eta:.0f}s")

        if not ticker:
            skipped += 1
            continue

        df = get_history_stooq(ticker, days=60)
        if df.empty:
            skipped += 1
            continue

        tech = _compute_technicals(df)

        # 硬性条件
        if not tech["stage2_check"]:
            continue

        # 评分门槛
        if tech["technical_score"] < MIN_TECH_SCORE:
            continue

        # 保存最近2天 OHLCV，供 ep_detector 复用（避免重复请求 Stooq）
        last2 = {}
        if len(df) >= 2:
            t = df.iloc[-1]
            y = df.iloc[-2]
            last2 = {
                "last_open":   round(float(t["open"]),   2),
                "last_high":   round(float(t["high"]),   2),
                "last_low":    round(float(t["low"]),    2),
                "last_close":  round(float(t["close"]),  2),
                "last_volume": int(t["volume"]),
                "prev_close":  round(float(y["close"]),  2),
                "last_date":   str(t["date"]),
            }

        passed.append({**stock, **tech, **last2})

    elapsed_total = time.time() - t_start
    passed.sort(key=lambda x: x.get("technical_score", 0), reverse=True)

    print(f"\n[technical_filter] 完成")
    print(f"  输入: {total} 只  输出: {len(passed)} 只  跳过: {skipped} 只")
    print(f"  总耗时: {elapsed_total:.1f}s（{elapsed_total/60:.1f} 分钟）")

    _save_cache(cache_key, passed)
    return passed


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from screener.fundamental_filter import run as fundamental_run

    t0 = time.time()

    print("=" * 60)
    print("Step 1: 基本面候选股")
    print("=" * 60)
    fund_candidates = fundamental_run()
    print(f"基本面候选: {len(fund_candidates)} 只\n")

    print("=" * 60)
    print("Step 2: 技术面过滤")
    print("=" * 60)
    results = run(fund_candidates)

    total_elapsed = time.time() - t0

    print(f"\n总耗时: {total_elapsed:.1f}s")
    print(f"输入: {len(fund_candidates)} 只 → 输出: {len(results)} 只")

    if not results:
        print("无候选股票通过技术面过滤")
    else:
        # 得分分布
        buckets = {"≥75": 0, "60-74": 0, "45-59": 0, "35-44": 0}
        for r in results:
            s = r.get("technical_score", 0)
            if s >= 75:
                buckets["≥75"] += 1
            elif s >= 60:
                buckets["60-74"] += 1
            elif s >= 45:
                buckets["45-59"] += 1
            else:
                buckets["35-44"] += 1
        print("\n得分分布:")
        for rng, cnt in buckets.items():
            print(f"  {rng}分: {cnt:3d} 只  {'█' * min(cnt, 40)}")

        # 前15名
        n = min(15, len(results))
        print(f"\n前 {n} 名（按技术得分降序）:")
        hdr = (f"{'#':>3} {'Ticker':<7} {'Sector':<20} {'Score':>5} "
               f"{'G20d':>6} {'G60d':>6} {'RVol':>5} "
               f"{'A10':>6} {'A30':>6} {'Cons':>5} {'MA20':>8}")
        print(hdr)
        print("-" * 85)
        for rank, r in enumerate(results[:n], 1):
            g20  = f"{r['gain_20d']:+.1f}%"  if r.get("gain_20d")  is not None else "   -"
            g60  = f"{r['gain_60d']:+.1f}%"  if r.get("gain_60d")  is not None else "   -"
            rv   = f"{r['relative_volume']:.2f}x" if r.get("relative_volume") is not None else "  -"
            a10  = f"{r['atr_10']:.2f}"       if r.get("atr_10")   is not None else "  -"
            a30  = f"{r['atr_30']:.2f}"       if r.get("atr_30")   is not None else "  -"
            cons = "✓" if r.get("consolidating") else "✗"
            ma20 = f"{r['ma20']:.2f}"         if r.get("ma20")     is not None else "     -"
            print(
                f"{rank:>3} {r['ticker']:<7} {str(r.get('sector',''))[:19]:<20} "
                f"{r['technical_score']:>5} "
                f"{g20:>6} {g60:>6} {rv:>5} "
                f"{a10:>6} {a30:>6} {cons:>5} {ma20:>8}"
            )
