"""
fundamental_filter.py — 两步走基本面筛选模块

第一步（Polygon）: 量价初筛，全市场 ~10,000 只 → ~2,000-3,000 只
  - volume > 500,000
  - close > $5
  - close > open * 0.5（排除异常数据）

第二步（finvizfinance quote）: 基本面精筛，逐只查询
  - EPS Q/Q > 10%
  - Sales Q/Q > 10%
  - Gross Margin > 20%
  - Market Cap 5亿–500亿美元
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.polygon_client import get_grouped_daily

CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"

# ── 第一步：量价初筛阈值 ───────────────────────────────────────────────────────
STAGE1_MIN_VOLUME     = 500_000
STAGE1_MIN_CLOSE      = 5.0
STAGE1_CLOSE_OPEN_RATIO = 0.5   # close > open * 此值，排除腰斩异常数据

# ── 第二步：基本面精筛阈值 ────────────────────────────────────────────────────
STAGE2_MIN_EPS_GROWTH   = 10.0  # %
STAGE2_MIN_SALES_GROWTH = 10.0  # %
STAGE2_MIN_GROSS_MARGIN = 20.0  # %
STAGE2_MIN_MARKET_CAP   = 500_000_000       # 5亿
STAGE2_MAX_MARKET_CAP   = 50_000_000_000    # 500亿

STAGE2_REQUEST_DELAY    = 0.2   # 秒，控制 finviz 请求频率


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


def _save_cache(name: str, data: list) -> None:
    path = _cache_path(name)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"  [cache] 已写入缓存: {path.name}")


# ── 字段解析工具 ──────────────────────────────────────────────────────────────

def _parse_pct(val) -> Optional[float]:
    """'98.89%' 或 '-5.2%' → float；无法解析返回 None。"""
    if val is None or val in ("-", "", "N/A"):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).strip().replace("%", "").replace(",", ""))
    except ValueError:
        return None


def _parse_market_cap(val) -> Optional[float]:
    """
    解析 finviz 返回的市值字符串：
      '12.34B' → 12_340_000_000
      '345.6M' → 345_600_000
      '-'      → None
    """
    if val is None or val in ("-", "", "N/A"):
        return None
    s = str(val).strip().replace(",", "")
    multipliers = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}
    if s and s[-1].upper() in multipliers:
        try:
            return float(s[:-1]) * multipliers[s[-1].upper()]
        except ValueError:
            pass
    try:
        return float(s)
    except ValueError:
        return None


def _format_market_cap(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    if val >= 1e12:
        return f"{val/1e12:.2f}T"
    if val >= 1e9:
        return f"{val/1e9:.2f}B"
    if val >= 1e6:
        return f"{val/1e6:.0f}M"
    return str(val)


# ── 第一步：Polygon 量价初筛 ──────────────────────────────────────────────────

def _last_trading_date(max_lookback: int = 60) -> str:
    """
    返回 Polygon 免费套餐可访问的最近交易日。

    Polygon 免费套餐对近期数据有访问限制（403），
    从昨天起逐日往前回溯，跳过周末，直到 HTTP 探测返回 200。
    """
    import os
    import requests
    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY", "")

    d = datetime.today() - timedelta(days=1)
    for _ in range(max_lookback):
        if d.weekday() >= 5:
            d -= timedelta(days=1)
            continue
        date_str = d.strftime("%Y-%m-%d")
        # 轻量探测：只请求 limit=1
        url = (f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}"
               f"?apiKey={api_key}&adjusted=true&limit=1")
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code == 200 and resp.json().get("resultsCount", 0) > 0:
                print(f"  [polygon] 找到可用交易日: {date_str}")
                return date_str
        except Exception:
            pass
        d -= timedelta(days=1)

    return "2025-01-06"


def run_stage1(date: Optional[str] = None) -> list:
    """
    第一步：从 Polygon grouped daily 拉取全市场数据，做量价初筛。

    Args:
        date: 指定日期 "YYYY-MM-DD"，默认取最近交易日

    Returns:
        通过初筛的 ticker 列表
    """
    if date is None:
        date = _last_trading_date()

    today = datetime.today().strftime("%Y-%m-%d")
    cache_key = f"stage1_candidates_{date}"
    cached = _load_cache(cache_key)
    if cached is not None:
        print(f"[stage1] 从缓存加载 {len(cached)} 只初筛候选")
        return cached

    print(f"[stage1] 从 Polygon 拉取全市场日线数据（{date}）…")
    df = get_grouped_daily(date)

    if df.empty:
        print("  [warn] Polygon 返回空数据，无法执行初筛")
        return []

    total_raw = len(df)
    print(f"  原始记录数: {total_raw:,}")

    # 量价过滤
    mask = (
        (df["volume"] > STAGE1_MIN_VOLUME) &
        (df["close"] > STAGE1_MIN_CLOSE) &
        (df["close"] > df["open"] * STAGE1_CLOSE_OPEN_RATIO)
    )
    df_passed = df[mask]

    tickers = df_passed["ticker"].tolist()
    print(f"  volume>{STAGE1_MIN_VOLUME:,} & close>${STAGE1_MIN_CLOSE} & 价格合理: {len(tickers):,} 只")

    _save_cache(cache_key, tickers)
    return tickers


# ── 第二步：finvizfinance 基本面精筛 ──────────────────────────────────────────

def _fetch_fundamentals(ticker: str) -> Optional[dict]:
    """
    用 finvizfinance.quote 获取单只股票基本面数据。
    返回解析后的 dict，获取失败返回 None。
    """
    from finvizfinance.quote import finvizfinance as fvf

    try:
        info = fvf(ticker).ticker_fundament()
    except Exception:
        return None

    eps   = _parse_pct(info.get("EPS Q/Q"))
    sales = _parse_pct(info.get("Sales Q/Q"))
    gm    = _parse_pct(info.get("Gross Margin"))
    mc    = _parse_market_cap(info.get("Market Cap"))

    return {
        "ticker":           ticker,
        "company":          info.get("Company", ""),
        "sector":           info.get("Sector", ""),
        "industry":         info.get("Industry", ""),
        "eps_growth_qoq":   eps,
        "sales_growth_qoq": sales,
        "gross_margin":     gm,
        "market_cap":       mc,
        "market_cap_raw":   _format_market_cap(mc),
        "float_shares":     info.get("Shs Float"),
        "float_short":      _parse_pct(info.get("Short Float")),
    }


def _passes_stage2(data: dict) -> bool:
    """检查是否通过第二步基本面过滤条件。"""
    eps   = data.get("eps_growth_qoq")
    sales = data.get("sales_growth_qoq")
    gm    = data.get("gross_margin")
    mc    = data.get("market_cap")

    if eps   is None or eps   <= STAGE2_MIN_EPS_GROWTH:
        return False
    if sales is None or sales <= STAGE2_MIN_SALES_GROWTH:
        return False
    if gm    is None or gm    <= STAGE2_MIN_GROSS_MARGIN:
        return False
    if mc    is None or not (STAGE2_MIN_MARKET_CAP <= mc <= STAGE2_MAX_MARKET_CAP):
        return False
    return True


def run_stage2(tickers: list, polygon_date: Optional[str] = None) -> list:
    """
    第二步：对初筛候选逐只查询 finviz 基本面，精筛出最终候选。

    Args:
        tickers:      第一步筛出的 ticker 列表
        polygon_date: 用于从 Polygon 数据附加价格/成交量信息

    Returns:
        通过精筛的股票 dict 列表
    """
    today = datetime.today().strftime("%Y-%m-%d")
    cache_key = f"fundamental_candidates_{today}"
    cached = _load_cache(cache_key)
    if cached is not None:
        print(f"[stage2] 从缓存加载 {len(cached)} 只最终候选")
        return cached

    # 加载 Polygon 价格/成交量数据，用于最终附加到结果
    pdate = polygon_date or _last_trading_date()
    poly_df = get_grouped_daily(pdate)
    poly_map = {}
    if not poly_df.empty:
        poly_map = poly_df.set_index("ticker")[["close", "volume", "vwap"]].to_dict("index")

    total     = len(tickers)
    passed    = []
    skipped   = 0
    t_start   = time.time()

    print(f"[stage2] 开始基本面精筛，共 {total:,} 只…")
    print(f"  条件: EPS Q/Q>{STAGE2_MIN_EPS_GROWTH}% | Sales Q/Q>{STAGE2_MIN_SALES_GROWTH}% | "
          f"GM>{STAGE2_MIN_GROSS_MARGIN}% | MarketCap 0.5B–50B")

    for i, ticker in enumerate(tickers, 1):
        # 进度 + 预估剩余时间
        if i % 100 == 0 or i == total:
            elapsed  = time.time() - t_start
            rate     = i / elapsed if elapsed > 0 else 1
            eta_sec  = (total - i) / rate
            eta_str  = f"{int(eta_sec//60)}m{int(eta_sec%60)}s"
            print(f"  [进度] {i:4d}/{total}  通过 {len(passed):3d} 只  "
                  f"跳过 {skipped:3d} 只  ETA {eta_str}")

        data = _fetch_fundamentals(ticker)
        if data is None:
            skipped += 1
            time.sleep(STAGE2_REQUEST_DELAY)
            continue

        if _passes_stage2(data):
            # 附加价格和成交量
            poly_info = poly_map.get(ticker, {})
            data["price"]  = poly_info.get("close")
            data["volume"] = int(poly_info.get("volume", 0)) or None
            data["vwap"]   = poly_info.get("vwap")
            passed.append(data)

        time.sleep(STAGE2_REQUEST_DELAY)

    # 按成交量降序
    passed.sort(key=lambda x: x.get("volume") or 0, reverse=True)

    elapsed_total = time.time() - t_start
    print(f"\n[stage2] 完成！处理 {total:,} 只，通过 {len(passed)} 只，"
          f"跳过 {skipped} 只，耗时 {elapsed_total/60:.1f} 分钟")

    _save_cache(cache_key, passed)
    return passed


# ── 主入口 ────────────────────────────────────────────────────────────────────

def run(date: Optional[str] = None) -> list:
    """
    执行两步走基本面筛选流程。

    Args:
        date: 指定交易日 "YYYY-MM-DD"，默认取最近交易日

    Returns:
        最终候选股票列表（按成交量降序）
    """
    # Step 1
    stage1 = run_stage1(date)
    if not stage1:
        return []

    # Step 2
    results = run_stage2(stage1, polygon_date=date)
    return results


def _print_stats(results: list) -> None:
    """打印最终结果统计和板块分布。"""
    if not results:
        print("  [warn] 无候选股票")
        return

    sector_counts: dict = {}
    for r in results:
        sec = r.get("sector") or "Unknown"
        sector_counts[sec] = sector_counts.get(sec, 0) + 1

    print(f"\n  板块分布（共 {len(results)} 只）:")
    for sec, cnt in sorted(sector_counts.items(), key=lambda x: -x[1]):
        bar = "█" * min(cnt, 50)
        print(f"    {sec:<28} {cnt:3d}  {bar}")


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    results = run()

    _print_stats(results)

    total = len(results)
    print()
    print("=" * 90)
    print(f"最终候选：{total} 只（前 20 条，按成交量降序）")
    print("=" * 90)

    hdr = (f"{'Ticker':<7} {'Company':<24} {'Sector':<18} "
           f"{'Price':>7} {'Volume':>11} "
           f"{'EPS Q/Q':>8} {'S Q/Q':>7} {'GM':>6} "
           f"{'MCap':>7}")
    print(hdr)
    print("-" * 100)

    for r in results[:20]:
        eps   = f"{r['eps_growth_qoq']:.0f}%"   if r.get("eps_growth_qoq")   is not None else "  -"
        sales = f"{r['sales_growth_qoq']:.0f}%"  if r.get("sales_growth_qoq") is not None else "  -"
        gm    = f"{r['gross_margin']:.0f}%"       if r.get("gross_margin")     is not None else "  -"
        vol   = f"{r['volume']:,}"                if r.get("volume")           else "-"
        price = f"{r['price']:.2f}"               if r.get("price")            else "-"

        print(
            f"{r['ticker']:<7} {str(r.get('company',''))[:23]:<24} "
            f"{str(r.get('sector',''))[:17]:<18} "
            f"{price:>7} {vol:>11} "
            f"{eps:>8} {sales:>7} {gm:>6} "
            f"{r.get('market_cap_raw','N/A'):>7}"
        )
