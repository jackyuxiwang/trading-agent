"""
eodhd_client.py — 基本面数据客户端（yfinance 实现）

原计划使用 EODHD API，因免费套餐限制改用 yfinance。
提供 S&P500 成分股列表获取和单股基本面数据查询。
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import io

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

CACHE_DIR = Path(__file__).parent / "cache"
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def _cache_path(name: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{name}.json"


def _load_cache(name: str) -> Optional[object]:
    path = _cache_path(name)
    if path.exists():
        print(f"  [cache] 命中缓存: {path.name}")
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def _save_cache(name: str, data) -> None:
    path = _cache_path(name)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"  [cache] 已写入缓存: {path.name}")


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def screen_fundamentals() -> list:
    """
    从 Wikipedia 解析 S&P500 成分股列表，返回 ticker 列表（约 500 只）。

    数据来源: https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
    结果按当日缓存，同一天不重复请求。

    Returns:
        ticker 字符串列表，如 ["AAPL", "MSFT", ...]
    """
    print("[yfinance] screen_fundamentals — 获取 S&P500 成分股")

    cache_key = f"sp500_tickers_{datetime.today().strftime('%Y-%m-%d')}"
    cached = _load_cache(cache_key)
    if cached is not None:
        print(f"  [info] 从缓存加载 {len(cached)} 个 ticker")
        return cached

    print(f"  [http] 解析 Wikipedia S&P500 列表…")
    try:
        resp = requests.get(
            SP500_WIKI_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; trading-agent/1.0)"},
            timeout=15,
        )
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text))
    except Exception as e:
        raise RuntimeError(f"无法获取 S&P500 列表: {e}") from e

    # 第一张表是成分股，Symbol 列即 ticker
    df = tables[0]
    if "Symbol" not in df.columns:
        raise RuntimeError(f"Wikipedia 表格结构变化，未找到 Symbol 列，现有列: {list(df.columns)}")

    # BRK.B / BRK.A 在 yfinance 中用 "-" 而非 "."
    tickers = [t.replace(".", "-") for t in df["Symbol"].tolist()]

    print(f"  [info] screen_fundamentals 完成，共 {len(tickers)} 个 ticker")
    _save_cache(cache_key, tickers)
    return tickers


def get_fundamentals(ticker: str) -> dict:
    """
    用 yfinance 获取单只股票的关键基本面指标。

    提取字段：
      - revenue_growth_yoy: 营收同比增速（%），来自 yfinance revenueGrowth
      - eps_growth_yoy:     EPS 同比增速（%），来自 yfinance earningsGrowth
      - gross_margin:       毛利率（%），来自 yfinance grossMargins
      - market_cap:         市值（美元）
      - avg_volume:         平均日成交量（10日）

    结果按当日缓存。

    Args:
        ticker: 股票代码，如 "AAPL"

    Returns:
        包含上述字段的 dict，无法获取的字段值为 None
    """
    print(f"[yfinance] get_fundamentals ticker={ticker}")

    cache_key = f"fundamentals_{ticker}_{datetime.today().strftime('%Y-%m-%d')}"
    cached = _load_cache(cache_key)
    if cached is not None:
        return cached

    try:
        info = yf.Ticker(ticker).info
    except Exception as e:
        raise RuntimeError(f"yfinance 获取 {ticker} 数据失败: {e}") from e

    if not info or info.get("trailingPegRatio") is None and info.get("marketCap") is None:
        # yfinance 返回空 dict 通常意味着 ticker 无效
        print(f"  [warn] {ticker} 返回数据为空，可能是无效代码")

    def pct(val) -> Optional[float]:
        """将 0.157 → 15.7，None → None。"""
        return round(val * 100, 2) if val is not None else None

    result = {
        "revenue_growth_yoy": pct(info.get("revenueGrowth")),
        "eps_growth_yoy":     pct(info.get("earningsGrowth")),
        "gross_margin":       pct(info.get("grossMargins")),
        "market_cap":         info.get("marketCap"),
        "avg_volume":         info.get("averageVolume10days") or info.get("averageVolume"),
    }

    print(f"  [info] get_fundamentals 完成: {result}")
    _save_cache(cache_key, result)
    return result


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("测试 screen_fundamentals（S&P500 成分股）")
    print("=" * 50)
    try:
        tickers = screen_fundamentals()
        print(f"\n总数量: {len(tickers)}")
        print(f"前 10 个 ticker: {tickers[:10]}")
    except RuntimeError as e:
        print(f"❌ screen_fundamentals 失败: {e}")

    print()
    print("=" * 50)
    print("测试 get_fundamentals（AAPL）")
    print("=" * 50)
    try:
        result = get_fundamentals("AAPL")
        print("\nAAPL 基本面数据:")
        for k, v in result.items():
            print(f"  {k}: {v}")
    except RuntimeError as e:
        print(f"❌ get_fundamentals 失败: {e}")
