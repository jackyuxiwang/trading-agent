"""
tiingo_client.py — 历史 OHLCV 数据客户端（内部改用 Polygon aggregates API）

对外接口不变：
  get_history(ticker, days) → DataFrame(date, open, high, low, close, volume)

- 当天缓存到 data/cache/tiingo_{ticker}_{date}.json（文件名保持兼容）
- Polygon Starter 无限请求，无需请求间隔
- Polygon aggregates: /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

BASE_URL  = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
CACHE_DIR = Path(__file__).parent / "cache"

# 統一快取天數（交易日）：始終拉取此量以確保 Weinstein(200天) 等長週期 detector 命中快取
MAX_CACHE_TRADING_DAYS = 250


def _get_api_key() -> str:
    key = os.getenv("POLYGON_API_KEY", "")
    if not key:
        raise EnvironmentError("POLYGON_API_KEY 未设置，请检查 .env 文件")
    return key


def _cache_path(ticker: str, today: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"tiingo_{ticker.upper()}_{today}.json"


def get_history(ticker: str, days: int = 60) -> pd.DataFrame:
    """
    获取股票最近 N 个交易日的 OHLCV 数据（via Polygon aggregates）。

    Args:
        ticker: 股票代码，如 "AAPL"
        days:   返回最近 N 个交易日

    Returns:
        DataFrame，列: date, open, high, low, close, volume（升序）
        获取失败时返回空 DataFrame
    """
    today   = datetime.today().strftime("%Y-%m-%d")
    cache_p = _cache_path(ticker, today)

    # ── 读缓存 ────────────────────────────────────────────────────────────────
    if cache_p.exists():
        try:
            raw = json.loads(cache_p.read_text(encoding="utf-8"))
            df  = pd.DataFrame(raw)
            # 快取必須有足夠的行數滿足本次請求
            # （避免短週期 detector 先寫入少量數據，長週期 detector 誤用）
            if not df.empty and len(df) >= min(days, MAX_CACHE_TRADING_DAYS - 5):
                return df.tail(days).reset_index(drop=True)
        except Exception:
            pass

    # ── 请求 Polygon aggregates ───────────────────────────────────────────────
    try:
        api_key    = _get_api_key()
        # 始終拉取 MAX_CACHE_TRADING_DAYS，確保快取夠大給所有 detector 使用
        fetch_days = max(days, MAX_CACHE_TRADING_DAYS)
        start_date = (datetime.today() - timedelta(days=int(fetch_days * 1.6) + 10)).strftime("%Y-%m-%d")
        end_date   = today

        url    = BASE_URL.format(ticker=ticker.upper(), start=start_date, end=end_date)
        params = {
            "apiKey":   api_key,
            "adjusted": "true",
            "limit":    50000,
        }

        for attempt in range(3):
            resp = requests.get(url, params=params, timeout=15)

            if resp.status_code == 404:
                return pd.DataFrame()

            if resp.status_code == 429:
                # 不应出现（Starter 无限），但保留重试
                wait = 2 ** attempt
                print(f"  [polygon_hist] {ticker} 429 限速，等待 {wait}s")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                print(f"  [polygon_hist] {ticker} HTTP {resp.status_code}")
                return pd.DataFrame()

            break
        else:
            return pd.DataFrame()

        payload = resp.json()

    except EnvironmentError as e:
        print(f"  [polygon_hist] {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"  [polygon_hist] {ticker} 请求失败: {e}")
        return pd.DataFrame()

    results = payload.get("results")
    if not results:
        return pd.DataFrame()

    # ── 规范化字段 ────────────────────────────────────────────────────────────
    try:
        df = pd.DataFrame(results)

        # Polygon 字段: t(ms timestamp), o, h, l, c, v
        df["date"]   = pd.to_datetime(df["t"], unit="ms").dt.strftime("%Y-%m-%d")
        df["open"]   = pd.to_numeric(df["o"], errors="coerce")
        df["high"]   = pd.to_numeric(df["h"], errors="coerce")
        df["low"]    = pd.to_numeric(df["l"], errors="coerce")
        df["close"]  = pd.to_numeric(df["c"], errors="coerce")
        df["volume"] = pd.to_numeric(df["v"], errors="coerce").fillna(0).astype(int)

        df = df[["date", "open", "high", "low", "close", "volume"]]
        df = df[df["close"] > 0].copy()
        df = df.sort_values("date").reset_index(drop=True)

        if df.empty or len(df) < 2:
            return pd.DataFrame()

    except Exception as e:
        print(f"  [polygon_hist] {ticker} 数据解析失败: {e}")
        return pd.DataFrame()

    # ── 写缓存 ────────────────────────────────────────────────────────────────
    try:
        cache_p.write_text(
            json.dumps(df.to_dict(orient="records"), ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass

    return df.tail(days).reset_index(drop=True)


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tickers = ["AAOI", "KRMN", "OS"]
    print(f"测试 Polygon 历史数据获取（60天）\n{'='*50}")

    for tkr in tickers:
        t0 = time.time()
        df = get_history(tkr, days=60)
        elapsed = time.time() - t0

        if df.empty:
            print(f"\n{tkr}: ❌ 获取失败")
        else:
            print(f"\n{tkr}: ✅ 共 {len(df)} 行  耗时 {elapsed:.2f}s")
            print(df.head(3).to_string(index=False))
