"""
polygon_client.py — Polygon.io 数据客户端

封装 Polygon.io REST API，提供全市场日线快照和单股历史 K 线获取方法。
文档参考: https://polygon.io/docs
"""

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.polygon.io"
CACHE_DIR = Path(__file__).parent / "cache"


def _get_api_key() -> str:
    key = os.getenv("POLYGON_API_KEY", "")
    if not key:
        raise EnvironmentError("POLYGON_API_KEY 未设置，请检查 .env 文件")
    return key


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


def _request(url: str, params: dict, retries: int = 3) -> dict:
    """发起 GET 请求，支持超时重试和限流等待。"""
    for attempt in range(1, retries + 1):
        try:
            print(f"  [http] GET {url}  (attempt {attempt})")
            resp = requests.get(url, params=params, timeout=15)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                print(f"  [warn] 触发限流，等待 {wait}s 后重试…")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.Timeout:
            print(f"  [warn] 请求超时 (attempt {attempt}/{retries})")
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            if status == 403:
                raise RuntimeError(
                    f"Polygon 403 Forbidden：该端点可能需要付费套餐（当前 Key 无权限）"
                ) from e
            raise RuntimeError(f"Polygon HTTP 错误 {status}: {e}") from e

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Polygon 请求失败: {e}") from e

    raise RuntimeError("超过最大重试次数")


def _last_weekday(ref: Optional[datetime] = None) -> str:
    """返回最近的工作日日期字符串（跳过周末）。"""
    d = ref or datetime.today()
    while d.weekday() >= 5:          # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def get_grouped_daily(date: Optional[str] = None) -> pd.DataFrame:
    """
    获取全市场指定日期所有股票的 OHLCV 数据。

    Args:
        date: 日期字符串 "YYYY-MM-DD"，默认取最近交易日

    Returns:
        DataFrame，列: ticker, open, high, low, close, volume, vwap, trades
    """
    if date is None:
        date = _last_weekday()

    print(f"[polygon] get_grouped_daily date={date}")

    cache_key = f"grouped_daily_{date}"
    cached = _load_cache(cache_key)
    if cached is not None:
        results = cached
    else:
        url = f"{BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{date}"
        data = _request(url, params={"apiKey": _get_api_key(), "adjusted": "true"})

        if data.get("status") == "NOT_FOUND" or not data.get("resultsCount"):
            print(f"  [warn] {date} 无数据（可能是假期或非交易日）")
            return pd.DataFrame()

        results = data.get("results", [])
        if not results:
            print("  [warn] 返回 results 为空")
            return pd.DataFrame()

        _save_cache(cache_key, results)

    df = pd.DataFrame(results)

    col_map = {
        "T": "ticker",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "vw": "vwap",
        "n": "trades",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # 保留目标列（部分字段可能缺失时容错）
    keep = [c for c in ["ticker", "open", "high", "low", "close", "volume", "vwap", "trades"]
            if c in df.columns]
    df = df[keep]

    # 过滤成交量为 0 的记录
    if "volume" in df.columns:
        before = len(df)
        df = df[df["volume"] > 0].reset_index(drop=True)
        print(f"  [info] 过滤成交量=0：{before} → {len(df)} 条")

    print(f"  [info] get_grouped_daily 完成，共 {len(df)} 条记录")
    return df


def get_history(ticker: str, days: int = 60,
                end_date: Optional[str] = None) -> pd.DataFrame:
    """
    获取单只股票最近 N 天的日线 OHLCV 数据。

    Args:
        ticker:   股票代码，如 "AAPL"
        days:     往前取多少个日历天（实际交易日更少）
        end_date: 截止日期 "YYYY-MM-DD"，默认取最近交易日（回测时传入指定日期）

    Returns:
        DataFrame，列: date, ticker, open, high, low, close, volume, vwap, trades
    """
    print(f"[polygon] get_history ticker={ticker} days={days}"
          + (f" end_date={end_date}" if end_date else ""))

    to_date = end_date if end_date else _last_weekday()
    from_date = (datetime.strptime(to_date, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")

    cache_key = f"history_{ticker}_{from_date}_{to_date}"
    cached = _load_cache(cache_key)
    if cached is not None:
        results = cached
    else:
        url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
        data = _request(url, params={
            "apiKey": _get_api_key(),
            "adjusted": "true",
            "sort": "asc",
            "limit": 500,
        })

        if not data.get("resultsCount"):
            print(f"  [warn] {ticker} 在 {from_date}~{to_date} 无数据")
            return pd.DataFrame()

        results = data.get("results", [])
        _save_cache(cache_key, results)

    df = pd.DataFrame(results)

    col_map = {
        "t": "timestamp",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "vw": "vwap",
        "n": "trades",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # timestamp (ms) → date 字符串
    if "timestamp" in df.columns:
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.strftime("%Y-%m-%d")
        df = df.drop(columns=["timestamp"])

    df.insert(0, "ticker", ticker)

    keep = [c for c in ["date", "ticker", "open", "high", "low", "close", "volume", "vwap", "trades"]
            if c in df.columns]
    df = df[keep].reset_index(drop=True)

    print(f"  [info] get_history 完成，共 {len(df)} 条记录（{df['date'].iloc[0]} ~ {df['date'].iloc[-1]}）")
    return df


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("测试 get_grouped_daily（2025-01-06）")
    print("=" * 50)
    try:
        df_daily = get_grouped_daily("2025-01-06")
        if not df_daily.empty:
            print(f"\n总行数: {len(df_daily)}")
            print(df_daily.head())
    except RuntimeError as e:
        print(f"❌ get_grouped_daily 失败: {e}")

    print()
    print("=" * 50)
    print("测试 get_history（AAPL 最近 60 天）")
    print("=" * 50)
    try:
        df_hist = get_history("AAPL", days=60)
        if not df_hist.empty:
            print(df_hist.head())
    except RuntimeError as e:
        print(f"❌ get_history 失败: {e}")
