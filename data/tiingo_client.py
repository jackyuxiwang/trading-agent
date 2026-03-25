"""
tiingo_client.py — Tiingo EOD 历史数据客户端

API 文档: https://api.tiingo.com/documentation/end-of-day

get_history(ticker, days) → DataFrame(date, open, high, low, close, volume)

- 当天缓存到 data/cache/tiingo_{ticker}_{date}.json
- 无需请求间隔（Tiingo 免费档：1000次/小时）
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

BASE_URL  = "https://api.tiingo.com/tiingo/daily/{ticker}/prices"
CACHE_DIR = Path(__file__).parent / "cache"


def _get_api_key() -> str:
    key = os.getenv("TIINGO_API_KEY", "")
    if not key:
        raise EnvironmentError("TIINGO_API_KEY 未设置，请检查 .env 文件")
    return key


def _cache_path(ticker: str, today: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"tiingo_{ticker.upper()}_{today}.json"


def get_history(ticker: str, days: int = 60) -> pd.DataFrame:
    """
    从 Tiingo 获取股票最近 N 个交易日的 EOD 数据。

    Args:
        ticker: 股票代码，如 "AAPL"
        days:   返回最近 N 个交易日（实际按日历天数 * 1.5 回溯，保证覆盖）

    Returns:
        DataFrame，列: date, open, high, low, close, volume（升序）
        获取失败时返回空 DataFrame
    """
    today    = datetime.today().strftime("%Y-%m-%d")
    cache_p  = _cache_path(ticker, today)

    # ── 读缓存 ────────────────────────────────────────────────────────────────
    if cache_p.exists():
        try:
            raw = json.loads(cache_p.read_text(encoding="utf-8"))
            df  = pd.DataFrame(raw)
            if not df.empty and len(df) >= 2:
                return df.tail(days).reset_index(drop=True)
        except Exception:
            pass

    # ── 请求 Tiingo ───────────────────────────────────────────────────────────
    try:
        api_key    = _get_api_key()
        # 多取一些日历天数，确保能覆盖 N 个交易日（剔除周末/节假日）
        start_date = (datetime.today() - timedelta(days=int(days * 1.6) + 10)).strftime("%Y-%m-%d")

        url    = BASE_URL.format(ticker=ticker.lower())
        params = {
            "startDate": start_date,
            "token":     api_key,
        }
        resp = requests.get(url, params=params, timeout=15)

        if resp.status_code == 404:
            # 股票不存在或 Tiingo 无数据
            return pd.DataFrame()
        if resp.status_code == 401:
            print(f"  [tiingo] 401 认证失败，请检查 TIINGO_API_KEY")
            return pd.DataFrame()
        resp.raise_for_status()

        data = resp.json()
    except EnvironmentError as e:
        print(f"  [tiingo] {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"  [tiingo] {ticker} 请求失败: {e}")
        return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    # ── 规范化字段 ────────────────────────────────────────────────────────────
    try:
        df = pd.DataFrame(data)

        # Tiingo 返回字段：date, open, high, low, close, volume (adjClose 等可选)
        rename = {
            "date":        "date",
            "open":        "open",
            "high":        "high",
            "low":         "low",
            "close":       "close",
            "volume":      "volume",
            "adjClose":    "adj_close",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

        required = {"date", "open", "high", "low", "close", "volume"}
        if not required.issubset(df.columns):
            return pd.DataFrame()

        # 日期只保留 YYYY-MM-DD
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        for col in ("open", "high", "low", "close"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)

        df = df[df["close"] > 0].copy()
        df = df.sort_values("date").reset_index(drop=True)

        if df.empty or len(df) < 2:
            return pd.DataFrame()

        # 只保留需要的列，按升序存缓存
        df = df[["date", "open", "high", "low", "close", "volume"]]

    except Exception as e:
        print(f"  [tiingo] {ticker} 数据解析失败: {e}")
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
    print("测试 Tiingo 数据获取: AAOI 最近60天")
    df = get_history("AAOI", days=60)
    if df.empty:
        print("  ❌ 获取失败，请检查 TIINGO_API_KEY 配置")
    else:
        print(f"  ✅ 共 {len(df)} 行")
        print(df.head(5).to_string(index=False))
