"""
fmp_client.py — Financial Modeling Prep (FMP) 数据客户端

使用 FMP stable API 获取季度损益表和市场数据，
计算营收/EPS 同比增速、毛利率、增速加速度等基本面指标。
文档参考: https://financialmodelingprep.com/developer/docs/stable
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://financialmodelingprep.com/stable"
CACHE_DIR = Path(__file__).parent / "cache"


def _get_api_key() -> str:
    key = os.getenv("FMP_API_KEY", "")
    if not key:
        raise EnvironmentError("FMP_API_KEY 未设置，请检查 .env 文件")
    return key


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


def _request(endpoint: str, params: dict, retries: int = 3) -> object:
    """发起 GET 请求，支持超时重试和限流等待。"""
    url = f"{BASE_URL}{endpoint}"
    params = {"apikey": _get_api_key(), **params}

    for attempt in range(1, retries + 1):
        try:
            print(f"  [http] GET {url}  params={_redact(params)}  (attempt {attempt})")
            resp = requests.get(url, params=params, timeout=15)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                print(f"  [warn] 触发限流，等待 {wait}s 后重试…")
                time.sleep(wait)
                continue

            if resp.status_code == 402:
                body = resp.text[:120]
                print(f"  [warn] 402 Payment Required：{body}")
                return []

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            if status == 403:
                raise RuntimeError("FMP 403 Forbidden：Key 无效或套餐权限不足") from e
            raise RuntimeError(f"FMP HTTP 错误 {status}: {e}") from e

        except requests.exceptions.Timeout:
            print(f"  [warn] 请求超时 (attempt {attempt}/{retries})")
            if attempt == retries:
                raise RuntimeError(f"FMP 请求超时（已重试 {retries} 次）")
            time.sleep(2 ** attempt)

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"FMP 请求失败: {e}") from e

    raise RuntimeError("超过最大重试次数")


def _redact(params: dict) -> dict:
    """打印时隐藏 API Key。"""
    return {k: ("***" if k == "apikey" else v) for k, v in params.items()}


def _yoy_growth(current, prior) -> Optional[float]:
    """同比增速（%）。任一值为 None 或 prior 为 0 时返回 None。"""
    try:
        c, p = float(current), float(prior)
        if p == 0:
            return None
        return round((c - p) / abs(p) * 100, 2)
    except (TypeError, ValueError):
        return None


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def get_income_statements(ticker: str, limit: int = 5) -> list:
    """
    获取股票最近 N 个季度的损益表数据。

    Args:
        ticker: 股票代码，如 "AAPL"
        limit:  返回季度数，默认 8（覆盖两年，用于同比计算）

    Returns:
        季度损益表列表，按日期降序排列（最新在前）
    """
    print(f"[fmp] get_income_statements ticker={ticker} limit={limit}")

    today = datetime.today().strftime("%Y-%m-%d")
    cache_key = f"fmp_income_{ticker}_{today}"
    cached = _load_cache(cache_key)
    if cached is not None:
        return cached

    data = _request(
        "/income-statement",
        params={"symbol": ticker, "period": "quarter", "limit": limit},
    )

    if not isinstance(data, list) or len(data) == 0:
        print(f"  [warn] {ticker} 损益表返回空")
        return []

    print(f"  [info] 获取到 {len(data)} 个季度数据（{data[-1].get('date')} ~ {data[0].get('date')}）")
    _save_cache(cache_key, data)
    return data


def get_fundamentals(ticker: str) -> dict:
    """
    基于季度损益表计算关键基本面指标。

    指标说明：
      - revenue_growth_yoy:    最新季度营收同比增速（Q0 vs Q4）
      - eps_growth_yoy:        最新季度 EPS（稀释）同比增速（Q0 vs Q4）
      - gross_margin:          最新季度毛利率（%）
      - revenue_acceleration:  营收增速加速度（最新季度增速 - 上一季度增速）
      - ticker:                股票代码

    数据不足时相关字段返回 None，不抛出异常。

    Args:
        ticker: 股票代码

    Returns:
        包含上述字段的 dict
    """
    print(f"[fmp] get_fundamentals ticker={ticker}")

    today = datetime.today().strftime("%Y-%m-%d")
    cache_key = f"fmp_fundamentals_{ticker}_{today}"
    cached = _load_cache(cache_key)
    if cached is not None:
        return cached

    quarters = get_income_statements(ticker, limit=5)

    result: dict = {
        "ticker": ticker,
        "revenue_growth_yoy": None,
        "eps_growth_yoy": None,
        "gross_margin": None,
        "revenue_acceleration": None,
    }

    if not quarters:
        _save_cache(cache_key, result)
        return result

    q = quarters  # 简写，q[0] = 最新季度

    # ── 毛利率（仅需最新季度） ────────────────────────────────────────────────
    rev0 = q[0].get("revenue")
    gp0 = q[0].get("grossProfit")
    if rev0 and gp0 and float(rev0) != 0:
        result["gross_margin"] = round(float(gp0) / float(rev0) * 100, 2)

    # ── 同比指标（需要至少5个季度） ──────────────────────────────────────────
    if len(q) >= 5:
        result["revenue_growth_yoy"] = _yoy_growth(
            q[0].get("revenue"), q[4].get("revenue")
        )
        result["eps_growth_yoy"] = _yoy_growth(
            q[0].get("epsDiluted"), q[4].get("epsDiluted")
        )

    # ── 营收增速加速度（需要至少6个季度，才能算上一季度的同比） ───────────────
    if len(q) >= 6:
        growth_latest = _yoy_growth(q[0].get("revenue"), q[4].get("revenue"))
        growth_prev   = _yoy_growth(q[1].get("revenue"), q[5].get("revenue"))
        if growth_latest is not None and growth_prev is not None:
            result["revenue_acceleration"] = round(growth_latest - growth_prev, 2)

    print(f"  [info] get_fundamentals 完成: {result}")
    _save_cache(cache_key, result)
    return result


def get_market_data(ticker: str) -> dict:
    """
    获取股票当前市场数据（市值、平均成交量、价格）。

    Args:
        ticker: 股票代码

    Returns:
        dict with keys: market_cap, avg_volume, price
    """
    print(f"[fmp] get_market_data ticker={ticker}")

    today = datetime.today().strftime("%Y-%m-%d")
    cache_key = f"fmp_market_{ticker}_{today}"
    cached = _load_cache(cache_key)
    if cached is not None:
        return cached

    data = _request("/profile", params={"symbol": ticker})

    result = {"market_cap": None, "avg_volume": None, "price": None}

    # /quote 返回列表或单个 dict
    quote = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else {}

    if quote:
        result["market_cap"] = quote.get("marketCap")
        result["avg_volume"] = quote.get("averageVolume")
        result["price"] = quote.get("price")

    print(f"  [info] get_market_data 完成: {result}")
    _save_cache(cache_key, result)
    return result


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # AAOI / LITE / MARA 在 FMP 免费套餐不可用（402），用可用 ticker 验证
    TEST_TICKERS = ["NVDA", "COIN", "AAPL", "MSFT", "META"]

    for i, ticker in enumerate(TEST_TICKERS):
        print()
        print("=" * 55)
        print(f"  {ticker}")
        print("=" * 55)
        try:
            fund = get_fundamentals(ticker)
            mkt  = get_market_data(ticker)

            mc = mkt.get("market_cap")
            mc_str = f"${mc/1e9:.1f}B" if mc else "None"

            print(f"  revenue_growth_yoy   : {fund.get('revenue_growth_yoy')}%")
            print(f"  eps_growth_yoy       : {fund.get('eps_growth_yoy')}%")
            print(f"  gross_margin         : {fund.get('gross_margin')}%")
            print(f"  revenue_acceleration : {fund.get('revenue_acceleration')}%")
            print(f"  market_cap           : {mc_str}")
            print(f"  avg_volume           : {mkt.get('avg_volume')}")
            print(f"  price                : ${mkt.get('price')}")

        except RuntimeError as e:
            print(f"  ❌ 失败: {e}")

        if i < len(TEST_TICKERS) - 1:
            time.sleep(1)  # 避免触发 FMP 每秒限流

    # EPS 同比增速覆盖验证（AAOI/LITE 的初衷：FMP 季报可直接计算 epsDiluted YoY）
    print()
    print("=" * 55)
    print("EPS 同比增速覆盖验证")
    print("=" * 55)
    for ticker in TEST_TICKERS:
        cache_key = f"fmp_fundamentals_{ticker}_{datetime.today().strftime('%Y-%m-%d')}"
        cached = _load_cache(cache_key)
        if cached:
            eps = cached.get("eps_growth_yoy")
            status = f"✅ {eps}%" if eps is not None else "❌ None（数据不足或 epsDiluted 为负转正）"
            print(f"  {ticker}: eps_growth_yoy = {status}")
        else:
            print(f"  {ticker}: ⚠️  无缓存（可能 402 跳过）")
