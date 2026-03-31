"""
polygon_snapshot.py — Polygon.io Snapshot API 封裝

提供即時行情快照（漲幅榜、單隻/批量查詢），用於盤前 EP 掃描。
與 polygon_client.py 風格一致，不共用快取（snapshot 是即時數據）。

Polygon Starter Plan Rate Limit: 5 calls/min
每 5 分鐘掃描一次僅需 1-2 次請求，遠低於上限。

端點說明：
  /v2/snapshot/locale/us/markets/stocks/gainers → 漲幅榜（盤中即時）
  /v3/snapshot?ticker.any_of=T1,T2,...          → 批量即時快照（含盤前數據）
"""

import os
import time
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL   = "https://api.polygon.io"
BATCH_SIZE = 250      # /v3/snapshot 每次最多 250 個 ticker
MIN_PRICE  = 5.0      # 最低股價門檻
MIN_VOLUME = 100_000  # 最低成交量門檻


def _get_api_key() -> str:
    key = os.getenv("POLYGON_API_KEY", "")
    if not key:
        raise EnvironmentError("POLYGON_API_KEY 未設置，請檢查 .env 文件")
    return key


def _request(url: str, params: dict, retries: int = 3) -> dict:
    """GET 請求，支持超時重試和限流等待（與 polygon_client 風格一致）。"""
    for attempt in range(1, retries + 1):
        try:
            print(f"  [http] GET {url}  (attempt {attempt})")
            resp = requests.get(url, params=params, timeout=15)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                print(f"  [warn] 觸發限流，等待 {wait}s 後重試…")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.Timeout:
            print(f"  [warn] 請求超時 (attempt {attempt}/{retries})")
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            if status == 403:
                raise RuntimeError(
                    f"Polygon 403 Forbidden：該端點可能需要升級套餐"
                ) from e
            raise RuntimeError(f"Polygon HTTP 錯誤 {status}: {e}") from e

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Polygon 請求失敗: {e}") from e

    raise RuntimeError("超過最大重試次數")


def get_gainers(min_change_pct: float = 5.0) -> list:
    """
    GET /v2/snapshot/locale/us/markets/stocks/gainers
    返回當前漲幅最大的股票列表。

    閉市時或數據不足時返回空列表（正常現象）。

    Args:
        min_change_pct: 最低漲幅門檻（%）

    Returns:
        符合條件的股票列表，每項包含:
        ticker, price, prev_close, open, high, low, change_pct, volume, vwap
    """
    print(f"[snapshot] get_gainers min_change_pct={min_change_pct}%")
    try:
        data = _request(
            f"{BASE_URL}/v2/snapshot/locale/us/markets/stocks/gainers",
            params={"apiKey": _get_api_key(), "include_otc": "false"},
        )
    except RuntimeError as e:
        print(f"  [warn] get_gainers 失敗: {e}")
        return []

    tickers_data = data.get("tickers", [])
    if not tickers_data:
        print("  [info] 漲幅榜為空（閉市或盤前太早）")
        return []

    results = []
    for item in tickers_data:
        ticker     = item.get("ticker", "")
        change_pct = float(item.get("todaysChangePerc") or 0)
        day        = item.get("day") or {}
        prev_day   = item.get("prevDay") or {}

        price      = float(day.get("c") or day.get("vw") or 0)
        volume     = int(day.get("v") or 0)
        prev_close = float(prev_day.get("c") or 0)

        if not ticker or price < MIN_PRICE or volume < MIN_VOLUME:
            continue
        if abs(change_pct) < min_change_pct:
            continue

        results.append({
            "ticker":     ticker,
            "price":      round(price, 2),
            "prev_close": round(prev_close, 2),
            "open":       round(float(day.get("o") or 0), 2),
            "high":       round(float(day.get("h") or 0), 2),
            "low":        round(float(day.get("l") or 0), 2),
            "change_pct": round(change_pct, 2),
            "volume":     volume,
            "vwap":       round(float(day.get("vw") or 0), 2),
        })

    print(f"  [info] get_gainers 返回 {len(results)} 只符合條件的股票")
    return results


def get_ticker_snapshot(ticker: str) -> Optional[dict]:
    """
    查詢單隻股票的即時快照。

    Returns:
        包含 price, open, prev_close, change_pct, premarket_change_pct, volume 的 dict；
        查詢失敗或無數據時返回 None
    """
    results = get_batch_snapshots([ticker])
    return results[0] if results else None


def get_batch_snapshots(tickers: list) -> list:
    """
    批量查詢多隻 ticker 的即時快照。
    使用 /v3/snapshot?ticker.any_of=T1,T2,...（最多 250 個一批）。

    返回的 session 欄位說明：
      price                      → 最新成交價（盤中或盤前）
      open                       → 今日開盤價
      previous_close             → 昨日收盤
      change_percent             → 今日漲跌幅（%）
      early_trading_change       → 盤前漲跌金額
      early_trading_change_percent → 盤前漲跌幅（%）
      volume                     → 今日累計成交量

    Args:
        tickers: ticker 列表（超過 250 自動分批）

    Returns:
        快照列表，每項包含標準化後的欄位
    """
    if not tickers:
        return []

    print(f"[snapshot] get_batch_snapshots {len(tickers)} tickers")
    all_results = []

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i: i + BATCH_SIZE]

        try:
            data = _request(
                f"{BASE_URL}/v3/snapshot",
                params={
                    "apiKey":         _get_api_key(),
                    "ticker.any_of": ",".join(batch),
                },
            )
        except RuntimeError as e:
            print(f"  [warn] batch snapshot 失敗 (batch {i//BATCH_SIZE + 1}): {e}")
            continue

        for item in (data.get("results") or []):
            ticker  = item.get("ticker", "")
            session = item.get("session") or {}
            stype   = item.get("type", "")

            price      = float(session.get("price") or session.get("close") or 0)
            open_price = float(session.get("open") or 0)
            prev_close = float(session.get("previous_close") or 0)
            volume     = int(session.get("volume") or 0)
            change_pct = float(session.get("change_percent") or 0)

            # 盤前漲跌數據
            pre_change     = float(session.get("early_trading_change") or 0)
            pre_change_pct = float(session.get("early_trading_change_percent") or 0)

            if not ticker or price < MIN_PRICE:
                continue

            all_results.append({
                "ticker":               ticker,
                "price":                round(price, 2),
                "open":                 round(open_price, 2),
                "prev_close":           round(prev_close, 2),
                "change_pct":           round(change_pct, 2),
                "premarket_change":     round(pre_change, 2),
                "premarket_change_pct": round(pre_change_pct, 2),
                "volume":               volume,
                "market_status":        item.get("market_status", ""),
                "type":                 stype,
            })

        # 批次間稍等，避免觸發 rate limit
        if i + BATCH_SIZE < len(tickers):
            time.sleep(0.5)

    print(f"  [info] get_batch_snapshots 返回 {len(all_results)} 只")
    return all_results


# ── 測試入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("測試 get_gainers（漲幅榜）")
    print("=" * 50)
    try:
        gainers = get_gainers(min_change_pct=3.0)
        if gainers:
            print(f"漲幅榜前5名:")
            for g in gainers[:5]:
                print(f"  {g['ticker']:<7} {g['change_pct']:+.1f}%  ${g['price']}  "
                      f"vol={g['volume']:,}")
        else:
            print("漲幅榜為空（閉市或非交易時段）")
    except Exception as e:
        print(f"❌ get_gainers 失敗: {e}")

    print()
    print("=" * 50)
    print("測試 get_batch_snapshots（NVDA + AAPL）")
    print("=" * 50)
    try:
        snaps = get_batch_snapshots(["NVDA", "AAPL"])
        for s in snaps:
            print(f"  {s['ticker']:<7} price=${s['price']}  "
                  f"change={s['change_pct']:+.1f}%  "
                  f"pre={s['premarket_change_pct']:+.1f}%  "
                  f"vol={s['volume']:,}")
    except Exception as e:
        print(f"❌ get_batch_snapshots 失敗: {e}")
