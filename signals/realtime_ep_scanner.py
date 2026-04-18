"""
realtime_ep_scanner.py — 即時 EP 掃描器（盤前 + 開盤）

不依賴 Claude API，速度優先，每 5 分鐘執行一次。

掃描流程：
  盤前（4:00–9:30 ET）：
    來源1：Polygon /v2/snapshot gainers → 漲幅榜
    來源2：最新 fundamental_candidates_*.json 快取（~300 只基本面候選）
    來源3：靜態 WATCHLIST（手動添加的高關注 ticker）
    合併去重後批量 /v3/snapshot，篩選 premarket_change_pct ≥ min_gap_pct

  Rate limit 估算（Polygon Starter = 5 calls/min）：
    gainers 1 次 + fundamental 2 次（300只/250批）+ watchlist 1 次 = 4 次/輪 ✓

  開盤後（9:30–10:30 ET）：
    批量查詢盤前候選 + WATCHLIST，BUY / WATCH / FADE 分類
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.polygon_snapshot import get_batch_snapshots, get_gainers
from signals.fib_entry_calculator import calculate_fib_entry

CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"

# ── 閾值 ─────────────────────────────────────────────────────────────────────
MIN_PREMARKET_CHANGE_PCT = 5.0    # 盤前漲幅門檻（%）
MIN_OPENING_GAP_PCT      = 5.0    # 開盤跳空門檻（%）
MIN_PRICE                = 5.0    # 最低股價
MIN_VOLUME               = 100_000  # 最低成交量
MIN_PREMARKET_VOLUME     = 50_000   # 盤前最低成交量

# 成交量比分類門檻
RVOL_BUY_MIN   = 2.0  # 量比 ≥ 2.0 → BUY
RVOL_WATCH_MIN = 1.5  # 量比 ≥ 1.5 → WATCH

# 開盤後收盤位置判斷（收盤 / (高 - 低)）
CLOSE_POSITION_BUY  = 0.6   # 收盤在當日價格幅度的 60% 以上 → 強
CLOSE_POSITION_FADE = 0.4   # 收盤在當日價格幅度的 40% 以下 → 弱（FADE）

# 靜態關注清單（手動維護，每輪必查，不依賴漲幅榜排名）
# 包含：半導體光電、高動量個股、長期追蹤標的
WATCHLIST: list = [
    # 大型科技（流動性基準）
    "NVDA", "AMD", "META", "GOOGL", "MSFT", "AMZN", "TSLA", "AAPL",
    # 高動量 / 波動性強
    "NFLX", "PLTR", "SMCI", "MSTR", "COIN", "HOOD",
    # 光電 / 光纖 / 半導體（容易被漲幅榜排擠的優質標的）
    "GLW", "AAOI", "AEHR", "LITE", "COHR", "MRVL", "FNSR",
    # 生技 / 醫療器械
    "RXRX", "EXAS", "INMD",
]


# ── 內部工具 ──────────────────────────────────────────────────────────────────

def _load_fund_cache_tickers() -> list:
    """
    讀取最新的 fundamental_candidates_*.json 快取，返回 ticker 列表。
    找不到快取時返回空列表（不影響其他來源）。
    """
    pattern = "fundamental_candidates_*.json"
    files = sorted(CACHE_DIR.glob(pattern))   # 按文件名升序 → 最新日期在末尾
    if not files:
        print("  [fund_cache] 找不到 fundamental_candidates 快取，跳過來源2")
        return []

    latest = files[-1]
    try:
        data = json.loads(latest.read_text(encoding="utf-8"))
        tickers = [item["ticker"] for item in data if item.get("ticker")]
        print(f"  [fund_cache] 載入 {latest.name}，共 {len(tickers)} 只")
        return tickers
    except Exception as e:
        print(f"  [fund_cache] 讀取失敗: {e}")
        return []


import re as _re
_WARRANT_RE = _re.compile(r'^[A-Z]{3,}(WS|W|U)$')

def _is_primary_ticker(ticker: str) -> bool:
    """
    返回 False 表示應排除的衍生品 ticker（權證 W/WS、Units U）。
    例：CRMLW → False，CRMU → False，GLW → True（只有 3 個字母，不含後綴）。
    規則：ticker 需要有 ≥3 個基礎字母 + 後綴 W / WS / U 才視為衍生品。
    """
    return not bool(_WARRANT_RE.match(ticker))


def _close_position(open_p: float, high: float, low: float, close: float) -> Optional[float]:
    """計算收盤在當日振幅中的相對位置（0 = 最低，1 = 最高）。"""
    rng = high - low
    if rng <= 0:
        return None
    return round((close - low) / rng, 3)


def _classify_action(gap_pct: float, vol_ratio: Optional[float],
                     close_pos: Optional[float], market_status: str) -> str:
    """
    根據跳空幅度、量比、收盤位置分類動作。

    BUY   — 跳空強、放量、收盤強
    WATCH — 跳空不足或量比偏低或收盤偏弱
    FADE  — 收盤顯著低於開盤（假突破）
    SKIP  — 負向跳空（gap down），EP 只做多，直接排除
    """
    if gap_pct <= 0:
        return "SKIP"
    if gap_pct < MIN_OPENING_GAP_PCT:
        return "WATCH"

    # 盤前數據（無收盤位置）
    if market_status in ("pre", "extended-hours") or close_pos is None:
        if vol_ratio is not None and vol_ratio >= RVOL_BUY_MIN:
            return "BUY"
        return "WATCH"

    # 開盤後：優先看收盤位置
    if close_pos is not None:
        if close_pos <= CLOSE_POSITION_FADE:
            return "FADE"
        if close_pos >= CLOSE_POSITION_BUY:
            if vol_ratio is not None and vol_ratio >= RVOL_BUY_MIN:
                return "BUY"
            return "WATCH"
        return "WATCH"

    # fallback
    if vol_ratio is not None and vol_ratio >= RVOL_WATCH_MIN:
        return "WATCH"
    return "WATCH"


def _build_signal(snap: dict, vol_ma: Optional[float] = None,
                  phase: str = "premarket") -> dict:
    """
    將快照 dict 轉換為信號 dict。

    phase: "premarket" | "opening"
    """
    ticker     = snap["ticker"]
    price      = snap["price"]
    prev_close = snap["prev_close"]
    volume     = snap["volume"]
    open_p     = snap.get("open", 0.0)
    high       = snap.get("high", price)
    low        = snap.get("low", price)

    # 計算跳空幅度
    if phase == "premarket":
        gap_pct = snap.get("premarket_change_pct", 0.0)
    else:
        if prev_close > 0 and open_p > 0:
            gap_pct = round((open_p - prev_close) / prev_close * 100, 2)
        else:
            gap_pct = snap.get("change_pct", 0.0)

    # 量比（如有 vol_ma 則計算，否則 None）
    vol_ratio = round(volume / vol_ma, 2) if vol_ma and vol_ma > 0 else None

    # 收盤位置（僅開盤後有意義）
    close_pos = None
    if phase == "opening" and open_p > 0:
        close_pos = _close_position(open_p, high, low, price)

    market_status = snap.get("market_status", "")
    action = _classify_action(gap_pct, vol_ratio, close_pos, market_status)

    # ── Fibonacci 入場分析（僅 gap up 有意義）────────────────────────────────
    # 盤前用當前價作為缺口高點；開盤後用實際開盤價
    # gap down 時 pm_high <= prev_close，calculate_fib_entry 內部會返回 None
    pm_high = open_p if (phase == "opening" and open_p > prev_close) else price
    fib = (calculate_fib_entry(prev_close, pm_high, current_price=price)
           if prev_close > 0 and pm_high > prev_close else None)

    return {
        "ticker":         ticker,
        "signal_type":    f"{phase.upper()}_EP",
        "action":         action,
        "phase":          phase,
        "price":          price,
        "prev_close":     prev_close,
        "open":           open_p,
        "gap_pct":        gap_pct,
        "volume":         volume,
        "vol_ratio":      vol_ratio,
        "close_position": close_pos,
        "market_status":  market_status,
        "fib":            fib,
        "scanned_at":     datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ── 公開接口 ──────────────────────────────────────────────────────────────────

def scan_premarket(
    extra_tickers: Optional[list] = None,
    min_change_pct: float = MIN_PREMARKET_CHANGE_PCT,
) -> list:
    """
    盤前 EP 掃描（4:00–9:30 ET）。三個來源合併，批量查詢後統一過濾。

    來源1：Polygon /v2/snapshot gainers（漲幅榜，不受數量限制問題影響）
    來源2：最新 fundamental_candidates_*.json 快取（~300 只基本面候選）
    來源3：靜態 WATCHLIST（手動維護，每輪必查）

    Rate limit：gainers 1次 + fund 2次（300/250批）= 共3次/輪 < 5/min ✓

    Args:
        extra_tickers:  額外追蹤的 ticker（如昨日盤前候選，由 main_realtime 傳入）
        min_change_pct: 盤前漲幅門檻（%），默認 5.0

    Returns:
        信號列表，每項包含 ticker, action, gap_pct, price, volume, fib 等字段
    """
    print(f"[realtime_ep] scan_premarket  min_change={min_change_pct}%")

    # ── 來源1：漲幅榜 ────────────────────────────────────────────────────────
    gainers = get_gainers(min_change_pct=min_change_pct)
    gainer_set = {g["ticker"] for g in gainers}
    print(f"  [來源1] gainers={len(gainer_set)} 只")

    # ── 來源2：fundamental_candidates 快取 ───────────────────────────────────
    fund_tickers = _load_fund_cache_tickers()
    fund_set = set(fund_tickers)
    print(f"  [來源2] fund_cache={len(fund_set)} 只")

    # ── 來源3：靜態 WATCHLIST + extra_tickers ────────────────────────────────
    watch_set = set(WATCHLIST)
    if extra_tickers:
        watch_set.update(extra_tickers)
    print(f"  [來源3] watchlist={len(watch_set)} 只（含 extra={len(extra_tickers or [])} 只）")

    # ── 合併去重 ──────────────────────────────────────────────────────────────
    all_tickers = list(gainer_set | fund_set | watch_set)
    print(f"  [合併] 共 {len(all_tickers)} 只待查詢"
          f"（gainers {len(gainer_set)} + fund {len(fund_set)} + watch {len(watch_set)}，去重後）")

    if not all_tickers:
        print("  [warn] 無 ticker 可查，返回空列表")
        return []

    # ── 批量快照（每批 250 只，約 1-2 次 API call）────────────────────────────
    snapshots = get_batch_snapshots(all_tickers)

    # ── 過濾 + 建信號 ─────────────────────────────────────────────────────────
    signals = []
    source_log: dict = {}   # ticker → 來自哪個來源（debug 用）

    for snap in snapshots:
        ticker     = snap["ticker"]
        price      = snap["price"]
        pre_pct    = snap.get("premarket_change_pct", 0.0)
        change_pct = snap.get("change_pct", 0.0)

        # 優先用盤前漲幅，若為 0 則用日漲幅（gainers 場景）
        # 只取正值：EP 只做多，gap down 直接跳過
        effective_pct = pre_pct if pre_pct >= 1.0 else change_pct

        if not _is_primary_ticker(ticker):     # 排除權證(W/WS)、Units(U)
            continue
        if price < MIN_PRICE:
            continue
        if effective_pct < min_change_pct:   # 負值或不足門檻均排除
            continue

        # 記錄來源（可能同時屬於多個來源）
        sources = []
        if ticker in gainer_set: sources.append("gainers")
        if ticker in fund_set:   sources.append("fund")
        if ticker in watch_set:  sources.append("watch")
        source_log[ticker] = "+".join(sources) if sources else "unknown"

        sig = _build_signal(snap, vol_ma=None, phase="premarket")
        sig["gap_pct"] = round(effective_pct, 2)
        sig["source"]  = source_log[ticker]
        signals.append(sig)

    # 按漲幅降序（gap_pct 已全為正值，無需 abs）
    signals.sort(key=lambda s: s["gap_pct"], reverse=True)

    # 來源分佈統計
    from_gainers = sum(1 for s in signals if "gainers" in s.get("source", ""))
    from_fund    = sum(1 for s in signals if "fund"    in s.get("source", ""))
    from_watch   = sum(1 for s in signals if "watch"   in s.get("source", ""))
    print(f"  [info] scan_premarket 返回 {len(signals)} 個信號"
          f"（gainers發現={from_gainers} fund發現={from_fund} watch發現={from_watch}）")
    return signals


def scan_opening(
    tickers: list,
    vol_ma_map: Optional[dict] = None,
    min_gap_pct: float = MIN_OPENING_GAP_PCT,
) -> list:
    """
    開盤後 EP 掃描（9:30–10:30 ET）。

    流程：
      1. 批量 /v3/snapshot 取即時開盤數據
      2. 計算開盤跳空幅度（open vs prev_close）
      3. 量比（若提供 vol_ma_map）
      4. 收盤位置判斷 → BUY / WATCH / FADE

    Args:
        tickers:    需要掃描的 ticker 列表（盤前候選 + 靜態關注清單）
        vol_ma_map: {ticker: vol_ma_20d} 字典（可選，提高量比準確度）
        min_gap_pct: 開盤跳空門檻（%），默認 5.0

    Returns:
        信號列表，按 gap_pct 降序排列（FADE 信號排在後面）
    """
    if not tickers:
        return []

    # 合併：傳入的盤前候選 + 靜態 WATCHLIST + fundamental 快取
    fund_tickers = _load_fund_cache_tickers()
    all_tickers = list(set(tickers) | set(WATCHLIST) | set(fund_tickers))
    print(f"[realtime_ep] scan_opening {len(all_tickers)} tickers  min_gap={min_gap_pct}%")

    snapshots = get_batch_snapshots(all_tickers)

    signals = []
    for snap in snapshots:
        ticker     = snap["ticker"]
        price      = snap["price"]
        prev_close = snap["prev_close"]
        open_p     = snap.get("open", 0.0)
        volume     = snap.get("volume", 0)

        if not _is_primary_ticker(ticker):     # 排除權證(W/WS)、Units(U)
            continue
        if price < MIN_PRICE or prev_close <= 0 or open_p <= 0:
            continue

        gap_pct = (open_p - prev_close) / prev_close * 100

        # EP 只做多：只保留正向跳空，負值（gap down）直接跳過
        if gap_pct < min_gap_pct:
            continue

        vol_ma  = (vol_ma_map or {}).get(ticker)
        sig = _build_signal(snap, vol_ma=vol_ma, phase="opening")
        signals.append(sig)

    # BUY 優先，FADE 排後；gap_pct 已全為正值
    order = {"BUY": 0, "WATCH": 1, "FADE": 2}
    signals.sort(key=lambda s: (order.get(s["action"], 9), -s["gap_pct"]))

    buy_cnt   = sum(1 for s in signals if s["action"] == "BUY")
    watch_cnt = sum(1 for s in signals if s["action"] == "WATCH")
    fade_cnt  = sum(1 for s in signals if s["action"] == "FADE")
    print(f"  [info] scan_opening: BUY={buy_cnt} WATCH={watch_cnt} FADE={fade_cnt}")
    return signals


# ── 測試入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("測試 scan_premarket（漲幅 ≥ 5%）")
    print("=" * 55)
    pre_signals = scan_premarket()
    if pre_signals:
        print(f"\n盤前 EP 候選（前 10）：")
        for s in pre_signals[:10]:
            act = s["action"]
            pct = s["gap_pct"]
            print(f"  {s['ticker']:<7} {act:<6} gap={pct:+.1f}%  "
                  f"price=${s['price']}  vol={s['volume']:,}")
    else:
        print("無盤前信號（可能尚未開盤或無漲幅股）")

    print()
    print("=" * 55)
    print("測試 scan_opening（NVDA + AAPL + AMD）")
    print("=" * 55)
    open_signals = scan_opening(["NVDA", "AAPL", "AMD"])
    if open_signals:
        for s in open_signals:
            print(f"  {s['ticker']:<7} {s['action']:<6} "
                  f"gap={s['gap_pct']:+.1f}%  "
                  f"close_pos={s['close_position']}  "
                  f"price=${s['price']}")
    else:
        print("無開盤信號（可能盤前或非交易時段）")
