"""
realtime_ep_scanner.py — 即時 EP 掃描器（盤前 + 開盤）

不依賴 Claude API，速度優先，每 5 分鐘執行一次。

掃描流程：
  盤前（4:00–9:30 ET）：
    1. Polygon /v2/snapshot gainers → 漲幅 ≥ MIN_PREMARKET_CHANGE_PCT
    2. 若 gainers 為空（市場未開）→ fallback 到靜態關注清單
    3. 批量 /v3/snapshot 取盤前數據 → 進一步過濾
    4. 返回 PREMARKET_EP 候選列表

  開盤後（9:30–10:30 ET）：
    1. 批量查詢盤前候選 + 關注清單的即時快照
    2. 計算開盤跳空幅度、成交量比、收盤位置
    3. 分類 BUY / WATCH / FADE 信號
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.polygon_snapshot import get_batch_snapshots, get_gainers

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

# 靜態關注清單（無漲幅榜數據時使用）
WATCHLIST: list = [
    "NVDA", "AMD", "META", "GOOGL", "MSFT", "AMZN", "TSLA",
    "AAPL", "NFLX", "PLTR", "SMCI", "MSTR", "COIN", "HOOD",
]


# ── 內部工具 ──────────────────────────────────────────────────────────────────

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
    """
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
        "scanned_at":     datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ── 公開接口 ──────────────────────────────────────────────────────────────────

def scan_premarket(
    extra_tickers: Optional[list] = None,
    min_change_pct: float = MIN_PREMARKET_CHANGE_PCT,
) -> list:
    """
    盤前 EP 掃描（4:00–9:30 ET）。

    流程：
      1. /v2/snapshot gainers → 初篩漲幅 ≥ min_change_pct
      2. 合併 extra_tickers + WATCHLIST（靜態關注清單）
      3. 批量 /v3/snapshot 取即時盤前數據
      4. 再次過濾：盤前漲幅 ≥ min_change_pct，股價 ≥ MIN_PRICE

    Args:
        extra_tickers:  額外追蹤的 ticker 列表（如昨日候選）
        min_change_pct: 盤前漲幅門檻（%），默認 5.0

    Returns:
        信號列表，每項為包含 ticker, action, gap_pct, price, volume 等字段的 dict
    """
    print(f"[realtime_ep] scan_premarket  min_change={min_change_pct}%")

    # Step 1: 漲幅榜（盤中即時）
    gainers = get_gainers(min_change_pct=min_change_pct)
    gainer_tickers = [g["ticker"] for g in gainers]
    print(f"  [info] gainers={len(gainer_tickers)} 只")

    # Step 2: 合併關注清單
    watch_set = set(gainer_tickers)
    if extra_tickers:
        watch_set.update(extra_tickers)
    watch_set.update(WATCHLIST)
    all_tickers = list(watch_set)
    print(f"  [info] 合併後共 {len(all_tickers)} 只待查詢")

    # Step 3: 批量快照
    if not all_tickers:
        print("  [warn] 無 ticker 可查，返回空列表")
        return []

    snapshots = get_batch_snapshots(all_tickers)
    snap_map  = {s["ticker"]: s for s in snapshots}

    # Step 4: 過濾 + 建信號
    signals = []
    for snap in snapshots:
        ticker     = snap["ticker"]
        price      = snap["price"]
        pre_pct    = snap.get("premarket_change_pct", 0.0)
        change_pct = snap.get("change_pct", 0.0)
        volume     = snap.get("volume", 0)

        # 優先用盤前漲幅，若為 0 則用日漲幅（gainers 場景）
        effective_pct = pre_pct if abs(pre_pct) >= 1.0 else change_pct

        if price < MIN_PRICE:
            continue
        if abs(effective_pct) < min_change_pct:
            continue

        sig = _build_signal(snap, vol_ma=None, phase="premarket")
        sig["gap_pct"] = round(effective_pct, 2)
        signals.append(sig)

    # 按漲幅降序排列
    signals.sort(key=lambda s: abs(s["gap_pct"]), reverse=True)
    print(f"  [info] scan_premarket 返回 {len(signals)} 個信號")
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

    # 合併靜態關注清單
    all_tickers = list(set(tickers) | set(WATCHLIST))
    print(f"[realtime_ep] scan_opening {len(all_tickers)} tickers  min_gap={min_gap_pct}%")

    snapshots = get_batch_snapshots(all_tickers)

    signals = []
    for snap in snapshots:
        ticker     = snap["ticker"]
        price      = snap["price"]
        prev_close = snap["prev_close"]
        open_p     = snap.get("open", 0.0)
        volume     = snap.get("volume", 0)

        if price < MIN_PRICE or prev_close <= 0 or open_p <= 0:
            continue

        gap_pct = (open_p - prev_close) / prev_close * 100

        if abs(gap_pct) < min_gap_pct:
            continue

        vol_ma  = (vol_ma_map or {}).get(ticker)
        sig = _build_signal(snap, vol_ma=vol_ma, phase="opening")
        signals.append(sig)

    # BUY 優先，FADE 排後
    order = {"BUY": 0, "WATCH": 1, "FADE": 2}
    signals.sort(key=lambda s: (order.get(s["action"], 9), -abs(s["gap_pct"])))

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
