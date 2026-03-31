"""
bottom_finder_detector.py — 底部反轉型態偵測模組

偵測類似 AAOI 的「長期下跌 → 底部築底 → 放量突破」型態。

觸發邏輯（5步驟）：
  1. 確認先前下跌：過去12個月高點到低點下跌 >= 35%
  2. 識別底部 base：最低點後橫盤 25-150 天，振幅 <= 40%
  3. Higher lows：base 分3段，低點遞增（容許2%容差）
  4. 量縮確認：base 後半段平均量 < 前半段的 80%
  5. 突破偵測：收盤接近或突破 base_high，放量 >= 1.5x 50日均量

評分系統 0-100：
  - 下跌幅度：>=60% 得20，>=50% 得15，>=35% 得10
  - Base 天數：40-120天 得20，25-39天 得10
  - Higher lows：每個 +7（最多21）
  - 量縮：<=50% 得20，<=65% 得15，<=80% 得8
  - 突破量能：>=3x 得20，>=2x 得15，>=1.5x 得8
  - MA20 > MA50：+5
"""

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.polygon_client import get_history as polygon_get_history

# ── 偵測閾值 ──────────────────────────────────────────────────────────────────
MIN_DECLINE_PCT   = 35.0   # 先前下跌最低幅度（%）
MIN_BASE_DAYS     = 25     # 底部築底最少天數
MAX_BASE_DAYS     = 250    # 底部築底最多天數（允許長達10個月的底部）
MAX_BASE_AMP_PCT  = 60.0   # 底部振幅上限（%，允許底部有一定幅度的波動）
VOL_CONTRACT_PCT  = 80.0   # 量縮門檻：後半段均量 < 前半段的此比例
BREAKOUT_VOL_MIN  = 1.5    # 突破最低放量倍數（相對50日均量）
BREAKOUT_NEAR_PCT = 3.0    # 距 base_high 多少%以內算「接近突破」
HL_TOLERANCE      = 2.0    # Higher lows 容差（%）

MIN_BUY_SCORE   = 60  # BUY 門檻（同時需要已突破）
MIN_WATCH_SCORE = 40  # WATCH 門檻

HISTORY_DAYS = 730  # 拉取24個月日線（覆蓋長期下跌的原始高點）


def _find_base_window(df: pd.DataFrame, low_pos: int) -> Optional[pd.DataFrame]:
    """
    在 low_pos 之後，找到最近的 tight consolidation 視窗（振幅 <= MAX_BASE_AMP_PCT）。

    先嘗試不含今天最後一根（避免財報爆發等極端 K 線污染振幅），
    再嘗試含今天。對每個視窗終點，從 MIN_BASE_DAYS 逐步放寬到 MAX_BASE_DAYS，
    返回「最短能通過振幅檢驗的視窗」（優先找近期 tight base）。

    Returns:
        通過振幅的 base DataFrame；找不到時返回 None
    """
    n = len(df)
    # 先試「到昨天」（排除今天可能的爆發 K 線），再試「含今天」
    for window_end in (n - 1, n):
        for size in range(MIN_BASE_DAYS, MAX_BASE_DAYS + 1, 5):
            start = max(low_pos, window_end - size)
            candidate = df.iloc[start:window_end]
            if len(candidate) < MIN_BASE_DAYS:
                continue
            ch = float(candidate["high"].max())
            cl = float(candidate["low"].min())
            if cl <= 0:
                continue
            amp = (ch - cl) / cl * 100
            if amp <= MAX_BASE_AMP_PCT:
                return candidate.copy()
    return None


def _compute_bottom_metrics(df: pd.DataFrame) -> Optional[dict]:
    """
    從日線 DataFrame 計算底部反轉相關指標。

    兩階段設計：
      Phase 1 — 驗證「先前大幅下跌」：找全局低點，再在低點之前找峰值
      Phase 2 — 找「近期築底 base」：滑動視窗從今天往回找 tight consolidation

    Returns:
        指標 dict；如果基本條件不滿足則返回 None
    """
    n = len(df)
    if n < 60:
        return None

    # ── Phase 1：確認先前下跌 ────────────────────────────────────────────────
    # 用全局最低點（idxmin 對 RangeIndex 直接是位置）
    low_pos    = int(df["low"].idxmin())
    period_low = float(df["low"].iloc[low_pos])

    # 低點前需要至少10根K線作為「下跌期」
    if low_pos < 10:
        return None

    # Pre-trough peak（低點之前的數據段找最高點，避免財報爆發當天干擾）
    pre_df      = df.iloc[:low_pos]
    period_high = float(pre_df["high"].max())

    if period_high <= 0 or period_low <= 0:
        return None

    decline_pct = (period_high - period_low) / period_high * 100
    if decline_pct < MIN_DECLINE_PCT:
        return None

    # ── Phase 2：找近期 tight consolidation base ─────────────────────────────
    # 從今天往回滑動視窗，找振幅 <= MAX_BASE_AMP_PCT 的最短近期整理段
    base_df = _find_base_window(df, low_pos)
    if base_df is None:
        return None

    base_days    = len(base_df)
    base_high    = float(base_df["high"].max())
    base_low     = float(base_df["low"].min())
    base_amp_pct = (base_high - base_low) / base_low * 100

    # ── Step 3：Higher lows（base 分3段，低點遞增）───────────────────────────
    seg_size = base_days // 3
    higher_lows_count = 0
    if seg_size >= 3:
        seg1 = base_df.iloc[:seg_size]
        seg2 = base_df.iloc[seg_size: seg_size * 2]
        seg3 = base_df.iloc[seg_size * 2:]

        seg1_low = float(seg1["low"].min())
        seg2_low = float(seg2["low"].min())
        seg3_low = float(seg3["low"].min())

        tol = 1 - HL_TOLERANCE / 100
        if seg2_low >= seg1_low * tol:
            higher_lows_count += 1
        if seg3_low >= seg2_low * tol:
            higher_lows_count += 1
        if seg3_low >= seg1_low * tol:
            higher_lows_count += 1
    else:
        seg1_low = seg2_low = seg3_low = base_low

    # ── Step 4：量縮確認（base 後半段均量 < 前半段）──────────────────────────
    half = base_days // 2
    first_half_vol  = float(base_df.iloc[:half]["volume"].mean()) if half > 0 else 1.0
    second_half_vol = float(base_df.iloc[half:]["volume"].mean())
    vol_contract_ratio = (second_half_vol / first_half_vol * 100
                          if first_half_vol > 0 else 100.0)

    # ── Step 5：突破偵測（相對 base_high 這個阻力位）────────────────────────
    vol_ma50 = float(df["volume"].tail(50).mean()) if n >= 50 else float(df["volume"].mean())
    latest_close  = float(df["close"].iloc[-1])
    latest_volume = float(df["volume"].iloc[-1])

    breakout_vol_ratio = latest_volume / vol_ma50 if vol_ma50 > 0 else 0.0
    is_breakout   = latest_close >= base_high
    near_breakout = latest_close >= base_high * (1 - BREAKOUT_NEAR_PCT / 100)

    # MA20 & MA50（收盤價）
    ma20       = float(df["close"].tail(20).mean()) if n >= 20 else None
    ma50_price = float(df["close"].tail(50).mean()) if n >= 50 else None
    ma20_above_ma50 = (ma20 is not None and ma50_price is not None
                       and ma20 > ma50_price)

    return {
        "decline_pct":        round(decline_pct, 1),
        "period_high":        round(period_high, 2),
        "period_low":         round(period_low, 2),
        "base_high":          round(base_high, 2),
        "base_low":           round(base_low, 2),
        "base_days":          base_days,
        "base_amp_pct":       round(base_amp_pct, 1),
        "seg1_low":           round(seg1_low, 2),
        "seg2_low":           round(seg2_low, 2),
        "seg3_low":           round(seg3_low, 2),
        "higher_lows_count":  higher_lows_count,
        "vol_contract_ratio": round(vol_contract_ratio, 1),
        "breakout_vol_ratio": round(breakout_vol_ratio, 2),
        "is_breakout":        is_breakout,
        "near_breakout":      near_breakout,
        "latest_close":       round(latest_close, 2),
        "ma20":               round(ma20, 2) if ma20 is not None else None,
        "ma50_price":         round(ma50_price, 2) if ma50_price is not None else None,
        "ma20_above_ma50":    ma20_above_ma50,
    }


def _bottom_score(m: dict) -> int:
    """計算底部反轉評分（0–100）。"""
    score = 0

    # 下跌幅度
    dd = m["decline_pct"]
    if dd >= 60:
        score += 20
    elif dd >= 50:
        score += 15
    elif dd >= 35:
        score += 10

    # Base 天數
    bd = m["base_days"]
    if 40 <= bd <= 120:
        score += 20
    elif MIN_BASE_DAYS <= bd < 40:
        score += 10

    # Higher lows（每個 +7，最多21）
    score += min(m["higher_lows_count"] * 7, 21)

    # 量縮
    vc = m["vol_contract_ratio"]
    if vc <= 50:
        score += 20
    elif vc <= 65:
        score += 15
    elif vc <= 80:
        score += 8

    # 突破量能
    bv = m["breakout_vol_ratio"]
    if bv >= 3.0:
        score += 20
    elif bv >= 2.0:
        score += 15
    elif bv >= 1.5:
        score += 8

    # MA20 > MA50
    if m["ma20_above_ma50"]:
        score += 5

    return score


def detect(candidates: list, date: Optional[str] = None) -> list:
    """
    從基本面候選股（fund_candidates）中偵測底部反轉信號。

    使用 fund_candidates 而非 tech_candidates，因為底部反轉股尚未進入 Stage 2，
    無法通過技術面篩選（close < MA20 或 close < MA50）。

    Args:
        candidates: fundamental_filter.run() 返回的股票 dict 列表
        date:       截止日期 "YYYY-MM-DD"（回測用），默認 None 表示今天

    Returns:
        觸發底部反轉信號的股票列表，按 score 降序
    """
    total   = len(candidates)
    signals = []
    skipped = 0

    print(f"[bottom_finder] 開始底部反轉偵測，共 {total} 只候選"
          + (f"（截至 {date}）" if date else ""))
    print(f"  觸發條件: 下跌>={MIN_DECLINE_PCT}% + 築底{MIN_BASE_DAYS}-{MAX_BASE_DAYS}天 "
          f"+ 放量突破>={BREAKOUT_VOL_MIN}x")

    for stock in candidates:
        ticker = stock.get("ticker", "")
        if not ticker:
            skipped += 1
            continue

        # 拉取12個月日線（end_date 讓回測只看到指定日期為止的數據）
        df = polygon_get_history(ticker, days=HISTORY_DAYS, end_date=date)
        if df.empty or len(df) < 60:
            skipped += 1
            continue

        metrics = _compute_bottom_metrics(df)
        if metrics is None:
            continue

        # 必須接近或已突破才值得關注
        if not metrics["near_breakout"]:
            continue

        score = _bottom_score(metrics)
        if score < MIN_WATCH_SCORE:
            continue

        # 確定 action
        if score >= MIN_BUY_SCORE and metrics["is_breakout"]:
            action = "BUY"
        else:
            action = "WATCH"

        entry_price    = metrics["latest_close"]
        base_high      = metrics["base_high"]
        base_low       = metrics["base_low"]
        stop_loss_price = round(base_low * 0.97, 2)   # 底部低點下方3%
        target_price   = round(entry_price * 1.30, 2)  # 預設+30%目標

        risk   = (entry_price - stop_loss_price) / entry_price * 100 if entry_price > 0 else 0
        reward = (target_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
        risk_reward = round(reward / risk, 1) if risk > 0 else 0.0

        decline_pct  = metrics["decline_pct"]
        base_days    = metrics["base_days"]
        higher_lows  = metrics["higher_lows_count"]
        vol_contract = metrics["vol_contract_ratio"]
        breakout_vol = metrics["breakout_vol_ratio"]

        reason = (
            f"底部反轉：過去12個月下跌{decline_pct:.0f}%，"
            f"築底{base_days}天，{higher_lows}個Higher Lows，"
            f"量縮至前半段{vol_contract:.0f}%，"
            f"突破量能{breakout_vol:.1f}x均量。"
            f"{'已突破底部高點' if metrics['is_breakout'] else '接近突破底部高點'}，"
            f"評分{score}/100。"
        )

        signal = {
            **stock,
            # 標準欄位
            "signal_type":  "BOTTOM_FINDER",
            "action":       action,
            "last_close":   entry_price,
            "entry_price":  entry_price,
            "entry_zone":   f"{entry_price:.2f}–{base_high:.2f}",
            "stop_loss":    f"{stop_loss_price:.2f}",
            "target_price": target_price,
            "risk_reward":  risk_reward,
            "reason":       reason,
            # 底部反轉特有欄位
            "base_high":          base_high,
            "base_low":           base_low,
            "score":              score,
            "decline_pct":        decline_pct,
            "base_days":          base_days,
            "higher_lows":        higher_lows,
            "vol_contract_ratio": vol_contract,
            "breakout_vol_ratio": breakout_vol,
            "is_breakout":        metrics["is_breakout"],
            "metrics":            metrics,
        }
        signals.append(signal)

    signals.sort(key=lambda x: x.get("score", 0), reverse=True)

    print(f"\n[bottom_finder] 完成")
    print(f"  偵測: {total} 只  底部反轉信號: {len(signals)} 個  跳過: {skipped} 只")

    if signals:
        print()
        hdr = (f"{'#':>3} {'Ticker':<7} {'Sector':<20} {'Score':>5} "
               f"{'Action':<7} {'Decline':>8} {'BaseDays':>8} "
               f"{'HL':>3} {'VolCont':>8} {'BrkVol':>7}")
        print(hdr)
        print("-" * 88)
        for rank, s in enumerate(signals, 1):
            print(
                f"{rank:>3} {s['ticker']:<7} {str(s.get('sector', ''))[:19]:<20} "
                f"{s['score']:>5} {s['action']:<7} "
                f"{s['decline_pct']:>7.1f}% {s['base_days']:>8} "
                f"{s['higher_lows']:>3} {s['vol_contract_ratio']:>7.0f}% "
                f"{s['breakout_vol_ratio']:>7.1f}x"
            )

    return signals


# ── 測試入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from screener.fundamental_filter import run as fundamental_run

    print("=" * 60)
    print("Step 1: 基本面候選股")
    print("=" * 60)
    fund_candidates = fundamental_run()
    print(f"基本面候選: {len(fund_candidates)} 只\n")

    print("=" * 60)
    print("Step 2: 底部反轉偵測")
    print("=" * 60)
    bottom_signals = detect(fund_candidates)

    print()
    if not bottom_signals:
        print("今日無底部反轉信號")
    else:
        print(f"共發現 {len(bottom_signals)} 個底部反轉信號：")
        for s in bottom_signals:
            print(f"  {s['ticker']}: score={s['score']}  action={s['action']}  "
                  f"decline={s['decline_pct']:.1f}%  base_days={s['base_days']}  "
                  f"entry={s['entry_zone']}  stop={s['stop_loss']}")
