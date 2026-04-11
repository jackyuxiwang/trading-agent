"""
post_ep_tight_detector.py — EP後緊縮整理型態偵測模組

偵測「Episodic Pivot 後緊縮整理 → 突破」型態（Post-EP Tight Area）。

觸發邏輯：
  1. 在近期（10天內）找到符合條件的 EP 事件：
     - 跳空缺口 >= 5%，收陽線，放量 >= 2x 50日均量
  2. EP 後緊縮整理 3–10 天：
     - 振幅 <= EP 當天漲幅的 50%（以美元計）
     - 平均量 <= EP 當天量的 50%
     - 整理未跌破 EP 開盤價（缺口頂部）
  3. 收盤接近或突破整理高點（breakout point）

評分系統 0-100：
  - EP 跳空幅度：>=10% +20，>=7% +15，>=5% +10
  - 整理天數：3-5天 +20，6-8天 +15，9-10天 +10
  - 振幅比率（佔EP漲幅）：<=30% +20，<=40% +15，<=50% +10
  - 量比（整理均量/EP量）：<30% +20，<40% +15，<50% +10
  - 缺口維持：完整維持 +10，破後收回 +5
"""

import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.polygon_client import get_history as polygon_get_history

# ── 偵測閾值 ──────────────────────────────────────────────────────────────────
EP_MIN_GAP_PCT      = 5.0   # EP 跳空缺口最小幅度（%）
EP_MIN_VOL_RATIO    = 2.0   # EP 日成交量 / 50日均量
EP_LOOKBACK_DAYS    = 10    # 往前找 EP 最多幾天
CONSOL_MIN_DAYS     = 3     # 整理最少天數
CONSOL_MAX_DAYS     = 10    # 整理最多天數
MAX_AMP_RATIO       = 0.50  # 整理振幅 <= EP 當天漲幅的 50%（美元計）
MAX_VOL_RATIO       = 0.50  # 整理均量 <= EP 當天量的 50%
BREAKOUT_NEAR_PCT   = 2.0   # 距整理高點多少%以內算「接近突破」

MIN_BUY_SCORE   = 60  # BUY 門檻（同時需要已突破）
MIN_WATCH_SCORE = 40  # WATCH 門檻

HISTORY_DAYS = 75  # 拉取日線天數


def _find_ep_and_consol(df: pd.DataFrame) -> Optional[dict]:
    """
    在 df 中搜尋最近的 EP 事件並驗證其後的緊縮整理型態。

    Returns:
        指標 dict；找不到符合型態時返回 None
    """
    n = len(df)

    closes  = df["close"].values
    opens   = df["open"].values
    highs   = df["high"].values
    lows    = df["low"].values
    volumes = df["volume"].values

    # EP 搜尋範圍（最近優先）
    ep_search_start = max(1, n - EP_LOOKBACK_DAYS - CONSOL_MAX_DAYS)
    ep_search_end   = n - CONSOL_MIN_DAYS  # EP 後至少要有 CONSOL_MIN_DAYS 天整理

    ep_idx = None
    ep_gap_pct    = 0.0
    ep_vol_ratio  = 0.0

    # 從最近往舊搜尋，取最新的符合 EP
    for i in range(ep_search_end - 1, ep_search_start - 1, -1):
        gap_pct = (opens[i] - closes[i - 1]) / closes[i - 1] * 100
        if gap_pct < EP_MIN_GAP_PCT:
            continue
        if closes[i] <= opens[i]:   # 必須收陽線
            continue
        pre_vol_ma = float(volumes[max(0, i - 50):i].mean()) if i > 0 else 0.0
        if pre_vol_ma <= 0:
            continue
        vr = volumes[i] / pre_vol_ma
        if vr < EP_MIN_VOL_RATIO:
            continue
        # 找到 EP
        ep_idx       = i
        ep_gap_pct   = gap_pct
        ep_vol_ratio = vr
        break

    if ep_idx is None:
        return None

    # ── 整理段分析 ────────────────────────────────────────────────────────────
    consol_df   = df.iloc[ep_idx + 1:]
    consol_days = len(consol_df)

    if not (CONSOL_MIN_DAYS <= consol_days <= CONSOL_MAX_DAYS):
        return None

    consol_high   = float(consol_df["high"].max())
    consol_low    = float(consol_df["low"].min())
    ep_close      = closes[ep_idx]
    ep_open       = opens[ep_idx]
    ep_gain_pct   = (ep_close - ep_open) / ep_open * 100

    amp_ref = ep_close * ep_gain_pct / 100   # EP 當天漲幅（美元計）
    if amp_ref <= 0:
        return None

    amp_ratio         = (consol_high - consol_low) / amp_ref
    consol_closes     = consol_df["close"].values
    gap_maintained    = bool((consol_closes >= ep_open).all())
    gap_broken_but_back = (not gap_maintained) and (consol_closes[-1] >= ep_open)

    vol_ratio_to_ep   = float(consol_df["volume"].mean()) / float(volumes[ep_idx])
    is_breakout       = consol_closes[-1] >= consol_high
    near_breakout     = consol_closes[-1] >= consol_high * (1 - BREAKOUT_NEAR_PCT / 100)

    ep_date = str(df["date"].iloc[ep_idx]) if "date" in df.columns else ""

    return {
        "ep_date":              ep_date,
        "ep_gap_pct":           round(float(ep_gap_pct), 2),
        "ep_vol_ratio":         round(float(ep_vol_ratio), 2),
        "ep_open":              round(float(ep_open), 2),
        "ep_close":             round(float(ep_close), 2),
        "ep_gain_pct":          round(float(ep_gain_pct), 2),
        "consol_days":          consol_days,
        "consol_high":          round(float(consol_high), 2),
        "consol_low":           round(float(consol_low), 2),
        "consol_amp_ratio":     round(amp_ratio * 100, 1),   # 以百分比表示
        "vol_ratio_to_ep_pct":  round(vol_ratio_to_ep * 100, 1),
        "gap_maintained":       gap_maintained,
        "gap_broken_but_back":  gap_broken_but_back,
        "is_breakout":          is_breakout,
        "near_breakout":        near_breakout,
        "latest_close":         round(float(consol_closes[-1]), 2),
    }


def _post_ep_score(m: dict) -> int:
    """計算 Post-EP Tight 評分（0–100）。"""
    score = 0

    # EP 跳空幅度
    gap = m["ep_gap_pct"]
    if gap >= 10:
        score += 20
    elif gap >= 7:
        score += 15
    elif gap >= 5:
        score += 10

    # 整理天數
    cd = m["consol_days"]
    if 3 <= cd <= 5:
        score += 20
    elif 6 <= cd <= 8:
        score += 15
    elif 9 <= cd <= 10:
        score += 10

    # 振幅比率（consol_amp_ratio 是百分比，即佔 EP 漲幅的%）
    ar = m["consol_amp_ratio"]
    if ar <= 30:
        score += 20
    elif ar <= 40:
        score += 15
    elif ar <= 50:
        score += 10

    # 量比（vol_ratio_to_ep_pct）
    vr = m["vol_ratio_to_ep_pct"]
    if vr < 30:
        score += 20
    elif vr < 40:
        score += 15
    elif vr < 50:
        score += 10

    # 缺口維持
    if m["gap_maintained"]:
        score += 10
    elif m["gap_broken_but_back"]:
        score += 5

    return score


def detect(candidates: list, date: Optional[str] = None) -> list:
    """
    從候選股中偵測 Post-EP Tight 信號。

    Args:
        candidates: fundamental_filter.run() 或 technical_filter.run() 返回的股票 dict 列表
        date:       截止日期 "YYYY-MM-DD"（回測用），默認 None 表示今天

    Returns:
        觸發 POST_EP_TIGHT 信號的股票列表，按 score 降序
    """
    total   = len(candidates)
    signals = []
    skipped = 0

    print(f"[post_ep_tight] 開始偵測，共 {total} 只候選"
          + (f"（截至 {date}）" if date else ""))
    print(f"  觸發條件: EP跳空>={EP_MIN_GAP_PCT}% + 整理{CONSOL_MIN_DAYS}-{CONSOL_MAX_DAYS}天 "
          f"+ 振幅<={MAX_AMP_RATIO*100:.0f}%EP漲幅 + 量縮<={MAX_VOL_RATIO*100:.0f}%EP量")

    for stock in candidates:
        ticker = stock.get("ticker", "")
        if not ticker:
            skipped += 1
            continue

        try:
            df = polygon_get_history(ticker, days=HISTORY_DAYS, end_date=date)
            if df.empty or len(df) < 20:
                skipped += 1
                continue

            metrics = _find_ep_and_consol(df)
            if metrics is None:
                continue

            # 只需接近突破即可（不要求已突破）
            if not metrics["near_breakout"]:
                continue

            score = _post_ep_score(metrics)
            if score < MIN_WATCH_SCORE:
                continue

            # 確定 action
            if score >= MIN_BUY_SCORE and metrics["is_breakout"]:
                action = "BUY"
            else:
                action = "WATCH"

            entry_price  = metrics["consol_high"]
            stop_price   = round(metrics["ep_open"] * 0.99, 2)   # EP 開盤價下方1%
            ep_move      = metrics["ep_close"] - metrics["ep_open"]
            target_price = round(entry_price + ep_move * 0.618, 2)

            risk   = (entry_price - stop_price) / entry_price * 100 if entry_price > 0 else 0
            reward = (target_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
            risk_reward = round(reward / risk, 1) if risk > 0 else 0.0

            reason = (
                f"POST_EP_TIGHT：{metrics['ep_date']} EP跳空{metrics['ep_gap_pct']:.1f}%，"
                f"放量{metrics['ep_vol_ratio']:.1f}x，"
                f"EP後整理{metrics['consol_days']}天，"
                f"振幅{metrics['consol_amp_ratio']:.0f}%EP漲幅，"
                f"均量{metrics['vol_ratio_to_ep_pct']:.0f}%EP量，"
                f"{'缺口完整維持' if metrics['gap_maintained'] else '缺口破後收回' if metrics['gap_broken_but_back'] else '缺口已跌破'}，"
                f"{'已突破' if metrics['is_breakout'] else '接近突破'}整理高點{metrics['consol_high']:.2f}，"
                f"評分{score}/100。"
            )

            signal = {
                **stock,
                # 標準欄位
                "signal_type":  "POST_EP_TIGHT",
                "action":       action,
                "last_close":   metrics["latest_close"],
                "entry_price":  entry_price,
                "entry_zone":   f"{metrics['latest_close']:.2f}–{metrics['consol_high']:.2f}",
                "stop_loss":    f"{stop_price:.2f}",
                "target_price": target_price,
                "risk_reward":  risk_reward,
                "reason":       reason,
                # POST_EP_TIGHT 特有欄位
                "score":                score,
                "ep_gap_pct":           metrics["ep_gap_pct"],
                "ep_vol_ratio":         metrics["ep_vol_ratio"],
                "ep_open":              metrics["ep_open"],
                "ep_close":             metrics["ep_close"],
                "consol_days":          metrics["consol_days"],
                "consol_high":          metrics["consol_high"],
                "consol_amp_ratio":     metrics["consol_amp_ratio"],
                "vol_ratio_to_ep":      metrics["vol_ratio_to_ep_pct"],
                "gap_maintained":       metrics["gap_maintained"],
                "is_breakout":          metrics["is_breakout"],
                "metrics":              metrics,
            }
            signals.append(signal)

        except Exception as e:
            print(f"  [post_ep_tight] {ticker} 發生錯誤：{e}")
            skipped += 1
            continue

    signals.sort(key=lambda x: x.get("score", 0), reverse=True)

    print(f"\n[post_ep_tight] 完成")
    print(f"  偵測: {total} 只  POST_EP_TIGHT信號: {len(signals)} 個  跳過: {skipped} 只")

    if signals:
        print()
        hdr = (f"{'#':>3} {'Ticker':<7} {'Score':>5} {'Action':<7} "
               f"{'EPGap':>7} {'EPVol':>6} {'ConsD':>6} {'AmpR%':>6} {'VolR%':>6}")
        print(hdr)
        print("-" * 65)
        for rank, s in enumerate(signals, 1):
            print(
                f"{rank:>3} {s['ticker']:<7} {s['score']:>5} {s['action']:<7} "
                f"{s['ep_gap_pct']:>6.1f}% {s['ep_vol_ratio']:>5.1f}x "
                f"{s['consol_days']:>6} {s['consol_amp_ratio']:>5.0f}% "
                f"{s['vol_ratio_to_ep']:>5.0f}%"
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
    print("Step 2: Post-EP Tight 偵測")
    print("=" * 60)
    ep_tight_signals = detect(fund_candidates)

    print()
    if not ep_tight_signals:
        print("今日無 Post-EP Tight 信號")
    else:
        print(f"共發現 {len(ep_tight_signals)} 個 POST_EP_TIGHT 信號：")
        for s in ep_tight_signals:
            print(f"  {s['ticker']}: score={s['score']}  action={s['action']}  "
                  f"ep_gap={s['ep_gap_pct']:.1f}%  consol_days={s['consol_days']}  "
                  f"entry={s['entry_zone']}  stop={s['stop_loss']}")
