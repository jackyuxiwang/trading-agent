"""
cup_handle_detector.py — 杯柄型態偵測模組

偵測 O'Neil 經典「Cup with Handle」突破型態。

觸發邏輯：
  1. 識別杯型：左側高點 → 下跌 15–45% → U 形底部 → 右側回升至左高的 85% 以上
  2. 形成杯柄：杯右側頂點後 5–25 天，整理深度 <= 杯深的 50%，
                杯柄必須在杯深的上半部（不低於杯深中點）
  3. 突破確認：收盤接近或突破杯柄高點（handle_high），
                突破日成交量 >= 1.5x 50日均量

評分系統 0-100：
  - 杯深：20–30% +20，15–20% 或 30–40% +15，40–45% +10
  - U 形：u_shape_ratio >= 0.30 +20，>= 0.15 +10
  - 柄深比率：<= 33% +20，<= 50% +10
  - 柄量縮（後半 vs 前半）< 0.70 +10
  - 突破量比：>= 2.0x +20，>= 1.5x +10
  - 右側回升 >= 95% +10
"""

import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.polygon_client import get_history as polygon_get_history

# ── 偵測閾值 ──────────────────────────────────────────────────────────────────
CUP_MIN_DEPTH           = 15.0  # 杯深最小值（%）
CUP_MAX_DEPTH           = 45.0  # 杯深最大值（%）
CUP_MIN_DAYS            = 30    # 杯型最少天數
CUP_MAX_DAYS            = 180   # 杯型最多天數
RIGHT_RECOVERY_MIN      = 85.0  # 右側回升至左高的最低比率（%）
HANDLE_MIN_DAYS         = 5     # 杯柄最少天數
HANDLE_MAX_DAYS         = 25    # 杯柄最多天數
MAX_HANDLE_DEPTH_RATIO  = 0.50  # 柄深 <= 杯深的 50%
BREAKOUT_MIN_VOL_RATIO  = 1.5   # 突破量 >= 50日均量的 1.5 倍
BREAKOUT_NEAR_PCT       = 2.0   # 距杯柄高點多少%以內算「接近突破」

MIN_BUY_SCORE   = 60  # BUY 門檻（同時需要已突破）
MIN_WATCH_SCORE = 40  # WATCH 門檻

HISTORY_DAYS = 220  # 拉取日線天數


def _find_cup_handle(df: pd.DataFrame) -> Optional[dict]:
    """
    在 df 中搜尋最優杯柄型態。

    依序嘗試不同柄長（從短到長），找到第一個通過所有條件的型態後返回。

    Returns:
        指標 dict；找不到符合型態時返回 None
    """
    closes  = df["close"].values
    highs   = df["high"].values
    lows    = df["low"].values
    volumes = df["volume"].values
    n       = len(closes)

    best = None

    for handle_len in range(HANDLE_MIN_DAYS, min(HANDLE_MAX_DAYS + 1, n // 4)):
        handle_start = n - handle_len

        handle_closes = closes[handle_start:]
        handle_highs  = highs[handle_start:]
        pre_closes    = closes[:handle_start]
        pre_highs     = highs[:handle_start]
        pre_lows      = lows[:handle_start]

        if len(pre_closes) < 40:
            continue

        # ── 找左側高點 ──────────────────────────────────────────────────────
        left_high_pos = int(pre_closes.argmax())
        left_high     = float(pre_closes[left_high_pos])

        if left_high_pos > len(pre_closes) - 20:
            continue   # 左高後空間不足以形成杯
        if left_high_pos < 3:
            continue   # 左高前需有一定上漲趨勢

        # ── 杯型段（左高之後到柄開始之前）──────────────────────────────────
        cup_section_closes = pre_closes[left_high_pos:]
        cup_section_lows   = pre_lows[left_high_pos:]
        cup_duration       = len(cup_section_closes)

        if not (CUP_MIN_DAYS <= cup_duration <= CUP_MAX_DAYS):
            continue

        cup_low   = float(cup_section_lows.min())
        cup_depth = (left_high - cup_low) / left_high * 100

        if not (CUP_MIN_DEPTH <= cup_depth <= CUP_MAX_DEPTH):
            continue

        # 右側回升
        right_side_high    = float(cup_section_closes.max())
        right_recovery_pct = right_side_high / left_high * 100

        if right_recovery_pct < RIGHT_RECOVERY_MIN:
            continue

        # ── 杯柄分析 ────────────────────────────────────────────────────────
        handle_high      = float(handle_highs.max())
        handle_low       = float(lows[handle_start:].min())
        handle_close_now = float(handle_closes[-1])

        # 柄必須在杯深上半部
        cup_midpoint = cup_low + (left_high - cup_low) * 0.5
        if handle_low < cup_midpoint:
            continue

        cup_depth_abs    = left_high - cup_low
        handle_depth_abs = handle_high - handle_low
        handle_depth_ratio = handle_depth_abs / cup_depth_abs if cup_depth_abs > 0 else 1.0

        if handle_depth_ratio > MAX_HANDLE_DEPTH_RATIO:
            continue

        # 柄量縮（後半 vs 前半）
        handle_vols = volumes[handle_start:]
        half = handle_len // 2
        if half >= 2:
            handle_vol_contract = float(handle_vols[half:].mean()) / float(handle_vols[:half].mean()) \
                                  if handle_vols[:half].mean() > 0 else 1.0
        else:
            handle_vol_contract = 1.0

        # 突破分析
        vol_ma50 = float(volumes[max(0, n - 55):n - 5].mean()) \
                   if n >= 55 else float(volumes.mean())
        breakout_vol_ratio = float(volumes[-1]) / vol_ma50 if vol_ma50 > 0 else 0.0

        is_breakout   = handle_close_now >= handle_high
        near_breakout = handle_close_now >= handle_high * (1 - BREAKOUT_NEAR_PCT / 100)

        if not near_breakout:
            continue

        # U 形指標：杯底部附近停留天數
        cup_bottom_threshold = cup_low * 1.05
        days_at_bottom = int((cup_section_closes <= cup_bottom_threshold).sum())
        u_shape_ratio  = days_at_bottom / cup_duration if cup_duration > 0 else 0.0

        result = {
            "left_high":           round(left_high, 2),
            "cup_low":             round(cup_low, 2),
            "cup_depth":           round(cup_depth, 1),
            "cup_duration":        cup_duration,
            "right_side_high":     round(right_side_high, 2),
            "right_recovery_pct":  round(right_recovery_pct, 1),
            "u_shape_ratio":       round(u_shape_ratio, 3),
            "days_at_bottom":      days_at_bottom,
            "handle_len":          handle_len,
            "handle_high":         round(handle_high, 2),
            "handle_low":          round(handle_low, 2),
            "handle_depth_abs":    round(handle_depth_abs, 2),
            "handle_depth_ratio":  round(handle_depth_ratio, 3),
            "handle_vol_contract": round(handle_vol_contract, 3),
            "breakout_vol_ratio":  round(breakout_vol_ratio, 2),
            "is_breakout":         is_breakout,
            "near_breakout":       near_breakout,
            "latest_close":        round(float(handle_close_now), 2),
        }

        # 取評分最高的型態
        if best is None or _cup_handle_score(result) > _cup_handle_score(best):
            best = result

    return best


def _cup_handle_score(m: dict) -> int:
    """計算 Cup with Handle 評分（0–100）。"""
    score = 0

    # 杯深
    cd = m["cup_depth"]
    if 20 <= cd <= 30:
        score += 20
    elif (15 <= cd < 20) or (30 < cd <= 40):
        score += 15
    elif 40 < cd <= 45:
        score += 10

    # U 形
    ur = m["u_shape_ratio"]
    if ur >= 0.30:
        score += 20
    elif ur >= 0.15:
        score += 10

    # 柄深比率（handle_depth_ratio 轉為百分比）
    hdr_pct = m["handle_depth_ratio"] * 100
    if hdr_pct <= 33:
        score += 20
    elif hdr_pct <= 50:
        score += 10

    # 柄量縮
    if m["handle_vol_contract"] < 0.70:
        score += 10

    # 突破量比
    bvr = m["breakout_vol_ratio"]
    if bvr >= 2.0:
        score += 20
    elif bvr >= 1.5:
        score += 10

    # 右側回升 >= 95%
    if m["right_recovery_pct"] >= 95.0:
        score += 10

    return score


def detect(candidates: list, date: Optional[str] = None) -> list:
    """
    從候選股中偵測 Cup with Handle 信號。

    Args:
        candidates: fundamental_filter.run() 或 technical_filter.run() 返回的股票 dict 列表
        date:       截止日期 "YYYY-MM-DD"（回測用），默認 None 表示今天

    Returns:
        觸發 CUP_HANDLE 信號的股票列表，按 score 降序
    """
    total   = len(candidates)
    signals = []
    skipped = 0

    print(f"[cup_handle] 開始杯柄型態偵測，共 {total} 只候選"
          + (f"（截至 {date}）" if date else ""))
    print(f"  觸發條件: 杯深{CUP_MIN_DEPTH}–{CUP_MAX_DEPTH}% + 柄深<={MAX_HANDLE_DEPTH_RATIO*100:.0f}%杯深 "
          f"+ 突破量>={BREAKOUT_MIN_VOL_RATIO}x")

    for stock in candidates:
        ticker = stock.get("ticker", "")
        if not ticker:
            skipped += 1
            continue

        try:
            df = polygon_get_history(ticker, days=HISTORY_DAYS, end_date=date)
            if df.empty or len(df) < 60:
                skipped += 1
                continue

            metrics = _find_cup_handle(df)
            if metrics is None:
                continue

            score = _cup_handle_score(metrics)
            if score < MIN_WATCH_SCORE:
                continue

            # 確定 action
            if score >= MIN_BUY_SCORE and metrics["is_breakout"]:
                action = "BUY"
            else:
                action = "WATCH"

            entry_price  = metrics["handle_high"]   # 突破點 = 柄高
            stop_loss    = round(metrics["handle_low"] * 0.99, 2)
            target_price = round(
                entry_price + (metrics["left_high"] - metrics["cup_low"]), 2
            )   # 量度升幅 = 杯深

            risk   = (entry_price - stop_loss) / entry_price * 100 if entry_price > 0 else 0
            reward = (target_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
            risk_reward = round(reward / risk, 1) if risk > 0 else 0.0

            reason = (
                f"CUP_HANDLE：杯深{metrics['cup_depth']:.1f}%（{metrics['cup_duration']}天），"
                f"右側回升{metrics['right_recovery_pct']:.0f}%，"
                f"柄深{metrics['handle_depth_ratio']*100:.0f}%杯深（{metrics['handle_len']}天），"
                f"柄量縮比{metrics['handle_vol_contract']:.2f}，"
                f"突破量{metrics['breakout_vol_ratio']:.1f}x均量，"
                f"{'已突破' if metrics['is_breakout'] else '接近突破'}柄高{metrics['handle_high']:.2f}，"
                f"評分{score}/100。"
            )

            signal = {
                **stock,
                # 標準欄位
                "signal_type":  "CUP_HANDLE",
                "action":       action,
                "last_close":   metrics["latest_close"],
                "entry_price":  entry_price,
                "entry_zone":   f"{metrics['latest_close']:.2f}–{metrics['handle_high']:.2f}",
                "stop_loss":    f"{stop_loss:.2f}",
                "target_price": target_price,
                "risk_reward":  risk_reward,
                "reason":       reason,
                # CUP_HANDLE 特有欄位
                "score":               score,
                "cup_depth":           metrics["cup_depth"],
                "cup_duration":        metrics["cup_duration"],
                "right_recovery_pct":  metrics["right_recovery_pct"],
                "handle_len":          metrics["handle_len"],
                "handle_depth_ratio":  metrics["handle_depth_ratio"],
                "handle_vol_contract": metrics["handle_vol_contract"],
                "breakout_vol_ratio":  metrics["breakout_vol_ratio"],
                "handle_high":         metrics["handle_high"],
                "handle_low":          metrics["handle_low"],
                "left_high":           metrics["left_high"],
                "cup_low":             metrics["cup_low"],
                "is_breakout":         metrics["is_breakout"],
                "u_shape_ratio":       metrics["u_shape_ratio"],
                "metrics":             metrics,
            }
            signals.append(signal)

        except Exception as e:
            print(f"  [cup_handle] {ticker} 發生錯誤：{e}")
            skipped += 1
            continue

    signals.sort(key=lambda x: x.get("score", 0), reverse=True)

    print(f"\n[cup_handle] 完成")
    print(f"  偵測: {total} 只  CUP_HANDLE信號: {len(signals)} 個  跳過: {skipped} 只")

    if signals:
        print()
        hdr = (f"{'#':>3} {'Ticker':<7} {'Score':>5} {'Action':<7} "
               f"{'CupDepth':>9} {'CupDays':>8} {'RightRec%':>10} "
               f"{'HandleD':>8} {'BrkVol':>7}")
        print(hdr)
        print("-" * 75)
        for rank, s in enumerate(signals, 1):
            print(
                f"{rank:>3} {s['ticker']:<7} {s['score']:>5} {s['action']:<7} "
                f"{s['cup_depth']:>8.1f}% {s['cup_duration']:>8} "
                f"{s['right_recovery_pct']:>9.0f}% "
                f"{s['handle_depth_ratio']*100:>7.0f}% "
                f"{s['breakout_vol_ratio']:>6.1f}x"
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
    print("Step 2: Cup with Handle 偵測")
    print("=" * 60)
    cup_signals = detect(fund_candidates)

    print()
    if not cup_signals:
        print("今日無 Cup with Handle 信號")
    else:
        print(f"共發現 {len(cup_signals)} 個 CUP_HANDLE 信號：")
        for s in cup_signals:
            print(f"  {s['ticker']}: score={s['score']}  action={s['action']}  "
                  f"cup_depth={s['cup_depth']:.1f}%  cup_days={s['cup_duration']}  "
                  f"entry={s['entry_zone']}  stop={s['stop_loss']}")
