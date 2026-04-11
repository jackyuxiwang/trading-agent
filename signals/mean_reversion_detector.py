"""
mean_reversion_detector.py — 均值回歸型態偵測模組

偵測「超賣反彈」機會，適用於優質股票短期過度回調後的反彈交易。

觸發邏輯：
  1. 超賣確認（至少滿足 5 個條件中的 2 個）：
     - BB 下軌以下
     - RSI < 30
     - 低於 MA50 超過 15%
     - 連跌 >= 3 天且累跌 >= 10%
     - 接近 52 週低點（距離 <= 5%）
  2. 反彈信號（至少 1 個）：
     - 放量陽線 / 陽線
     - 錘子線
     - 吞沒形態
     - RSI 從超賣區回升
  3. 風險回報比 >= 1.5（止損=近5日低點下方3%，目標=MA50）

評分系統 0-100：
  - RSI：< 20 +20，< 25 +15，< 30 +10
  - MA50 偏離：< -25% +20，< -20% +15，< -15% +10
  - 反彈：放量陽線 +20，其他反彈信號 +10
  - EPS 增速：> 30% +15，> 15% +10
  - VIX 獎勵（VIX > 25）：+10
  - RR >= 3：+5（在 detect() 中計算後加分）
"""

import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.polygon_client import get_history as polygon_get_history

# ── 偵測閾值 ──────────────────────────────────────────────────────────────────
RSI_OVERSOLD         = 30.0   # RSI 超賣門檻
MA50_DEVIATION_MIN   = 15.0   # 價格低於 MA50 的最低偏離%
BB_PERIOD            = 20     # 布林帶週期
BB_STD               = 2.0    # 布林帶標準差倍數
CONSEC_DOWN_MIN      = 3      # 最少連跌天數
CONSEC_DECLINE_MIN   = 10.0   # 連跌期間累跌最低%
MIN_OVERSOLD_COUNT   = 2      # 最少觸發超賣條件數量
MIN_BOUNCE_COUNT     = 1      # 最少反彈信號數量
MIN_BUY_SCORE        = 50     # BUY 門檻
MIN_WATCH_SCORE      = 35     # WATCH 門檻
RR_MIN               = 1.5    # 最低風險回報比

HISTORY_DAYS = 380  # 拉取日線天數（涵蓋52週低點計算）


def _compute_rsi(closes_array: np.ndarray, period: int = 14) -> Optional[float]:
    """
    用 Wilder 平滑法計算 RSI。

    Args:
        closes_array: 收盤價 numpy 陣列
        period:       RSI 計算週期（默認14）

    Returns:
        RSI 值（0-100），資料不足時返回 None
    """
    if len(closes_array) < period + 1:
        return None

    # 取最近 3 倍週期的資料做計算（穩定性更佳）
    recent = closes_array[-period * 3:] if len(closes_array) >= period * 3 else closes_array
    deltas = np.diff(recent)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    if len(gains) < period:
        return None

    # Wilder 平滑：先用簡單均值作為種子
    avg_gain = float(gains[:period].mean())
    avg_loss = float(losses[:period].mean())

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 1)


def _compute_metrics(df: pd.DataFrame, market_env: Optional[dict]) -> Optional[dict]:
    """
    計算均值回歸相關指標。

    Args:
        df:         日線 DataFrame
        market_env: 大盤環境 dict（含 vix 等）

    Returns:
        指標 dict；條件不足時返回 None
    """
    n = len(df)
    if n < 60:
        return None

    closes  = df["close"].values
    highs   = df["high"].values
    lows    = df["low"].values
    volumes = df["volume"].values
    opens   = df["open"].values

    # ── 基礎指標 ──────────────────────────────────────────────────────────────
    ma20       = float(closes[-20:].mean())
    ma50       = float(closes[-50:].mean()) if n >= 50 else float(closes.mean())
    std20      = float(closes[-20:].std())
    bb_lower   = ma20 - BB_STD * std20
    rsi        = _compute_rsi(closes)
    latest_close = float(closes[-1])

    pct_below_ma50 = (latest_close - ma50) / ma50 * 100   # 負數 = 低於 MA50
    pct_below_bb   = (latest_close - bb_lower) / bb_lower * 100

    # 52 週低點
    wk52_low          = float(lows[-min(n, 260):].min())
    pct_above_52wk_low = (latest_close - wk52_low) / wk52_low * 100

    # ── 連跌天數 ──────────────────────────────────────────────────────────────
    consec_down = 0
    for i in range(n - 1, max(0, n - 8), -1):
        if closes[i] < closes[i - 1]:
            consec_down += 1
        else:
            break

    if consec_down > 0:
        consec_start_price = float(closes[n - 1 - consec_down])
        consec_decline_pct = (latest_close - consec_start_price) / consec_start_price * 100
    else:
        consec_start_price = latest_close
        consec_decline_pct = 0.0

    # ── 超賣條件（5 選 2）────────────────────────────────────────────────────
    oversold = []

    if latest_close < bb_lower:
        oversold.append("BB下軌")

    if rsi is not None and rsi < RSI_OVERSOLD:
        oversold.append(f"RSI={rsi:.0f}")

    if pct_below_ma50 < -MA50_DEVIATION_MIN:
        oversold.append(f"MA50偏離{pct_below_ma50:.0f}%")

    if (consec_down >= CONSEC_DOWN_MIN
            and abs(consec_decline_pct) >= CONSEC_DECLINE_MIN):
        oversold.append(f"連跌{consec_down}天-{abs(consec_decline_pct):.0f}%")

    if pct_above_52wk_low <= 5.0:
        oversold.append("近52週低")

    oversold_count = len(oversold)
    if oversold_count < MIN_OVERSOLD_COUNT:
        return None

    # ── 反彈信號 ──────────────────────────────────────────────────────────────
    bounce      = []
    latest_open = float(opens[-1])
    latest_vol  = float(volumes[-1])
    vol_ma20    = float(volumes[-21:-1].mean()) if n >= 21 else float(volumes.mean())

    # 1. 陽線 ± 放量
    if latest_close > latest_open:
        if latest_vol > vol_ma20:
            bounce.append("放量陽線")
        else:
            bounce.append("陽線")

    # 2. 錘子線
    body        = abs(latest_close - latest_open)
    lower_wick  = min(latest_close, latest_open) - float(lows[-1])
    upper_wick  = float(highs[-1]) - max(latest_close, latest_open)
    if body > 0 and lower_wick >= body * 2 and lower_wick >= upper_wick * 2:
        bounce.append("錘子線")

    # 3. 看漲吞沒
    if n >= 2:
        prev_open  = float(opens[-2])
        prev_close = float(closes[-2])
        if (latest_close > latest_open          # 今日陽線
                and prev_close < prev_open      # 昨日陰線
                and latest_close > prev_open    # 今收 > 昨開
                and latest_open < prev_close):  # 今開 < 昨收
            bounce.append("吞沒形態")

    # 4. RSI 從超賣區回升
    if rsi is not None:
        rsi_prev = _compute_rsi(closes[:-1])
        if rsi_prev is not None and rsi_prev < RSI_OVERSOLD and rsi >= RSI_OVERSOLD:
            bounce.append("RSI回升")

    bounce_count = len(bounce)
    if bounce_count < MIN_BOUNCE_COUNT:
        return None

    # ── VIX 獎勵 ──────────────────────────────────────────────────────────────
    vix       = market_env.get("vix") if market_env else None
    vix_bonus = bool(vix is not None and vix > 25)

    vol_ratio = round(float(latest_vol / vol_ma20), 2) if vol_ma20 > 0 else None

    return {
        "rsi":                  rsi,
        "ma20":                 round(ma20, 2),
        "ma50":                 round(ma50, 2),
        "bb_lower":             round(bb_lower, 2),
        "pct_below_ma50":       round(pct_below_ma50, 1),
        "pct_below_bb_lower":   round(pct_below_bb, 1),
        "consec_down_days":     consec_down,
        "consec_decline_pct":   round(consec_decline_pct, 1),
        "pct_above_52wk_low":   round(pct_above_52wk_low, 1),
        "wk52_low":             round(wk52_low, 2),
        "oversold_conditions":  oversold,
        "oversold_count":       oversold_count,
        "bounce_signals":       bounce,
        "bounce_count":         bounce_count,
        "vix_bonus":            vix_bonus,
        "latest_close":         round(float(closes[-1]), 2),
        "vol_ma20":             round(float(vol_ma20), 0),
        "vol_ratio":            vol_ratio,
    }


def _mr_score(m: dict, stock: dict) -> int:
    """計算均值回歸評分（0–100，RR獎勵在 detect() 中另加）。"""
    score = 0

    # RSI
    rsi = m.get("rsi")
    if rsi is not None:
        if rsi < 20:
            score += 20
        elif rsi < 25:
            score += 15
        elif rsi < 30:
            score += 10

    # MA50 偏離（負數越大越超賣）
    dev = m["pct_below_ma50"]
    if dev < -25:
        score += 20
    elif dev < -20:
        score += 15
    elif dev < -15:
        score += 10

    # 反彈信號
    bounce = m["bounce_signals"]
    if "放量陽線" in bounce:
        score += 20
    elif bounce:
        score += 10

    # EPS 增速
    eps_growth = stock.get("eps_growth_qoq", 0) or 0
    if eps_growth > 30:
        score += 15
    elif eps_growth > 15:
        score += 10

    # VIX 獎勵
    if m.get("vix_bonus"):
        score += 10

    return score


def detect(
    candidates: list,
    date: Optional[str] = None,
    market_env: Optional[dict] = None,
) -> list:
    """
    從候選股中偵測均值回歸（超賣反彈）信號。

    Args:
        candidates:  fundamental_filter.run() 或 technical_filter.run() 返回的股票 dict 列表
        date:        截止日期 "YYYY-MM-DD"（回測用），默認 None 表示今天
        market_env:  大盤環境 dict（含 vix 等），用於 VIX 獎勵判斷

    Returns:
        觸發 MEAN_REVERSION 信號的股票列表，按 score 降序
    """
    total   = len(candidates)
    signals = []
    skipped = 0

    print(f"[mean_reversion] 開始均值回歸偵測，共 {total} 只候選"
          + (f"（截至 {date}）" if date else ""))
    print(f"  觸發條件: 超賣條件>={MIN_OVERSOLD_COUNT}個 + 反彈信號>={MIN_BOUNCE_COUNT}個 "
          f"+ R/R>={RR_MIN}")

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

            metrics = _compute_metrics(df, market_env)
            if metrics is None:
                continue

            # ── 風險回報計算 ──────────────────────────────────────────────────
            lows_arr     = df["low"].values
            entry_price  = metrics["latest_close"]
            recent_low   = float(lows_arr[-5:].min())
            stop_price   = round(recent_low * 0.97, 2)
            target_price = round(float(metrics["ma50"]), 2)

            risk   = entry_price - stop_price
            reward = target_price - entry_price

            if risk <= 0 or reward <= 0:
                continue

            rr = round(reward / risk, 1)
            if rr < RR_MIN:
                continue

            score = _mr_score(metrics, stock)
            if rr >= 3:
                score += 5

            if score < MIN_WATCH_SCORE:
                continue

            action = "BUY" if score >= MIN_BUY_SCORE else "WATCH"

            risk_pct   = risk / entry_price * 100 if entry_price > 0 else 0
            reward_pct = reward / entry_price * 100 if entry_price > 0 else 0
            risk_reward = round(reward_pct / risk_pct, 1) if risk_pct > 0 else rr

            oversold_str = ", ".join(metrics["oversold_conditions"])
            bounce_str   = ", ".join(metrics["bounce_signals"])

            reason = (
                f"MEAN_REVERSION：超賣信號（{oversold_str}），"
                f"反彈信號（{bounce_str}），"
                f"距MA50偏離{metrics['pct_below_ma50']:.0f}%，"
                f"{'RSI=' + str(metrics['rsi']) + ' ' if metrics['rsi'] is not None else ''}"
                f"R/R={rr:.1f}，"
                f"{'VIX>' + str(int(market_env.get('vix', 0))) + ' 波動獎勵，' if metrics.get('vix_bonus') else ''}"
                f"評分{score}/100。"
            )

            signal = {
                **stock,
                # 標準欄位
                "signal_type":  "MEAN_REVERSION",
                "action":       action,
                "last_close":   entry_price,
                "entry_price":  entry_price,
                "entry_zone":   f"{entry_price:.2f}–{entry_price:.2f}",
                "stop_loss":    f"{stop_price:.2f}",
                "target_price": target_price,
                "risk_reward":  risk_reward,
                "reason":       reason,
                # MEAN_REVERSION 特有欄位
                "score":               score,
                "rsi":                 metrics["rsi"],
                "pct_below_ma50":      metrics["pct_below_ma50"],
                "pct_below_bb_lower":  metrics["pct_below_bb_lower"],
                "oversold_count":      metrics["oversold_count"],
                "oversold_conditions": oversold_str,
                "bounce_signals":      bounce_str,
                "bounce_count":        metrics["bounce_count"],
                "vix_bonus":           metrics["vix_bonus"],
                "consec_down_days":    metrics["consec_down_days"],
                "consec_decline_pct":  metrics["consec_decline_pct"],
                "ma50":                metrics["ma50"],
                "wk52_low":            metrics["wk52_low"],
                "pct_above_52wk_low":  metrics["pct_above_52wk_low"],
                "vol_ratio":           metrics["vol_ratio"],
                "metrics":             metrics,
            }
            signals.append(signal)

        except Exception as e:
            print(f"  [mean_reversion] {ticker} 發生錯誤：{e}")
            skipped += 1
            continue

    signals.sort(key=lambda x: x.get("score", 0), reverse=True)

    print(f"\n[mean_reversion] 完成")
    print(f"  偵測: {total} 只  MEAN_REVERSION信號: {len(signals)} 個  跳過: {skipped} 只")

    if signals:
        print()
        hdr = (f"{'#':>3} {'Ticker':<7} {'Score':>5} {'Action':<7} "
               f"{'RSI':>5} {'MA50%':>7} {'BB%':>7} {'OvCnt':>6} {'BounceSig':<20}")
        print(hdr)
        print("-" * 75)
        for rank, s in enumerate(signals, 1):
            rsi_str = f"{s['rsi']:.0f}" if s["rsi"] is not None else " N/A"
            print(
                f"{rank:>3} {s['ticker']:<7} {s['score']:>5} {s['action']:<7} "
                f"{rsi_str:>5} {s['pct_below_ma50']:>6.1f}% "
                f"{s['pct_below_bb_lower']:>6.1f}% "
                f"{s['oversold_count']:>6} "
                f"{s['bounce_signals'][:19]:<20}"
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
    print("Step 2: 均值回歸偵測（使用模擬大盤環境）")
    print("=" * 60)
    mock_market_env = {"vix": 28.0, "risk_on": True}
    mr_signals = detect(fund_candidates, market_env=mock_market_env)

    print()
    if not mr_signals:
        print("今日無均值回歸信號")
    else:
        print(f"共發現 {len(mr_signals)} 個 MEAN_REVERSION 信號：")
        for s in mr_signals:
            rsi_disp = f"{s['rsi']:.0f}" if s["rsi"] is not None else "N/A"
            print(f"  {s['ticker']}: score={s['score']}  action={s['action']}  "
                  f"rsi={rsi_disp}  ma50_dev={s['pct_below_ma50']:.1f}%  "
                  f"oversold=[{s['oversold_conditions']}]  "
                  f"bounce=[{s['bounce_signals']}]  "
                  f"entry={s['entry_zone']}  stop={s['stop_loss']}")
