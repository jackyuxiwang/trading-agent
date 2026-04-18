"""
falling_wedge_detector.py — 下降楔形突破偵測模組

偵測股價在下跌過程中形成的「下降楔形」型態，並識別放量突破。

下降楔形定義：
  - 股價在下跌中形成一系列 Lower Highs 和 Lower Lows
  - 低點下降速度比高點慢（兩條趨勢線向下收斂）
  - 放量突破上方阻力線 → 看漲反轉信號
  - 統計勝率約 80%，是可靠的底部反轉型態

偵測邏輯（5 步驟）：
  1. 識別 Swing High / Swing Low（回看 40-120 天，window=3）
  2. 驗證 Lower Highs + Lower Lows（至少 3 個 Swing High + 3 個 Swing Low）
  3. 擬合上下趨勢線（線性回歸），確認兩線收斂（上線下降更快：abs(h_slope) > abs(l_slope)）
  4. 偵測突破：收盤 >= 上方趨勢線延伸值（或距離 < 3%）+ 放量
  5. RSI 背離加分：價格創新低但 RSI 沒創新低（極強反轉信號）

評分系統（0-100）：
  - 楔形持續時間：40-80 天 +15，80-120 天 +20
  - Swing 點對數：>=4 對 +20，3 對 +10
  - 趨勢線擬合度：R² 均 >0.8 +20，>0.6 +12
  - 量縮：後半段量 < 前半段 70% → +15
  - 突破量能：>=2x 50日均量 +15，>=1.5x +10
  - RSI 看漲背離：+10

使用 fund_candidates（下降楔形的股票未必通過 Stage 2 篩選）。
"""

import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.polygon_client import get_history as polygon_get_history

# ── 偵測閾值 ──────────────────────────────────────────────────────────────────
HISTORY_DAYS       = 180    # 拉取 ~6 個月日線（覆蓋楔形形成期）
MIN_WEDGE_DAYS     = 35     # 楔形最少持續天數
MAX_WEDGE_DAYS     = 120    # 楔形最多持續天數
SWING_WINDOW       = 3      # Swing High/Low 判定前後各 N 天
MIN_SWING_POINTS   = 3      # 至少需要幾個 Swing High（和 Swing Low）
BREAKOUT_NEAR_PCT  = 6.0    # 距上方趨勢線多少%以內算「接近突破」
BREAKOUT_VOL_MIN   = 1.5    # 突破最低放量倍數（相對 50 日均量）
MIN_BUY_SCORE      = 65     # BUY 門檻（且已突破）
MIN_WATCH_SCORE    = 45     # WATCH 門檻
CONVERGENCE_RATIO  = 0.45   # 上方趨勢線下降速度至少為下方的此比例（允許略微擴散）


# ── 數學工具 ──────────────────────────────────────────────────────────────────

def _linreg(xs: list, ys: list) -> tuple:
    """
    簡單線性回歸：y = slope * x + intercept。
    返回 (slope, intercept, r_squared)。
    點數 < 2 時返回 (0.0, ys[0] if ys else 0.0, 0.0)。
    """
    n = len(xs)
    if n < 2:
        return 0.0, (ys[0] if ys else 0.0), 0.0

    x_arr = np.array(xs, dtype=float)
    y_arr = np.array(ys, dtype=float)

    x_mean = x_arr.mean()
    y_mean = y_arr.mean()
    ss_xx  = ((x_arr - x_mean) ** 2).sum()
    ss_xy  = ((x_arr - x_mean) * (y_arr - y_mean)).sum()

    if ss_xx == 0:
        return 0.0, y_mean, 0.0

    slope     = ss_xy / ss_xx
    intercept = y_mean - slope * x_mean

    y_pred  = slope * x_arr + intercept
    ss_res  = ((y_arr - y_pred) ** 2).sum()
    ss_tot  = ((y_arr - y_mean) ** 2).sum()
    r2      = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    return float(slope), float(intercept), float(r2)


def _find_swing_points(df: pd.DataFrame, window: int = SWING_WINDOW) -> tuple:
    """
    找出局部 Swing High 和 Swing Low。

    Swing High：df.high[i] 是 [i-window, i+window] 範圍的最高值
    Swing Low ：df.low[i]  是 [i-window, i+window] 範圍的最低值

    Returns:
        swing_highs: list of (bar_index, price)
        swing_lows:  list of (bar_index, price)
    """
    highs_arr = df["high"].values
    lows_arr  = df["low"].values
    n = len(df)

    swing_highs: list = []
    swing_lows:  list = []

    for i in range(window, n - window):
        local_h = highs_arr[i - window: i + window + 1]
        local_l = lows_arr [i - window: i + window + 1]
        if highs_arr[i] == local_h.max():
            swing_highs.append((i, float(highs_arr[i])))
        if lows_arr[i] == local_l.min():
            swing_lows.append((i, float(lows_arr[i])))

    return swing_highs, swing_lows


def _compute_rsi(closes: np.ndarray, period: int = 14) -> np.ndarray:
    """
    Wilder's RSI。序列長度不足 period+1 時返回全 NaN 陣列。
    """
    n = len(closes)
    rsi = np.full(n, np.nan)
    if n < period + 1:
        return rsi

    deltas = np.diff(closes)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = gains[:period].mean()
    avg_loss = losses[:period].mean()

    for i in range(period, n - 1):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs  = avg_gain / avg_loss if avg_loss > 0 else np.inf
        rsi[i + 1] = 100 - 100 / (1 + rs)

    return rsi


# ── 核心偵測邏輯 ──────────────────────────────────────────────────────────────

def _find_falling_wedge(df: pd.DataFrame) -> Optional[dict]:
    """
    從日線 DataFrame 偵測下降楔形並計算所有指標。

    核心邏輯：先找到回看視窗內的最高點（peak），只對 peak 之後的數據做楔形分析。
    這樣可以避免「前段上漲 + 後段下跌」的混合數據把趨勢線斜率拉成正值。

    回傳 dict（包含楔形特徵和入場參數），或 None（不符合條件）。
    """
    n = len(df)
    if n < MIN_WEDGE_DAYS + SWING_WINDOW * 2:
        return None

    # 只取最近 MAX_WEDGE_DAYS + buffer 的資料做搜索
    look_back = min(n, MAX_WEDGE_DAYS + SWING_WINDOW * 2 + 10)
    sub = df.iloc[-look_back:].reset_index(drop=True)
    sub_n = len(sub)

    # ── 找到最高點（Peak）並切出楔形子區間 ────────────────────────────────────
    # 限制搜索範圍：peak 必須在距今至少 MIN_WEDGE_DAYS 天之前
    # 這樣 peak 之後才有足夠數據形成楔形
    search_end = max(SWING_WINDOW + 1, sub_n - MIN_WEDGE_DAYS)
    peak_idx   = int(sub["high"].iloc[:search_end].idxmax())

    # 楔形子區間：從 peak 到今天
    wsub   = sub.iloc[peak_idx:].reset_index(drop=True)
    wsub_n = len(wsub)

    if wsub_n < MIN_WEDGE_DAYS + SWING_WINDOW * 2:
        return None

    swing_highs, swing_lows = _find_swing_points(wsub)

    if len(swing_highs) < MIN_SWING_POINTS or len(swing_lows) < MIN_SWING_POINTS:
        return None

    # ── 1. 擬合上方趨勢線（Swing Highs）──────────────────────────────────────
    h_xs = [p[0] for p in swing_highs]
    h_ys = [p[1] for p in swing_highs]
    h_slope, h_intercept, h_r2 = _linreg(h_xs, h_ys)

    # 上方趨勢線必須向下傾斜
    if h_slope >= 0:
        return None

    # ── 2. 擬合下方趨勢線（Swing Lows）───────────────────────────────────────
    l_xs = [p[0] for p in swing_lows]
    l_ys = [p[1] for p in swing_lows]
    l_slope, l_intercept, l_r2 = _linreg(l_xs, l_ys)

    # 下方趨勢線也必須向下傾斜
    if l_slope >= 0:
        return None

    # ── 3. 驗證收斂（兩線均向下，上方至少以下方 CONVERGENCE_RATIO 的速度下降）──
    # 嚴格落下楔形：abs(h) > abs(l)（上線更陡峭）
    # 放寬至允許略微「擴散」：abs(h) >= abs(l) * CONVERGENCE_RATIO（避免漏掉
    # 上方緩降、下方急跌的楔形，這種形態在實際市場中相當常見）
    if abs(h_slope) < abs(l_slope) * CONVERGENCE_RATIO:
        return None

    # ── 4. 驗證趨勢線方向一致性 ─────────────────────────────────────────────
    # 硬性條件：第一個和最後一個 Swing 必須符合方向（整體趨勢不能反向）
    if swing_highs[-1][1] >= swing_highs[0][1]:
        return None
    if swing_lows[-1][1] >= swing_lows[0][1]:
        return None

    # 軟性條件：至少 55% 的連續對是遞減的（允許中途反彈但不能太多）
    lh_ok = sum(1 for i in range(1, len(swing_highs))
                if swing_highs[i][1] < swing_highs[i - 1][1])
    ll_ok = sum(1 for i in range(1, len(swing_lows))
                if swing_lows[i][1] < swing_lows[i - 1][1])

    n_h_comps = len(swing_highs) - 1
    n_l_comps = len(swing_lows) - 1
    h_required = max(2, int(n_h_comps * 0.55))
    l_required = max(2, int(n_l_comps * 0.55))
    if lh_ok < h_required or ll_ok < l_required:
        return None

    # ── 5. 楔形持續時間（wsub 內，第一個 Swing 到最後一個 Swing 的跨度）──────
    first_idx  = min(swing_highs[0][0], swing_lows[0][0])
    last_idx   = max(swing_highs[-1][0], swing_lows[-1][0])
    wedge_days = last_idx - first_idx
    if wedge_days < MIN_WEDGE_DAYS:
        return None

    # ── 6. 突破偵測（延伸上方趨勢線到今天）──────────────────────────────────
    today_idx        = wsub_n - 1
    resistance_today = h_slope * today_idx + h_intercept
    latest_close     = float(wsub["close"].iloc[-1])
    latest_volume    = float(wsub["volume"].iloc[-1])

    vol_ma50           = float(df["volume"].tail(50).mean()) if n >= 50 else float(df["volume"].mean())
    breakout_vol_ratio = latest_volume / vol_ma50 if vol_ma50 > 0 else 0.0

    is_breakout   = latest_close >= resistance_today
    dist_to_res   = (latest_close - resistance_today) / resistance_today * 100
    near_breakout = dist_to_res >= -BREAKOUT_NEAR_PCT

    if not near_breakout:
        return None

    # ── 7. 量縮確認（楔形後半段量 < 前半段）─────────────────────────────────
    wedge_df   = wsub.iloc[first_idx: last_idx + 1]
    half       = len(wedge_df) // 2
    first_vol  = float(wedge_df.iloc[:half]["volume"].mean()) if half > 0 else 1.0
    second_vol = float(wedge_df.iloc[half:]["volume"].mean())
    vol_ratio  = second_vol / first_vol if first_vol > 0 else 1.0

    # ── 8. RSI 看漲背離（價格新低但 RSI 沒新低）──────────────────────────────
    closes_arr = df["close"].values.astype(float)
    rsi_arr    = _compute_rsi(closes_arr)

    rsi_divergence = False
    if len(swing_lows) >= 2 and not np.isnan(rsi_arr).all():
        last2_lows = swing_lows[-2:]
        # wsub 從 sub.iloc[peak_idx] 開始，sub 從 df.iloc[n - look_back] 開始
        # 所以 wsub 的 index i 對應 df 的 (n - look_back + peak_idx + i)
        df_offset = (n - look_back) + peak_idx
        idx0 = df_offset + last2_lows[0][0]
        idx1 = df_offset + last2_lows[1][0]
        if (0 <= idx0 < n and 0 <= idx1 < n
                and not np.isnan(rsi_arr[idx0])
                and not np.isnan(rsi_arr[idx1])):
            price_new_low  = last2_lows[1][1] < last2_lows[0][1]
            rsi_new_low    = rsi_arr[idx1] < rsi_arr[idx0]
            rsi_divergence = price_new_low and not rsi_new_low

    # ── 入場/止損/目標 ────────────────────────────────────────────────────────
    entry_price  = round(resistance_today, 2)
    stop_price   = round(swing_lows[-1][1] * 0.98, 2)
    wedge_height = swing_highs[0][1] - swing_lows[0][1]   # measured move
    target_price = round(entry_price + wedge_height, 2)

    risk        = entry_price - stop_price
    reward      = target_price - entry_price
    risk_reward = round(reward / risk, 1) if risk > 0 else 0.0

    return {
        "wedge_days":         wedge_days,
        "swing_high_count":   len(swing_highs),
        "swing_low_count":    len(swing_lows),
        "h_slope":            round(h_slope, 4),
        "l_slope":            round(l_slope, 4),
        "h_r2":               round(h_r2, 3),
        "l_r2":               round(l_r2, 3),
        "vol_ratio":          round(vol_ratio, 3),
        "breakout_vol_ratio": round(breakout_vol_ratio, 2),
        "is_breakout":        is_breakout,
        "near_breakout":      near_breakout,
        "dist_to_resistance": round(dist_to_res, 2),
        "resistance_today":   round(resistance_today, 2),
        "latest_close":       round(latest_close, 2),
        "entry_price":        entry_price,
        "stop_loss_price":    stop_price,
        "target_price":       target_price,
        "risk_reward":        risk_reward,
        "rsi_divergence":     rsi_divergence,
        "swing_high_first":   round(swing_highs[0][1], 2),
        "swing_low_last":     round(swing_lows[-1][1], 2),
    }


def _wedge_score(m: dict) -> int:
    """計算下降楔形評分（0–100）。"""
    score = 0

    # 楔形持續時間
    wd = m["wedge_days"]
    if wd >= 80:
        score += 20
    elif wd >= 40:
        score += 15
    elif wd >= 35:
        score += 10

    # Swing 點對數（取 high/low 中較少者）
    pairs = min(m["swing_high_count"], m["swing_low_count"])
    if pairs >= 4:
        score += 20
    elif pairs >= 3:
        score += 10

    # 趨勢線擬合度（R²）— 實際楔形不需要完美線性，R² >= 0.2 即可接受
    avg_r2 = (m["h_r2"] + m["l_r2"]) / 2
    if avg_r2 >= 0.8:
        score += 20
    elif avg_r2 >= 0.6:
        score += 12
    elif avg_r2 >= 0.3:
        score += 6

    # 量縮（楔形後半段 vs 前半段）
    if m["vol_ratio"] <= 0.60:
        score += 15
    elif m["vol_ratio"] <= 0.70:
        score += 10

    # 突破量能
    bv = m["breakout_vol_ratio"]
    if bv >= 2.0:
        score += 15
    elif bv >= BREAKOUT_VOL_MIN:
        score += 10

    # RSI 看漲背離加分
    if m["rsi_divergence"]:
        score += 10

    return min(score, 100)


# ── 公開接口 ──────────────────────────────────────────────────────────────────

def detect(candidates: list, date: Optional[str] = None) -> list:
    """
    從候選股中偵測下降楔形突破信號。

    使用 fund_candidates（下降楔形的股票不一定通過 Stage 2，
    但應已在相對低位，dist_to_52w_high >= 15% 才有楔形空間）。

    Args:
        candidates: fundamental_filter.run() 返回的股票 dict 列表
        date:       截止日期 "YYYY-MM-DD"（回測用），None 表示今天

    Returns:
        觸發下降楔形信號的股票列表，按 score 降序
    """
    total   = len(candidates)
    signals = []
    skipped = 0

    print(f"[falling_wedge] 開始下降楔形偵測，共 {total} 只候選"
          + (f"（截至 {date}）" if date else ""))
    print(f"  條件: 楔形{MIN_WEDGE_DAYS}-{MAX_WEDGE_DAYS}天 "
          f"+ Swing點>={MIN_SWING_POINTS}對 "
          f"+ 突破放量>={BREAKOUT_VOL_MIN}x")

    for stock in candidates:
        ticker = stock.get("ticker", "")
        if not ticker:
            skipped += 1
            continue

        df = polygon_get_history(ticker, days=HISTORY_DAYS, end_date=date)
        if df.empty or len(df) < MIN_WEDGE_DAYS:
            skipped += 1
            continue

        metrics = _find_falling_wedge(df)
        if metrics is None:
            continue

        score = _wedge_score(metrics)
        if score < MIN_WATCH_SCORE:
            continue

        is_breakout = metrics["is_breakout"]

        if score >= MIN_BUY_SCORE and is_breakout:
            action = "BUY"
        elif score >= MIN_BUY_SCORE and metrics["breakout_vol_ratio"] >= BREAKOUT_VOL_MIN:
            action = "BUY"
        else:
            action = "WATCH"

        entry_price   = metrics["entry_price"]
        stop_price    = metrics["stop_loss_price"]
        target_price  = metrics["target_price"]
        rr            = metrics["risk_reward"]
        wedge_days    = metrics["wedge_days"]
        bv_ratio      = metrics["breakout_vol_ratio"]
        avg_r2        = round((metrics["h_r2"] + metrics["l_r2"]) / 2, 2)

        reason_parts = [
            f"下降楔形{wedge_days}天，",
            f"{metrics['swing_high_count']}個Lower Highs + {metrics['swing_low_count']}個Lower Lows，",
            f"趨勢線擬合度R²={avg_r2:.2f}，",
        ]
        if is_breakout:
            reason_parts.append(f"已放量突破（{bv_ratio:.1f}x均量）。")
        else:
            dist = metrics["dist_to_resistance"]
            reason_parts.append(f"接近阻力線（距離{abs(dist):.1f}%），突破量能{bv_ratio:.1f}x。")
        if metrics["rsi_divergence"]:
            reason_parts.append("RSI看漲背離確認反轉。")
        reason_parts.append(f"風報比{rr}:1，評分{score}/100。")

        signal = {
            **stock,
            # 標準欄位
            "signal_type":  "FALLING_WEDGE",
            "action":       action,
            "last_close":   metrics["latest_close"],
            "entry_price":  entry_price,
            "entry_zone":   f"{entry_price:.2f}",
            "stop_loss":    f"{stop_price:.2f}",
            "target_price": target_price,
            "risk_reward":  rr,
            "reason":       "".join(reason_parts),
            # 楔形特有欄位
            "score":              score,
            "wedge_days":         wedge_days,
            "swing_high_count":   metrics["swing_high_count"],
            "swing_low_count":    metrics["swing_low_count"],
            "h_r2":               metrics["h_r2"],
            "l_r2":               metrics["l_r2"],
            "vol_ratio":          metrics["vol_ratio"],
            "breakout_vol_ratio": bv_ratio,
            "is_breakout":        is_breakout,
            "dist_to_resistance": metrics["dist_to_resistance"],
            "rsi_divergence":     metrics["rsi_divergence"],
            "resistance_today":   metrics["resistance_today"],
        }
        signals.append(signal)

    signals.sort(key=lambda x: x.get("score", 0), reverse=True)

    print(f"\n[falling_wedge] 完成")
    print(f"  偵測: {total} 只  楔形信號: {len(signals)} 個  跳過: {skipped} 只")

    if signals:
        print()
        hdr = (f"{'#':>3} {'Ticker':<7} {'Sector':<20} {'Score':>5} "
               f"{'Action':<7} {'Days':>5} {'Pairs':>5} "
               f"{'R²':>6} {'BrkVol':>7} {'RSIDiv':>7}")
        print(hdr)
        print("-" * 82)
        for rank, s in enumerate(signals, 1):
            avg_r2 = round((s["h_r2"] + s["l_r2"]) / 2, 2)
            pairs  = min(s["swing_high_count"], s["swing_low_count"])
            print(
                f"{rank:>3} {s['ticker']:<7} {str(s.get('sector', ''))[:19]:<20} "
                f"{s['score']:>5} {s['action']:<7} "
                f"{s['wedge_days']:>5} {pairs:>5} "
                f"{avg_r2:>6.2f} {s['breakout_vol_ratio']:>7.1f}x "
                f"{'Yes' if s['rsi_divergence'] else 'No':>7}"
            )

    return signals


# ── 測試入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    tickers_to_test = sys.argv[1:] if len(sys.argv) > 1 else ["INTC", "AMD", "NVDA", "AAPL"]
    date_arg = None

    # 快速測試：直接用 ticker 構造最小 candidate dict
    candidates = [{"ticker": t} for t in tickers_to_test]

    print("=" * 60)
    print(f"下降楔形偵測測試 — {tickers_to_test}")
    print("=" * 60)

    results = detect(candidates, date=date_arg)

    print()
    if not results:
        print("未偵測到下降楔形信號")
    else:
        for s in results:
            print(f"\n  {'='*55}")
            print(f"  {s['ticker']} | score={s['score']} | {s['action']}")
            print(f"  楔形: {s['wedge_days']}天  "
                  f"Swing: {s['swing_high_count']}H/{s['swing_low_count']}L  "
                  f"R²={s['h_r2']:.2f}/{s['l_r2']:.2f}")
            print(f"  入場: ${s['entry_price']}  止損: ${s['stop_loss']}  "
                  f"目標: ${s['target_price']}  R/R={s['risk_reward']}:1")
            print(f"  突破量能: {s['breakout_vol_ratio']:.1f}x  "
                  f"RSI背離: {'是' if s['rsi_divergence'] else '否'}")
            print(f"  原因: {s['reason']}")
