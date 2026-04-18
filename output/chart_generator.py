"""
chart_generator.py — 信號圖表生成模組

生成簡潔深色主題的股票信號圖表，含：
- OHLC 蠟燭圖 + MA20/MA50 均線
- 入場/止損/目標水平線（右側標籤）
- 信號特定疊加層：
    FALLING_WEDGE → 上下趨勢線 + 楔形填充
    EP             → 缺口帶（prev_close ~ today_open）
    VCP / BULL_FLAG → 最近基部區間
    MEAN_REVERSION → Bollinger Bands
- 成交量子圖（帶50日均量線）
- 底部信息條（Entry / Stop / Target / 日期）

使用 matplotlib Agg backend，支持服務器無頭渲染（不需 DISPLAY）。
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")  # 必須在 pyplot import 前設置
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
from matplotlib.transforms import blended_transform_factory
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from data.polygon_client import get_history

# ── 輸出目錄 ──────────────────────────────────────────────────────────────────
CHART_DIR = Path(__file__).parent.parent / "output" / "charts"

# ── 尺寸 ──────────────────────────────────────────────────────────────────────
CHART_DAYS   = 120   # 顯示最近 N 交易日（足以涵蓋 35-120 天楔形）
CHART_WIDTH  = 12    # inches
CHART_HEIGHT = 7     # inches
CHART_DPI    = 100   # 1200×700 px

# ── 深色主題顏色（類 TradingView Dark） ────────────────────────────────────────
BG_COLOR       = "#0d1117"
PANEL_COLOR    = "#161b22"
GRID_COLOR     = "#21262d"
TEXT_COLOR     = "#c9d1d9"
DIM_COLOR      = "#8b949e"
UP_COLOR       = "#26a641"    # 上漲蠟燭
DOWN_COLOR     = "#f85149"    # 下跌蠟燭
MA20_COLOR     = "#f0883e"    # MA20 橙
MA50_COLOR     = "#58a6ff"    # MA50 藍
ENTRY_COLOR    = "#3fb950"    # 入場綠虛線
STOP_COLOR     = "#f85149"    # 止損紅虛線
TARGET_COLOR   = "#79c0ff"    # 目標藍dash-dot
CURR_COLOR     = "#ffffff"    # 當前價白點線
WEDGE_UP_CLR   = "#ff7b72"    # 楔形上方趨勢線
WEDGE_LO_CLR   = "#79c0ff"    # 楔形下方趨勢線
VOL_UP_CLR     = "#1e4620"    # 成交量上漲
VOL_DN_CLR     = "#3d1010"    # 成交量下跌
BB_COLOR       = "#6e40c9"    # Bollinger Bands 紫
GAP_COLOR      = "#f0883e"    # EP 缺口帶橙


# ── 蠟燭圖 ────────────────────────────────────────────────────────────────────

def _plot_candles(ax, df: pd.DataFrame) -> None:
    """繪製 OHLC 蠟燭圖（影線 + 實體）。"""
    for i in range(len(df)):
        row   = df.iloc[i]
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        color = UP_COLOR if c >= o else DOWN_COLOR
        ax.plot([i, i], [l, h], color=color, linewidth=0.7, alpha=0.9, zorder=2)
        body_h = max(abs(c - o), (h - l) * 0.005)
        ax.bar(i, body_h, bottom=min(o, c), width=0.6, color=color,
               alpha=0.9, zorder=3, linewidth=0)


# ── 技術指標計算 ──────────────────────────────────────────────────────────────

def _compute_bb(closes: np.ndarray, period: int = 20, std_mult: float = 2.0):
    """Bollinger Bands，返回 (upper, middle, lower) ndarray。"""
    n      = len(closes)
    upper  = np.full(n, np.nan)
    middle = np.full(n, np.nan)
    lower  = np.full(n, np.nan)
    for i in range(period - 1, n):
        w = closes[i - period + 1: i + 1]
        m = w.mean()
        s = w.std(ddof=1)
        middle[i] = m
        upper[i]  = m + std_mult * s
        lower[i]  = m - std_mult * s
    return upper, middle, lower


# ── 右側標籤水平線 ────────────────────────────────────────────────────────────

def _hline_labeled(ax, price: Optional[float], color: str, ls: str,
                   lw: float = 1.3, label: str = "") -> None:
    """繪製水平線 + 右側帶前綴的價格標籤（使用混合坐標系，標籤不被截斷）。"""
    if price is None:
        return
    ax.axhline(price, color=color, linestyle=ls, linewidth=lw, alpha=0.85, zorder=5)
    trans = blended_transform_factory(ax.transAxes, ax.transData)
    tag = f"{label} ${price:.2f}" if label else f"${price:.2f}"
    ax.text(1.003, price, tag, color=color, fontsize=9,
            va="center", ha="left", transform=trans, clip_on=False,
            fontweight="bold")


# ── 信號特定疊加層 ────────────────────────────────────────────────────────────

def _overlay_falling_wedge(ax, signal: dict, n: int) -> None:
    """
    繪製下降楔形上下趨勢線。

    signal 須含：h_slope, h_intercept, l_slope, l_intercept, peak_bar_from_end
    其中 x_wsub = chart_bar_index - peak_bar_chart，趨勢線在 wsub 坐標系下成立。
    """
    h_slope          = signal.get("h_slope")
    h_intercept      = signal.get("h_intercept")
    l_slope          = signal.get("l_slope")
    l_intercept      = signal.get("l_intercept")
    peak_bar_from_end = signal.get("peak_bar_from_end")

    if None in (h_slope, h_intercept, l_slope, l_intercept, peak_bar_from_end):
        return

    # peak_bar_chart：peak 在本圖表窗口中的 bar index
    # 若為負數（peak 在圖表窗口之前），趨勢線仍可向前外推繪製
    peak_bar_chart = int(n - peak_bar_from_end)
    x_start = max(0, peak_bar_chart)

    xs     = np.arange(x_start, n)
    x_wsub = xs - peak_bar_chart
    h_vals = h_slope * x_wsub + h_intercept
    l_vals = l_slope * x_wsub + l_intercept

    ax.plot(xs, h_vals, color=WEDGE_UP_CLR, linewidth=1.6,
            linestyle="-", alpha=0.85, zorder=4, label="Upper TL")
    ax.plot(xs, l_vals, color=WEDGE_LO_CLR, linewidth=1.6,
            linestyle="-", alpha=0.85, zorder=4, label="Lower TL")

    # 楔形半透明填充
    ax.fill_between(xs, l_vals, h_vals, alpha=0.06,
                    color=WEDGE_LO_CLR, zorder=1)


def _overlay_ep(ax, signal: dict) -> None:
    """EP 缺口區：prev_close ~ today_open 橙色帶。"""
    m = signal.get("metrics") or {}
    today_open = signal.get("today_open") or m.get("today_open")
    prev_close = signal.get("prev_close") or m.get("prev_close")
    if today_open and prev_close and float(today_open) > float(prev_close):
        lo, hi = float(prev_close), float(today_open)
        ax.axhspan(lo, hi, alpha=0.12, color=GAP_COLOR, zorder=1)
        ax.axhline(hi, color=GAP_COLOR, linewidth=0.8, linestyle="--", alpha=0.6, zorder=2)
        ax.axhline(lo, color=DIM_COLOR,  linewidth=0.8, linestyle="--", alpha=0.5, zorder=2)


def _overlay_base_zone(ax, df: pd.DataFrame) -> None:
    """VCP / Bull Flag：最近20天最高最低帶（基部區間）。"""
    n = len(df)
    if n < 5:
        return
    recent = df.tail(min(20, n))
    lo = float(recent["low"].min())
    hi = float(recent["high"].max())
    ax.axhspan(lo, hi, alpha=0.08, color=GAP_COLOR, zorder=1)


def _overlay_bollinger(ax, closes: np.ndarray, xs: np.ndarray) -> None:
    """Mean Reversion：Bollinger Bands（±2σ）。"""
    upper, middle, lower = _compute_bb(closes)
    ax.plot(xs, upper,  color=BB_COLOR, linewidth=0.9, linestyle="--", alpha=0.7, zorder=3)
    ax.plot(xs, middle, color=BB_COLOR, linewidth=0.7, linestyle="-",  alpha=0.5, zorder=3)
    ax.plot(xs, lower,  color=BB_COLOR, linewidth=0.9, linestyle="--", alpha=0.7, zorder=3)
    ax.fill_between(xs, lower, upper, alpha=0.05, color=BB_COLOR, zorder=1)


# ── 主函數 ────────────────────────────────────────────────────────────────────

def generate_signal_chart(signal: dict, save_path: str = None) -> str:
    """
    生成信號圖表 PNG。

    Args:
        signal:    信號 dict，需含 ticker, signal_type, action, entry_price/entry_zone,
                   stop_loss, target_price。FALLING_WEDGE 還需含 h_slope/h_intercept/
                   l_slope/l_intercept/peak_bar_from_end。
        save_path: 指定保存路徑，None 時自動命名到 output/charts/

    Returns:
        保存的 PNG 文件路徑（字符串）

    Raises:
        ValueError: 無法獲取歷史數據時
    """
    ticker      = signal.get("ticker", "UNKNOWN").upper()
    signal_type = signal.get("signal_type", "")
    action      = signal.get("action", "")

    # ── 1. 獲取歷史數據 ───────────────────────────────────────────────────────
    end_date = signal.get("date") or None
    df = get_history(ticker, days=CHART_DAYS, end_date=end_date)
    if df.empty or len(df) < 10:
        raise ValueError(f"[chart] {ticker} 無法獲取歷史數據")

    n      = len(df)
    xs     = np.arange(n)
    closes = df["close"].values.astype(float)
    ma20   = pd.Series(closes).rolling(20).mean().values
    ma50   = pd.Series(closes).rolling(50).mean().values

    # ── 2. 解析交易線價格 ─────────────────────────────────────────────────────
    def _flt(key):
        v = signal.get(key)
        if v is None:
            return None
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return None

    entry_price   = _flt("entry_price") or _flt("entry_zone")
    stop_loss     = _flt("stop_loss")
    target_price  = _flt("target_price")
    current_price = float(df["close"].iloc[-1])

    # ── 3. 創建圖形（4:1 高低比，價格+成交量） ────────────────────────────────
    fig = plt.figure(figsize=(CHART_WIDTH, CHART_HEIGHT), dpi=CHART_DPI,
                     facecolor=BG_COLOR)
    gs = GridSpec(4, 1, figure=fig, hspace=0.04,
                  top=0.91, bottom=0.09, left=0.02, right=0.92)

    ax_p = fig.add_subplot(gs[:3, 0])
    ax_v = fig.add_subplot(gs[3, 0], sharex=ax_p)

    for ax in (ax_p, ax_v):
        ax.set_facecolor(PANEL_COLOR)
        ax.tick_params(colors=DIM_COLOR, labelsize=8, length=3)
        ax.yaxis.tick_right()
        ax.spines["bottom"].set_color(GRID_COLOR)
        ax.spines["top"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["right"].set_color(GRID_COLOR)
        ax.grid(axis="y", color=GRID_COLOR, linewidth=0.5, alpha=0.7)
        ax.grid(axis="x", visible=False)

    # ── 4. 蠟燭 + 均線 ───────────────────────────────────────────────────────
    _plot_candles(ax_p, df)
    ax_p.plot(xs, ma20, color=MA20_COLOR, linewidth=1.2, alpha=0.9, zorder=4)
    ax_p.plot(xs, ma50, color=MA50_COLOR, linewidth=1.2, alpha=0.9, zorder=4)

    # ── 5. 信號特定疊加 ───────────────────────────────────────────────────────
    if signal_type == "FALLING_WEDGE":
        _overlay_falling_wedge(ax_p, signal, n)
    elif signal_type == "EP":
        _overlay_ep(ax_p, signal)
    elif signal_type in ("VCP", "BULL_FLAG", "VCP_CHEAT_ENTRY"):
        _overlay_base_zone(ax_p, df)
    elif signal_type == "MEAN_REVERSION":
        _overlay_bollinger(ax_p, closes, xs)

    # ── 6. 交易線（Entry / Stop / Target / 當前價） ───────────────────────────
    _hline_labeled(ax_p, entry_price,  ENTRY_COLOR,  "--",  lw=1.3, label="Entry")
    _hline_labeled(ax_p, stop_loss,    STOP_COLOR,   "--",  lw=1.3, label="Stop")
    _hline_labeled(ax_p, target_price, TARGET_COLOR, "-.",  lw=1.3, label="Target")

    # 利潤區（Entry→Target）和風險區（Stop→Entry）半透明填充
    if entry_price and target_price and target_price > entry_price:
        ax_p.axhspan(entry_price, target_price, alpha=0.08, color="#3fb950", zorder=1)
    if stop_loss and entry_price and entry_price > stop_loss:
        ax_p.axhspan(stop_loss, entry_price, alpha=0.08, color="#f85149", zorder=1)

    # Entry 附近最近 K 線的 BUY 箭頭標記
    if entry_price:
        closes_arr = df["close"].values.astype(float)
        diffs = np.abs(closes_arr - entry_price)
        x_buy = int(np.argmin(diffs[-20:]) + max(0, n - 20))
        ax_p.annotate("▲ BUY", xy=(x_buy, entry_price),
                      fontsize=10, color=ENTRY_COLOR, fontweight="bold",
                      ha="center", va="bottom", zorder=6)

    ax_p.axhline(current_price, color=CURR_COLOR, linestyle="--",
                 linewidth=1.1, alpha=0.75, zorder=4)
    trans_curr = blended_transform_factory(ax_p.transAxes, ax_p.transData)
    ax_p.text(1.003, current_price, f"Now ${current_price:.2f}",
              color=CURR_COLOR, fontsize=9, va="center", ha="left",
              transform=trans_curr, clip_on=False, fontweight="bold")

    # Y 軸留白（包含 entry/stop/target 確保所有交易線可見）
    y_lo = float(df["low"].min())
    y_hi = float(df["high"].max())
    for p in (entry_price, stop_loss, target_price):
        if p is not None:
            y_lo = min(y_lo, p)
            y_hi = max(y_hi, p)
    pr = y_hi - y_lo
    ax_p.set_ylim(y_lo - pr * 0.05, y_hi + pr * 0.15)

    # ── 7. 成交量子圖 ─────────────────────────────────────────────────────────
    vols   = df["volume"].values.astype(float)
    v_clrs = [VOL_UP_CLR if df["close"].iloc[i] >= df["open"].iloc[i]
              else VOL_DN_CLR for i in range(n)]
    ax_v.bar(xs, vols, color=v_clrs, width=0.8, zorder=2)
    vol_ma50 = pd.Series(vols).rolling(50).mean().values
    ax_v.plot(xs, vol_ma50, color=DIM_COLOR, linewidth=0.8, alpha=0.7, zorder=3)
    ax_v.set_ylim(0, vols.max() * 3.8)
    ax_v.yaxis.set_major_formatter(
        plt.FuncFormatter(
            lambda x, _: f"{x/1e6:.0f}M" if x >= 1e6 else (f"{x/1e3:.0f}K" if x >= 1e3 else str(int(x)))
        )
    )

    # ── 8. X 軸日期標籤（只在下方子圖顯示） ──────────────────────────────────
    dates     = df["date"].tolist()
    n_ticks   = 6
    step      = max(1, n // n_ticks)
    tick_pos  = list(range(0, n, step))
    tick_lbs  = [dates[i][5:] for i in tick_pos]  # "MM-DD"
    ax_v.set_xticks(tick_pos)
    ax_v.set_xticklabels(tick_lbs, color=DIM_COLOR, fontsize=7.5)
    plt.setp(ax_p.get_xticklabels(), visible=False)

    # ── 9. 圖例 ───────────────────────────────────────────────────────────────
    legend_elems = [
        mpatches.Patch(color=MA20_COLOR, label="MA20"),
        mpatches.Patch(color=MA50_COLOR, label="MA50"),
    ]
    if signal_type == "FALLING_WEDGE":
        legend_elems += [
            mpatches.Patch(color=WEDGE_UP_CLR, label="Upper TL"),
            mpatches.Patch(color=WEDGE_LO_CLR, label="Lower TL"),
        ]
    elif signal_type == "MEAN_REVERSION":
        legend_elems.append(mpatches.Patch(color=BB_COLOR, label="BB±2σ"))

    ax_p.legend(handles=legend_elems, loc="upper left", fancybox=False,
                framealpha=0.25, facecolor=BG_COLOR, edgecolor=GRID_COLOR,
                labelcolor=TEXT_COLOR, fontsize=8, ncol=2)

    # ── 10. 標題（含當前價格和日期） ─────────────────────────────────────────
    score_str      = f"  Score {signal.get('score')}/100" if signal.get("score") else ""
    rr_val         = signal.get("risk_reward")
    rr_str         = f"  R/R {rr_val}" if rr_val else ""
    action_bracket = f"[{action}]" if action else ""
    last_date      = dates[-1] if dates else ""
    title = (f"{ticker}  {signal_type}  {action_bracket}{score_str}{rr_str}"
             f"  |  ${current_price:.2f}  |  {last_date}")
    fig.suptitle(title, color=TEXT_COLOR, fontsize=12, fontweight="bold",
                 x=0.02, ha="left")

    # ── 11. 底部信息條 ────────────────────────────────────────────────────────
    parts = []
    if entry_price:
        parts.append(f"Entry {entry_price:.2f}")
    if stop_loss:
        parts.append(f"Stop {stop_loss:.2f}")
    if target_price:
        parts.append(f"Target {target_price:.2f}")
    parts.append(f"Date {dates[-1] if dates else ''}")
    fig.text(0.02, 0.015, "   |   ".join(parts),
             color=DIM_COLOR, fontsize=8, ha="left", va="bottom")

    # ── 12. 保存 ──────────────────────────────────────────────────────────────
    if save_path is None:
        CHART_DIR.mkdir(parents=True, exist_ok=True)
        today_str = datetime.today().strftime("%Y%m%d")
        save_path = str(CHART_DIR / f"{ticker}_{signal_type}_{today_str}.png")

    plt.savefig(save_path, dpi=CHART_DPI, bbox_inches="tight",
                facecolor=BG_COLOR, edgecolor="none")
    plt.close(fig)
    print(f"  [chart] {ticker} 圖表已保存: {save_path}")
    return save_path


# ── 清理工具 ──────────────────────────────────────────────────────────────────

def cleanup_old_charts(days: int = 1) -> int:
    """
    刪除超過 N 天的舊圖表。

    Args:
        days: 保留最近 N 天的圖表

    Returns:
        刪除的文件數量
    """
    if not CHART_DIR.exists():
        return 0
    cutoff = datetime.today() - timedelta(days=days)
    removed = 0
    for f in CHART_DIR.glob("*.png"):
        try:
            if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
                f.unlink()
                removed += 1
        except Exception:
            pass
    if removed:
        print(f"  [chart] 清理舊圖表: {removed} 個")
    return removed


# ── 測試入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    signal = {
        "ticker": "INTC", "signal_type": "FALLING_WEDGE",
        "action": "WATCH", "score": 52,
        "entry_price": 21.50, "stop_loss": 18.50,
        "target_price": 26.00, "risk_reward": 1.5,
    }
    path = generate_signal_chart(signal)
    print(f"圖表已生成: {path}")
