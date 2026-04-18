"""
main_realtime.py — 即時 EP 掃描器入口

獨立運行，不影響現有 main.py 日報流程。

用法：
  python main_realtime.py           # 持續模式（按市場時段自動切換）
  python main_realtime.py --once    # 執行一次後退出
  python main_realtime.py --test    # 測試模式（不傳送 Discord，打印輸出）

持續模式時間邏輯（美東時間 ET）：
  04:00–09:29  盤前模式：每 5 分鐘掃描 scan_premarket()
  09:30–10:30  開盤模式：每 5 分鐘掃描 scan_opening()
  10:30–16:00  靜默模式：不掃描（EP 通常在開盤後 1 小時內完成）
  其他時間     等待下次盤前

信號去重：盤內已推送的 ticker 不重複推送（in-memory sent_tickers）。
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

sys.path.insert(0, str(Path(__file__).parent))

from output.discord_alert import send_report
from signals.fib_entry_calculator import fmt_fib_summary
from signals.realtime_ep_scanner import WATCHLIST, scan_opening, scan_premarket

# ── 設定 ──────────────────────────────────────────────────────────────────────
ET = ZoneInfo("US/Eastern")

SCAN_INTERVAL_SEC    = 300    # 每次掃描間隔（秒）
PREMARKET_START      = (4, 0)   # 盤前開始 HH:MM（ET）
MARKET_OPEN          = (9, 30)  # 開盤
OPENING_WINDOW_END   = (10, 30) # 開盤掃描結束
MARKET_CLOSE         = (16, 0)  # 收盤


# ── Discord 格式化 ─────────────────────────────────────────────────────────────

def _fmt_discord_premarket(signals: list, scan_time: str) -> str:
    """格式化盤前 EP 候選為 Discord 消息。"""
    if not signals:
        return ""

    buy_sigs   = [s for s in signals if s["action"] == "BUY"]
    watch_sigs = [s for s in signals if s["action"] == "WATCH"]

    lines = [f"**盤前 EP 掃描** — {scan_time} ET"]
    lines.append(f"共 {len(signals)} 個候選")

    if buy_sigs:
        lines.append("")
        lines.append("**直接關注：**")
        for s in buy_sigs[:8]:
            src = f" [{s['source']}]" if s.get("source") else ""
            fib_line = fmt_fib_summary(s.get("fib"))
            lines.append(
                f"  `{s['ticker']:<6}` +{s['gap_pct']:.1f}%  "
                f"${s['price']}  vol={s['volume']:,}{src}"
            )
            if fib_line:
                lines.append(f"    ↳ {fib_line}")

    if watch_sigs:
        lines.append("")
        lines.append("**觀察：**")
        for s in watch_sigs[:6]:
            src = f" [{s['source']}]" if s.get("source") else ""
            fib_line = fmt_fib_summary(s.get("fib"))
            lines.append(
                f"  `{s['ticker']:<6}` +{s['gap_pct']:.1f}%  ${s['price']}{src}"
            )
            if fib_line:
                lines.append(f"    ↳ {fib_line}")

    return "\n".join(lines)


def _fmt_discord_opening(signals: list, scan_time: str) -> str:
    """格式化開盤 EP 信號為 Discord 消息。"""
    if not signals:
        return ""

    buy_sigs   = [s for s in signals if s["action"] == "BUY"]
    watch_sigs = [s for s in signals if s["action"] == "WATCH"]
    fade_sigs  = [s for s in signals if s["action"] == "FADE"]

    lines = [f"**開盤 EP 掃描** — {scan_time} ET"]

    if buy_sigs:
        lines.append("")
        lines.append("**BUY 信號：**")
        for s in buy_sigs[:8]:
            cp = f"  收盤位={s['close_position']:.0%}" if s["close_position"] is not None else ""
            vr = f"  量比={s['vol_ratio']:.1f}x" if s["vol_ratio"] is not None else ""
            lines.append(
                f"  `{s['ticker']:<6}` gap={s['gap_pct']:+.1f}%  "
                f"${s['price']}{vr}{cp}"
            )
            fib_line = fmt_fib_summary(s.get("fib"))
            if fib_line:
                lines.append(f"    ↳ {fib_line}")

    if watch_sigs:
        lines.append("")
        lines.append("**WATCH 信號：**")
        for s in watch_sigs[:6]:
            lines.append(
                f"  `{s['ticker']:<6}` gap={s['gap_pct']:+.1f}%  ${s['price']}"
            )
            fib_line = fmt_fib_summary(s.get("fib"))
            if fib_line:
                lines.append(f"    ↳ {fib_line}")

    if fade_sigs:
        lines.append("")
        lines.append("**FADE（假突破）：**")
        for s in fade_sigs[:4]:
            cp = f"收盤位={s['close_position']:.0%}" if s["close_position"] is not None else ""
            lines.append(
                f"  `{s['ticker']:<6}` gap={s['gap_pct']:+.1f}%  {cp}"
            )

    return "\n".join(lines)


# ── 市場時段判斷 ───────────────────────────────────────────────────────────────

def _current_phase(now: Optional[datetime] = None) -> str:
    """
    返回當前市場時段：
      "premarket"  — 04:00–09:29 ET
      "opening"    — 09:30–10:30 ET
      "silent"     — 10:30–16:00 ET（不掃描）
      "closed"     — 其他時間
    """
    t = (now or datetime.now(ET))
    hm = (t.hour, t.minute)

    if PREMARKET_START <= hm < MARKET_OPEN:
        return "premarket"
    if MARKET_OPEN <= hm < OPENING_WINDOW_END:
        return "opening"
    if OPENING_WINDOW_END <= hm < MARKET_CLOSE:
        return "silent"
    return "closed"


def _seconds_until_premarket(now: Optional[datetime] = None) -> int:
    """計算距離下次盤前（4:00 ET）的秒數。"""
    from datetime import timedelta

    t = (now or datetime.now(ET))
    next_pre = t.replace(hour=PREMARKET_START[0], minute=PREMARKET_START[1],
                         second=0, microsecond=0)
    if next_pre <= t:
        next_pre += timedelta(days=1)
    return int((next_pre - t).total_seconds())


# ── 單次掃描 ──────────────────────────────────────────────────────────────────

def run_scan_once(
    phase: str,
    sent_tickers: set,
    premarket_candidates: list,
    test_mode: bool = False,
) -> list:
    """
    執行一次掃描（盤前或開盤），返回新觸發的信號列表。

    Args:
        phase:                "premarket" | "opening"
        sent_tickers:         已推送過的 ticker 集合（in-memory 去重）
        premarket_candidates: 盤前候選 ticker 列表（開盤掃描使用）
        test_mode:            True 時只打印，不推送 Discord

    Returns:
        本次新觸發的信號列表
    """
    scan_time = datetime.now(ET).strftime("%H:%M")

    if phase == "premarket":
        all_signals = scan_premarket()
    else:
        tickers = list(set(premarket_candidates) | set(WATCHLIST))
        all_signals = scan_opening(tickers)

    # 過濾已推送
    new_signals = [s for s in all_signals if s["ticker"] not in sent_tickers]

    if not new_signals:
        print(f"  [info] {phase} 掃描完成，無新信號")
        return []

    # 格式化消息
    if phase == "premarket":
        msg = _fmt_discord_premarket(new_signals, scan_time)
    else:
        msg = _fmt_discord_opening(new_signals, scan_time)

    if not msg:
        return []

    print("\n" + msg + "\n")

    if not test_mode:
        send_report(msg)
        # 開盤 BUY 信號附帶圖表
        if phase == "opening":
            try:
                from output.chart_generator import generate_signal_chart
                from output.discord_alert import send_signal_with_chart
                buy_sigs = [s for s in new_signals
                            if s.get("action") in ("BUY", "BUY_RISKY")]
                for sig in buy_sigs:
                    try:
                        chart_path = generate_signal_chart(sig)
                        send_signal_with_chart(sig, chart_path)
                    except Exception as ce:
                        print(f"  [chart] {sig.get('ticker')} 圖表失敗: {ce}")
            except Exception as e:
                print(f"  [warn] 圖表模組異常: {e}")

    # 記錄已推送
    for s in new_signals:
        sent_tickers.add(s["ticker"])

    return new_signals


# ── 持續模式 ──────────────────────────────────────────────────────────────────

def run_continuous(test_mode: bool = False) -> None:
    """
    持續運行掃描器：
      - 根據 ET 時段自動選擇 premarket / opening / silent / closed
      - 每個交易日重置 sent_tickers
      - 閉市時等待至下次盤前
    """
    print("[realtime_ep] 啟動持續掃描模式")
    if test_mode:
        print("[realtime_ep] *** 測試模式：不推送 Discord ***")

    sent_tickers:         set  = set()
    premarket_candidates: list = []
    last_date: Optional[str]   = None

    while True:
        now      = datetime.now(ET)
        today    = now.strftime("%Y-%m-%d")
        phase    = _current_phase(now)
        hm_str   = now.strftime("%H:%M")

        # 每日重置
        if today != last_date:
            print(f"\n[realtime_ep] 新的一天：{today}，重置 sent_tickers")
            sent_tickers.clear()
            premarket_candidates.clear()
            last_date = today

        if phase == "closed":
            wait = _seconds_until_premarket(now)
            print(f"[realtime_ep] {hm_str} ET — 閉市，{wait//3600}h{wait%3600//60}m 後進入盤前")
            time.sleep(min(wait, 3600))  # 最多睡 1 小時後重新判斷
            continue

        if phase == "silent":
            print(f"[realtime_ep] {hm_str} ET — 靜默期（10:30–16:00），等待 {SCAN_INTERVAL_SEC}s")
            time.sleep(SCAN_INTERVAL_SEC)
            continue

        print(f"\n[realtime_ep] {hm_str} ET — 執行 {phase} 掃描")
        new_sigs = run_scan_once(phase, sent_tickers, premarket_candidates, test_mode)

        # 盤前新信號 → 更新 premarket_candidates（供開盤掃描使用）
        if phase == "premarket" and new_sigs:
            for s in new_sigs:
                if s["ticker"] not in premarket_candidates:
                    premarket_candidates.append(s["ticker"])

        time.sleep(SCAN_INTERVAL_SEC)


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="即時 EP 掃描器")
    parser.add_argument("--test", action="store_true",
                        help="測試模式：執行一次盤前掃描，打印結果，不推送 Discord")
    parser.add_argument("--once", action="store_true",
                        help="執行一次（根據當前時段），推送 Discord 後退出")
    args = parser.parse_args()

    if args.test:
        print("[realtime_ep] === 測試模式 ===")
        now   = datetime.now(ET)
        phase = _current_phase(now)
        print(f"  當前時段: {phase}  ({now.strftime('%H:%M')} ET)")
        print()

        # 測試模式：強制執行盤前掃描（不受市場時間限制）
        sent: set  = set()
        cands: list = []
        print("--- 盤前掃描測試 ---")
        pre_sigs = run_scan_once("premarket", sent, cands, test_mode=True)
        cands = [s["ticker"] for s in pre_sigs]

        print()
        print("--- 開盤掃描測試（min_gap=5.0%）---")
        from signals.realtime_ep_scanner import scan_opening as _open
        open_sigs = _open(cands or WATCHLIST)
        if open_sigs:
            msg = _fmt_discord_opening(open_sigs, now.strftime("%H:%M"))
            print(msg)
        else:
            print("  無開盤信號（非交易時段正常）")

        print("\n[realtime_ep] 測試完成")
        return

    if args.once:
        now   = datetime.now(ET)
        phase = _current_phase(now)
        print(f"[realtime_ep] --once 模式，當前時段: {phase}")
        sent: set   = set()
        cands: list = []
        run_scan_once(phase, sent, cands, test_mode=False)
        return

    # 持續模式
    run_continuous(test_mode=False)


if __name__ == "__main__":
    main()
