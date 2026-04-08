"""
fib_entry_calculator.py — 基於盤前跳空缺口計算 Fibonacci 回撤入場位

用缺口本身（prev_close → pm_high）計算 Fibonacci 回撤，
不是從歷史底部算，適用於 EP（Episodic Pivot）跳空缺口分析。

標準 EP 交易邏輯：
  - 跳空開高後往往有回撤，Fib 38.2%–50% 是理想買入區
  - Fib 61.8% 是最後防線，跌破則缺口填補風險高
  - 止損設在 Fib 61.8% 下方一點

用法：
  fib = calculate_fib_entry(prev_close=110.0, pm_high=120.0, current_price=118.5)
  # → entry_low=114.0, entry_high=115.4, stop_loss=118.0（後者為示意值）
"""

from typing import Optional

# 有效信號門檻（低於此 R:R 不推薦入場）
MIN_RISK_REWARD = 1.5

# 止損緩衝：Fib 0.618 下方再扣 1% 的缺口大小
STOP_BUFFER_RATIO = 0.01


def calculate_fib_entry(
    prev_close: float,
    pm_high: float,
    current_price: Optional[float] = None,
) -> dict:
    """
    基於盤前跳空缺口計算 Fibonacci 回撤入場位。

    Args:
        prev_close:    前一天收盤價（缺口起點）
        pm_high:       盤前最高價（缺口終點，開盤後用開盤價）
        current_price: 當前盤前價格（可選，用於計算距入場區距離）

    Returns:
        {
            "gap_range":        [prev_close, pm_high],
            "gap_pct":          float,           # 缺口幅度（%）
            "gap_size":         float,           # 缺口絕對大小
            "fib_236":          float,           # 回撤 23.6%
            "fib_382":          float,           # 回撤 38.2%（入場區上沿）
            "fib_500":          float,           # 回撤 50.0%（入場區下沿）
            "fib_618":          float,           # 回撤 61.8%（止損參考）
            "entry_low":        float,           # 入場區下沿（Fib 50.0%）
            "entry_high":       float,           # 入場區上沿（Fib 38.2%）
            "entry_mid":        float,           # 入場區中點
            "stop_loss":        float,           # 止損（Fib 61.8% 下方 1% 缺口大小）
            "risk_per_share":   float,           # 每股風險
            "reward_per_share": float,           # 每股回報（回到 pm_high）
            "risk_reward":      float,           # R:R 比
            "valid":            bool,            # R:R ≥ MIN_RISK_REWARD
            "distance_pct":     Optional[float], # 當前價距入場區上沿（負=已進入區間）
        }
        若輸入無效（pm_high <= prev_close）返回 None
    """
    if pm_high <= prev_close or prev_close <= 0:
        return None

    gap = pm_high - prev_close
    gap_pct = round((pm_high - prev_close) / prev_close * 100, 2)

    # ── Fibonacci 回撤位 ──────────────────────────────────────────────────────
    fib_236 = round(pm_high - gap * 0.236, 2)
    fib_382 = round(pm_high - gap * 0.382, 2)
    fib_500 = round(pm_high - gap * 0.500, 2)
    fib_618 = round(pm_high - gap * 0.618, 2)

    # ── 入場區 & 止損 ─────────────────────────────────────────────────────────
    entry_high = fib_382
    entry_low  = fib_500
    entry_mid  = round((entry_high + entry_low) / 2, 2)
    stop_loss  = round(fib_618 - gap * STOP_BUFFER_RATIO, 2)

    # ── 風報比 ────────────────────────────────────────────────────────────────
    risk   = round(entry_mid - stop_loss, 2)
    reward = round(pm_high - entry_mid, 2)

    if risk <= 0:
        risk_reward = 0.0
    else:
        risk_reward = round(reward / risk, 1)

    # ── 距入場區距離（當前盤前價 → 入場區上沿） ─────────────────────────────
    distance_pct: Optional[float] = None
    if current_price and current_price > 0 and entry_high > 0:
        distance_pct = round((current_price - entry_high) / entry_high * 100, 1)
        # 負值 = 當前價已低於入場區上沿（已進入或穿越入場區）

    return {
        "gap_range":        [round(prev_close, 2), round(pm_high, 2)],
        "gap_pct":          gap_pct,
        "gap_size":         round(gap, 2),
        "fib_236":          fib_236,
        "fib_382":          fib_382,
        "fib_500":          fib_500,
        "fib_618":          fib_618,
        "entry_low":        entry_low,
        "entry_high":       entry_high,
        "entry_mid":        entry_mid,
        "stop_loss":        stop_loss,
        "risk_per_share":   risk,
        "reward_per_share": reward,
        "risk_reward":      risk_reward,
        "valid":            risk_reward >= MIN_RISK_REWARD,
        "distance_pct":     distance_pct,
    }


def fmt_fib_summary(fib: dict) -> str:
    """
    單行格式化 Fib 入場摘要，供 Discord 消息使用。

    範例：入場 $114.08–$113.21  止損 $111.80  R/R 2.3r  (距入場 +2.1%)
    """
    if not fib:
        return ""
    entry_str = f"入場 ${fib['entry_high']}–${fib['entry_low']}"
    stop_str  = f"止損 ${fib['stop_loss']}"
    rr_str    = f"R/R {fib['risk_reward']}r"
    parts = [entry_str, stop_str, rr_str]

    if fib.get("distance_pct") is not None:
        dist = fib["distance_pct"]
        if dist > 0:
            parts.append(f"距入場 +{dist:.1f}%")
        else:
            parts.append(f"已進入區間 ({dist:.1f}%)")

    return "  ".join(parts)


# ── 測試入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cases = [
        # (prev_close, pm_high, current_price, label)
        (110.0, 120.0, 118.5, "標準 10% 跳空"),
        (100.0, 109.0, 107.5, "MSTR-like 9% 跳空"),
        (50.0,  58.0,  57.2,  "小型股 16% 跳空"),
        (100.0, 100.0, None,  "無效（無跳空）"),
    ]

    for prev_c, pm_h, cur_p, label in cases:
        print(f"\n{'='*55}")
        print(f"測試：{label}  prev_close={prev_c}  pm_high={pm_h}")
        result = calculate_fib_entry(prev_c, pm_h, cur_p)
        if result is None:
            print("  → None（無效輸入）")
            continue
        print(f"  缺口：{result['gap_pct']}%  大小 ${result['gap_size']}")
        print(f"  Fib 23.6%: ${result['fib_236']}")
        print(f"  Fib 38.2%: ${result['fib_382']}  ← 入場區上沿")
        print(f"  Fib 50.0%: ${result['fib_500']}  ← 入場區下沿")
        print(f"  Fib 61.8%: ${result['fib_618']}  ← 止損參考")
        print(f"  止損：     ${result['stop_loss']}")
        print(f"  R/R：      {result['risk_reward']}r  (有效={result['valid']})")
        if result["distance_pct"] is not None:
            print(f"  距入場區： {result['distance_pct']:+.1f}%")
        print(f"  摘要：{fmt_fib_summary(result)}")
