"""
market_env_client.py — 市场环境数据客户端

使用 yfinance 获取大盘指数和 VIX 数据，判断当前市场是否适合交易。
"""

import json
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

CACHE_DIR = Path(__file__).parent / "cache"

# 美股固定节假日（月/日），不随年份变化的部分
_FIXED_HOLIDAYS = {
    (1, 1),   # New Year's Day
    (6, 19),  # Juneteenth
    (7, 4),   # Independence Day
    (11, 11), # Veterans Day (NYSE 不休，但作为参考保留，下面会剔除)
    (12, 25), # Christmas
}

# NYSE 2024-2026 浮动假日（手动维护近两年）
_FLOATING_HOLIDAYS = {
    date(2024, 1, 15),  # MLK Day
    date(2024, 2, 19),  # Presidents Day
    date(2024, 3, 29),  # Good Friday
    date(2024, 5, 27),  # Memorial Day
    date(2024, 9, 2),   # Labor Day
    date(2024, 11, 28), # Thanksgiving
    date(2025, 1, 9),   # National Day of Mourning (Carter)
    date(2025, 1, 20),  # MLK Day
    date(2025, 2, 17),  # Presidents Day
    date(2025, 4, 18),  # Good Friday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 9, 1),   # Labor Day
    date(2025, 11, 27), # Thanksgiving
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving
}


def _cache_path(name: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{name}.json"


def _load_cache(name: str) -> Optional[dict]:
    path = _cache_path(name)
    if path.exists():
        print(f"  [cache] 命中缓存: {path.name}")
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def _save_cache(name: str, data: dict) -> None:
    path = _cache_path(name)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"  [cache] 已写入缓存: {path.name}")


def _fetch_closes(symbol: str, period: str = "30d") -> pd.Series:
    """获取指定 symbol 的收盘价序列，已 dropna（yfinance 偶尔返回 NaN 行）。"""
    df = yf.Ticker(symbol).history(period=period)
    if df.empty:
        raise RuntimeError(f"yfinance 返回空数据: {symbol}")
    closes = df["Close"].dropna()
    if closes.empty:
        raise RuntimeError(f"yfinance 收盘价全为 NaN: {symbol}")
    return closes


def _pct_change(series: pd.Series, n: int) -> float:
    """计算最近 n 个交易日的累计涨跌幅（%）。NaN 值已在 _fetch_closes 中剔除。"""
    if len(series) < n + 1:
        raise RuntimeError(f"数据不足 {n+1} 条，无法计算 {n} 日涨跌幅")
    latest = float(series.iloc[-1])
    base   = float(series.iloc[-(n + 1)])
    if base == 0:
        raise RuntimeError(f"基准价为 0，无法计算涨跌幅")
    return round((latest - base) / base * 100, 2)


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def is_trading_day(ref: Optional[date] = None) -> bool:
    """
    判断指定日期是否为美股交易日（NYSE）。

    排除：周六、周日、固定节假日、浮动节假日。

    Args:
        ref: 待判断的日期，默认为今天

    Returns:
        True 表示交易日，False 表示休市
    """
    d = ref or date.today()

    if d.weekday() >= 5:  # 0=Mon … 6=Sun
        return False

    if (d.month, d.day) in _FIXED_HOLIDAYS:
        # 若节假日落在周末，NYSE 通常顺延至周一或提前至周五
        # 此处做简单判断，精确处理可引入 pandas_market_calendars
        return False

    if d in _FLOATING_HOLIDAYS:
        return False

    return True


def get_market_env() -> dict:
    """
    获取当前市场环境数据，判断是否适合做多。

    判断规则（以下任一触发则 risk_on = False）：
      - VIX > 25
      - SPY 最近3日累计跌幅 > 3%
      - QQQ 最近3日累计跌幅 > 3%

    SPY trend（基于20日均线）：
      - 收盘价 > MA20 → "up"
      - 收盘价 < MA20 → "down"
      - 否则 → "sideways"

    Returns:
        dict with keys: risk_on, vix, spy_trend,
                        spy_change_5d, nasdaq_change_5d, reason
    """
    print("[market_env] get_market_env")

    today_str = datetime.today().strftime("%Y-%m-%d")
    cache_key = f"market_env_{today_str}"
    cached = _load_cache(cache_key)
    if cached is not None:
        return cached

    # ── VIX ─────────────────────────────────────────────────────────────────
    print("  [fetch] VIX")
    vix_closes = _fetch_closes("^VIX", period="5d")
    vix = round(float(vix_closes.iloc[-1]), 2)

    # ── SPY ──────────────────────────────────────────────────────────────────
    print("  [fetch] SPY")
    spy_closes = _fetch_closes("SPY", period="60d")
    spy_change_3d = _pct_change(spy_closes, 3)
    spy_change_5d = _pct_change(spy_closes, 5)

    ma20 = spy_closes.iloc[-20:].dropna().mean()
    spy_latest = float(spy_closes.iloc[-1])
    if spy_latest > ma20 * 1.001:
        spy_trend = "up"
    elif spy_latest < ma20 * 0.999:
        spy_trend = "down"
    else:
        spy_trend = "sideways"

    # ── QQQ ──────────────────────────────────────────────────────────────────
    print("  [fetch] QQQ")
    qqq_closes = _fetch_closes("QQQ", period="30d")
    nasdaq_change_3d = _pct_change(qqq_closes, 3)
    nasdaq_change_5d = _pct_change(qqq_closes, 5)

    # ── risk_on 判断 ─────────────────────────────────────────────────────────
    reasons_off = []
    if vix > 25:
        reasons_off.append(f"VIX={vix} > 25")
    if spy_change_3d < -3:
        reasons_off.append(f"SPY 3日跌幅={spy_change_3d}% < -3%")
    if nasdaq_change_3d < -3:
        reasons_off.append(f"QQQ 3日跌幅={nasdaq_change_3d}% < -3%")

    risk_on = len(reasons_off) == 0
    if risk_on:
        reason = (
            f"VIX={vix} 正常，SPY 3日{spy_change_3d:+.2f}%，"
            f"QQQ 3日{nasdaq_change_3d:+.2f}%，趋势={spy_trend}"
        )
    else:
        reason = "风险信号触发：" + "；".join(reasons_off)

    result = {
        "risk_on": risk_on,
        "vix": vix,
        "spy_trend": spy_trend,
        "spy_change_5d": spy_change_5d,
        "nasdaq_change_5d": nasdaq_change_5d,
        "reason": reason,
    }

    print(f"  [info] market_env 完成: risk_on={risk_on}, vix={vix}, spy_trend={spy_trend}")
    _save_cache(cache_key, result)
    return result


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    today = date.today()

    print("=" * 50)
    print(f"今天是否交易日（{today}）")
    print("=" * 50)
    trading = is_trading_day()
    status = "✅ 是交易日" if trading else "🚫 非交易日（周末或节假日）"
    print(status)

    print()
    print("=" * 50)
    print("市场环境 get_market_env()")
    print("=" * 50)
    try:
        env = get_market_env()
        print()
        for k, v in env.items():
            print(f"  {k}: {v}")

        if not env["risk_on"]:
            print()
            print("⚠️  WARNING: 市场风险偏高，今日不建议做多！")
            print(f"   原因: {env['reason']}")
        else:
            print()
            print("✅ 市场环境良好，可以正常扫描信号。")
    except RuntimeError as e:
        print(f"❌ get_market_env 失败: {e}")
