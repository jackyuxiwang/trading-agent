"""
virtual_account.py — 虚拟账户管理

账户数据持久化至 data/portfolio/virtual_account.json
汇率固定 USD/HKD = 7.8
"""

import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

USD_TO_HKD      = 7.8
INITIAL_CASH    = 100_000.0          # 港币
STOP_LOSS_PCT   = 0.93               # 入场价 * 0.93
TARGET_1_PCT    = 1.20
TARGET_2_PCT    = 1.35
ACCOUNT_JSON    = Path(__file__).parent.parent / "data" / "portfolio" / "virtual_account.json"


# ── 持久化 ────────────────────────────────────────────────────────────────────

def _load() -> dict:
    ACCOUNT_JSON.parent.mkdir(parents=True, exist_ok=True)
    if ACCOUNT_JSON.exists():
        with open(ACCOUNT_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "cash_hkd":   INITIAL_CASH,
        "positions":  [],            # open 持仓
        "closed":     [],            # 已平仓记录
        "created_at": date.today().isoformat(),
    }


def _save(account: dict) -> None:
    ACCOUNT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(ACCOUNT_JSON, "w", encoding="utf-8") as f:
        json.dump(account, f, ensure_ascii=False, indent=2)


# ── 公开方法 ──────────────────────────────────────────────────────────────────

def open_position(signal: dict, shares: int, entry_price: float) -> dict:
    """
    开仓。

    Args:
        signal:       信号 dict（含 ticker、signal_type 等）
        shares:       买入股数
        entry_price:  实际入场价（美元）

    Returns:
        新建的持仓 dict
    """
    account    = _load()
    ticker     = signal.get("ticker", "")
    cost_usd   = shares * entry_price
    cost_hkd   = cost_usd * USD_TO_HKD

    if cost_hkd > account["cash_hkd"]:
        raise ValueError(
            f"资金不足：需要 {cost_hkd:,.0f} 港币，账户现金 {account['cash_hkd']:,.0f} 港币"
        )

    position = {
        "ticker":          ticker,
        "company":         signal.get("company", ""),
        "signal_type":     signal.get("signal_type", ""),
        "entry_date":      date.today().isoformat(),
        "entry_price":     entry_price,
        "shares":          shares,
        "stop_loss":       round(entry_price * STOP_LOSS_PCT, 2),
        "target_1":        round(entry_price * TARGET_1_PCT, 2),
        "target_2":        round(entry_price * TARGET_2_PCT, 2),
        "current_price":   entry_price,
        "unrealized_pnl_usd": 0.0,
        "unrealized_pnl_hkd": 0.0,
        "stop_moved_to_cost": False,   # 是否已移止损至成本
        "partial_sold":    False,       # 是否已减仓1/3
        "status":          "open",
    }

    account["positions"].append(position)
    account["cash_hkd"] -= cost_hkd
    _save(account)

    print(f"  [virtual_account] 开仓 {ticker}  {shares}股 @ ${entry_price:.2f}"
          f"  成本 {cost_hkd:,.0f} 港币  止损 ${position['stop_loss']:.2f}")
    return position


def update_positions() -> list:
    """
    用 yfinance 获取最新收盘价，更新所有 open 持仓的浮盈浮亏。
    自动处理止损平仓、目标一减仓。

    Returns:
        更新后的 open 持仓列表
    """
    account = _load()
    opens   = [p for p in account["positions"] if p["status"] == "open"]

    if not opens:
        print("  [virtual_account] 无持仓需要更新")
        return []

    tickers = [p["ticker"] for p in opens]
    prices  = _fetch_prices(tickers)

    from portfolio.trade_logger import log_trade

    for pos in opens:
        ticker  = pos["ticker"]
        price   = prices.get(ticker)
        if price is None:
            print(f"  [virtual_account] ⚠️  {ticker} 无法获取价格，跳过")
            continue

        pos["current_price"]      = price
        pos["unrealized_pnl_usd"] = round((price - pos["entry_price"]) * pos["shares"], 2)
        pos["unrealized_pnl_hkd"] = round(pos["unrealized_pnl_usd"] * USD_TO_HKD, 2)

        # ── 止损触发 ──────────────────────────────────────────────────────────
        if price <= pos["stop_loss"]:
            _close_pos(account, pos, price, "stop_loss")
            log_trade(_make_trade_log(pos, price, "stop_loss"))
            print(f"  [virtual_account] 🔴 止损平仓 {ticker} @ ${price:.2f}"
                  f"  PnL {pos['unrealized_pnl_hkd']:+,.0f} 港币")
            continue

        # ── 目标一触发：减仓 1/3，移止损至成本 ──────────────────────────────
        if price >= pos["target_1"] and not pos["partial_sold"]:
            sell_shares = max(1, pos["shares"] // 3)
            pnl_usd     = (price - pos["entry_price"]) * sell_shares
            pnl_hkd     = pnl_usd * USD_TO_HKD
            account["cash_hkd"] += sell_shares * price * USD_TO_HKD
            pos["shares"]        -= sell_shares
            pos["partial_sold"]   = True
            pos["stop_loss"]      = pos["entry_price"]   # 移止损至成本
            pos["stop_moved_to_cost"] = True
            log_trade({
                **_make_trade_log(pos, price, "target_1"),
                "shares":  sell_shares,
                "pnl_usd": round(pnl_usd, 2),
                "pnl_hkd": round(pnl_hkd, 2),
                "pnl_pct": round((price / pos["entry_price"] - 1) * 100, 2),
                "action":  "PARTIAL_SELL",
            })
            print(f"  [virtual_account] 🟡 目标一减仓 {ticker}  卖出{sell_shares}股 @ ${price:.2f}"
                  f"  止损移至成本 ${pos['entry_price']:.2f}")

    _save(account)
    return [p for p in account["positions"] if p["status"] == "open"]


def close_position(ticker: str, reason: str = "manual") -> Optional[dict]:
    """
    手动平仓。

    Args:
        ticker: 股票代码
        reason: 平仓原因

    Returns:
        平仓记录 dict，或 None（找不到持仓）
    """
    account = _load()
    pos     = next((p for p in account["positions"]
                    if p["ticker"] == ticker and p["status"] == "open"), None)
    if pos is None:
        print(f"  [virtual_account] ⚠️  未找到 {ticker} 的 open 持仓")
        return None

    prices = _fetch_prices([ticker])
    price  = prices.get(ticker, pos["current_price"])

    from portfolio.trade_logger import log_trade
    _close_pos(account, pos, price, reason)
    log_trade(_make_trade_log(pos, price, reason))
    _save(account)

    pnl = (price - pos["entry_price"]) * pos["shares"] * USD_TO_HKD
    print(f"  [virtual_account] 平仓 {ticker} @ ${price:.2f}  PnL {pnl:+,.0f} 港币  原因: {reason}")
    return pos


def get_account_summary() -> dict:
    """
    返回账户概况。
    """
    account    = _load()
    cash_hkd   = account["cash_hkd"]
    opens      = [p for p in account["positions"] if p["status"] == "open"]

    holding_value_usd = sum(p["current_price"] * p["shares"] for p in opens)
    holding_value_hkd = holding_value_usd * USD_TO_HKD
    total_assets_hkd  = cash_hkd + holding_value_hkd

    total_pnl_hkd     = total_assets_hkd - INITIAL_CASH
    total_pnl_pct     = total_pnl_hkd / INITIAL_CASH * 100

    positions_detail = []
    for p in opens:
        entry_days = (date.today() - datetime.strptime(p["entry_date"], "%Y-%m-%d").date()).days
        pnl_pct    = (p["current_price"] / p["entry_price"] - 1) * 100 if p["entry_price"] else 0
        positions_detail.append({
            "ticker":          p["ticker"],
            "company":         p.get("company", ""),
            "signal_type":     p.get("signal_type", ""),
            "entry_date":      p["entry_date"],
            "entry_price":     p["entry_price"],
            "current_price":   p["current_price"],
            "shares":          p["shares"],
            "stop_loss":       p["stop_loss"],
            "target_1":        p["target_1"],
            "target_2":        p["target_2"],
            "unrealized_pnl_hkd": p["unrealized_pnl_hkd"],
            "pnl_pct":         round(pnl_pct, 2),
            "holding_days":    entry_days,
            "partial_sold":    p.get("partial_sold", False),
        })

    return {
        "total_assets_hkd":   round(total_assets_hkd, 2),
        "cash_hkd":           round(cash_hkd, 2),
        "holding_value_hkd":  round(holding_value_hkd, 2),
        "total_pnl_hkd":      round(total_pnl_hkd, 2),
        "total_pnl_pct":      round(total_pnl_pct, 2),
        "positions":          positions_detail,
        "open_count":         len(opens),
    }


# ── 内部工具 ──────────────────────────────────────────────────────────────────

def _fetch_prices(tickers: list) -> dict:
    """用 yfinance 批量获取最新收盘价。"""
    prices = {}
    try:
        import yfinance as yf
        data = yf.download(tickers, period="2d", auto_adjust=True, progress=False)
        close = data["Close"] if "Close" in data else data
        if len(tickers) == 1:
            ticker = tickers[0]
            val = close.iloc[-1]
            if hasattr(val, "item"):
                val = val.item()
            prices[ticker] = float(val)
        else:
            for ticker in tickers:
                if ticker in close.columns:
                    val = close[ticker].dropna().iloc[-1]
                    prices[ticker] = float(val)
    except Exception as e:
        print(f"  [virtual_account] ⚠️  yfinance 获取价格失败: {e}")
    return prices


def _close_pos(account: dict, pos: dict, price: float, reason: str) -> None:
    """内部平仓操作（修改 account dict，不写文件）。"""
    proceeds_hkd = pos["shares"] * price * USD_TO_HKD
    account["cash_hkd"] += proceeds_hkd
    pos["status"]        = "closed"
    pos["close_price"]   = price
    pos["close_date"]    = date.today().isoformat()
    pos["close_reason"]  = reason
    pos["realized_pnl_usd"] = round((price - pos["entry_price"]) * pos["shares"], 2)
    pos["realized_pnl_hkd"] = round(pos["realized_pnl_usd"] * USD_TO_HKD, 2)
    account["closed"].append(pos)
    account["positions"] = [p for p in account["positions"] if p is not pos]


def _make_trade_log(pos: dict, price: float, reason: str) -> dict:
    entry_date   = pos.get("entry_date", "")
    holding_days = 0
    if entry_date:
        try:
            holding_days = (date.today() - datetime.strptime(entry_date, "%Y-%m-%d").date()).days
        except Exception:
            pass
    pnl_usd = (price - pos["entry_price"]) * pos["shares"]
    pnl_pct = (price / pos["entry_price"] - 1) * 100 if pos["entry_price"] else 0
    return {
        "date":         date.today().isoformat(),
        "ticker":       pos["ticker"],
        "action":       "CLOSE",
        "shares":       pos["shares"],
        "price_usd":    price,
        "price_hkd":    round(price * USD_TO_HKD, 2),
        "pnl_usd":      round(pnl_usd, 2),
        "pnl_hkd":      round(pnl_usd * USD_TO_HKD, 2),
        "pnl_pct":      round(pnl_pct, 2),
        "reason":       reason,
        "holding_days": holding_days,
        "signal_type":  pos.get("signal_type", ""),
        "entry_date":   entry_date,
    }


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("虚拟账户测试")
    print("=" * 60)

    # 模拟开仓
    open_position(
        signal={"ticker": "AAOI", "company": "Applied Optoelectronics", "signal_type": "EP"},
        shares=100,
        entry_price=86.0,
    )
    open_position(
        signal={"ticker": "LITE", "company": "Lumentum Holdings", "signal_type": "VCP"},
        shares=50,
        entry_price=62.0,
    )

    print("\n更新持仓价格...")
    update_positions()

    print("\n账户概况：")
    summary = get_account_summary()
    print(f"  总资产:   {summary['total_assets_hkd']:>12,.0f} 港币")
    print(f"  现金:     {summary['cash_hkd']:>12,.0f} 港币")
    print(f"  持仓市值: {summary['holding_value_hkd']:>12,.0f} 港币")
    print(f"  总盈亏:   {summary['total_pnl_hkd']:>+12,.0f} 港币  ({summary['total_pnl_pct']:+.2f}%)")
    print(f"  持仓数:   {summary['open_count']}")
    print()
    for p in summary["positions"]:
        print(f"  {p['ticker']:<6} {p['shares']:>4}股  入场${p['entry_price']:.2f}"
              f"  现价${p['current_price']:.2f}"
              f"  浮盈 {p['unrealized_pnl_hkd']:>+8,.0f}港币"
              f"  ({p['pnl_pct']:+.1f}%)"
              f"  持仓{p['holding_days']}天")
