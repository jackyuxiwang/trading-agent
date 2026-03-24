"""
trade_logger.py — 交易历史记录器

每笔交易（开仓/平仓/减仓）写入 data/portfolio/trade_history.csv
"""

import csv
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

TRADE_CSV = Path(__file__).parent.parent / "data" / "portfolio" / "trade_history.csv"

FIELDS = [
    "date", "ticker", "action", "shares", "price_usd",
    "price_hkd", "pnl_usd", "pnl_hkd", "pnl_pct", "reason",
    "holding_days", "signal_type", "entry_date",
]


def _ensure_csv():
    TRADE_CSV.parent.mkdir(parents=True, exist_ok=True)
    if not TRADE_CSV.exists():
        with open(TRADE_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()


def log_trade(trade: dict) -> None:
    """
    记录一笔交易到 trade_history.csv。

    trade dict 的字段与 FIELDS 对应，缺失字段自动填空。
    """
    _ensure_csv()

    row = {field: trade.get(field, "") for field in FIELDS}
    if not row["date"]:
        row["date"] = date.today().isoformat()

    with open(TRADE_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writerow(row)


def read_trades(days: int = None) -> list[dict]:
    """
    读取交易历史。

    Args:
        days: 只返回最近 N 天的记录，None 返回全部

    Returns:
        list of trade dicts
    """
    _ensure_csv()

    trades = []
    with open(TRADE_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trades.append(dict(row))

    if days is not None:
        cutoff = date.today().toordinal() - days
        trades = [
            t for t in trades
            if _parse_date(t.get("date", "")).toordinal() >= cutoff
        ]

    return trades


def _parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return date.today()
