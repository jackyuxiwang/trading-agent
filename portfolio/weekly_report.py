"""
weekly_report.py — 每周复盘报告生成器

读取最近7天已平仓交易，计算胜率/盈亏比，生成格式化报告。
"""

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def generate_weekly_report() -> str:
    """
    生成最近7天的交易复盘报告。

    Returns:
        格式化报告字符串
    """
    from portfolio.trade_logger import read_trades
    from portfolio.virtual_account import get_account_summary

    today    = date.today()
    week_ago = today - timedelta(days=7)
    date_range = f"{week_ago.isoformat()} ~ {today.isoformat()}"

    # 最近7天已平仓的交易（排除减仓）
    all_trades = read_trades(days=7)
    closed_trades = [
        t for t in all_trades
        if t.get("action") in ("CLOSE",)
        and t.get("pnl_pct", "") != ""
    ]

    # 账户概况
    summary = get_account_summary()

    lines = []
    lines.append(f"📊 每周复盘报告 — {date_range}")
    lines.append("")

    # ── 账户概况 ──────────────────────────────────────────────────────────────
    lines.append("💰 账户概况")
    week_pnl_hkd = sum(_f(t.get("pnl_hkd")) for t in closed_trades)
    week_pnl_pct = week_pnl_hkd / 100_000 * 100   # 相对初始资金
    total_pnl    = summary["total_pnl_hkd"]
    total_pnl_pct = summary["total_pnl_pct"]
    total_assets  = summary["total_assets_hkd"]

    lines.append(f"本周盈亏: {week_pnl_hkd:+,.0f}港币 ({week_pnl_pct:+.2f}%)")
    lines.append(f"累计盈亏: {total_pnl:+,.0f}港币 ({total_pnl_pct:+.2f}%)")
    lines.append(f"当前总资产: {total_assets:,.0f}港币")
    lines.append("")

    # ── 交易统计 ──────────────────────────────────────────────────────────────
    lines.append("📈 交易统计")
    total_trades = len(closed_trades)
    if total_trades == 0:
        lines.append("本周无已平仓交易")
        lines.append("")
    else:
        winners = [t for t in closed_trades if _f(t.get("pnl_pct")) > 0]
        losers  = [t for t in closed_trades if _f(t.get("pnl_pct")) <= 0]
        win_rate = len(winners) / total_trades * 100

        avg_win  = (sum(_f(t.get("pnl_pct")) for t in winners) / len(winners)) if winners else 0
        avg_loss = (sum(_f(t.get("pnl_pct")) for t in losers)  / len(losers))  if losers  else 0
        rr_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

        lines.append(f"本周交易笔数: {total_trades}")
        lines.append(f"胜率: {win_rate:.0f}%（{len(winners)}胜 {len(losers)}负）")
        lines.append(f"平均盈利: +{avg_win:.1f}%")
        lines.append(f"平均亏损: {avg_loss:.1f}%")
        lines.append(f"盈亏比: {rr_ratio:.1f}:1")
        lines.append("")

        # ── 最佳/最差 ─────────────────────────────────────────────────────────
        sorted_by_pct = sorted(closed_trades, key=lambda t: _f(t.get("pnl_pct")), reverse=True)
        best  = sorted_by_pct[0]
        worst = sorted_by_pct[-1]

        lines.append("🏆 本周最佳")
        lines.append(f"{best['ticker']}: {_f(best.get('pnl_pct')):+.1f}% ({best.get('signal_type', 'N/A')})")
        lines.append("")

        lines.append("💀 本周最差")
        lines.append(f"{worst['ticker']}: {_f(worst.get('pnl_pct')):+.1f}% ({worst.get('reason', '止损')})")
        lines.append("")

    # ── 持仓明细 ──────────────────────────────────────────────────────────────
    lines.append("📋 持仓明细")
    open_positions = summary.get("positions", [])
    if not open_positions:
        lines.append("  当前无持仓")
    else:
        for p in open_positions:
            pnl_sign = "+" if p["pnl_pct"] >= 0 else ""
            stop_note = " [止损已移至成本]" if p.get("partial_sold") else ""
            lines.append(
                f"  {p['ticker']:<6} {p['shares']:>4}股"
                f"  入场${p['entry_price']:.2f} → 现价${p['current_price']:.2f}"
                f"  浮盈 {p['unrealized_pnl_hkd']:>+,.0f}港币 ({pnl_sign}{p['pnl_pct']:.1f}%)"
                f"  持仓{p['holding_days']}天{stop_note}"
            )
    lines.append("")

    # ── 风险提示 ──────────────────────────────────────────────────────────────
    warnings = _risk_warnings(closed_trades, week_pnl_pct)
    if warnings:
        lines.append("⚠️ 风险提示")
        for w in warnings:
            lines.append(f"- {w}")
        lines.append("")

    return "\n".join(lines)


def _risk_warnings(closed_trades: list, week_pnl_pct: float) -> list:
    warnings = []

    if week_pnl_pct < -5:
        warnings.append("本周亏损较大，建议减少仓位")

    # 检查最近3笔是否连续止损
    recent = sorted(closed_trades, key=lambda t: t.get("date", ""), reverse=True)[:3]
    if len(recent) == 3 and all(t.get("reason") == "stop_loss" for t in recent):
        warnings.append("连续止损，建议暂停交易1天冷静")

    return warnings


def _f(val) -> float:
    """安全转 float。"""
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    report = generate_weekly_report()
    print(report)
