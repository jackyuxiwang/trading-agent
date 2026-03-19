"""
report_formatter.py — 每日报告格式化模块

format_daily_report(signals, market_env, summary) -> str
生成适合 Telegram / 控制台输出的纯文本报告。
"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def format_daily_report(signals: list, market_env: dict, summary: dict) -> str:
    """
    生成每日交易信号报告。

    Args:
        signals:    signal_generator.generate() 的完整信号列表（含 BUY/WATCH/SKIP）
        market_env: market_env_client.get_market_env() 的结果
        summary:    扫描统计 dict（字段同 log_writer.SCAN_FIELDS）

    Returns:
        格式化的报告字符串
    """
    today      = datetime.today().strftime("%Y-%m-%d")
    vix        = market_env.get("vix")
    spy_trend  = market_env.get("spy_trend", "N/A")
    risk_on    = market_env.get("risk_on", True)
    mkt_reason = market_env.get("reason", "")

    risk_label = "✅ 做多" if risk_on else "⚠️ 观望"
    vix_str    = f"{vix:.1f}" if isinstance(vix, (int, float)) else "N/A"

    lines = []

    # ── 标题 ──────────────────────────────────────────────────────────────────
    lines.append(f"📊 每日交易信号报告 — {today}")
    lines.append("")

    # ── 大盘环境 ──────────────────────────────────────────────────────────────
    lines.append("🌍 大盘环境")
    lines.append(f"VIX: {vix_str} | SPY趋势: {spy_trend} | 风险状态: {risk_label}")
    if mkt_reason:
        lines.append(f"评估: {mkt_reason}")
    lines.append("")

    # ── 扫描统计 ──────────────────────────────────────────────────────────────
    total_mkt  = summary.get("total_market", "?")
    stage2_cnt = summary.get("stage2_count", "?")
    stage3_cnt = summary.get("stage3_count", "?")
    sig_cnt    = summary.get("buy_signals",  "?")

    lines.append("📈 扫描统计")
    lines.append(
        f"全市场扫描: {total_mkt}只 → "
        f"基本面筛选: {stage2_cnt}只 → "
        f"技术确认: {stage3_cnt}只 → "
        f"信号: {sig_cnt}只"
    )
    runtime = summary.get("runtime_minutes")
    if runtime is not None:
        lines.append(f"扫描耗时: {runtime:.1f} 分钟")
    lines.append("")

    # ── BUY 信号 ──────────────────────────────────────────────────────────────
    buy_signals   = [s for s in signals if str(s.get("action", "")).upper() == "BUY"]
    watch_signals = [s for s in signals if str(s.get("action", "")).upper() == "WATCH"]

    if buy_signals:
        lines.append(f"🔔 买入信号（共 {len(buy_signals)} 个）")
        lines.append("")
        for s in buy_signals:
            ticker     = s.get("ticker", "")
            stype      = s.get("signal_type", "")
            conf       = s.get("confidence", "?")
            company    = s.get("company", "")
            sector     = s.get("sector", "")
            entry      = s.get("entry_zone", "N/A")
            stop       = s.get("stop_loss", "N/A")
            t1         = s.get("target_1", "N/A")
            t2         = s.get("target_2", "N/A")
            reason     = s.get("reason", "")
            risk_warn  = s.get("risk_warning", "")

            lines.append(f"🔥 {ticker} — {stype} 信号（置信度 {conf}/10）")
            if company or sector:
                lines.append(f"公司：{company}（{sector}）")
            lines.append(f"入场区间：{entry}")
            lines.append(f"止损：{stop} | 目标一：{t1} | 目标二：{t2}")
            if reason:
                lines.append(f"逻辑：{reason}")
            if risk_warn:
                lines.append(f"风险：{risk_warn}")
            lines.append("---")
        lines.append("")

    else:
        lines.append("今日无符合条件的买入信号，继续观察以下关注股：")
        lines.append("")
        if watch_signals:
            for s in watch_signals[:5]:
                ticker = s.get("ticker", "")
                stype  = s.get("signal_type", "")
                entry  = s.get("entry_zone", "N/A")
                lines.append(f"  👀 {ticker} ({stype}) — 关注入场区间: {entry}")
        else:
            lines.append("  （暂无关注股）")
        lines.append("")

    return "\n".join(lines)


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mock_signals = [
        {
            "ticker": "DAWN", "company": "Dawn Acquisition", "sector": "Healthcare",
            "signal_type": "EP", "action": "BUY", "confidence": 8,
            "entry_zone": "50.00–55.00", "stop_loss": "48.51",
            "target_1": "$63.00", "target_2": "$75.00",
            "reason": "EP突破后量价配合良好，基本面强劲支撑。",
            "risk_warning": "市值偏小，流动性风险。",
        },
        {
            "ticker": "BSY", "company": "Bentley Systems", "sector": "Technology",
            "signal_type": "VCP", "action": "WATCH", "confidence": 6,
            "entry_zone": "41.58–45.00", "stop_loss": "38.00",
            "target_1": "$52.00", "target_2": "$60.00",
            "reason": "VCP整理形态良好，等待突破确认。",
            "risk_warning": "尚未放量突破。",
        },
    ]
    mock_market = {
        "risk_on": True, "vix": 18.5,
        "spy_trend": "above_ma20", "reason": "市场正常，风险偏好良好",
    }
    mock_summary = {
        "total_market": 11848, "stage2_count": 264,
        "stage3_count": 43, "buy_signals": 1,
        "runtime_minutes": 20.3,
    }

    report = format_daily_report(mock_signals, mock_market, mock_summary)
    print(report)
