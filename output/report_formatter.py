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

    # ── 大盘风险警告（risk_on=False 时置顶）─────────────────────────────────
    if not risk_on:
        lines.append("🔴 大盘风险警告")
        lines.append(f"VIX: {vix_str} | 原因: {mkt_reason}")
        lines.append("建议：降低仓位，严格止损，以下信号仅供参考")
        lines.append("─" * 45)
        lines.append("")

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
    buy_signals   = [s for s in signals if str(s.get("action", "")).upper() in ("BUY", "BUY_RISKY")]
    watch_signals = [s for s in signals if str(s.get("action", "")).upper() == "WATCH"]

    total_mkt  = summary.get("total_market", "?")
    stage2_cnt = summary.get("stage2_count", "?")
    stage3_cnt = summary.get("stage3_count", "?")

    lines.append("📈 扫描统计")
    lines.append(
        f"全市场扫描: {total_mkt}只 → "
        f"基本面筛选: {stage2_cnt}只 → "
        f"技术确认: {stage3_cnt}只 → "
        f"BUY信号: {len(buy_signals)}只 | WATCH: {len(watch_signals)}只"
    )
    runtime = summary.get("runtime_minutes")
    if runtime is not None:
        lines.append(f"扫描耗时: {runtime:.1f} 分钟")
    lines.append("")

    # ── BUY 信号 ──────────────────────────────────────────────────────────────
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

            if stype == "VCP_CHEAT_ENTRY":
                orig_breakout    = s.get("original_breakout", "N/A")
                stop_loss_pct    = s.get("stop_loss_pct", "")
                stop_pct_str     = f"（止损幅度 {stop_loss_pct:.1f}%）" if isinstance(stop_loss_pct, (int, float)) else ""
                cur_price        = s.get("current_price")
                cur_price_str    = f"${cur_price:.2f}" if isinstance(cur_price, (int, float)) else "N/A"
                distance         = s.get("distance_to_cheat")
                distance_str     = f"{distance:.1f}%" if isinstance(distance, (int, float)) else "N/A"
                slope_5d         = s.get("slope_5d")
                slope_str        = f"{slope_5d:+.1f}%" if isinstance(slope_5d, (int, float)) else "N/A"
                feasibility      = s.get("cheat_entry_feasibility", "N/A")
                ce_score         = s.get("cheat_entry_score", "N/A")
                vol_trend        = s.get("vol_trend", "N/A")
                ma_support       = s.get("ma_support", "N/A")

                lines.append(f"🎯 {ticker} — VCP 低吸策略（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"📍 当前价：{cur_price_str} | 距低吸区：{distance_str}")
                lines.append(f"⏳ 低吸买入区：{entry}")
                lines.append(f"📊 低吸可行性：{feasibility}（{ce_score}/100）")
                lines.append(f"近5日趋势：{slope_str} | 量能：{vol_trend}")
                lines.append(f"均线支撑：{ma_support}")
                lines.append(f"止损：{stop}{stop_pct_str}")
                lines.append(f"突破目标：{orig_breakout} | 目标一：{t1} | 目标二：{t2}")
                if reason:
                    lines.append(f"💬 {reason}")
                if risk_warn:
                    lines.append(f"⚠️ {risk_warn}")
            elif stype == "BOTTOM_FINDER":
                bf_score     = s.get("score", "?")
                decline_pct  = s.get("decline_pct")
                base_days    = s.get("base_days", "?")
                higher_lows  = s.get("higher_lows", "?")
                vol_contract = s.get("vol_contract_ratio")
                breakout_vol = s.get("breakout_vol_ratio")
                rr           = s.get("risk_reward", "N/A")

                decline_str  = f"{decline_pct:.1f}%" if isinstance(decline_pct, (int, float)) else "N/A"
                vol_cont_str = f"{vol_contract:.0f}%" if isinstance(vol_contract, (int, float)) else "N/A"
                brk_vol_str  = f"{breakout_vol:.1f}x" if isinstance(breakout_vol, (int, float)) else "N/A"
                rr_str       = f"{rr:.1f}:1" if isinstance(rr, (int, float)) else str(rr)

                lines.append(f"🏔 {ticker} — 底部反轉（評分 {bf_score}/100）（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"入場區間：{entry}")
                lines.append(f"止損：{stop} | 目標一：{t1} | 目標二：{t2} | 風報比：{rr_str}")
                lines.append(f"型態：下跌{decline_str} | 築底{base_days}天 | "
                              f"{higher_lows}個HL | 量縮{vol_cont_str} | 突破量{brk_vol_str}")
                if reason:
                    lines.append(f"逻辑：{reason}")
                if risk_warn:
                    lines.append(f"风险：{risk_warn}")
            elif stype == "POST_EP_TIGHT":
                score        = s.get("score", "?")
                ep_gap       = s.get("ep_gap_pct")
                consol_days  = s.get("consol_days", "?")
                amp_ratio    = s.get("amp_ratio")
                vol_ratio_ep = s.get("vol_ratio_to_ep")
                gap_ok       = "是" if s.get("gap_maintained") else "否"

                ep_gap_str   = f"+{ep_gap:.1f}%" if isinstance(ep_gap, (int, float)) else "N/A"
                amp_str      = f"{amp_ratio:.2f}x" if isinstance(amp_ratio, (int, float)) else "N/A"
                vol_ep_str   = f"{vol_ratio_ep:.2f}x" if isinstance(vol_ratio_ep, (int, float)) else "N/A"

                lines.append(f"⚡ {ticker} — EP後盤整（評分 {score}/100）（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"入場區間：{entry}")
                lines.append(f"止損：{stop} | 目標一：{t1} | 目標二：{t2}")
                lines.append(f"EP缺口：{ep_gap_str} | 盤整{consol_days}天 | 振幅{amp_str} | "
                              f"量比{vol_ep_str} | 缺口保持：{gap_ok}")
                if reason:
                    lines.append(f"逻辑：{reason}")
                if risk_warn:
                    lines.append(f"风险：{risk_warn}")
            elif stype == "CUP_HANDLE":
                score        = s.get("score", "?")
                cup_depth    = s.get("cup_depth_pct")
                cup_length   = s.get("cup_length", "?")
                handle_days  = s.get("handle_length", "?")
                right_rec    = s.get("right_recovery_pct")
                brk_vol      = s.get("breakout_vol_ratio")

                cup_depth_str = f"{cup_depth:.1f}%" if isinstance(cup_depth, (int, float)) else "N/A"
                right_rec_str = f"{right_rec:.1f}%" if isinstance(right_rec, (int, float)) else "N/A"
                brk_vol_str   = f"{brk_vol:.1f}x" if isinstance(brk_vol, (int, float)) else "N/A"

                lines.append(f"🏆 {ticker} — 杯柄型態（評分 {score}/100）（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"入場區間：{entry}")
                lines.append(f"止損：{stop} | 目標一：{t1} | 目標二：{t2}")
                lines.append(f"杯深{cup_depth_str} | 杯寬{cup_length}天 | 柄{handle_days}天 | "
                              f"右側回升{right_rec_str} | 突破量{brk_vol_str}")
                if reason:
                    lines.append(f"逻辑：{reason}")
                if risk_warn:
                    lines.append(f"风险：{risk_warn}")
            elif stype == "MEAN_REVERSION":
                score        = s.get("score", "?")
                rsi          = s.get("rsi")
                ma50_dev     = s.get("ma50_dev_pct")
                bounce_type  = s.get("bounce_type", "N/A")
                oversold_cnt = s.get("oversold_count", "?")
                rr           = s.get("risk_reward")

                rsi_str      = f"{rsi:.1f}" if isinstance(rsi, (int, float)) else "N/A"
                ma50_str     = f"{ma50_dev:.1f}%" if isinstance(ma50_dev, (int, float)) else "N/A"
                rr_str       = f"{rr:.1f}:1" if isinstance(rr, (int, float)) else "N/A"

                lines.append(f"🔄 {ticker} — 均值回歸（評分 {score}/100）（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"入場區間：{entry}")
                lines.append(f"止損：{stop} | 目標（MA50）：{t1} | 風報比：{rr_str}")
                lines.append(f"RSI：{rsi_str} | MA50偏離：{ma50_str} | 超賣信號：{oversold_cnt}/5 | 反彈：{bounce_type}")
                if reason:
                    lines.append(f"逻辑：{reason}")
                if risk_warn:
                    lines.append(f"风险：{risk_warn}")
            else:
                lines.append(f"🔥 {ticker} — {stype} 信号（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"入场区间：{entry}")
                lines.append(f"止损：{stop} | 目标一：{t1} | 目标二：{t2}")
                if reason:
                    lines.append(f"逻辑：{reason}")
                if risk_warn:
                    lines.append(f"风险：{risk_warn}")

            # 仓位建议
            rec_shares = s.get("recommended_shares")
            pos_hkd    = s.get("position_size_hkd")
            pos_pct    = s.get("position_pct")
            max_loss   = s.get("max_loss_hkd")
            rr         = s.get("reward_risk_ratio")
            if rec_shares and pos_hkd:
                lines.append(f"💼 仓位建议（基于10万港币，1%风险）")
                pos_pct_str = f"（占{pos_pct:.1f}%）" if pos_pct else ""
                rr_str      = f" | 盈亏比: {rr:.1f}:1" if rr else ""
                loss_str    = f" | 最大亏损: {max_loss:,.0f}港币" if max_loss else ""
                lines.append(
                    f"建议买入: {rec_shares}股"
                    f" | 仓位: {pos_hkd:,.0f}港币{pos_pct_str}"
                    f"{loss_str}{rr_str}"
                )
            lines.append("---")
        lines.append("")

    # ── WATCH 信号 ────────────────────────────────────────────────────────────
    if watch_signals:
        if not buy_signals:
            lines.append("今日无买入信号，以下为关注标的：")
            lines.append("")
        else:
            lines.append(f"👀 关注标的（共 {len(watch_signals)} 个）")
            lines.append("")

        for s in watch_signals:
            ticker    = s.get("ticker", "")
            stype     = s.get("signal_type", "")
            conf      = s.get("confidence", "?")
            company   = s.get("company", "")
            sector    = s.get("sector", "")
            entry     = s.get("entry_zone", "N/A")
            stop      = s.get("stop_loss", "N/A")
            t1        = s.get("target_1", "N/A")
            reason    = s.get("reason", "")

            if stype == "VCP_CHEAT_ENTRY":
                orig_breakout = s.get("original_breakout", "N/A")
                stop_loss_pct = s.get("stop_loss_pct", "")
                stop_pct_str  = f"（止损幅度 {stop_loss_pct:.1f}%）" if isinstance(stop_loss_pct, (int, float)) else ""
                cur_price     = s.get("current_price")
                cur_price_str = f"${cur_price:.2f}" if isinstance(cur_price, (int, float)) else "N/A"
                distance      = s.get("distance_to_cheat")
                distance_str  = f"{distance:.1f}%" if isinstance(distance, (int, float)) else "N/A"
                slope_5d      = s.get("slope_5d")
                slope_str     = f"{slope_5d:+.1f}%" if isinstance(slope_5d, (int, float)) else "N/A"
                feasibility   = s.get("cheat_entry_feasibility", "N/A")
                ce_score      = s.get("cheat_entry_score", "N/A")
                vol_trend     = s.get("vol_trend", "N/A")
                ma_support    = s.get("ma_support", "N/A")

                lines.append(f"🎯 {ticker} — VCP 低吸关注（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"📍 当前价：{cur_price_str} | 距低吸区：{distance_str}")
                lines.append(f"⏳ 低吸买入区：{entry}")
                lines.append(f"📊 低吸可行性：{feasibility}（{ce_score}/100）")
                lines.append(f"近5日趋势：{slope_str} | 量能：{vol_trend}")
                lines.append(f"均线支撑：{ma_support}")
                lines.append(f"止损参考：{stop}{stop_pct_str}")
                lines.append(f"突破目标：{orig_breakout} | 目标一：{t1}")
                if reason:
                    lines.append(f"💬 {reason}")
            elif stype == "BOTTOM_FINDER":
                bf_score     = s.get("score", "?")
                decline_pct  = s.get("decline_pct")
                base_days    = s.get("base_days", "?")
                higher_lows  = s.get("higher_lows", "?")
                vol_contract = s.get("vol_contract_ratio")
                breakout_vol = s.get("breakout_vol_ratio")
                rr           = s.get("risk_reward", "N/A")

                decline_str  = f"{decline_pct:.1f}%" if isinstance(decline_pct, (int, float)) else "N/A"
                vol_cont_str = f"{vol_contract:.0f}%" if isinstance(vol_contract, (int, float)) else "N/A"
                brk_vol_str  = f"{breakout_vol:.1f}x" if isinstance(breakout_vol, (int, float)) else "N/A"
                rr_str       = f"{rr:.1f}:1" if isinstance(rr, (int, float)) else str(rr)

                lines.append(f"🏔 {ticker} — 底部反轉關注（評分 {bf_score}/100）（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"關注區間：{entry}")
                lines.append(f"止損參考：{stop} | 目標一：{t1} | 風報比：{rr_str}")
                lines.append(f"型態：下跌{decline_str} | 築底{base_days}天 | "
                              f"{higher_lows}個HL | 量縮{vol_cont_str} | 突破量{brk_vol_str}")
                if reason:
                    lines.append(f"逻辑：{reason}")
            elif stype == "POST_EP_TIGHT":
                score        = s.get("score", "?")
                ep_gap       = s.get("ep_gap_pct")
                consol_days  = s.get("consol_days", "?")
                amp_ratio    = s.get("amp_ratio")
                vol_ratio_ep = s.get("vol_ratio_to_ep")
                gap_ok       = "是" if s.get("gap_maintained") else "否"

                ep_gap_str   = f"+{ep_gap:.1f}%" if isinstance(ep_gap, (int, float)) else "N/A"
                amp_str      = f"{amp_ratio:.2f}x" if isinstance(amp_ratio, (int, float)) else "N/A"
                vol_ep_str   = f"{vol_ratio_ep:.2f}x" if isinstance(vol_ratio_ep, (int, float)) else "N/A"

                lines.append(f"⚡ {ticker} — EP後盤整關注（評分 {score}/100）（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"關注區間：{entry}")
                lines.append(f"止損參考：{stop} | 目標一：{t1}")
                lines.append(f"EP缺口：{ep_gap_str} | 盤整{consol_days}天 | 振幅{amp_str} | "
                              f"量比{vol_ep_str} | 缺口保持：{gap_ok}")
                if reason:
                    lines.append(f"逻辑：{reason}")
            elif stype == "CUP_HANDLE":
                score        = s.get("score", "?")
                cup_depth    = s.get("cup_depth_pct")
                cup_length   = s.get("cup_length", "?")
                handle_days  = s.get("handle_length", "?")
                right_rec    = s.get("right_recovery_pct")
                brk_vol      = s.get("breakout_vol_ratio")

                cup_depth_str = f"{cup_depth:.1f}%" if isinstance(cup_depth, (int, float)) else "N/A"
                right_rec_str = f"{right_rec:.1f}%" if isinstance(right_rec, (int, float)) else "N/A"
                brk_vol_str   = f"{brk_vol:.1f}x" if isinstance(brk_vol, (int, float)) else "N/A"

                lines.append(f"🏆 {ticker} — 杯柄型態關注（評分 {score}/100）（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"關注區間：{entry}")
                lines.append(f"止損參考：{stop} | 目標一：{t1}")
                lines.append(f"杯深{cup_depth_str} | 杯寬{cup_length}天 | 柄{handle_days}天 | "
                              f"右側回升{right_rec_str} | 突破量{brk_vol_str}")
                if reason:
                    lines.append(f"逻辑：{reason}")
            elif stype == "MEAN_REVERSION":
                score        = s.get("score", "?")
                rsi          = s.get("rsi")
                ma50_dev     = s.get("ma50_dev_pct")
                bounce_type  = s.get("bounce_type", "N/A")
                oversold_cnt = s.get("oversold_count", "?")
                rr           = s.get("risk_reward")

                rsi_str      = f"{rsi:.1f}" if isinstance(rsi, (int, float)) else "N/A"
                ma50_str     = f"{ma50_dev:.1f}%" if isinstance(ma50_dev, (int, float)) else "N/A"
                rr_str       = f"{rr:.1f}:1" if isinstance(rr, (int, float)) else "N/A"

                lines.append(f"🔄 {ticker} — 均值回歸關注（評分 {score}/100）（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"關注區間：{entry}")
                lines.append(f"止損參考：{stop} | 目標（MA50）：{t1} | 風報比：{rr_str}")
                lines.append(f"RSI：{rsi_str} | MA50偏離：{ma50_str} | 超賣信號：{oversold_cnt}/5 | 反彈：{bounce_type}")
                if reason:
                    lines.append(f"逻辑：{reason}")
            else:
                lines.append(f"👀 {ticker} — {stype} 关注（置信度 {conf}/10）")
                if company or sector:
                    lines.append(f"公司：{company}（{sector}）")
                lines.append(f"关注区间：{entry}")
                lines.append(f"止损参考：{stop} | 目标一：{t1}")
                if reason:
                    lines.append(f"逻辑：{reason}")
            lines.append("⚠️ 大盘风险较高，建议等待更好入场时机")
            lines.append("---")
        lines.append("")

        # 全部为 WATCH 时的总结提示
        if not buy_signals:
            lines.append(f"今日大盘风险偏高（VIX={vix_str}），以上均为观察标的，建议等待大盘稳定后入场")
            lines.append("")

    elif not buy_signals:
        lines.append("今日无符合条件的买入或关注信号")
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
