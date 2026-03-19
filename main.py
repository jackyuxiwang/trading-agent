"""
main.py — Trading Agent 主入口

用法：
  python main.py                      → 运行今日扫描
  python main.py --date 2025-12-10    → 回测指定日期
  python main.py --test               → 检查各模块连接状态
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

# 确保项目根目录在 sys.path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")


# ── 步骤执行工具 ──────────────────────────────────────────────────────────────

def _step(name: str):
    """打印步骤标题，返回开始时间。"""
    print(f"\n{'=' * 60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {name}")
    print("=" * 60)
    return time.time()


def _done(t0: float, label: str = "") -> float:
    """打印步骤耗时，返回耗时秒数。"""
    elapsed = time.time() - t0
    print(f"  ✓ {label}耗时: {elapsed:.1f}s")
    return elapsed


# ── 核心扫描流程 ──────────────────────────────────────────────────────────────

def run_daily_scan(date: str = None) -> dict:
    """
    完整的每日扫描流程。

    Args:
        date: 指定日期 "YYYY-MM-DD"（回测用），默认 None 表示今天

    Returns:
        包含扫描结果和统计信息的 dict
    """
    scan_start   = time.time()
    today_label  = date or datetime.today().strftime("%Y-%m-%d")
    is_backtest  = date is not None

    print(f"\n{'#' * 60}")
    print(f"  Trading Agent 每日扫描  {'（回测: ' + date + '）' if is_backtest else ''}")
    print(f"  {today_label}")
    print(f"{'#' * 60}")

    summary = {
        "date":            today_label,
        "total_market":    0,
        "stage1_count":    0,
        "stage2_count":    0,
        "stage3_count":    0,
        "ep_signals":      0,
        "vcp_signals":     0,
        "buy_signals":     0,
        "risk_on":         True,
        "vix":             None,
        "spy_trend":       None,
        "runtime_minutes": 0,
    }
    all_signals  = []
    market_env   = {"risk_on": True, "vix": None, "spy_trend": None, "reason": ""}
    fund_candidates  = []
    tech_candidates  = []
    ep_signals_list  = []
    vcp_signals_list = []
    buy_signals      = []

    # ── Step 1: 交易日检查 ────────────────────────────────────────────────────
    if not is_backtest:
        t0 = _step("Step 1: 交易日检查")
        try:
            from data.market_env_client import is_trading_day
            if not is_trading_day():
                print("  今天不是交易日，跳过扫描")
                return summary
            print("  ✓ 今天是交易日，继续扫描")
        except Exception as e:
            print(f"  [warn] 交易日检查失败: {e}，继续执行")
        _done(t0, "交易日检查")
    else:
        print("\n[回测模式] 跳过交易日检查")

    # ── Step 2: 大盘环境 ──────────────────────────────────────────────────────
    t0 = _step("Step 2: 大盘环境")
    try:
        from data.market_env_client import get_market_env
        market_env = get_market_env()
        summary["risk_on"]   = market_env.get("risk_on", True)
        summary["vix"]       = market_env.get("vix")
        summary["spy_trend"] = market_env.get("spy_trend")
        print(f"  risk_on  : {market_env['risk_on']}")
        print(f"  VIX      : {market_env.get('vix')}")
        print(f"  SPY趋势  : {market_env.get('spy_trend')}")
        print(f"  评估     : {market_env.get('reason', '')}")

        if not market_env.get("risk_on", True) and not is_backtest:
            print(f"\n  ⚠️  大盘风险偏好关闭，今日不扫描")
            print(f"  原因: {market_env.get('reason', '')}")
            summary["runtime_minutes"] = (time.time() - scan_start) / 60
            _write_summary(summary, [], market_env)
            return summary
    except Exception as e:
        print(f"  [warn] 大盘环境获取失败: {e}，使用默认值继续")
    _done(t0, "大盘环境")

    # ── Step 3: 基本面初筛 ────────────────────────────────────────────────────
    t0 = _step("Step 3: 基本面初筛")
    try:
        from screener.fundamental_filter import run as fundamental_run
        fund_candidates = fundamental_run(date)
        summary["stage2_count"] = len(fund_candidates)
        print(f"  基本面候选: {len(fund_candidates)} 只")
    except Exception as e:
        print(f"  [error] 基本面初筛失败: {e}")
    _done(t0, "基本面初筛")

    if not fund_candidates:
        print("  无基本面候选，提前结束")
        summary["runtime_minutes"] = (time.time() - scan_start) / 60
        _write_summary(summary, [], market_env)
        return summary

    # ── Step 4: 技术面过滤 ────────────────────────────────────────────────────
    t0 = _step("Step 4: 技术面过滤")
    try:
        from screener.technical_filter import run as technical_run
        tech_candidates = technical_run(fund_candidates)
        summary["stage3_count"] = len(tech_candidates)
        print(f"  技术面候选: {len(tech_candidates)} 只")
    except Exception as e:
        print(f"  [error] 技术面过滤失败: {e}")
    _done(t0, "技术面过滤")

    if not tech_candidates:
        print("  无技术面候选，提前结束")
        summary["runtime_minutes"] = (time.time() - scan_start) / 60
        _write_summary(summary, [], market_env)
        return summary

    # ── Step 5: 信号检测 ──────────────────────────────────────────────────────
    t0 = _step("Step 5: 信号检测（EP + VCP）")
    try:
        from signals.ep_detector import detect as ep_detect
        ep_signals_list = ep_detect(tech_candidates)
        summary["ep_signals"] = len(ep_signals_list)
        print(f"  EP 信号: {len(ep_signals_list)} 个")
    except Exception as e:
        print(f"  [error] EP 检测失败: {e}")

    try:
        from signals.vcp_scorer import score as vcp_score
        vcp_signals_list = vcp_score(tech_candidates)
        summary["vcp_signals"] = len(vcp_signals_list)
        print(f"  VCP 信号: {len(vcp_signals_list)} 个")
    except Exception as e:
        print(f"  [error] VCP 评分失败: {e}")
    _done(t0, "信号检测")

    # ── Step 6: Claude 信号生成 ───────────────────────────────────────────────
    t0 = _step("Step 6: Claude 综合分析")
    try:
        from signals.signal_generator import generate
        all_signals = ep_signals_list + vcp_signals_list   # WATCH/SKIP 也保留，供报告使用
        buy_signals = generate(ep_signals_list, vcp_signals_list, market_env)
        summary["buy_signals"] = len(buy_signals)
        print(f"  最终 BUY 信号: {len(buy_signals)} 个")
    except Exception as e:
        print(f"  [error] 信号生成失败: {e}")
    _done(t0, "Claude 分析")

    # ── Step 7: 输出 ──────────────────────────────────────────────────────────
    t0 = _step("Step 7: 输出报告")
    summary["runtime_minutes"] = (time.time() - scan_start) / 60

    report_text = ""
    try:
        from output.report_formatter import format_daily_report
        report_text = format_daily_report(buy_signals + all_signals, market_env, summary)
        print("\n" + report_text)
    except Exception as e:
        print(f"  [error] 报告格式化失败: {e}")

    _write_summary(summary, buy_signals, market_env)

    try:
        from output.discord_alert import send_report
        if report_text:
            send_report(report_text)
    except Exception as e:
        print(f"  [warn] Telegram 推送失败: {e}")

    _done(t0, "输出")

    # ── 最终摘要 ──────────────────────────────────────────────────────────────
    total_elapsed = time.time() - scan_start
    summary["runtime_minutes"] = total_elapsed / 60

    print(f"\n{'#' * 60}")
    print(f"  扫描完成  总耗时: {total_elapsed/60:.1f} 分钟")
    print(f"  漏斗: 全市场 → {summary['stage2_count']} (基本面)"
          f" → {summary['stage3_count']} (技术面)"
          f" → {summary['ep_signals']} EP + {summary['vcp_signals']} VCP"
          f" → {summary['buy_signals']} BUY")
    print(f"{'#' * 60}\n")

    return summary


def _write_summary(summary: dict, buy_signals: list, market_env: dict) -> None:
    """安全写入 CSV 日志，失败不抛异常。"""
    try:
        from output.log_writer import write_signals, write_scan_summary
        write_signals(buy_signals, summary.get("date"))
        write_scan_summary(summary)
    except Exception as e:
        print(f"  [warn] 日志写入失败: {e}")


# ── 回测入口 ──────────────────────────────────────────────────────────────────

def run_backtest(date: str) -> dict:
    """
    对指定历史日期运行扫描（回测模式）。

    Args:
        date: "YYYY-MM-DD" 格式的历史日期

    Returns:
        同 run_daily_scan()
    """
    print(f"[backtest] 回测日期: {date}")
    return run_daily_scan(date=date)


# ── 连接测试 ──────────────────────────────────────────────────────────────────

def run_connection_test() -> None:
    """检查所有外部依赖是否可访问，打印 ✅ / ❌。"""
    print("\n" + "=" * 60)
    print("  连接测试模式")
    print("=" * 60)

    results = {}

    # Polygon API
    try:
        import os, requests as req
        key = os.getenv("POLYGON_API_KEY", "")
        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/2025-01-06?apiKey={key}&limit=1"
        resp = req.get(url, timeout=8)
        ok = resp.status_code == 200 and resp.json().get("resultsCount", 0) > 0
        results["Polygon API"] = ok
    except Exception as e:
        results["Polygon API"] = False

    # Finviz
    try:
        import requests as req
        resp = req.get("https://finviz.com", timeout=8,
                       headers={"User-Agent": "Mozilla/5.0"})
        results["Finviz"] = resp.status_code == 200
    except Exception:
        results["Finviz"] = False

    # Stooq
    try:
        import requests as req
        resp = req.get("https://stooq.com/q/d/l/?s=aapl.us&i=d", timeout=8)
        results["Stooq"] = (resp.status_code == 200
                            and "Exceeded" not in resp.text
                            and "Date" in resp.text)
    except Exception:
        results["Stooq"] = False

    # Claude API
    try:
        import os, anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
        # 用极短 prompt 验证 key 有效性和余额
        client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=5,
            messages=[{"role": "user", "content": "hi"}],
        )
        results["Claude API"] = True
    except anthropic.APIError as e:
        # 余额不足也算 key 有效（连接成功）
        results["Claude API"] = "credit" in str(e).lower() or "balance" in str(e).lower()
    except Exception:
        results["Claude API"] = False

    # Discord Webhook
    try:
        from output.discord_alert import test_connection
        results["Discord Webhook"] = test_connection()
    except Exception:
        results["Discord Webhook"] = False

    print()
    for name, ok in results.items():
        icon = "✅" if ok else "❌"
        print(f"  {icon}  {name}")

    all_ok = all(results.values())
    print()
    if all_ok:
        print("  所有连接正常，可以运行扫描 🚀")
    else:
        failed = [k for k, v in results.items() if not v]
        print(f"  以下项目有问题: {', '.join(failed)}")
        print("  请检查 .env 配置和网络连接")
    print()


# ── 命令行入口 ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Trading Agent — 美股信号扫描器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "示例:\n"
            "  python main.py                      # 运行今日扫描\n"
            "  python main.py --date 2025-12-10    # 回测指定日期\n"
            "  python main.py --test               # 测试各模块连接"
        ),
    )
    parser.add_argument("--date", type=str, default=None,
                        help="回测日期，格式 YYYY-MM-DD")
    parser.add_argument("--test", action="store_true",
                        help="仅测试各模块连接状态")
    args = parser.parse_args()

    if args.test:
        run_connection_test()
    elif args.date:
        run_backtest(args.date)
    else:
        run_daily_scan()
