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
from datetime import datetime, timedelta
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


# ── 歷史數據預載 ──────────────────────────────────────────────────────────────

def _preload_history(tickers: list, end_date: str = None) -> dict:
    """
    預先拉取所有 ticker 的 730 天歷史數據，寫入 polygon_client 快取。
    之後各 detector 調用 get_history() 時直接命中快取，零 API call。

    節流：每 5 個 API 請求暫停 61 秒（Polygon 5 calls/min 限制）。
    已有快取的 ticker 不計入節流計數。
    進度：每 10 只打印一次。

    Returns:
        {"cached": N, "fetched": N, "failed": N}
    """
    from data.polygon_client import (
        CACHE_DIR, MAX_CACHE_DAYS, _last_weekday,
        get_history as _poly_get_history,
    )

    to_date    = end_date or _last_weekday()
    cache_days = MAX_CACHE_DAYS                                       # 730
    from_date  = (datetime.strptime(to_date, "%Y-%m-%d")
                  - timedelta(days=cache_days)).strftime("%Y-%m-%d")

    unique_tickers = sorted({t for t in tickers if t})

    # 分類：哪些已有快取 → 跳過；哪些需要 API 拉取
    to_fetch: list[str] = []
    for ticker in unique_tickers:
        cache_file = CACHE_DIR / f"history_{ticker}_{from_date}_{to_date}.json"
        if not cache_file.exists():
            to_fetch.append(ticker)

    cached_count = len(unique_tickers) - len(to_fetch)
    print(f"  總計 {len(unique_tickers)} 只  ·  "
          f"已快取 {cached_count} 只  ·  "
          f"需拉取 {len(to_fetch)} 只")

    if not to_fetch:
        print("  全部命中快取，跳過 API 請求")
        return {"cached": cached_count, "fetched": 0, "failed": 0}

    BATCH_SIZE  = 5
    BATCH_SLEEP = 61   # 略超過 60s，確保不超過 5 calls/min

    fetched = failed = 0
    api_calls = 0      # 本輪實際發出的 API 請求數

    for i, ticker in enumerate(to_fetch, 1):
        # 進度：第 1 只、每 10 只、最後一只
        if i == 1 or i % 10 == 0 or i == len(to_fetch):
            print(f"  [{i}/{len(to_fetch)}] 拉取 {ticker} …")

        try:
            df = _poly_get_history(ticker, days=730, end_date=end_date)
            if not df.empty:
                fetched += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  [warn] {ticker} 預載失敗: {e}")
            failed += 1

        api_calls += 1

        # 每 BATCH_SIZE 次 API 請求後節流（最後一批不需要等）
        if api_calls % BATCH_SIZE == 0 and i < len(to_fetch):
            print(f"  [節流] 已請求 {api_calls} 次，暫停 {BATCH_SLEEP}s …")
            time.sleep(BATCH_SLEEP)

    print(f"  預載完成：命中快取 {cached_count}  |  新拉取 {fetched}  |  失敗 {failed}")
    return {"cached": cached_count, "fetched": fetched, "failed": failed}


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
        "bottom_signals":  0,
        "post_ep_signals": 0,
        "cup_signals":     0,
        "mr_signals":      0,
        "buy_signals":     0,
        "risk_on":         True,
        "vix":             None,
        "spy_trend":       None,
        "runtime_minutes": 0,
    }
    all_signals       = []
    market_env        = {"risk_on": True, "vix": None, "spy_trend": None, "reason": ""}
    fund_candidates   = []
    tech_candidates   = []
    ep_signals_list      = []
    vcp_signals_list     = []
    bottom_signals_list  = []
    post_ep_signals_list = []
    cup_signals_list     = []
    mr_signals_list      = []
    buy_signals          = []

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

        if not market_env.get("risk_on", True):
            print(f"\n  ⚠️  大盘风险偏好关闭（VIX={market_env.get('vix')}），但仍继续扫描")
            print(f"  原因: {market_env.get('reason', '')}")
            print(f"  所有信号将标注风险警告，建议降低仓位至正常的50%")
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
        tech_candidates = technical_run(fund_candidates, date=date)
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

    # ── Step 4.5: 歷史數據預載（52W 預篩 + 共享快取）────────────────────────────
    t0 = _step("Step 4.5: 歷史數據預載（智能預篩）")

    # 判斷閾值
    DIST_BOTTOM_MIN = 25.0   # 離 52W High ≥ 25% → 底部/超跌候選（BottomFinder + MeanReversion）
    DIST_CUP_MAX    = 20.0   # 離 52W High ≤ 20% → 接近前高候選（CupHandle）

    tech_set = {s.get("ticker") or s.get("T", "") for s in tech_candidates}

    preload_set: set = set(tech_set)  # tech_candidates 一律預載

    skip_cnt = bottom_pool_cnt = cup_pool_cnt = nodata_cnt = 0

    for s in fund_candidates:
        ticker  = s.get("ticker") or s.get("T", "")
        if not ticker or ticker in tech_set:
            continue  # tech_candidates 已加入，跳過重複

        price    = s.get("price")
        high_52w = s.get("52w_high")

        # 52W 數據缺失（FMP 路徑或 Finviz 未返回）→ 保守地保留，避免漏掉
        if not high_52w or not price or price <= 0:
            preload_set.add(ticker)
            nodata_cnt += 1
            continue

        dist_pct = (high_52w - price) / high_52w * 100  # 距 52W High 的跌幅（%）

        if dist_pct >= DIST_BOTTOM_MIN:
            # 股價遠低於 52W High → 可能是底部反轉或超跌均值回歸
            preload_set.add(ticker)
            bottom_pool_cnt += 1
        elif dist_pct <= DIST_CUP_MAX:
            # 股價接近 52W High → 可能正在形成杯柄
            preload_set.add(ticker)
            cup_pool_cnt += 1
        else:
            # 中間位置（20-25% 區間）→ 不適合底部類也不適合杯柄類，跳過
            skip_cnt += 1

    total_fund_non_tech = len(fund_candidates) - sum(
        1 for s in fund_candidates
        if (s.get("ticker") or s.get("T", "")) in tech_set
    )
    print(f"  fund_candidates 非tech部分：{total_fund_non_tech} 只")
    print(f"  52W預篩結果：底部池 {bottom_pool_cnt} + 杯柄池 {cup_pool_cnt} "
          f"+ 無數據保留 {nodata_cnt} + 跳過 {skip_cnt}")
    print(f"  Tech candidates：{len(tech_set)} 只（無條件預載）")
    print(f"  預載總計：{len(preload_set)} 只（原 {len(fund_candidates)} 只，"
          f"節省 {len(fund_candidates) - len(preload_set)} 只 API calls）")

    preload_stats = _preload_history(list(preload_set), end_date=date)
    _done(t0, f"預載（拉取 {preload_stats['fetched']} / 快取 {preload_stats['cached']} / 失敗 {preload_stats['failed']}）")

    # ── Step 5: 信号检测 ──────────────────────────────────────────────────────
    ep_signals_list      = []
    vcp_signals_list     = []
    bf_signals_list      = []
    ws_signals_list      = []
    bottom_signals_list  = []
    post_ep_signals_list = []
    cup_signals_list     = []
    mr_signals_list      = []
    t0 = _step("Step 5: 信号检测（EP+VCP+BullFlag+Weinstein+BottomFinder+PostEP+CupHandle+MeanReversion）")
    _t5 = time.time()

    _ts = time.time()
    try:
        from signals.ep_detector import detect as ep_detect
        ep_signals_list = ep_detect(tech_candidates)
        summary["ep_signals"] = len(ep_signals_list)
    except Exception as e:
        print(f"  [error] EP 检测失败: {e}")
    print(f"  ⏱ EP 耗时: {time.time()-_ts:.1f}s → {len(ep_signals_list)}只信号")

    _ts = time.time()
    try:
        from signals.vcp_scorer import score as vcp_score
        vcp_signals_list = vcp_score(tech_candidates)
        summary["vcp_signals"] = len(vcp_signals_list)
    except Exception as e:
        print(f"  [error] VCP 评分失败: {e}")
    print(f"  ⏱ VCP 耗时: {time.time()-_ts:.1f}s → {len(vcp_signals_list)}只信号")

    _ts = time.time()
    try:
        from signals.bull_flag_detector import detect as bf_detect
        bf_signals_list = bf_detect(tech_candidates)
        summary["bf_signals"] = len(bf_signals_list)
    except Exception as e:
        print(f"  [error] Bull Flag 检测失败: {e}")
    print(f"  ⏱ Bull Flag 耗时: {time.time()-_ts:.1f}s → {len(bf_signals_list)}只信号")

    _ts = time.time()
    try:
        from signals.weinstein_detector import detect as ws_detect
        ws_signals_list = ws_detect(tech_candidates)
        summary["ws_signals"] = len(ws_signals_list)
    except Exception as e:
        print(f"  [error] Weinstein 检测失败: {e}")
    print(f"  ⏱ Weinstein 耗时: {time.time()-_ts:.1f}s → {len(ws_signals_list)}只信号")

    # Bottom Finder 使用 fund_candidates（未进入 Stage 2 的底部反转股）
    _ts = time.time()
    try:
        from signals.bottom_finder_detector import detect as bottom_detect
        bottom_signals_list = bottom_detect(fund_candidates, date=date)
        summary["bottom_signals"] = len(bottom_signals_list)
    except Exception as e:
        print(f"  [error] Bottom Finder 检测失败: {e}")
    print(f"  ⏱ Bottom Finder 耗时: {time.time()-_ts:.1f}s → {len(bottom_signals_list)}只信号")

    # Post-EP Tight 使用 tech_candidates
    _ts = time.time()
    try:
        from signals.post_ep_tight_detector import detect as post_ep_detect
        post_ep_signals_list = post_ep_detect(tech_candidates, date=date)
        summary["post_ep_signals"] = len(post_ep_signals_list)
    except Exception as e:
        print(f"  [error] Post-EP Tight 检测失败: {e}")
    print(f"  ⏱ Post-EP Tight 耗时: {time.time()-_ts:.1f}s → {len(post_ep_signals_list)}只信号")

    # Cup & Handle 使用 tech_candidates
    _ts = time.time()
    try:
        from signals.cup_handle_detector import detect as cup_detect
        cup_signals_list = cup_detect(tech_candidates, date=date)
        summary["cup_signals"] = len(cup_signals_list)
    except Exception as e:
        print(f"  [error] Cup & Handle 检测失败: {e}")
    print(f"  ⏱ Cup & Handle 耗时: {time.time()-_ts:.1f}s → {len(cup_signals_list)}只信号")

    # Mean Reversion 使用 fund_candidates（超賣股票未必在 Stage 2）
    _ts = time.time()
    try:
        from signals.mean_reversion_detector import detect as mr_detect
        mr_signals_list = mr_detect(fund_candidates, date=date, market_env=market_env)
        summary["mr_signals"] = len(mr_signals_list)
    except Exception as e:
        print(f"  [error] Mean Reversion 检测失败: {e}")
    print(f"  ⏱ Mean Reversion 耗时: {time.time()-_ts:.1f}s → {len(mr_signals_list)}只信号")

    # 去重：同一 ticker 保留最高分信号
    _seen = set()
    merged_signals = []
    for s in (ep_signals_list + vcp_signals_list + bf_signals_list
              + ws_signals_list + bottom_signals_list
              + post_ep_signals_list + cup_signals_list + mr_signals_list):
        t = s.get("ticker", "")
        if t not in _seen:
            _seen.add(t)
            merged_signals.append(s)

    print(f"\n  EP: {len(ep_signals_list)}只  VCP: {len(vcp_signals_list)}只  "
          f"BullFlag: {len(bf_signals_list)}只  Weinstein: {len(ws_signals_list)}只  "
          f"BottomFinder: {len(bottom_signals_list)}只  "
          f"PostEP: {len(post_ep_signals_list)}只  CupHandle: {len(cup_signals_list)}只  "
          f"MeanReversion: {len(mr_signals_list)}只  合并去重: {len(merged_signals)}只")
    _done(t0, "信号检测")

    # ── Step 6: Claude 信号生成 ───────────────────────────────────────────────
    t0 = _step("Step 6: Claude 综合分析")
    try:
        from signals.signal_generator import generate
        all_signals  = generate(ep_signals_list, vcp_signals_list, market_env,
                                bf_signals=bf_signals_list, ws_signals=ws_signals_list,
                                bottom_signals=bottom_signals_list,
                                post_ep_signals=post_ep_signals_list,
                                cup_signals=cup_signals_list,
                                mr_signals=mr_signals_list)
        buy_signals  = [s for s in all_signals if str(s.get("action", "")).upper() in ("BUY", "BUY_RISKY")]
        watch_signals = [s for s in all_signals if str(s.get("action", "")).upper() == "WATCH"]
        summary["buy_signals"] = len(buy_signals)
        print(f"  最终 BUY 信号: {len(buy_signals)} 个  WATCH: {len(watch_signals)} 个")
    except Exception as e:
        print(f"  [error] 信号生成失败: {e}")
    _done(t0, "Claude 分析")

    # ── Step 7: 输出 ──────────────────────────────────────────────────────────
    t0 = _step("Step 7: 输出报告")
    summary["runtime_minutes"] = (time.time() - scan_start) / 60

    report_text = ""
    try:
        from output.report_formatter import format_daily_report
        report_text = format_daily_report(all_signals, market_env, summary)
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


def _send_risk_alert(market_env: dict, date: str) -> None:
    """推送大盘风险提醒到 Discord，失败不抛异常。"""
    vix       = market_env.get("vix")
    spy_trend = market_env.get("spy_trend", "N/A")
    reason    = market_env.get("reason", "")
    vix_str   = f"{vix:.1f}" if isinstance(vix, (int, float)) else "N/A"

    report = (
        f"⚠️ 今日市场风险提醒 — {date}\n"
        f"\n"
        f"🔴 大盘环境：不适合做多\n"
        f"VIX: {vix_str} | SPY趋势: {spy_trend}\n"
        f"原因: {reason}\n"
        f"\n"
        f"今日建议：观望，不开新仓\n"
        f"系统已自动跳过今日扫描"
    )
    print(report)
    try:
        from output.discord_alert import send_report
        send_report(report)
    except Exception as e:
        print(f"  [warn] Discord 风险提醒推送失败: {e}")


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

    # Polygon 历史数据
    try:
        from data.tiingo_client import get_history as get_history_client
        df_test = get_history_client("AAPL", days=5)
        results["Polygon 历史数据"] = not df_test.empty
    except Exception:
        results["Polygon 历史数据"] = False

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
