"""
signal_generator.py — Claude API 综合分析，生成最终交易信号

流程：
  1. 合并 EP + VCP 信号，去重（同只股票保留分数高的）
  2. 逐只调用 claude-sonnet-4-6 分析，大盘风险状态注入 prompt
     - risk_on=True:  action 可以是 BUY / WATCH / SKIP
     - risk_on=False: action 可以是 BUY_RISKY / WATCH / SKIP
  3. 保留 BUY 和 BUY_RISKY，按 confidence 降序
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

import anthropic
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

load_dotenv()

MODEL      = "claude-sonnet-4-6"
API_DELAY  = 0.5   # 秒，避免 Claude API 限速

SYSTEM_PROMPT = """你是一个基于 Minervini/Qullamaggie/O'Neil 交易体系的股票信号分析师。
你的任务是评估候选股票是否值得买入，给出具体的买入区间、止损位和止盈目标。
交易风格：短线到中线（持仓1天到数周），追求高胜率和好的风险回报比。"""


# ── 合并去重 ──────────────────────────────────────────────────────────────────

def _merge_signals(ep_signals: list, vcp_signals: list,
                   bf_signals: list, ws_signals: list) -> list:
    """
    合并 EP / VCP / Bull Flag 信号，同一股票只保留得分更高的那条。
    统一映射到 signal_score 字段。
    """
    merged: dict = {}  # ticker → signal dict

    for s in ep_signals:
        ticker = s.get("ticker", "")
        s      = {**s, "signal_score": s.get("ep_score", 0)}
        if ticker not in merged or s["signal_score"] > merged[ticker]["signal_score"]:
            merged[ticker] = s

    for s in vcp_signals:
        ticker = s.get("ticker", "")
        s      = {**s, "signal_score": s.get("vcp_score", 0)}
        if ticker not in merged or s["signal_score"] > merged[ticker]["signal_score"]:
            merged[ticker] = s

    for s in bf_signals:
        ticker = s.get("ticker", "")
        s      = {**s, "signal_score": s.get("bf_score", 0)}
        if ticker not in merged or s["signal_score"] > merged[ticker]["signal_score"]:
            merged[ticker] = s

    for s in ws_signals:
        ticker = s.get("ticker", "")
        s      = {**s, "signal_score": s.get("weinstein_score", 0)}
        if ticker not in merged or s["signal_score"] > merged[ticker]["signal_score"]:
            merged[ticker] = s

    result = list(merged.values())
    result.sort(key=lambda x: x.get("signal_score", 0), reverse=True)
    return result


# ── Prompt 构建 ───────────────────────────────────────────────────────────────

def _build_prompt(stock: dict, market_env: dict) -> str:
    """构建发给 Claude 的用户消息。"""

    def fmt(val, suffix="", decimals=1, prefix=""):
        if val is None:
            return "N/A"
        if isinstance(val, float):
            return f"{prefix}{val:.{decimals}f}{suffix}"
        return f"{prefix}{val}{suffix}"

    signal_type = stock.get("signal_type", "UNKNOWN")
    if signal_type == "EP":
        score_label, score_val = "EP评分",          stock.get("ep_score")
    elif signal_type == "BULL_FLAG":
        score_label, score_val = "Bull Flag评分",   stock.get("bf_score")
    elif signal_type in ("WEINSTEIN_S2", "WEINSTEIN_S2_PULLBACK"):
        score_label, score_val = "Weinstein评分",   stock.get("weinstein_score")
    else:
        score_label, score_val = "VCP评分",         stock.get("vcp_score")

    risk_on = market_env.get("risk_on", True)
    risk_warning_lines = []
    if not risk_on:
        risk_warning_lines = [
            f"⚠️ 当前大盘处于高风险状态（VIX={fmt(market_env.get('vix'), decimals=1)}，原因：{market_env.get('reason', 'N/A')}）",
            f"请在分析时考虑这个风险，建议仓位不超过正常的50%，并在 risk_warning 字段里说明大盘风险。",
            f"在此环境下 action 可以是：BUY_RISKY（可买但风险高，需小仓位）、WATCH（值得关注但暂时观望）、SKIP（完全不适合）。",
            f"",
        ]

    lines = [
        *risk_warning_lines,
        f"请分析以下股票的买入信号：",
        f"",
        f"股票：{stock.get('ticker')}（{stock.get('company', 'N/A')}，{stock.get('sector', 'N/A')}）",
        f"信号类型：{signal_type}",
        f"",
        f"技术面数据：",
        f"- 当前价格：{fmt(stock.get('last_close') or stock.get('price'), prefix='$', decimals=2)}",
        f"- 60天涨幅：{fmt(stock.get('gain_60d'), suffix='%')}",
        f"- 相对成交量：{fmt(stock.get('relative_volume'), suffix='x')}",
        f"- 技术评分：{stock.get('technical_score', 'N/A')}/100",
        f"- 整理形态：{'是' if stock.get('consolidating') else '否'}",
        f"- 距60天高点回撤：{fmt(stock.get('drawdown_from_high'), suffix='%')}",
        f"",
        f"信号数据：",
        f"- {score_label}：{score_val if score_val is not None else 'N/A'}",
        f"- 建议入场区间：{stock.get('entry_zone', 'N/A')}",
        f"- 建议止损位：{stock.get('stop_loss', 'N/A')}",
        *(
            [
                f"- 旗杆涨幅：+{fmt(stock.get('pole_gain_pct'), suffix='%')}（{stock.get('pole_duration', 'N/A')}天完成）",
                f"- 旗杆量放大：{fmt(stock.get('pole_vol_ratio'), suffix='x')}",
                f"- 旗面回调：-{fmt(stock.get('flag_pullback_pct'), suffix='%')}",
                f"- 旗面量收缩至：{fmt(stock.get('volume_contraction_pct'), suffix='%')}",
                f"- 突破日成交量：{fmt(stock.get('today_vol_ratio'), suffix='x')}",
            ]
            if signal_type == "BULL_FLAG" else
            [
                f"- 30周均线(MA30W)：${fmt(stock.get('ma30w'), decimals=2)}",
                f"- 10周均线(MA10W)：${fmt(stock.get('ma10w'), decimals=2)}",
                f"- 均线斜率：{fmt(stock.get('ma30w_slope'), decimals=3)}（>0表示上翘）",
                f"- 价格距MA30W：{fmt(stock.get('price_vs_ma30w_pct'), suffix='%')}",
                f"- 近20天涨幅：{fmt(stock.get('gain_20d'), suffix='%')}",
                f"- 阶段描述：{stock.get('stage_description', 'N/A')}",
            ]
            if signal_type in ("WEINSTEIN_S2", "WEINSTEIN_S2_PULLBACK") else []
        ),
        f"",
        f"基本面数据：",
        f"- EPS季度增速：{fmt(stock.get('eps_growth_qoq'), suffix='%')}",
        f"- 营收季度增速：{fmt(stock.get('sales_growth_qoq'), suffix='%')}",
        f"- 毛利率：{fmt(stock.get('gross_margin'), suffix='%')}",
        f"",
        f"大盘环境：",
        f"- VIX：{fmt(market_env.get('vix'), decimals=1)}",
        f"- SPY趋势：{market_env.get('spy_trend', 'N/A')}",
        f"- 大盘评估：{market_env.get('reason', 'N/A')}",
        f"",
        f'请返回 JSON 格式（不要有其他文字）：',
        f'{{',
        f'  "ticker": "股票代码",',
        f'  "action": "BUY / BUY_RISKY / WATCH / SKIP",',
        f'  "confidence": 1-10,',
        f'  "entry_zone": "具体价格区间",',
        f'  "stop_loss": "止损价格",',
        f'  "target_1": "第一目标价（+15-20%）",',
        f'  "target_2": "第二目标价（+30-40%）",',
        f'  "holding_period": "预计持仓时间",',
        f'  "reason": "2-3句话说明买入逻辑",',
        f'  "risk_warning": "主要风险点"',
        f'}}',
    ]
    return "\n".join(lines)


# ── Claude API 调用 ───────────────────────────────────────────────────────────

def _call_claude(client: anthropic.Anthropic, prompt: str, ticker: str) -> Optional[dict]:
    """
    调用 Claude API，解析返回的 JSON。
    解析失败时返回 None。
    """
    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()

        # 提取 JSON（防止 Claude 额外输出 markdown 代码块）
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        result = json.loads(raw)
        result["ticker"] = ticker   # 确保 ticker 一致
        return result

    except json.JSONDecodeError as e:
        print(f"  [warn] {ticker} JSON 解析失败: {e}")
        return None
    except anthropic.APIError as e:
        print(f"  [warn] {ticker} Claude API 错误: {e}")
        return None
    except Exception as e:
        print(f"  [warn] {ticker} 未知错误: {e}")
        return None


# ── 仓位计算附加 ──────────────────────────────────────────────────────────────

def _attach_position_size(stock: dict, result: dict) -> dict:
    """
    调用 position_sizer，把仓位建议字段附加到 result dict。
    解析 entry_zone / stop_loss 字符串，提取数值。
    失败时静默返回原 result。
    """
    try:
        from portfolio.position_sizer import calculate_position

        # 解析入场价：取区间中点或单一价格
        entry_raw = result.get("entry_zone") or stock.get("entry_zone") or ""
        stop_raw  = result.get("stop_loss")  or stock.get("stop_loss")  or ""

        entry_price = _parse_price(str(entry_raw))
        stop_price  = _parse_price(str(stop_raw))

        if entry_price and stop_price and stop_price < entry_price:
            pos = calculate_position(entry_price, stop_price)
            result["recommended_shares"]  = pos["shares"]
            result["position_size_hkd"]   = pos["position_size_hkd"]
            result["max_loss_hkd"]        = pos["risk_amount_hkd"]
            result["reward_risk_ratio"]   = pos["reward_risk_ratio"]
            result["position_pct"]        = pos["position_pct"]
    except Exception as e:
        pass   # 仓位计算非关键路径，失败不影响信号
    return result


def _parse_price(s: str) -> Optional[float]:
    """从字符串中提取价格数值，支持区间（取中点）和单值。"""
    import re
    nums = re.findall(r"\d+\.?\d*", s.replace(",", ""))
    if not nums:
        return None
    vals = [float(n) for n in nums if float(n) > 0]
    if not vals:
        return None
    return sum(vals) / len(vals)   # 区间取中点，单值直接返回


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def generate(ep_signals: list, vcp_signals: list, market_env: dict,
             bf_signals: list = None, ws_signals: list = None) -> list:
    """
    综合 EP + VCP + Bull Flag + Weinstein 信号，调用 Claude 分析，返回最终 BUY 信号列表。

    Args:
        ep_signals:  ep_detector.detect() 的结果
        vcp_signals: vcp_scorer.score() 的结果
        market_env:  market_env_client.get_market_env() 的结果
        bf_signals:  bull_flag_detector.detect() 的结果（可选）
        ws_signals:  weinstein_detector.detect() 的结果（可选）

    Returns:
        action="BUY"/"BUY_RISKY" 的信号列表，按 confidence 降序
    """
    # ── 合并去重 ──────────────────────────────────────────────────────────────
    candidates = _merge_signals(ep_signals, vcp_signals,
                                bf_signals or [], ws_signals or [])
    total      = len(candidates)

    print(f"[signal_generator] 开始 Claude 分析")
    print(f"  EP信号: {len(ep_signals)}  VCP信号: {len(vcp_signals)}"
          f"  BullFlag信号: {len(bf_signals or [])}  Weinstein信号: {len(ws_signals or [])}"
          f"  合并后: {total} 只")
    print(f"  模型: {MODEL}")

    if total == 0:
        print("  无候选信号，跳过分析")
        return []

    # ── 初始化 Claude 客户端 ──────────────────────────────────────────────────
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY 未设置，请检查 .env 文件")
    client = anthropic.Anthropic(api_key=api_key)

    buy_signals = []

    for i, stock in enumerate(candidates, 1):
        ticker = stock.get("ticker", "")
        stype  = stock.get("signal_type", "?")
        score  = stock.get("signal_score", 0)

        print(f"  [{i}/{total}] 分析 {ticker} ({stype} score={score})…", end=" ", flush=True)

        prompt = _build_prompt(stock, market_env)
        result = _call_claude(client, prompt, ticker)

        if result is None:
            print("跳过（API失败）")
            time.sleep(API_DELAY)
            continue

        action     = result.get("action", "SKIP").upper()
        confidence = result.get("confidence", 0)
        print(f"→ {action} (confidence={confidence})")

        if action in ("BUY", "BUY_RISKY", "WATCH"):
            result = _attach_position_size(stock, result)

        if action in ("BUY", "BUY_RISKY"):
            buy_signals.append({**stock, **result})

        time.sleep(API_DELAY)

    buy_signals.sort(key=lambda x: x.get("confidence", 0), reverse=True)

    print(f"\n[signal_generator] 完成")
    print(f"  分析: {total} 只  BUY: {len(buy_signals)} 个")

    if buy_signals:
        print()
        hdr = (f"{'#':>3} {'Ticker':<7} {'Signal':<6} {'Conf':>4} "
               f"{'Entry':<18} {'Stop':<10} {'T1':<10} {'T2':<10} {'Hold':<12}")
        print(hdr)
        print("-" * 90)
        for rank, s in enumerate(buy_signals, 1):
            print(
                f"{rank:>3} {s['ticker']:<7} {s.get('signal_type', ''):<6} "
                f"{s.get('confidence', '-'):>4} "
                f"{str(s.get('entry_zone', '')):<18} "
                f"{str(s.get('stop_loss', '')):<10} "
                f"{str(s.get('target_1', '')):<10} "
                f"{str(s.get('target_2', '')):<10} "
                f"{str(s.get('holding_period', '')):<12}"
            )
        print()
        for s in buy_signals:
            print(f"  {s['ticker']} — {s.get('reason', '')}")
            print(f"    风险: {s.get('risk_warning', '')}")

    return buy_signals


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from data.market_env_client import get_market_env
    from screener.fundamental_filter import run as fundamental_run
    from screener.technical_filter import run as technical_run
    from signals.ep_detector import detect as ep_detect
    from signals.vcp_scorer import score as vcp_score

    print("=" * 60)
    print("Step 1: 大盘环境")
    print("=" * 60)
    market_env = get_market_env()
    print(f"risk_on={market_env['risk_on']}  VIX={market_env['vix']}  "
          f"SPY趋势={market_env['spy_trend']}")
    print(f"评估: {market_env['reason']}\n")

    print("=" * 60)
    print("Step 2: 基本面候选股")
    print("=" * 60)
    fund_candidates = fundamental_run()
    print(f"基本面候选: {len(fund_candidates)} 只\n")

    print("=" * 60)
    print("Step 3: 技术面过滤")
    print("=" * 60)
    tech_candidates = technical_run(fund_candidates)
    print(f"技术面候选: {len(tech_candidates)} 只\n")

    print("=" * 60)
    print("Step 4: EP 信号检测")
    print("=" * 60)
    ep_signals = ep_detect(tech_candidates)
    print(f"EP信号: {len(ep_signals)} 个\n")

    print("=" * 60)
    print("Step 5: VCP 评分")
    print("=" * 60)
    vcp_signals = vcp_score(tech_candidates)
    print(f"VCP信号: {len(vcp_signals)} 个\n")

    print("=" * 60)
    print("Step 6: Claude 综合分析")
    print("=" * 60)
    buy_signals = generate(ep_signals, vcp_signals, market_env)

    print()
    print("=" * 60)
    print("最终 BUY 信号")
    print("=" * 60)
    if not buy_signals:
        print("今日无买入信号")
    else:
        print(f"共 {len(buy_signals)} 个买入信号（按置信度降序）：")
        for s in buy_signals:
            print(f"\n  {'='*50}")
            print(f"  {s['ticker']} | {s.get('signal_type')} | confidence={s.get('confidence')}")
            print(f"  入场: {s.get('entry_zone')}  止损: {s.get('stop_loss')}")
            print(f"  目标1: {s.get('target_1')}  目标2: {s.get('target_2')}")
            print(f"  持仓: {s.get('holding_period')}")
            print(f"  逻辑: {s.get('reason')}")
            print(f"  风险: {s.get('risk_warning')}")
