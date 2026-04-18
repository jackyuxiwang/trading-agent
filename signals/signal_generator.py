"""
signal_generator.py — Claude API 综合分析，生成最终交易信号

流程：
  1. 合并 EP + VCP + BullFlag + Weinstein 信号，去重
  2. 并发调用 claude-sonnet-4-6（最多3个并发），大盘风险状态注入 prompt
     - 529 Overloaded 时 exponential backoff 重试（10/20/30s），最多3次
     - 3次均失败则降级到 claude-haiku-4-5
     - risk_on=True:  action 可以是 BUY / WATCH / SKIP
     - risk_on=False: action 可以是 BUY_RISKY / WATCH / SKIP
  3. 保留 BUY / BUY_RISKY / WATCH，按 confidence 降序
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import anthropic
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

load_dotenv()

MODEL       = "claude-sonnet-4-6"
MODEL_HAIKU = "claude-haiku-4-5-20251001"   # 529 降級備用
API_DELAY   = 0.5   # 秒，避免 Claude API 限速

# 529 重試設定
RETRY_MAX      = 3
RETRY_WAITS    = [10, 20, 30]   # 每次重試前等待秒數（exponential backoff）

SYSTEM_PROMPT = """你是一个基于 Minervini/Qullamaggie/O'Neil 交易体系的股票信号分析师。
你的任务是评估候选股票是否值得买入，给出具体的买入区间、止损位和止盈目标。
交易风格：短线到中线（持仓1天到数周），追求高胜率和好的风险回报比。"""


# ── 合并去重 ──────────────────────────────────────────────────────────────────

def _merge_signals(ep_signals: list, vcp_signals: list,
                   bf_signals: list, ws_signals: list,
                   bottom_signals: list = None,
                   post_ep_signals: list = None,
                   cup_signals: list = None,
                   mr_signals: list = None,
                   fw_signals: list = None) -> list:
    """
    合并 EP / VCP / Bull Flag / Weinstein / Bottom Finder /
    Post-EP Tight / Cup Handle / Mean Reversion / Falling Wedge 信号，
    同一股票只保留得分更高的那条。统一映射到 signal_score 字段。
    """
    merged: dict = {}  # ticker → signal dict

    def _put(s: dict, score_key: str):
        ticker = s.get("ticker", "")
        if not ticker:
            return
        s = {**s, "signal_score": s.get(score_key, 0)}
        if ticker not in merged or s["signal_score"] > merged[ticker]["signal_score"]:
            merged[ticker] = s

    for s in ep_signals:              _put(s, "ep_score")
    for s in vcp_signals:             _put(s, "vcp_score")
    for s in bf_signals:              _put(s, "bf_score")
    for s in ws_signals:              _put(s, "weinstein_score")
    for s in (bottom_signals or []):  _put(s, "score")
    for s in (post_ep_signals or []): _put(s, "score")
    for s in (cup_signals or []):     _put(s, "score")
    for s in (mr_signals or []):      _put(s, "score")
    for s in (fw_signals or []):      _put(s, "score")

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
    elif signal_type == "VCP_CHEAT_ENTRY":
        score_label, score_val = "VCP评分",         stock.get("vcp_score")
    elif signal_type == "BOTTOM_FINDER":
        score_label, score_val = "底部反轉評分",     stock.get("score")
    elif signal_type == "POST_EP_TIGHT":
        score_label, score_val = "EP後盤整評分",     stock.get("score")
    elif signal_type == "CUP_HANDLE":
        score_label, score_val = "杯柄評分",         stock.get("score")
    elif signal_type == "MEAN_REVERSION":
        score_label, score_val = "均值回歸評分",     stock.get("score")
    elif signal_type == "FALLING_WEDGE":
        score_label, score_val = "下降楔形評分",     stock.get("score")
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
        *(
            [
                f"- 策略类型：低吸等待策略（非追涨）",
                f"- 当前价格：${fmt(stock.get('current_price'), decimals=2)}",
                f"- 低吸买入区间：{stock.get('entry_zone', 'N/A')}",
                f"- 距离低吸区：{fmt(stock.get('distance_to_cheat'), suffix='%')}",
                f"- 原突破位（可加仓目标）：{stock.get('original_breakout', 'N/A')}",
                f"- 建议止损位：{stock.get('stop_loss', 'N/A')}（止损幅度 {stock.get('stop_loss_pct', 'N/A')}%）",
                f"",
                f"低吸可行性分析：",
                f"- 近5日价格趋势：{fmt(stock.get('slope_5d'), suffix='%', decimals=1)}（正=上涨，负=回调中）",
                f"- 成交量趋势：{stock.get('vol_trend', 'N/A')}",
                f"- 均线支撑：{stock.get('ma_support', 'N/A')}",
                f"- 低吸可行性评分：{stock.get('cheat_entry_score', 'N/A')}/100（{stock.get('cheat_entry_feasibility', 'N/A')}）",
                f"",
                f"请根据以上数据判断：",
                f"1. 这个低吸价位是否合理？是否有均线支撑？",
                f"2. 近期是否有可能回调到此区间？",
                f"3. 回调到低吸区时，成交量应如何配合才算有效？",
                f"4. action 建议：",
                f"   - 低吸可行性高 且 距离<5%：BUY（价格已接近低吸区）",
                f"   - 低吸可行性高 或 中：WATCH（等待回调）",
                f"   - 低吸可行性低：SKIP",
            ]
            if stock.get("cheat_entry") else
            [
                f"- 建议入场区间：{stock.get('entry_zone', 'N/A')}",
                f"- 建议止损位：{stock.get('stop_loss', 'N/A')}",
            ]
        ),
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
            if signal_type in ("WEINSTEIN_S2", "WEINSTEIN_S2_PULLBACK") else
            [
                f"- 底部反轉型態：長期下跌後在低位築底，量縮整理，放量突破",
                f"- 過去12個月下跌幅度：{fmt(stock.get('decline_pct'), suffix='%')}",
                f"- 築底天數：{stock.get('base_days', 'N/A')} 天",
                f"- Higher Lows 數量：{stock.get('higher_lows', 'N/A')} 個（滿分3個）",
                f"- 量縮比例：後半段均量為前半段的 {fmt(stock.get('vol_contract_ratio'), suffix='%')}（越低越好）",
                f"- 突破量能：{fmt(stock.get('breakout_vol_ratio'), suffix='x')} 50日均量",
                f"- 底部高點（base_high）：${fmt(stock.get('base_high'), decimals=2)}",
                f"- 底部低點（base_low）：${fmt(stock.get('base_low'), decimals=2)}",
                f"- 建議止損：${stock.get('stop_loss', 'N/A')}（底部低點下方3%）",
                f"- 預估目標：${fmt(stock.get('target_price'), decimals=2)}（+30%）",
                f"- 風報比：{stock.get('risk_reward', 'N/A')}:1",
                f"",
                f"重點評估：突破的有效性（量能是否足夠？）、止損是否合理？",
                f"底部反轉型態風險較高但報酬潛力大，重點看 entry_price、stop_loss、risk_reward。",
            ]
            if signal_type == "BOTTOM_FINDER" else
            [
                f"- EP後盤整型態：EP跳空後緊密整理（3–10天），量縮，伺機第二段突破",
                f"- EP缺口日期：{stock.get('ep_date', 'N/A')}",
                f"- EP缺口幅度：{fmt(stock.get('ep_gap_pct'), suffix='%')}",
                f"- EP收盤價：${fmt(stock.get('ep_close'), decimals=2)}",
                f"- 盤整天數：{stock.get('consol_days', 'N/A')} 天",
                f"- 盤整振幅（缺口倍數）：{fmt(stock.get('amp_ratio'), suffix='x')}（越小越緊）",
                f"- 盤整量/EP量：{fmt(stock.get('vol_ratio_to_ep'), suffix='x')}（越小越好）",
                f"- 缺口保持：{'是' if stock.get('gap_maintained') else '否'}",
                f"- 建議入場：${fmt(stock.get('entry_price'), decimals=2)}（盤整高點突破）",
                f"- 建議止損：${fmt(stock.get('stop_loss'), decimals=2)}（EP開盤下方1%）",
                f"- 預估目標：${fmt(stock.get('target_price'), decimals=2)}（EP漲幅 × 0.618）",
                f"",
                f"重點評估：盤整是否足夠緊密？量縮是否充分？突破時需量能配合。",
            ]
            if signal_type == "POST_EP_TIGHT" else
            [
                f"- 杯柄型態（O'Neil Cup with Handle）：",
                f"- 杯深度：{fmt(stock.get('cup_depth_pct'), suffix='%')}（理想 15–35%）",
                f"- 杯寬度：{stock.get('cup_length', 'N/A')} 天",
                f"- 右側恢復：{fmt(stock.get('right_recovery_pct'), suffix='%')}（需≥85%）",
                f"- 杯形評分（U形比）：{fmt(stock.get('u_shape_ratio'), decimals=2)}",
                f"- 杯柄天數：{stock.get('handle_length', 'N/A')} 天",
                f"- 柄深度比：{fmt(stock.get('handle_depth_ratio'), suffix='%')}（<50%為佳）",
                f"- 柄部量縮：{fmt(stock.get('handle_vol_ratio'), suffix='x')}（<0.8為佳）",
                f"- 突破量能：{fmt(stock.get('breakout_vol_ratio'), suffix='x')}（需≥1.5x）",
                f"- 建議入場：${fmt(stock.get('entry_price'), decimals=2)}（柄高突破）",
                f"- 建議止損：${fmt(stock.get('stop_loss'), decimals=2)}（柄低下方1%）",
                f"- 預估目標：${fmt(stock.get('target_price'), decimals=2)}（杯深度量升）",
                f"",
                f"重點評估：杯形是否圓潤？柄部量縮是否充分？突破放量是否有效？",
            ]
            if signal_type == "CUP_HANDLE" else
            [
                f"- 均值回歸型態：優質股票超跌後技術面反彈機會",
                f"- RSI(14)：{fmt(stock.get('rsi'), decimals=1)}（<30為嚴重超賣）",
                f"- MA50偏離度：{fmt(stock.get('ma50_dev_pct'), suffix='%')}（負值=跌破MA50）",
                f"- 超賣信號數量：{stock.get('oversold_count', 'N/A')}/5",
                f"- 反彈信號數量：{stock.get('bounce_count', 'N/A')}/4",
                f"- 反彈型態：{stock.get('bounce_type', 'N/A')}",
                f"- 連跌天數：{stock.get('consec_down_days', 'N/A')} 天",
                f"- 累計跌幅：{fmt(stock.get('recent_decline_pct'), suffix='%')}",
                f"- 距52週低點：{fmt(stock.get('near_52w_low_pct'), suffix='%')}",
                f"- 建議入場：${fmt(stock.get('entry_price'), decimals=2)}（當前價）",
                f"- 建議止損：${fmt(stock.get('stop_loss'), decimals=2)}（近5日低點×0.97）",
                f"- 目標回歸：${fmt(stock.get('target_price'), decimals=2)}（MA50）",
                f"- 風報比：{fmt(stock.get('risk_reward'), decimals=1)}:1",
                f"",
                f"重點評估：反彈信號是否可靠？基本面是否支撐估值？大盤環境是否適合逆勢操作？",
                f"注意：均值回歸策略需要嚴格止損，不宜重倉。",
            ]
            if signal_type == "MEAN_REVERSION" else
            [
                f"- 下降楔形突破型態：股價形成Lower Highs + Lower Lows收斂楔形，放量突破上方阻力線",
                f"- 楔形持續天數：{stock.get('wedge_days', 'N/A')} 天",
                f"- Swing High 數量：{stock.get('swing_high_count', 'N/A')} 個（Lower Highs序列）",
                f"- Swing Low 數量：{stock.get('swing_low_count', 'N/A')} 個（Lower Lows序列）",
                f"- 趨勢線擬合 R²：上方 {stock.get('h_r2', 'N/A')} / 下方 {stock.get('l_r2', 'N/A')}（越接近1越好）",
                f"- 量縮比例：後半段均量 / 前半段均量 = {stock.get('vol_ratio', 'N/A')}（<0.7為佳）",
                f"- 突破量能：{stock.get('breakout_vol_ratio', 'N/A')}x 50日均量",
                f"- 已突破：{'是' if stock.get('is_breakout') else '否（接近突破）'}",
                f"- 距阻力線：{stock.get('dist_to_resistance', 'N/A')}%（正=已突破，負=尚未突破）",
                f"- RSI看漲背離：{'是（極強反轉信號）' if stock.get('rsi_divergence') else '否'}",
                f"- 建議入場：${stock.get('entry_price', 'N/A')}（上方趨勢線阻力位）",
                f"- 建議止損：${stock.get('stop_loss', 'N/A')}（最近Swing Low下方2%）",
                f"- 預估目標：${stock.get('target_price', 'N/A')}（Measured Move = 楔形入口高度）",
                f"- 風報比：{stock.get('risk_reward', 'N/A')}:1",
                f"",
                f"重點評估：突破是否有效（放量 ≥1.5x均量）？RSI背離是否確認？楔形收斂是否清晰？",
                f"下降楔形統計勝率約80%，是可靠的底部反轉信號，但需要放量突破確認。",
            ]
            if signal_type == "FALLING_WEDGE" else []
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
    調用 Claude API，解析返回的 JSON。
    遇到 529 Overloaded 時 exponential backoff 重試（最多 3 次），
    3 次均失敗則降級到 claude-haiku 再試一次。
    解析失敗時返回 None。
    """
    def _do_call(model: str) -> Optional[dict]:
        message = client.messages.create(
            model=model,
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        result["ticker"] = ticker
        return result

    # 主模型重試（最多 RETRY_MAX 次）
    for attempt in range(1, RETRY_MAX + 1):
        try:
            return _do_call(MODEL)

        except json.JSONDecodeError as e:
            print(f"  [warn] {ticker} JSON 解析失敗: {e}")
            return None   # JSON 錯誤不需要重試

        except anthropic.APIStatusError as e:
            if e.status_code == 529:
                wait = RETRY_WAITS[attempt - 1]
                print(f"  [warn] {ticker} Claude 529 Overloaded (attempt {attempt}/{RETRY_MAX})，"
                      f"等待 {wait}s 後重試…")
                time.sleep(wait)
                continue
            print(f"  [warn] {ticker} Claude API 錯誤 {e.status_code}: {e}")
            return None

        except anthropic.APIError as e:
            print(f"  [warn] {ticker} Claude API 錯誤: {e}")
            return None

        except Exception as e:
            print(f"  [warn] {ticker} 未知錯誤: {e}")
            return None

    # 主模型 3 次全敗 → 降級到 haiku
    print(f"  [warn] {ticker} 主模型 {RETRY_MAX} 次重試均失敗，降級到 {MODEL_HAIKU}")
    try:
        return _do_call(MODEL_HAIKU)
    except Exception as e:
        print(f"  [warn] {ticker} haiku 降級也失敗: {e}")
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
             bf_signals: list = None, ws_signals: list = None,
             bottom_signals: list = None,
             post_ep_signals: list = None,
             cup_signals: list = None,
             mr_signals: list = None,
             fw_signals: list = None) -> list:
    """
    综合 EP + VCP + Bull Flag + Weinstein + Bottom Finder +
    Post-EP Tight + Cup Handle + Mean Reversion + Falling Wedge 信号，
    调用 Claude 分析，返回最终信号列表。

    Args:
        ep_signals:      ep_detector.detect() 的结果
        vcp_signals:     vcp_scorer.score() 的结果
        market_env:      market_env_client.get_market_env() 的结果
        bf_signals:      bull_flag_detector.detect() 的结果（可选）
        ws_signals:      weinstein_detector.detect() 的结果（可选）
        bottom_signals:  bottom_finder_detector.detect() 的结果（可选）
        post_ep_signals: post_ep_tight_detector.detect() 的结果（可选）
        cup_signals:     cup_handle_detector.detect() 的结果（可选）
        mr_signals:      mean_reversion_detector.detect() 的结果（可选）
        fw_signals:      falling_wedge_detector.detect() 的结果（可选）

    Returns:
        action="BUY"/"BUY_RISKY" 的信号列表，按 confidence 降序
    """
    # ── 合并去重 ──────────────────────────────────────────────────────────────
    candidates = _merge_signals(ep_signals, vcp_signals,
                                bf_signals or [], ws_signals or [],
                                bottom_signals or [],
                                post_ep_signals or [],
                                cup_signals or [],
                                mr_signals or [],
                                fw_signals or [])
    total      = len(candidates)

    print(f"[signal_generator] 开始 Claude 分析")
    print(f"  EP信号: {len(ep_signals)}  VCP信号: {len(vcp_signals)}"
          f"  BullFlag信号: {len(bf_signals or [])}  Weinstein信号: {len(ws_signals or [])}"
          f"  BottomFinder信号: {len(bottom_signals or [])}"
          f"  PostEP信号: {len(post_ep_signals or [])}  CupHandle信号: {len(cup_signals or [])}"
          f"  MeanReversion信号: {len(mr_signals or [])}  FallingWedge信号: {len(fw_signals or [])}"
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

    # ── 并发分析（每只独立线程，最多3个并发）─────────────────────────────────
    MAX_WORKERS  = 3
    t_parallel   = time.time()
    raw_results  = {}   # index → (stock, result, elapsed)

    def _analyze_one(idx_stock):
        idx, stock = idx_stock
        ticker = stock.get("ticker", "")
        stype  = stock.get("signal_type", "?")
        score  = stock.get("signal_score", 0)
        t_s    = time.time()

        prompt = _build_prompt(stock, market_env)
        result = _call_claude(client, prompt, ticker)
        elapsed = time.time() - t_s

        action     = result.get("action", "SKIP").upper() if result else "SKIP"
        confidence = result.get("confidence", 0)          if result else 0

        if result and action in ("BUY", "BUY_RISKY", "WATCH"):
            result = _attach_position_size(stock, result)

        return idx, ticker, stype, score, action, confidence, result, elapsed

    print(f"  並發分析（max_workers={MAX_WORKERS}，529 自動重試最多 {RETRY_MAX} 次）…")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_analyze_one, (i, s)): i
                   for i, s in enumerate(candidates)}
        for future in as_completed(futures):
            try:
                idx, ticker, stype, score, action, confidence, result, elapsed = future.result()
                raw_results[idx] = (candidates[idx], action, confidence, result, elapsed)
                status = f"→ {action} (confidence={confidence})  ⏱{elapsed:.1f}s"
                print(f"  [{idx+1}/{total}] {ticker} ({stype} score={score}) {status}")
            except Exception as e:
                orig_idx = futures[future]
                print(f"  [{orig_idx+1}/{total}] 分析失败: {e}")

    t_parallel_elapsed = time.time() - t_parallel
    t_serial_estimate  = sum(r[4] for r in raw_results.values())
    print(f"\n  ⏱ 并发总耗时: {t_parallel_elapsed:.1f}s  "
          f"（串行预计 {t_serial_estimate:.1f}s，"
          f"提速 {t_serial_estimate/max(t_parallel_elapsed,0.1):.1f}x）")

    buy_signals   = []
    watch_signals = []

    for idx in sorted(raw_results):
        stock, action, confidence, result, _ = raw_results[idx]
        if result is None:
            continue
        if action in ("BUY", "BUY_RISKY"):
            buy_signals.append({**stock, **result})
        elif action == "WATCH":
            watch_signals.append({**stock, **result})

    buy_signals.sort(key=lambda x: x.get("confidence", 0), reverse=True)
    watch_signals.sort(key=lambda x: x.get("confidence", 0), reverse=True)

    print(f"\n[signal_generator] 完成")
    print(f"  分析: {total} 只  BUY: {len(buy_signals)} 个  WATCH: {len(watch_signals)} 个")

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

    return buy_signals + watch_signals


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
