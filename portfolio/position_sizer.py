"""
position_sizer.py — 仓位计算器

基于固定风险模型：每笔交易最大亏损 = 总资金 * risk_pct
汇率固定 USD/HKD = 7.8
"""

USD_TO_HKD = 7.8
MAX_POSITION_PCT = 20.0   # 单笔仓位上限 20%


def calculate_position(
    entry_price: float,
    stop_loss_price: float,
    account_size_hkd: float = 100_000,
    risk_pct: float = 0.01,
) -> dict:
    """
    根据入场价和止损价计算建议仓位。

    Args:
        entry_price:       计划入场价（美元）
        stop_loss_price:   止损价（美元），必须 < entry_price
        account_size_hkd:  账户总资金（港币），默认 10 万
        risk_pct:          单笔最大风险占比，默认 1%

    Returns:
        仓位计算结果 dict
    """
    if stop_loss_price >= entry_price:
        raise ValueError(f"止损价 {stop_loss_price} 必须低于入场价 {entry_price}")

    risk_amount_hkd    = account_size_hkd * risk_pct
    risk_per_share_usd = entry_price - stop_loss_price
    risk_per_share_hkd = risk_per_share_usd * USD_TO_HKD

    shares = int(risk_amount_hkd / risk_per_share_hkd)
    if shares < 1:
        shares = 1

    position_size_usd = shares * entry_price
    position_size_hkd = position_size_usd * USD_TO_HKD
    position_pct      = position_size_hkd / account_size_hkd * 100

    # 仓位上限检查
    capped = False
    if position_pct > MAX_POSITION_PCT:
        capped             = True
        position_size_hkd  = account_size_hkd * MAX_POSITION_PCT / 100
        position_size_usd  = position_size_hkd / USD_TO_HKD
        shares             = int(position_size_usd / entry_price)
        position_size_usd  = shares * entry_price
        position_size_hkd  = position_size_usd * USD_TO_HKD
        position_pct       = position_size_hkd / account_size_hkd * 100

    target_1          = entry_price * 1.20
    target_2          = entry_price * 1.35
    reward_risk_ratio = (target_1 - entry_price) / risk_per_share_usd

    result = {
        "shares":             shares,
        "position_size_usd":  round(position_size_usd, 2),
        "position_size_hkd":  round(position_size_hkd, 2),
        "position_pct":       round(position_pct, 1),
        "risk_amount_hkd":    round(risk_amount_hkd, 2),
        "risk_per_share_usd": round(risk_per_share_usd, 2),
        "target_1":           round(target_1, 2),
        "target_2":           round(target_2, 2),
        "reward_risk_ratio":  round(reward_risk_ratio, 2),
        "capped":             capped,
    }

    if capped:
        print(f"  [position_sizer] ⚠️  仓位已调整到上限 {MAX_POSITION_PCT}%"
              f"（{shares}股，{position_size_hkd:,.0f} 港币）")

    return result


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = calculate_position(entry_price=86.0, stop_loss_price=80.0)
    print("AAOI 仓位计算：")
    for k, v in result.items():
        print(f"  {k}: {v}")
