"""
discord_alert.py — Discord Webhook 推送模块

从 .env 读取 DISCORD_WEBHOOK_URL。

send_report(report_text)   → 发送完整日报（自动分段，≤2000字符/段）
send_signal_alert(signal)  → 发送单条信号简报
test_connection()          → 发送测试消息验证配置
"""

import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

MAX_MSG_LEN   = 2000   # Discord 单条消息字符上限
SEGMENT_DELAY = 0.5    # 多段发送时每段间隔（秒）


# ── 内部工具 ──────────────────────────────────────────────────────────────────

def _get_webhook_url() -> str:
    """读取 Webhook URL，未配置时抛 EnvironmentError。"""
    url = os.getenv("DISCORD_WEBHOOK_URL", "")
    if not url or url.startswith("your_") or not url.startswith("https://"):
        raise EnvironmentError("DISCORD_WEBHOOK_URL 未配置，请检查 .env 文件")
    return url


def _post(webhook_url: str, text: str) -> bool:
    """向 Webhook 发送单条消息，返回是否成功。"""
    try:
        resp = requests.post(webhook_url, json={"content": text}, timeout=10)
        if resp.status_code in (200, 204):
            return True
        print(f"  [discord] 发送失败: HTTP {resp.status_code} — {resp.text[:120]}")
        return False
    except Exception as e:
        print(f"  [discord] 请求异常: {e}")
        return False


def _split_text(text: str, max_len: int = MAX_MSG_LEN) -> list:
    """按段落边界切分长文本，每段不超过 max_len 字符。"""
    if len(text) <= max_len:
        return [text]

    parts   = []
    current = ""
    for line in text.splitlines(keepends=True):
        if len(current) + len(line) > max_len:
            if current:
                parts.append(current.rstrip())
            current = line
        else:
            current += line
    if current.strip():
        parts.append(current.rstrip())
    return parts


# ── 公开接口 ──────────────────────────────────────────────────────────────────

def send_report(report_text: str) -> bool:
    """
    发送完整日报到 Discord。超过2000字符自动分段，每段间隔0.5秒。

    Args:
        report_text: format_daily_report() 生成的报告文本

    Returns:
        True 表示全部发送成功
    """
    try:
        webhook_url = _get_webhook_url()
    except EnvironmentError as e:
        print(f"[discord] 配置错误，跳过发送: {e}")
        return False

    parts   = _split_text(report_text)
    success = True

    print(f"[discord] 发送日报（共 {len(parts)} 段）…")
    for i, part in enumerate(parts, 1):
        ok = _post(webhook_url, part)
        if not ok:
            success = False
            print(f"  [discord] 第 {i}/{len(parts)} 段发送失败")
        else:
            print(f"  [discord] 第 {i}/{len(parts)} 段发送成功")
        if i < len(parts):
            time.sleep(SEGMENT_DELAY)

    return success


def send_signal_alert(signal: dict) -> bool:
    """
    发送单个信号的简短提醒。

    Args:
        signal: 含 ticker, signal_type, entry_zone, stop_loss, target_1, confidence 的 dict

    Returns:
        True 表示发送成功
    """
    try:
        webhook_url = _get_webhook_url()
    except EnvironmentError as e:
        print(f"[discord] 配置错误，跳过发送: {e}")
        return False

    ticker  = signal.get("ticker", "")
    stype   = signal.get("signal_type", "")
    entry   = signal.get("entry_zone", "N/A")
    stop    = signal.get("stop_loss", "N/A")
    t1      = signal.get("target_1", "N/A")
    conf    = signal.get("confidence", "?")

    text = (
        f"🚨 **{ticker}** {stype}信号\n"
        f"入场: {entry} | 止损: {stop} | 目标: {t1}\n"
        f"置信度: {conf}/10"
    )

    ok = _post(webhook_url, text)
    if ok:
        print(f"[discord] 信号提醒已发送: {ticker}")
    return ok


def send_signal_with_chart(signal: dict, chart_path: str) -> bool:
    """
    以 multipart/form-data 方式上傳圖表圖片到 Discord，附帶簡短信號摘要。

    Args:
        signal:     信號 dict（含 ticker, signal_type, action, entry_price 等）
        chart_path: 本地 PNG 文件路徑

    Returns:
        True 表示發送成功
    """
    try:
        webhook_url = _get_webhook_url()
    except EnvironmentError as e:
        print(f"[discord] 配置错误，跳过发送: {e}")
        return False

    import json
    ticker      = signal.get("ticker", "")
    signal_type = signal.get("signal_type", "")
    action      = signal.get("action", "")
    entry       = signal.get("entry_price") or signal.get("entry_zone", "N/A")
    stop        = signal.get("stop_loss", "N/A")
    target      = signal.get("target_price", "N/A")
    rr          = signal.get("risk_reward", "")
    score       = signal.get("score", "")

    parts = [f"📊 **{ticker}** {signal_type} [{action}]"]
    if entry and entry != "N/A":
        try:
            parts.append(f"Entry {float(entry):.2f}")
        except Exception:
            parts.append(f"Entry {entry}")
    if stop and stop != "N/A":
        try:
            parts.append(f"Stop {float(stop):.2f}")
        except Exception:
            pass
    if target and target != "N/A":
        try:
            parts.append(f"Target {float(target):.2f}")
        except Exception:
            pass
    if rr:
        parts.append(f"R/R {rr}")
    if score:
        parts.append(f"Score {score}/100")
    caption = "   |   ".join(parts)

    try:
        with open(chart_path, "rb") as f:
            resp = requests.post(
                webhook_url,
                data={"payload_json": json.dumps({"content": caption})},
                files={"file": (f"{ticker}_chart.png", f, "image/png")},
                timeout=20,
            )
        if resp.status_code in (200, 204):
            print(f"  [discord] 圖表已發送: {ticker}")
            return True
        print(f"  [discord] 圖表發送失敗: HTTP {resp.status_code} — {resp.text[:120]}")
        return False
    except Exception as e:
        print(f"  [discord] 圖表發送異常: {e}")
        return False


def test_connection() -> bool:
    """
    发送测试消息，验证 Webhook 配置是否正确。

    Returns:
        True 表示连接正常
    """
    try:
        webhook_url = _get_webhook_url()
    except EnvironmentError as e:
        print(f"[discord] 配置错误: {e}")
        return False

    from datetime import datetime
    text = f"✅ Trading Agent 连接测试成功\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ok   = _post(webhook_url, text)
    if ok:
        print("[discord] 连接测试成功")
    else:
        print("[discord] 连接测试失败")
    return ok


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ok = test_connection()
    if ok:
        print("Discord 配置正确")
    else:
        print("Discord 配置有误，请检查 .env 中的 DISCORD_WEBHOOK_URL")
