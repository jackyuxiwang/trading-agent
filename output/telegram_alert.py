"""
telegram_alert.py — Telegram Bot 推送模块

从 .env 读取 TELEGRAM_BOT_TOKEN 和 TELEGRAM_CHAT_ID。

send_report(report_text)    → 发送完整日报（自动分段）
send_signal_alert(signal)   → 发送单条信号简报
test_connection()           → 测试 bot 配置
"""

import os
import sys
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"
MAX_MSG_LEN  = 4096   # Telegram 单条消息字符上限


# ── 内部工具 ──────────────────────────────────────────────────────────────────

def _get_credentials() -> tuple:
    """读取 token 和 chat_id，任一缺失抛 EnvironmentError。"""
    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or token.startswith("your_"):
        raise EnvironmentError("TELEGRAM_BOT_TOKEN 未配置，请检查 .env 文件")
    if not chat_id or chat_id.startswith("your_"):
        raise EnvironmentError("TELEGRAM_CHAT_ID 未配置，请检查 .env 文件")
    return token, chat_id


def _send_message(token: str, chat_id: str, text: str) -> bool:
    """发送单条消息，返回是否成功。"""
    url  = TELEGRAM_API.format(token=token, method="sendMessage")
    data = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, data=data, timeout=10)
        result = resp.json()
        if not result.get("ok"):
            print(f"  [telegram] 发送失败: {result.get('description', '未知错误')}")
            return False
        return True
    except Exception as e:
        print(f"  [telegram] 请求异常: {e}")
        return False


def _split_text(text: str, max_len: int = MAX_MSG_LEN) -> list:
    """把长文本按段落边界切分成多条，每条不超过 max_len 字符。"""
    if len(text) <= max_len:
        return [text]

    parts  = []
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
    发送完整日报到 Telegram。超过4096字符自动分段。

    Args:
        report_text: format_daily_report() 生成的报告文本

    Returns:
        True 表示全部发送成功，False 表示任意一段失败
    """
    try:
        token, chat_id = _get_credentials()
    except EnvironmentError as e:
        print(f"[telegram] 配置错误，跳过发送: {e}")
        return False

    parts   = _split_text(report_text)
    success = True

    print(f"[telegram] 发送日报（共 {len(parts)} 段）…")
    for i, part in enumerate(parts, 1):
        ok = _send_message(token, chat_id, part)
        if not ok:
            success = False
            print(f"  [telegram] 第 {i}/{len(parts)} 段发送失败")
        else:
            print(f"  [telegram] 第 {i}/{len(parts)} 段发送成功")

    return success


def send_signal_alert(signal: dict) -> bool:
    """
    发送单个信号的简短提醒。

    Args:
        signal: 含 ticker, signal_type, entry_zone, stop_loss 的 dict

    Returns:
        True 表示发送成功
    """
    try:
        token, chat_id = _get_credentials()
    except EnvironmentError as e:
        print(f"[telegram] 配置错误，跳过发送: {e}")
        return False

    ticker     = signal.get("ticker", "")
    stype      = signal.get("signal_type", "")
    entry      = signal.get("entry_zone", "N/A")
    stop       = signal.get("stop_loss", "N/A")
    conf       = signal.get("confidence", "?")
    t1         = signal.get("target_1", "N/A")

    text = (
        f"🚨 <b>{ticker}</b> {stype}信号\n"
        f"入场: {entry} | 止损: {stop}\n"
        f"目标一: {t1} | 置信度: {conf}/10"
    )

    ok = _send_message(token, chat_id, text)
    if ok:
        print(f"[telegram] 信号提醒已发送: {ticker}")
    return ok


def test_connection() -> bool:
    """
    发送测试消息，验证 bot 配置是否正确。

    Returns:
        True 表示连接正常
    """
    try:
        token, chat_id = _get_credentials()
    except EnvironmentError as e:
        print(f"[telegram] 配置错误: {e}")
        return False

    from datetime import datetime
    text = f"✅ Trading Agent 连接测试\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    ok   = _send_message(token, chat_id, text)
    if ok:
        print("[telegram] 连接测试成功")
    else:
        print("[telegram] 连接测试失败")
    return ok


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ok = test_connection()
    if ok:
        print("Telegram 配置正确")
    else:
        print("Telegram 配置有误，请检查 .env 中的 TELEGRAM_BOT_TOKEN 和 TELEGRAM_CHAT_ID")
