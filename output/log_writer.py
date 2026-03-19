"""
log_writer.py — CSV 日志写入模块

write_signals()     → data/logs/signals_history.csv  （每日信号记录）
write_scan_summary() → data/logs/scan_history.csv    （每日扫描统计）

文件不存在时自动创建并写入表头，存在时追加。
"""

import csv
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

LOG_DIR = Path(__file__).parent.parent / "data" / "logs"

SIGNALS_CSV = LOG_DIR / "signals_history.csv"
SCAN_CSV    = LOG_DIR / "scan_history.csv"

SIGNALS_FIELDS = [
    "date", "ticker", "company", "sector", "signal_type", "action", "confidence",
    "entry_zone", "stop_loss", "target_1", "target_2", "reason",
    "ep_score", "vcp_score", "technical_score",
    "gap_pct", "volume_ratio", "gain_60d",
]

SCAN_FIELDS = [
    "date", "total_market", "stage1_count", "stage2_count", "stage3_count",
    "ep_signals", "vcp_signals", "buy_signals",
    "risk_on", "vix", "spy_trend", "runtime_minutes",
]


def _ensure_csv(path: Path, fields: list) -> None:
    """如果 CSV 不存在，创建并写入表头。"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fields).writeheader()


def write_signals(signals: list, date: str = None) -> None:
    """
    把当天的信号结果追加写入 signals_history.csv。

    Args:
        signals: signal_generator.generate() 返回的 BUY 信号列表
        date:    日期字符串 "YYYY-MM-DD"，默认取今天
    """
    _ensure_csv(SIGNALS_CSV, SIGNALS_FIELDS)
    today = date or datetime.today().strftime("%Y-%m-%d")

    rows = []
    for s in signals:
        rows.append({
            "date":            today,
            "ticker":          s.get("ticker", ""),
            "company":         s.get("company", ""),
            "sector":          s.get("sector", ""),
            "signal_type":     s.get("signal_type", ""),
            "action":          s.get("action", ""),
            "confidence":      s.get("confidence", ""),
            "entry_zone":      s.get("entry_zone", ""),
            "stop_loss":       s.get("stop_loss", ""),
            "target_1":        s.get("target_1", ""),
            "target_2":        s.get("target_2", ""),
            "reason":          s.get("reason", ""),
            "ep_score":        s.get("ep_score", ""),
            "vcp_score":       s.get("vcp_score", ""),
            "technical_score": s.get("technical_score", ""),
            "gap_pct":         s.get("gap_pct", ""),
            "volume_ratio":    s.get("volume_ratio", ""),
            "gain_60d":        s.get("gain_60d", ""),
        })

    if not rows:
        print("[log_writer] 无信号可写入 signals_history.csv")
        return

    with SIGNALS_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SIGNALS_FIELDS)
        writer.writerows(rows)

    print(f"[log_writer] 已写入 {len(rows)} 条信号 → {SIGNALS_CSV.name}")


def write_scan_summary(summary: dict) -> None:
    """
    把当天的扫描统计追加写入 scan_history.csv。

    Args:
        summary: 包含扫描统计字段的 dict（字段见 SCAN_FIELDS）
    """
    _ensure_csv(SCAN_CSV, SCAN_FIELDS)
    today = summary.get("date") or datetime.today().strftime("%Y-%m-%d")

    row = {field: summary.get(field, "") for field in SCAN_FIELDS}
    row["date"] = today

    with SCAN_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=SCAN_FIELDS).writerow(row)

    print(f"[log_writer] 已写入扫描统计 → {SCAN_CSV.name}")


# ── 测试入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 写入一条模拟信号
    mock_signals = [{
        "ticker": "DAWN", "company": "Dawn Acquisition", "sector": "Healthcare",
        "signal_type": "EP", "action": "BUY", "confidence": 8,
        "entry_zone": "50.00–55.00", "stop_loss": "48.51",
        "target_1": "$63.00", "target_2": "$75.00",
        "reason": "EP突破后量价配合良好。",
        "ep_score": 75, "vcp_score": None, "technical_score": 80,
        "gap_pct": 11.1, "volume_ratio": 3.5, "gain_60d": 113.5,
    }]
    write_signals(mock_signals)

    # 写入一条模拟扫描统计
    mock_summary = {
        "total_market": 11848, "stage1_count": 2754,
        "stage2_count": 264, "stage3_count": 43,
        "ep_signals": 2, "vcp_signals": 5, "buy_signals": 3,
        "risk_on": True, "vix": 18.5, "spy_trend": "above_ma20",
        "runtime_minutes": 20.3,
    }
    write_scan_summary(mock_summary)

    # 验证内容
    print()
    print("signals_history.csv:")
    print(SIGNALS_CSV.read_text(encoding="utf-8"))
    print("scan_history.csv:")
    print(SCAN_CSV.read_text(encoding="utf-8"))
