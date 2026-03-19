"""
test_env.py — 环境与 API 连通性验证

运行方式: python test_env.py
"""

import os
from dotenv import load_dotenv

load_dotenv()

PLACEHOLDERS = {
    "your_polygon_api_key_here",
    "your_eodhd_api_key_here",
    "your_anthropic_api_key_here",
}

results = []


def check(label, passed, detail=""):
    if passed:
        print(f"✅ {label}")
    else:
        print(f"❌ {label}: {detail}")
    results.append(passed)


# ── 1. 检查 .env 中的 Key ────────────────────────────────────────────────────
print("\n── 1. 检查 .env Key ──")

required_keys = ["ANTHROPIC_API_KEY", "POLYGON_API_KEY", "EODHD_API_KEY"]
all_keys_ok = True
for key in required_keys:
    val = os.getenv(key, "")
    if not val or val in PLACEHOLDERS:
        print(f"❌ {key}: 未设置或仍为占位符")
        all_keys_ok = False
    else:
        print(f"✅ {key}: 已设置 ({val[:6]}...)")

results.append(all_keys_ok)

# ── 2. 测试 Polygon.io ───────────────────────────────────────────────────────
print("\n── 2. 测试 Polygon.io ──")
try:
    import requests

    polygon_key = os.getenv("POLYGON_API_KEY")
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day"
        f"/2025-01-01/2025-01-10?apiKey={polygon_key}"
    )
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    results_list = data.get("results", [])
    if results_list:
        first = results_list[0]
        print(f"  第一条记录: {first}")
        check("Polygon.io 日线数据", True)
    else:
        check("Polygon.io 日线数据", False, f"返回为空，status={data.get('status')}")
except Exception as e:
    check("Polygon.io 日线数据", False, str(e))

# ── 3. 测试 EODHD ────────────────────────────────────────────────────────────
print("\n── 3. 测试 EODHD ──")
try:
    import requests

    eodhd_key = os.getenv("EODHD_API_KEY")
    url = f"https://eodhd.com/api/real-time/AAPL.US?api_token={eodhd_key}&fmt=json"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    print(f"  返回结果: {data}")
    check("EODHD 实时数据", True)
except Exception as e:
    check("EODHD 实时数据", False, str(e))

# ── 4. 测试 yfinance ─────────────────────────────────────────────────────────
print("\n── 4. 测试 yfinance ──")
try:
    import yfinance as yf

    spy = yf.Ticker("SPY")
    hist = spy.history(period="5d")

    if hist.empty:
        check("yfinance SPY 近5日收盘价", False, "返回数据为空")
    else:
        close = hist["Close"].round(2)
        print(f"  SPY 近5日收盘价:\n{close.to_string()}")
        check("yfinance SPY 近5日收盘价", True)
except Exception as e:
    check("yfinance SPY 近5日收盘价", False, str(e))

# ── 汇总 ─────────────────────────────────────────────────────────────────────
print("\n── 汇总 ──")
passed = sum(results)
failed = len(results) - passed
print(f"共 {len(results)} 项：✅ {passed} 项通过，❌ {failed} 项失败")
