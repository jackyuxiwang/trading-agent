# Trading Agent — 项目记忆文件

## 项目简介
基于 Minervini/Qullamaggie/O'Neil 交易体系的美股信号 agent
每天收盘后自动扫描全市场，识别 EP（Episodic Pivot）和 VCP 突破信号

## 技术栈
- Python 3.9，虚拟环境在 venv/
- 主要依赖：polygon、finvizfinance、stooq、anthropic、yfinance、python-telegram-bot

## 数据源
- Polygon.io：全市场每日 OHLCV（Grouped Daily，一次调用）
- Finviz：基本面初筛（EPS/Sales增速、毛利率）
- Stooq：个股历史日线（技术指标计算）
- yfinance：大盘环境（VIX/SPY）
- FMP：备用基本面数据源

## 当前进度
- P0 完成：项目骨架、依赖、API keys
- P1 完成：polygon_client、eodhd_client、market_env_client
- P2 完成：fundamental_filter（264只）、technical_filter（43只）
- P3 进行中：signals/ 目录（ep_detector、vcp_scorer、signal_generator）
- P4 待开始：output/（report_formatter、telegram_alert、log_writer）
- P5 待开始：main.py 主调度器

## 筛选漏斗
全市场11848只 → Polygon量价初筛2754只 → Finviz基本面精筛264只 → 技术面确认43只 → 信号引擎

## 筛选参数
- 市值：5亿–500亿美元
- EPS增速：>10%，营收增速：>10%，毛利率：>20%
- 成交量：>50万，股价：>5美元
- Stage2：收盘价 > MA20 且 > MA50
- technical_score >= 35

## 信号体系
四种信号类型：
1. EP（Episodic Pivot）：催化剂驱动跳空，缺口>10%，放量，突破开盘高点买入
2. VCP（Volatility Contraction Pattern）：整理收缩后放量突破，回调幅度递减
3. Bull Flag：旗杆（快速上涨>15%，3-10天）→ 旗面（量缩浅回调3-15%）→ 今日放量突破旗面高点
4. Weinstein Stage 2：30周均线上翘，突破积累区（S2突破）或回调到30周线支撑（S2回调）

## 关键设计决策
- fundamental_filter：Polygon量价初筛 + Finviz逐只查基本面
- technical_filter：Stooq拉历史数据，避免Polygon免费档限速
- 信号引擎：Claude API（claude-sonnet-4-6）综合判断
- 告警：Telegram Bot，每天收盘后推送
- 缓存目录：data/cache/，当天缓存不重复请求

## API Keys（在 .env 文件中）
ANTHROPIC_API_KEY, POLYGON_API_KEY, EODHD_API_KEY, FMP_API_KEY,
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
