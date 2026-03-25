# Trading Agent — 项目记忆文件

## 项目简介
基于 Minervini/Qullamaggie/O'Neil 交易体系的美股信号 agent
每天收盘后自动扫描全市场，识别 EP（Episodic Pivot）和 VCP 突破信号

## 技术栈
- Python 3.9，虚拟环境在 venv/
- 主要依赖：polygon、finvizfinance、anthropic、yfinance、requests、tiingo

## 数据源
- Polygon.io：全市场每日 OHLCV（Grouped Daily，一次调用）
- Finviz：基本面初筛（EPS/Sales增速、毛利率）
- Tiingo：个股历史日线（技术指标计算，EOD API，1000次/小时）
- yfinance：大盘环境（VIX/SPY）
- FMP：备用基本面数据源

## 当前进度
- P0 完成：项目骨架、依赖、API keys
- P1 完成：polygon_client、eodhd_client、market_env_client
- P2 完成：fundamental_filter、technical_filter
- P3 完成：signals/（ep_detector、vcp_scorer、bull_flag_detector、weinstein_detector、signal_generator）
- P4 完成：output/（report_formatter、discord_alert、log_writer）
- P5 完成：main.py 主调度器

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
- technical_filter：Tiingo拉历史数据（替代Stooq，无每日限额问题）
- EP detector：复用 technical_filter 缓存的 last_open/prev_close，零额外API请求
- VCP/BullFlag/Weinstein：各自调用 Tiingo，结果按日期缓存到 data/cache/
- 信号引擎：Claude API（claude-sonnet-4-6）综合判断，支持 BUY/BUY_RISKY/WATCH/SKIP
- 告警：Discord Webhook，每天收盘后推送（替代 Telegram）
- 缓存目录：data/cache/，当天缓存不重复请求（tiingo_{TICKER}_{date}.json 格式）
- risk_on=False 时：不退出，继续扫描，所有信号标注为 BUY_RISKY

## API Keys（在 .env 文件中）
ANTHROPIC_API_KEY, POLYGON_API_KEY, EODHD_API_KEY, FMP_API_KEY,
TIINGO_API_KEY, DISCORD_WEBHOOK_URL
