# Trading Agent — 项目记忆文件

## 项目简介
基于 Minervini/Qullamaggie/O'Neil/Livermore 交易体系的美股信号 agent
每天收盘后自动扫描全市场，识别多种形态信号并推送到 Discord

## 技术栈
- Python 3.9，虚拟环境在 venv/
- 主要依赖：polygon、finvizfinance、anthropic、yfinance、python-telegram-bot

## 数据源
- Polygon.io Starter（$29/月）：
  * 全市场每日 OHLCV（Grouped Daily，一次调用）
  * 个股历史日线（替代 Stooq/Tiingo，无限速）
- Finviz：基本面初筛（EPS/Sales增速、毛利率）
- yfinance：大盘环境（VIX/SPY）
- FMP：备用基本面数据源

## 当前进度
- P0 完成：项目骨架、依赖、API keys
- P1 完成：polygon_client、eodhd_client、market_env_client、tiingo_client（内部用Polygon）
- P2 完成：fundamental_filter（331只）、technical_filter（52-60只）
- P3 完成：ep_detector、vcp_scorer（含低吸策略）、bull_flag_detector、weinstein_detector、signal_generator
- P4 完成：discord_alert、report_formatter（含WATCH信号）、log_writer
- P5 完成：main.py 主调度器（并发Claude分析）
- Portfolio 完成：position_sizer、virtual_account、trade_logger、weekly_report
- 待完成：部署到服务器自动运行、Cup & Handle、Livermore Pivotal Point

## 扫描漏斗
全市场11000+只 → Polygon量价初筛 → Finviz基本面精筛331只 → 技术面确认52-60只 → 信号引擎 → Discord推送

## 筛选参数
- 市值：5亿–500亿美元
- EPS增速：>10%，营收增速：>10%，毛利率：>20%
- 成交量：>50万，股价：>5美元
- Stage2：收盘价 > MA20 且 > MA50
- technical_score >= 35

## 四种信号形态
1. EP（Episodic Pivot）：催化剂驱动跳空，缺口>5%，放量，收阳线
2. VCP（Volatility Contraction Pattern）：三段波动递减，量缩后突破
   - 含低吸策略（VCP_CHEAT_ENTRY）：止损>10%时计算低吸买入区，评估可行性
3. Bull Flag：旗杆涨幅>15%，旗面量缩浅回调，放量突破
4. Weinstein Stage 2：30周均线判断Stage，识别S2突破和回调买点

## 信号类型
- BUY：大盘正常，直接买入
- BUY_RISKY：大盘风险偏高，可买但仓位减半
- WATCH：大盘风险高或形态未完全确认，等待观察
- VCP_CHEAT_ENTRY：低吸策略，等回调到更低买入区再介入
- SKIP：不符合条件

## 仓位管理规则（10万港币账户）
- 单笔风险：总资金1% = 1000港币
- 最大仓位：单只不超过总资金20%
- 止损：入场价-7%到-8%，硬性执行
- 止盈：+20%减仓1/3，+35%再减仓1/3，剩余用10日均线移动止损
- 大盘风险高时仓位减半

## 关键设计决策
- fundamental_filter：Polygon量价初筛 + Finviz逐只查基本面
- technical_filter：Polygon历史数据计算技术指标（tiingo_client.py内部用Polygon aggregates）
- EP detector：复用 technical_filter 缓存的 last_open/prev_close，零额外API请求
- VCP/BullFlag/Weinstein：各自调用 tiingo_client（内部Polygon），结果按日期缓存到 data/cache/
- VCP低吸（VCP_CHEAT_ENTRY）：止损>10%时启用，计算低吸区+可行性评分（0-100），Claude据此判断WATCH/BUY/SKIP
- 信号引擎：Claude API（claude-sonnet-4-6）并发分析，最多5个并发，串行提速约2x
- 告警：Discord Webhook，每天收盘后推送（BUY+WATCH信号均显示）
- 缓存目录：data/cache/，当天缓存不重复请求（tiingo_{TICKER}_{date}.json 格式）
- risk_on=False 时：不退出，继续扫描，信号标注风险警告，报告置顶红色提示

## API Keys（在 .env 文件中）
ANTHROPIC_API_KEY, POLYGON_API_KEY, EODHD_API_KEY, FMP_API_KEY,
TIINGO_API_KEY, DISCORD_WEBHOOK_URL
