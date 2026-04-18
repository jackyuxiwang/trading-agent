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
- FMP（Financial Modeling Prep）：基本面批量筛选主路径（company-screener，需付费套餐）
- Finviz：基本面筛选备用路径（FMP 不可用时，并发5线程逐只查询，约15分钟）
- yfinance：大盘环境（VIX/SPY）

## 当前进度
- P0 完成：项目骨架、依赖、API keys
- P1 完成：polygon_client、eodhd_client、market_env_client、tiingo_client（内部用Polygon）
- P2 完成：fundamental_filter（329只）、technical_filter（52-60只）
- P3 完成：ep_detector、vcp_scorer（含低吸策略）、bull_flag_detector、weinstein_detector、bottom_finder_detector、post_ep_tight_detector、cup_handle_detector、mean_reversion_detector、falling_wedge_detector、signal_generator
- P4 完成：discord_alert、report_formatter（含WATCH信号）、log_writer
- P5 完成：main.py 主调度器（并发Claude分析）
- P6 完成：即时 EP 扫描器（polygon_snapshot.py + realtime_ep_scanner.py + main_realtime.py）
- P7 完成：chart_generator（matplotlib 深色主題圖表 + Discord 圖片推送）
- Portfolio 完成：position_sizer、virtual_account、trade_logger、weekly_report
- 部署完成：run_daily.sh + cron（HKT 5:30 AM 周一至周五）+ pmset 自动唤醒（5:25 AM）
- 待完成：Livermore Pivotal Point

## 扫描漏斗
全市场11000+只 → Polygon量价初筛（~2800只）→ FMP/Finviz基本面精筛（~329只）→ 技术面确认（52-60只）→ 信号引擎 → Discord推送
Bottom Finder / Mean Reversion / Falling Wedge 并行路径：基本面候选（~329只）→ 各自detector（跳过技术面）
  Falling Wedge 额外预筛：距52W High≥15%（股价在下跌才可能有楔形）
Post-EP Tight / Cup Handle 使用 tech_candidates（同 EP/VCP）

## 筛选参数
- 市值：5亿–500亿美元
- EPS增速：>10%，营收增速：>10%，毛利率：>20%
- 成交量：>50万，股价：>5美元
- Stage2：收盘价 > MA20 且 > MA50
- technical_score >= 35

## 九种信号形态
1. EP（Episodic Pivot）：催化剂驱动跳空，缺口>5%，放量，收阳线
2. VCP（Volatility Contraction Pattern）：三段波动递减，量缩后突破
   - 含低吸策略（VCP_CHEAT_ENTRY）：止损>10%时计算低吸买入区，评估可行性
3. Bull Flag：旗杆涨幅>15%，旗面量缩浅回调，放量突破
4. Weinstein Stage 2：30周均线判断Stage，识别S2突破和回调买点
5. Bottom Finder：长期下跌（>=35%）→底部築底（25-150天）→ Higher Lows → 量缩 → 放量突破
   - 使用 fund_candidates（非 tech_candidates），跳过 Stage 2 硬性条件
   - Polygon 365天日线，内建快取（history_{ticker}_{from}_{to}.json）
6. Post-EP Tight（post_ep_tight_detector）：EP跳空后3–10天紧密盘整，量缩，等待第二段突破
   - 搜索过去10天内EP事件，验证盘整振幅≤50%缺口、均量≤50% EP量、缺口未回补
   - entry=盘整高点, stop=EP开盘×0.99, target=entry+EP涨幅×0.618
7. Cup & Handle（cup_handle_detector）：O'Neil杯柄形态，杯深15–45%，右侧回升≥85%，柄部量缩后突破
   - 杯宽最长220天，柄5–25天，量缩<0.8x，突破量≥1.5x
   - entry=柄高, stop=柄低×0.99, target=entry+杯深（量升幅度）
8. Mean Reversion（mean_reversion_detector）：优质股超跌均值回归，RSI<30+BB下轨+MA50偏离>15%
   - 使用 fund_candidates，需≥2个超卖信号 + ≥1个反弹信号（锤子/吞没/RSI背离）
   - entry=当前价, stop=近5日低点×0.97, target=MA50；R/R≥1.5
9. Falling Wedge（falling_wedge_detector）：下降楔形突破，统计胜率约80%
   - 下跌中形成 Lower Highs + Lower Lows 收斂楔形（40–120天），放量突破上方趋势线
   - 使用 fund_candidates（距52W High≥15%预筛），线性回归拟合趋势线，RSI背离加分
   - entry=阻力线延伸值, stop=最近SwingLow×0.98, target=entry+楔形入口高度（measured move）
   - 评分系统：楔形天数+Swing对数+R²拟合度+量缩+突破量能+RSI背离（0–100分）

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
- fundamental_filter：两步走
  * Step1：Polygon量价初筛（~2800只）
  * Step2：FMP company-screener 批量一次请求取交集（主路径，秒级）；FMP 402/不可用时 fallback 到 Finviz 并发5线程逐只查询（~15分钟）
  * FMP 免费版不支持 company-screener（需付费套餐）；当前使用 Finviz fallback
  * 所有缓存按日期命名（fundamental_candidates_{YYYY-MM-DD}.json），每天只跑一次
- technical_filter：Polygon历史数据计算技术指标（tiingo_client.py内部用Polygon aggregates）
- EP detector：复用 technical_filter 缓存的 last_open/prev_close，零额外API请求
- VCP/BullFlag/Weinstein：各自调用 tiingo_client（内部Polygon），结果按日期缓存到 data/cache/
- VCP低吸（VCP_CHEAT_ENTRY）：止损>10%时启用，计算低吸区+可行性评分（0-100），Claude据此判断WATCH/BUY/SKIP
- 信号引擎：Claude API（claude-sonnet-4-6）并发分析，最多3个并发（MAX_WORKERS=3），529 自动重试+haiku降级
- 告警：Discord Webhook，每天收盘后推送（BUY+WATCH信号均显示）
- 缓存目录：data/cache/，当天缓存不重复请求（tiingo_{TICKER}_{date}.json 格式）
- risk_on=False 时：不退出，继续扫描，信号标注风险警告，报告置顶红色提示
- 定时运行：run_daily.sh + cron（HKT 5:30 AM 周一至周五）+ pmset 唤醒（5:25 AM）；日志写入 logs/daily_{date}.log，保留30天
- 即时 EP 扫描器（main_realtime.py）：
  * 数据源：Polygon /v2/snapshot gainers + /v3/snapshot 批量查询（无需Claude API）
  * 盘前 4:00–9:30 ET：scan_premarket()，漲幅 ≥ 5%，每 5 分钟扫描
  * 开盘 9:30–10:30 ET：scan_opening()，跳空 ≥ 5%，BUY/WATCH/FADE 分类
  * 信号去重：in-memory sent_tickers，每交易日重置
  * 用法：python main_realtime.py --test | --once | (持续模式)
- 圖表模組（chart_generator.py）：
  * matplotlib Agg（無頭服務器渲染），深色主題 #0d1117，1200×700px
  * OHLC 蠟燭 + MA20（橙）+ MA50（藍）+ 成交量子圖
  * Entry（綠虛線）/ Stop（紅虛線）/ Target（藍dash-dot），右側價格標籤
  * 信號疊加：FALLING_WEDGE 趨勢線 | EP 缺口帶 | VCP/BullFlag 基部區 | MeanReversion BB
  * FALLING_WEDGE 趨勢線需信號 dict 含 h_slope/h_intercept/l_slope/l_intercept/peak_bar_from_end
  * main.py CHART_ENABLED=True：收盤後為所有 BUY+WATCH 信號生成並推送圖表
  * main_realtime.py：開盤 BUY 信號自動附帶圖表推送
  * send_signal_with_chart(signal, chart_path)：multipart/form-data 上傳到 Discord
  * 圖表緩存在 output/charts/，每日自動清理（cleanup_old_charts(days=1)）

## API Keys（在 .env 文件中）
ANTHROPIC_API_KEY, POLYGON_API_KEY, EODHD_API_KEY, FMP_API_KEY,
TIINGO_API_KEY, DISCORD_WEBHOOK_URL
