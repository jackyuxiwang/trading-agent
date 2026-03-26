#!/bin/bash
# run_daily.sh — 每日收盘后自动扫描脚本
# cron 调用：30 5 * * 1-5 /Users/jackwang/trading-agent/run_daily.sh

set -e

PROJ_DIR="/Users/jackwang/trading-agent"
VENV="$PROJ_DIR/venv/bin/activate"
LOG_DIR="$PROJ_DIR/logs"
LOG_FILE="$LOG_DIR/daily_$(date +%Y-%m-%d).log"

# 进入项目目录
cd "$PROJ_DIR"

# 激活虚拟环境
source "$VENV"

echo "========================================"  | tee -a "$LOG_FILE"
echo "  Trading Agent 启动: $(date)"             | tee -a "$LOG_FILE"
echo "========================================"  | tee -a "$LOG_FILE"

# 运行主扫描（stdout + stderr 同时写入日志和终端）
PYTHONUNBUFFERED=1 python main.py 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

echo ""                                           | tee -a "$LOG_FILE"
echo "  结束: $(date)  exit=$EXIT_CODE"           | tee -a "$LOG_FILE"
echo "========================================"  | tee -a "$LOG_FILE"

# 保留最近 30 天日志，清理旧日志
find "$LOG_DIR" -name "daily_*.log" -mtime +30 -delete

exit $EXIT_CODE
