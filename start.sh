#!/bin/bash
set -e

# 1. 定义变量&构建
APP_NAME="serene_go"
LOG_FILE="output.log"

# 1.5 构建
mkdir -p logs
go build -o $APP_NAME main.go
chmod +x $APP_NAME

# 2. 杀掉已经在运行的旧进程
PID_FILE="${APP_NAME}.pid"
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p $OLD_PID > /dev/null; then
        echo "停止旧进程: $OLD_PID"
        kill $OLD_PID || kill -9 $OLD_PID
    fi
    rm "$PID_FILE"
fi

# 3. 归档旧日志
if [ -f "$LOG_FILE" ]; then
    mv $LOG_FILE "logs/$(date +%Y%m%d_%H%M%S).output.log"
fi

# 4. 启动新服务
nohup ./serene_go > $LOG_FILE 2>&1 &
echo $! > "${APP_NAME}.pid"

echo "服务 $APP_NAME 启动成功！"
