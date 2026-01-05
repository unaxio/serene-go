#!/bin/bash

# 1. 定义变量&构建
APP_NAME="serene_go""
LOG_FILE="output.log"
go build -o $APP_NAME main.go
chmod +x app

# 2. 杀掉已经在运行的旧进程
PID=$(ps -ef | grep $APP_NAME | grep -v grep | awk '{print $2}')
if [ -n "$PID" ]; then
    echo "停止旧进程: $PID"
    kill -9 $PID
fi

# 3. 归档旧日志
if [ -f "$LOG_FILE" ]; then
    mv $LOG_FILE "logs/$(date +%Y%m%d_%H%M%S).output.log"
fi

# 4. 启动新服务
nohup ./serene_go > $LOG_FILE 2>&1 &

echo "服务 $APP_NAME 启动成功！"
