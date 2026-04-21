#!/bin/bash

# 激活虚拟环境
source .venv/bin/activate

# 应用管理脚本
# 用于启动和停止应用服务

# 定义路径变量
APP_PATH="/usr/local/app/volume/interpretation_specification/interpretation_specification/scripts/app.py"
LOG_DIR="/usr/local/app/volume/interpretation_specification/interpretation_specification/logs"
PID_FILE="$LOG_DIR/PID.txt"

# 确保日志目录存在
mkdir -p "$LOG_DIR"

# 显示使用说明
usage() {
    echo "用法: $0 {start|stop|restart|status}"
    echo "  start   - 启动应用"
    echo "  stop    - 停止应用"
    echo "  restart - 重启应用"
    echo "  status  - 查看应用状态"
    exit 1
}

# 启动应用
start() {
    # 检查是否已经在运行
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "应用已在运行 (PID: $PID)"
            exit 1
        fi
    fi

    echo "正在启动应用..."

    # 检查Python程序是否存在
    if [ ! -f "$APP_PATH" ]; then
        echo "错误: Python程序不存在 -> $APP_PATH"
        exit 1
    fi

    # 启动程序并捕获PID
    nohup python3 "$APP_PATH" > "$LOG_DIR/output.log" 2>&1 &
    PYTHON_PID=$!

    # 写入PID文件
    echo $PYTHON_PID > "$PID_FILE"
    
    # 验证是否成功启动
    if ps -p "$PYTHON_PID" > /dev/null 2>&1; then
        echo "应用已启动，PID: $PYTHON_PID"
        echo "日志文件: $LOG_DIR/output.log"
        echo "PID文件: $PID_FILE"
    else
        echo "应用启动失败"
        rm -f "$PID_FILE"
        exit 1
    fi
}

# 停止应用
stop() {
    echo "正在停止应用..."
    
    if [ ! -f "$PID_FILE" ]; then
        echo "应用未运行 (PID文件不存在)"
        exit 1
    fi
    
    PID=$(cat "$PID_FILE")
    
    if ps -p "$PID" > /dev/null 2>&1; then
        kill "$PID"
        
        # 等待进程结束
        TIMEOUT=30
        COUNT=0
        while ps -p "$PID" > /dev/null 2>&1; do
            sleep 1
            COUNT=$((COUNT + 1))
            if [ $COUNT -ge $TIMEOUT ]; then
                echo "优雅停止超时，强制 kill 进程"
                kill -9 "$PID"
                break
            fi
        done
        
        rm -f "$PID_FILE"
        echo "应用已停止"
    else
        echo "应用未在运行 (PID: $PID)"
        rm -f "$PID_FILE"
    fi
}

# 查看应用状态
status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "应用正在运行 (PID: $PID)"
        else
            echo "应用未运行 (PID文件存在但进程不存在)"
        fi
    else
        echo "应用未运行"
    fi
}

# 根据参数执行相应操作
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        # 等待一段时间确保进程完全停止
        sleep 2
        start
        ;;
    status)
        status
        ;;
    *)
        usage
        ;;
esac

exit 0


# 启动应用: ./scripts/manage_app.sh start
# 停止应用: ./scripts/manage_app.sh stop
# 重启应用: ./scripts/manage_app.sh restart
# 查看状态: ./scripts/manage_app.sh status