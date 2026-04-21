#!/bin/bash

# 激活虚拟环境
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "警告: 未找到虚拟环境"
fi


set -e  # 遇到错误时退出

# 颜色输出定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # 无颜色

# 默认值
HOST="0.0.0.0"
PORT="64001"
LOG_LEVEL="info"

show_help() {
    echo "用法: ./manage_interp_kb.sh [命令]"
    echo ""
    echo "命令:"
    echo "  start              启动整合应用服务"
    echo "  stop               停止应用服务"
    echo "  stop -a            停止应用服务并 kill 所有python进程"
    echo "  status             查看应用状态"
    echo "  logs               查看应用日志"
    echo "  dev                开发模式启动"
    echo "  clean              清理临时文件和缓存"
    echo "  help               显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  ./manage_interp_kb.sh start     # 启动应用"
    echo "  ./manage_interp_kb.sh logs      # 查看应用日志"
    echo "  ./manage_interp_kb.sh dev       # 开发模式启动"
}

# 检查是否在正确的目录中
check_directory() {
    if [ ! -f "main.py" ]; then
        echo -e "${RED}错误: 未找到 main.py 文件，请在项目根目录中运行此脚本${NC}"
        exit 1
    fi
}

# 检查进程是否正在运行
is_running() {
    if [ -f ".pid" ]; then
        local pid=$(cat .pid)
        if ps -p $pid > /dev/null 2>&1; then
            return 0
        else
            rm -f .pid
            return 1
        fi
    else
        return 1
    fi
}

# 启动应用服务
start_app() {
    check_directory
    
    if is_running; then
        echo -e "${YELLOW}警告: 应用已在运行中${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}正在启动整合应用...${NC}"
    
    # 设置环境变量
    export LOG_VERBOSITY=$LOG_LEVEL
    
    # 创建日志目录
    mkdir -p logs
    
    # 后台启动应用
    nohup uv run main.py > logs/app.log 2>&1 &
    local pid=$!
    
    # 保存PID
    echo $pid > .pid
    
    # 等待几秒确认应用已启动
    sleep 3
    
    if ps -p $pid > /dev/null 2>&1; then
        echo -e "${GREEN}整合应用已成功启动，PID: $pid${NC}"
        echo -e "${GREEN}监听地址: http://$HOST:$PORT${NC}"
        echo -e "${GREEN}知识库管理API: http://$HOST:$PORT/api/v1${NC}"
        echo -e "${GREEN}规范解读API: http://$HOST:$PORT/api/v1${NC}"
    else
        echo -e "${RED}错误: 应用启动失败，请检查日志${NC}"
        rm -f .pid
        exit 1
    fi
}

# 开发模式启动
dev_mode() {
    check_directory
    
    echo -e "${GREEN}以开发模式启动整合应用...${NC}"
    echo -e "${YELLOW}按 Ctrl+C 停止应用${NC}"
    
    # 设置环境变量
    export LOG_VERBOSITY=$LOG_LEVEL
    
    # 创建日志目录
    mkdir -p logs
    
    # 使用uvicorn启动并启用重载
    uv run uvicorn main:app --host $HOST --port $PORT --reload
}

# 预处理停止操作 - 检查并处理活跃任务
pre_stop_cleanup() {
    echo -e "${GREEN}检查活跃任务并发送失败状态...${NC}"
    
    # 检查是否存在预处理脚本
    PRE_STOP_SCRIPT="/usr/local/app/volume/kb_cls_grad/interpretation_specification/services/pre_stop_handler.py"
    if [ -f "$PRE_STOP_SCRIPT" ]; then
        # 运行预处理脚本来检查活跃任务并发送失败消息
        python3 "$PRE_STOP_SCRIPT"
        echo -e "${GREEN}预处理完成，已处理活跃任务${NC}"
    else
        echo -e "${YELLOW}预处理脚本不存在，跳过任务清理${NC}"
    fi
}

# 停止应用服务
stop_app() {
    local aggressive_kill=false
    
    # 检查是否传入了-a参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -a|--aggressive)
                aggressive_kill=true
                shift
                ;;
            *)
                shift
                ;;
        esac
    done
    
    if is_running; then
        # 在停止应用前执行预处理操作
        pre_stop_cleanup
        
        local pid=$(cat .pid)
        echo -e "${GREEN}正在停止整合应用 (PID: $pid)...${NC}"
        kill $pid
        
        # 等待进程结束
        local count=0
        while ps -p $pid > /dev/null 2>&1; do
            sleep 1
            count=$((count + 1))
            if [ $count -gt 10 ]; then
                echo -e "${YELLOW}强制终止应用...${NC}"
                kill -9 $pid
                break
            fi
        done
        
        rm -f .pid
        echo -e "${GREEN}整合应用已停止${NC}"
    else
        # 即使应用未运行，也要检查是否有活跃任务
        pre_stop_cleanup
        echo -e "${YELLOW}整合应用未在运行${NC}"
    fi
    
    # 如果使用了-a参数，则 kill 所有python进程
    if [ "$aggressive_kill" = true ]; then
        echo -e "${GREEN}正在 kill 所有Python进程...${NC}"
        pkill -9 python 2>/dev/null || true
        echo -e "${GREEN}所有Python进程已被 kill ${NC}"
    fi
}

# 查看应用状态
status_app() {
    if is_running; then
        local pid=$(cat .pid)
        echo -e "${GREEN}整合应用正在运行 (PID: $pid)${NC}"
    else
        echo -e "${YELLOW}整合应用未运行${NC}"
    fi
}

# 查看日志
show_logs() {
    if [ ! -f "logs/app.log" ]; then
        echo -e "${YELLOW}日志文件不存在${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}显示最近的日志内容:${NC}"
    tail -f logs/app.log
}

# 清理临时文件和缓存
clean_project() {
    echo -e "${GREEN}清理项目...${NC}"
    
    # 删除Python缓存文件
    find . -type d -name "__pycache__" -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete
    find . -type f -name "*.pyo" -delete
    find . -type f -name "*~" -delete
    find . -type f -name "*py.class" -delete
    
    # 删除PID文件
    rm -f .pid
    
    # 删除日志文件
    rm -f logs/app.log
    
    echo -e "${GREEN}清理完成${NC}"
}

# 主逻辑
case "$1" in
    start)
        start_app
        ;;
    stop)
        stop_app "${@:2}"  # 传递除第一个参数外的所有参数
        ;;
    status)
        status_app
        ;;
    logs)
        show_logs
        ;;
    dev)
        dev_mode
        ;;
    clean)
        clean_project
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo -e "${RED}未知命令: $1${NC}"
        show_help
        exit 1
        ;;
esac