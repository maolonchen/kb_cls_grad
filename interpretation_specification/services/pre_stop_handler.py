#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
停止服务前的预处理脚本
检查是否有正在处理的任务，如果有则向Kafka发送任务失败的消息
"""

import json
import os
import sys
import datetime
import glob
import time
from pathlib import Path

# 添加项目根目录到系统路径
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

# 导入项目配置
try:
    from config.settings import PROJECT_ROOT, ENABLE_KAFKA
    from src.utils.logger import setup_logger
except ImportError as e:
    print(f"导入项目配置失败: {e}")
    # 设置默认值
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ENABLE_KAFKA = True
    def setup_logger(name):
        import logging
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

logger = setup_logger(__name__)

def get_last_line_of_file(filepath):
    """
    获取文件的最后一行
    """
    try:
        with open(filepath, 'rb') as f:
            # 移动到文件末尾
            f.seek(0, 2)  # SEEK_END
            file_size = f.tell()
            
            # 从文件末尾向前搜索
            buffer_size = 1024
            buffer = b''
            
            # 如果文件很小，调整缓冲区大小
            if file_size < buffer_size:
                buffer_size = file_size
            
            # 向前搜索直到找到换行符或到达文件开头
            position = file_size - buffer_size
            while position >= 0:
                f.seek(position)
                chunk = f.read(buffer_size)
                buffer = chunk + buffer
                
                # 查找最后的换行符
                lines = buffer.split(b'\n')
                if len(lines) > 1:
                    # 至少有一个完整的行
                    last_complete_line = lines[-2]  # 取倒数第二个（最后一个可能是空的）
                    if last_complete_line.strip():
                        return last_complete_line.decode('utf-8', errors='ignore').strip()
                
                position -= buffer_size
                buffer_size = min(buffer_size, position + buffer_size)
            
            # 如果整个文件都读完了还没找到多个换行符，返回整个缓冲区
            lines = buffer.split(b'\n')
            for line in reversed(lines):
                if line.strip():
                    return line.decode('utf-8', errors='ignore').strip()
        
        # 如果以上方法失败，回退到读取整个文件
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in reversed(lines):
                if line.strip():
                    return line.strip()
    except Exception as e:
        logger.error(f"读取文件 {filepath} 最后一行时出错: {e}")
        return ""

def get_active_tasks():
    """
    检查是否有正在处理的任务
    通过检查处理日志文件的内容来判断任务是否已完成
    """
    active_tasks = []
    
    try:
        # 检查处理日志目录中是否有处理日志文件
        log_dir = os.path.join(PROJECT_ROOT, "logs")
        if os.path.exists(log_dir):
            # 查找所有的处理日志文件
            for log_file in glob.glob(os.path.join(log_dir, "processing_*.log")):
                # 检查文件是否在最近几分钟内被修改过（表明任务可能仍在运行）
                mod_time = os.path.getmtime(log_file)
                current_time = time.time()
                
                # 如果文件在30分钟内被修改过，才检查其内容
                if current_time - mod_time < 1800:  # 30分钟 = 1800秒
                    # 获取文件最后一行
                    last_line = get_last_line_of_file(log_file)
                    
                    # 检查最后一行是否包含"数据处理流程执行完成"
                    if last_line and "数据处理流程执行完成" not in last_line:
                        # 从文件名提取任务ID
                        filename = os.path.basename(log_file)
                        if '_' in filename:
                            parts = filename.split('_')
                            if len(parts) >= 3:
                                task_id = parts[1]  # 提取规范ID作为任务标识的一部分
                                active_tasks.append({
                                    'task_id': task_id,
                                    'log_file': log_file,
                                    'mod_time': mod_time,
                                    'last_line': last_line
                                })
                                logger.info(f"发现未完成的任务: {task_id}, 日志: {filename}, 最后一行: {last_line}")
                    else:
                        logger.debug(f"任务已完成: {log_file}, 最后一行: {last_line}")
        
    except Exception as e:
        logger.error(f"检查活跃任务时出错: {e}")
    
    return active_tasks

def initialize_kafka_producer():
    """
    初始化Kafka生产者
    """
    try:
        from kafka import KafkaProducer
        if ENABLE_KAFKA:
            producer = KafkaProducer(
                bootstrap_servers=['192.168.10.15:39092'],
                sasl_plain_username='sw',
                sasl_plain_password='siweicn123',
                security_protocol='SASL_PLAINTEXT',
                sasl_mechanism='PLAIN',
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
            )
            logger.info("Kafka Producer 初始化成功")
            return producer
        else:
            logger.info("Kafka 已禁用，跳过初始化（分类分级暂不使用 kafka）")
            return None
    except ImportError:
        logger.error("无法导入kafka模块，请安装kafka-python: pip install kafka-python")
        return None
    except Exception as e:
        logger.error(f"Kafka Producer 初始化失败: {e}")
        return None

def send_kafka_task_status(producer, task_uid: str, state: int, error_msg: str = None):
    """
    向 Kafka 发送任务状态消息
    :param producer: Kafka生产者实例
    :param task_uid: 任务唯一标识
    :param state: 任务状态 (1: 待执行, 2: 执行中, 4: 任务完成, 6: 任务失败)
    :param error_msg: 错误信息 (仅在失败时使用)
    """
    if not ENABLE_KAFKA or not producer:
        logger.warning("Kafka未启用或生产者未初始化，跳过发送消息")
        return
    
    message = {
        "PushContentType": 0,
        "PushDate": datetime.datetime.now().isoformat(),
        "TaskUId": task_uid,
        "Value": {
            "State": state,
            "ErrorMsg": error_msg
        }
    }
    
    try:
        producer.send('scip_specification_analysis_task', message)
        producer.flush()  # 添加flush确保消息发送
        logger.info(f"已发送 Kafka 消息: {message}")
    except Exception as e:
        logger.error(f"发送 Kafka 消息失败: {e}", exc_info=True)

def main():
    """
    主函数
    """
    logger.info("开始检查活跃任务...")
    
    # 获取活跃任务
    active_tasks = get_active_tasks()
    
    if not active_tasks:
        logger.info("没有发现活跃的任务，无需发送失败消息")
        return 0
    
    logger.info(f"发现 {len(active_tasks)} 个未完成的任务:")
    for task in active_tasks:
        logger.info(f"  - 任务ID: {task.get('task_id', 'unknown')}, 日志: {os.path.basename(task.get('log_file', ''))}")
    
    # 初始化Kafka生产者
    producer = initialize_kafka_producer()
    if not producer:
        logger.error("无法初始化Kafka生产者，无法发送失败消息")
        return 1
    
    # 为每个活跃任务发送失败状态
    for task in active_tasks:
        task_id = task.get('task_id', 'unknown')
        error_msg = "服务停止，任务中断"
        
        logger.info(f"为任务 {task_id} 发送失败状态...")
        send_kafka_task_status(producer, task_id, state=6, error_msg=error_msg)
    
    # 等待消息发送完成
    time.sleep(2)
    
    logger.info("活跃任务检查和状态更新完成")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)