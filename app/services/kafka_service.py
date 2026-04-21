#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka消息服务
负责处理与Kafka相关的消息发送和接收
"""

import json
import logging
import os
import threading
from typing import Optional
from kafka import KafkaProducer

from app.core.config import KafkaConfig

logger = logging.getLogger(__name__)

class KafkaService:
    """Kafka服务类"""
    
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        """实现单例模式，确保只有一个KafkaService实例"""
        if cls._instance is None:
            with cls._lock:
                # 双重检查确保线程安全
                if cls._instance is None:
                    cls._instance = super(KafkaService, cls).__new__(cls)
        return cls._instance

    # def __init__(self):
    #     """初始化Kafka生产者"""
    #     # 防止重复初始化
    #     if KafkaService._initialized:
    #         return
            
    #     self.producer = None
    #     self._init_producer()
    #     # 标记已初始化
    #     KafkaService._initialized = True

    def __init__(self):
        """初始化Kafka生产者"""
        # 防止重复初始化
        if KafkaService._initialized:
            return
            
        # 检查是否启用 Kafka
        if not KafkaConfig.enable:
            logger.info("Kafka 已禁用，跳过初始化（分类分级暂不使用 kafka）")
            self.producer = None
            KafkaService._initialized = True
            return
            
        self.producer = None
        self._init_producer()
        # 标记已初始化
        KafkaService._initialized = True

    def _init_producer(self):
        """初始化Kafka生产者实例"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KafkaConfig.bootstrap_servers,
                sasl_plain_username=KafkaConfig.sasl_plain_username,
                sasl_plain_password=KafkaConfig.sasl_plain_password,
                security_protocol=KafkaConfig.security_protocol,
                sasl_mechanism=KafkaConfig.sasl_mechanism,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
            )
            logger.info("Kafka生产者初始化成功")
        except Exception as e:
            logger.error(f"Kafka生产者初始化失败: {e}")
            self.producer = None

    def send_task_status_message(
        self, 
        task_uid: str, 
        state: int, 
        error_msg: Optional[str] = None
    ) -> bool:
        """
        发送任务状态消息到Kafka
        
        Args:
            task_uid: 任务唯一标识
            state: 任务状态 (1: 待执行, 2: 执行中, 3: 任务完成, 4: 任务失败)
            error_msg: 错误信息（当状态为4时提供）
            
        Returns:
            bool: 消息发送是否成功
        """
        # 检查是否启用 Kafka
        if not KafkaConfig.enable:
            logger.debug("Kafka 已禁用，跳过消息发送")
            return True
            
        if not self.producer:
            logger.error("Kafka生产者未初始化，无法发送消息")
            return False
        if not self.producer:
            logger.error("Kafka生产者未初始化，无法发送消息")
            return False

        # 验证状态值是否合法
        if state not in [1, 2, 3, 4]:
            logger.error(f"无效的任务状态: {state}")
            return False

        try:
            # 构造消息体
            message = {
                "PushContentType": 0,
                "PushDate": self._get_current_time_iso(),
                "TaskUId": task_uid,
                "Value": {
                    "State": state,
                    "ErrorMsg": error_msg if state == 4 else None
                }
            }

            # 发送消息
            future = self.producer.send(
                KafkaConfig.topic,
                value=message,
                partition=KafkaConfig.partition
            )
            
            # 等待发送结果
            record_metadata = future.get(timeout=10)
            logger.info(f"消息发送成功，task_uid: {task_uid}, state: {state}, topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
            return True
            
        except Exception as e:
            logger.error(f"发送Kafka消息失败，task_uid: {task_uid}, state: {state}, error: {e}")
            return False

    def _get_current_time_iso(self) -> str:
        """
        获取当前时间的ISO格式字符串
        
        Returns:
            str: ISO格式的时间字符串
        """
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()

    def close(self):
        """关闭Kafka生产者连接"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka生产者连接已关闭")
            # 重置初始化状态，允许重新初始化
            KafkaService._initialized = False


# 创建全局Kafka服务实例
kafka_service = KafkaService()