#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
健康检查监控服务
负责定期检查算力服务的健康状态，并在需要时调用初始化接口
"""

import asyncio
import logging
import aiohttp
from typing import Dict, Any, List
from datetime import datetime
from .health_check_service import health_check_service

logger = logging.getLogger(__name__)


class HealthMonitorService:
    """健康检查监控服务类，用于定期检查算力服务状态并根据结果调用相应接口"""
    
    def __init__(self, 
                 init_api_url: str = None,
                 check_interval: int = None,
                 retry_count: int = None,
                 unhealthy_wait_time: int = None):
        """
        初始化健康监控服务
        
        Args:
            init_api_url: 初始化API的URL
            check_interval: 检查间隔时间（秒）
            retry_count: 每个回合的重试次数
            unhealthy_wait_time: 发现不健康后等待的时间（秒）
        """
        # 使用传入参数，如果未提供则从配置中获取
        self.init_api_url = init_api_url or self._get_config_value('init_api_url', 
            "http://ip:port/scip/Common/v1/Specification/aiAlgorithmInitiation")
        self.check_interval = check_interval or self._get_config_value('check_interval', 180)
        self.retry_count = retry_count or self._get_config_value('retry_count', 5)
        self.unhealthy_wait_time = unhealthy_wait_time or self._get_config_value('unhealthy_wait_time', 300)  # 修正为300秒（5分钟）
        
        self.is_monitoring = False
        self.session = None

    def _get_config_value(self, attr_name: str, default_value):
        """从配置中获取值，如果配置不存在则使用默认值"""
        try:
            from app.core.config import AlgorithmInitConfig
            return getattr(AlgorithmInitConfig, attr_name, default_value)
        except ImportError:
            # 如果配置不存在，则使用默认值
            return default_value
        except AttributeError:
            # 如果配置中没有相应的属性，则使用默认值
            return default_value

    async def call_init_api(self) -> bool:
        """调用初始化API"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # 准备请求数据（这里使用空的JSON体，实际可能需要根据API要求调整）
            request_data = {}
            
            headers = {
                "Content-Type": "application/json"
            }
            
            async with self.session.post(
                self.init_api_url,
                json=request_data,
                headers=headers,
                timeout=30  # 30秒超时
            ) as response:
                if response.status in [200, 201, 202, 204]:
                    logger.info(f"初始化API调用成功: {response.status}")
                    return True
                else:
                    logger.error(f"初始化API调用失败: {response.status}, 响应: {await response.text()}")
                    return False
        except Exception as e:
            logger.error(f"调用初始化API时发生错误: {str(e)}")
            return False

    def is_all_services_healthy(self, health_result: Dict[str, Any]) -> bool:
        """检查是否所有服务都健康"""
        if "services" not in health_result:
            return False
            
        services = health_result["services"]
        # 检查三个服务是否都健康（预处理服务、大模型服务、编码服务）
        required_services = ["预处理服务", "大模型服务", "编码服务"]
        
        total_required = 0
        healthy_count = 0
        
        for service_name in required_services:
            if service_name not in services:
                logger.warning(f"服务 {service_name} 未在健康检查结果中找到")
                continue
                
            total_required += 1
            status = services[service_name]
            if status == "healthy":
                healthy_count += 1
            else:
                logger.debug(f"服务 {service_name} 不健康: {status}")
        
        # 判断是否所有必需服务都健康
        is_all_healthy = (healthy_count == total_required and total_required > 0)
        logger.debug(f"服务健康检查结果: 总共需要{total_required}个服务, {healthy_count}个健康, 全部健康: {is_all_healthy}")
        return is_all_healthy

    async def perform_health_check_round(self) -> bool:
        """执行一个回合的健康检查（5次调用）"""
        logger.info("开始执行健康检查回合")
        
        for attempt in range(self.retry_count):
            logger.debug(f"执行第 {attempt + 1} 次健康检查")
            
            try:
                health_result = await health_check_service.check_all_services()
                logger.debug(f"健康检查结果: status={health_result.get('status')}, services={health_result.get('services', {})}")
                
                # 检查是否所有服务都健康
                if self.is_all_services_healthy(health_result):
                    logger.info("所有服务都健康，返回成功")
                    return True
                else:
                    logger.info(f"第 {attempt + 1} 次检查发现有服务不健康")
                    
            except Exception as e:
                logger.error(f"执行健康检查时发生错误: {str(e)}")
        
        logger.info("回合内所有检查都发现有服务不健康")
        return False

    async def monitor_health_loop(self):
        """健康监控主循环"""
        if self.is_monitoring:
            logger.warning("健康监控已经在运行中")
            return
            
        self.is_monitoring = True
        logger.info("启动健康监控服务")
        
        # 标记是否已经调用了初始化API（避免重复调用）
        initialized_after_unhealthy = False
        
        try:
            while self.is_monitoring:
                logger.debug("开始新的监控周期")
                
                # 执行一个回合的健康检查
                all_healthy_in_any_attempt = await self.perform_health_check_round()
                
                if all_healthy_in_any_attempt:
                    # 如果在回合中有任意一次检查所有服务都健康
                    if not initialized_after_unhealthy:
                        # 只有在之前标记为非健康后才调用初始化API
                        logger.info("所有服务健康，调用初始化API")
                        success = await self.call_init_api()
                        if success:
                            initialized_after_unhealthy = True
                    else:
                        logger.info("所有服务健康，跳过初始化API调用（已初始化过）")
                else:
                    # 如果回合内所有检查都发现有服务不健康
                    logger.warning(f"所有检查都显示服务不健康，等待 {self.unhealthy_wait_time} 秒")
                    initialized_after_unhealthy = False
                    await asyncio.sleep(self.unhealthy_wait_time)
                    continue
                
                # 等待下一个检查周期
                logger.debug(f"等待 {self.check_interval} 秒后进行下次检查")
                await asyncio.sleep(self.check_interval)
                
        except Exception as e:
            logger.error(f"健康监控循环发生错误: {str(e)}")
        finally:
            self.is_monitoring = False
            if self.session:
                await self.session.close()
            logger.info("健康监控服务已停止")

    def start_monitoring(self):
        """启动监控服务（在后台运行）"""
        if not self.is_monitoring:
            logger.info("正在启动健康监控服务...")
            # 在后台启动监控循环
            loop = asyncio.get_event_loop()
            loop.create_task(self.monitor_health_loop())
        else:
            logger.warning("健康监控服务已在运行中")

    def stop_monitoring(self):
        """停止监控服务"""
        self.is_monitoring = False
        logger.info("已请求停止健康监控服务")


# 创建全局实例
health_monitor_service = HealthMonitorService()