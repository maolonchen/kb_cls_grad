#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
健康检查服务
负责检查各个AI服务的可用性
"""

import logging
import asyncio
from typing import Dict
from datetime import datetime

logger = logging.getLogger(__name__)


class HealthCheckService:
    """健康检查服务类，用于检查各个AI服务的可用性"""
    
    @staticmethod
    async def check_mineru_service():
        """检查MinerU服务状态"""
        try:
            from app.core.config import MinerUConfig
            import aiohttp
            
            # 尝试使用POST方法检查服务状态，发送一个简单的测试请求
            async with aiohttp.ClientSession() as session:
                # 创建一个最小的测试请求数据
                test_data = {
                    "lang_list": ["ch", "en"],
                    "backend": "pipeline",
                    "parse_method": "auto",
                    "return_md": True
                }
                
                async with session.post(
                    MinerUConfig.url,
                    json=test_data,
                    timeout=20
                ) as response:
                    # 如果能成功建立连接，即使没有提供文件也认为服务是健康的
                    # 通常会返回400或类似的错误码，但表示服务是可达的
                    if response.status in [200, 400, 422]:
                        return "healthy"
                    else:
                        return f"unhealthy (status code: {response.status})"
        except Exception as e:
            return f"unhealthy (error: {str(e)})"

    @staticmethod
    async def check_chat_llm_service():
        """检查Chat LLM服务状态"""
        try:
            from app.core.config import ChatLLMConfig
            import aiohttp
            
            # 创建一个简单的健康检查消息
            messages = [{"role": "user", "content": "Hello"}]
            request_data = ChatLLMConfig.get_request_data(messages)
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    ChatLLMConfig.url,
                    headers=ChatLLMConfig.headers,
                    json=request_data,
                    timeout=20
                ) as response:
                    if response.status == 200:
                        return "healthy"
                    else:
                        return f"unhealthy (status code: {response.status})"
        except Exception as e:
            return f"unhealthy (error: {str(e)})"

    @staticmethod
    async def check_embedding_service():
        """检查Embedding服务状态"""
        try:
            from app.core.config import EmbeddingConfig
            import aiohttp
            
            # 使用短文本进行简单的嵌入请求
            payload = {
                "input": ["health check"],
                "model": EmbeddingConfig.model_name
            }
            
            # 检查是否有自定义headers
            headers = {"Content-Type": "application/json"}
            if hasattr(EmbeddingConfig, 'headers'):
                headers = EmbeddingConfig.headers
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    EmbeddingConfig.api_url,
                    json=payload,
                    headers=headers,
                    timeout=10
                ) as response:
                    if response.status == 200:
                        return "healthy"
                    else:
                        return f"unhealthy (status code: {response.status})"
        except Exception as e:
            return f"unhealthy (error: {str(e)})"

    @classmethod
    async def check_all_services(cls) -> Dict:
        """检查所有AI服务的状态"""
        # 并发执行所有检查
        mineru_status, chat_llm_status, embedding_status = await asyncio.gather(
            cls.check_mineru_service(), 
            cls.check_chat_llm_service(), 
            cls.check_embedding_service(),
            return_exceptions=True
        )
        
        services_status = {}
        
        # 处理异常
        services_status["预处理服务"] = str(mineru_status) if not isinstance(mineru_status, Exception) else f"unhealthy (exception: {str(mineru_status)})"
        services_status["大模型服务"] = str(chat_llm_status) if not isinstance(chat_llm_status, Exception) else f"unhealthy (exception: {str(chat_llm_status)})"
        services_status["编码服务"] = str(embedding_status) if not isinstance(embedding_status, Exception) else f"unhealthy (exception: {str(embedding_status)})"
        
        # 确定总体状态
        if all(status == "healthy" for status in services_status.values()):
            overall_status = "healthy"
        else:
            overall_status = "degraded"
        
        return {
            "status": overall_status,
            "services": services_status,
            "timestamp": str(datetime.now())
        }


health_check_service = HealthCheckService()