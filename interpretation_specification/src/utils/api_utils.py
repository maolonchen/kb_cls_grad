# -*- coding: utf-8 -*-
"""
API工具模块
提供统一的API调用功能
"""

import aiohttp
import asyncio
import json
from typing import List, Union
from config.settings import EmbeddingConfig


class EmbeddingAPI:
    """
    嵌入模型API客户端，用于通过HTTP请求获取文本嵌入向量
    """

    def __init__(self):
        self.api_url = EmbeddingConfig.api_url
        self.model = EmbeddingConfig.model
        self.session = None

    async def __aenter__(self):
        """
        异步上下文管理器入口，初始化aiohttp会话
        """
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        异步上下文管理器出口，关闭aiohttp会话
        """
        if self.session:
            await self.session.close()

    async def get_embedding(self, text: str) -> List[float]:
        """
        获取单个文本的嵌入向量
        
        Args:
            text (str): 输入文本
            
        Returns:
            List[float]: 文本的嵌入向量
        """
        if not self.session:
            raise RuntimeError("必须在异步上下文管理器中使用此方法")

        payload = {
            "model": self.model,
            "input": text
        }

        try:
            async with self.session.post(self.api_url, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    return result["data"][0]["embedding"]
                else:
                    error_text = await response.text()
                    raise Exception(f"API调用失败: {response.status} - {error_text}")
        except Exception as e:
            raise Exception(f"获取嵌入向量时出错: {str(e)}")

    async def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        批量获取多个文本的嵌入向量
        
        Args:
            texts (List[str]): 输入文本列表
            
        Returns:
            List[List[float]]: 文本的嵌入向量列表
        """
        if not self.session:
            raise RuntimeError("必须在异步上下文管理器中使用此方法")

        payload = {
            "model": self.model,
            "input": texts
        }

        try:
            async with self.session.post(self.api_url, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    return [item["embedding"] for item in result["data"]]
                else:
                    error_text = await response.text()
                    raise Exception(f"API调用失败: {response.status} - {error_text}")
        except Exception as e:
            raise Exception(f"批量获取嵌入向量时出错: {str(e)}")