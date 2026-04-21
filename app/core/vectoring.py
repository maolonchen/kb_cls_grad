#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
向量数据库客户端模块
负责与Milvus向量数据库进行交互
"""

import json
import aiohttp
from pathlib import Path
from pymilvus import MilvusClient, DataType
from app.core.config import DatabaseConfig, EmbeddingConfig, AsyncEmbeddingConfig
import asyncio
import logging

logger = logging.getLogger(__name__)


class VectorClient:
    """向量数据库客户端"""

    def __init__(self, max_concurrent: int = None):
        # 使用API而不是本地模型
        # self.milvus_client = MilvusClient(DatabaseConfig.path)
        self.milvus_client = MilvusClient(DatabaseConfig.uri)
        self.collection_name = DatabaseConfig.collection_name
        self.embedding_api_url = EmbeddingConfig.api_url

        # 维度
        self.embedding_dim = EmbeddingConfig.embedding_dim

        # 并发控制
        if max_concurrent is None:
            max_concurrent = AsyncEmbeddingConfig.max_concurrent_requests  # 嵌入模型配置
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)

        logger.info(
            f"向量维度: {self.embedding_dim}, 最大并发数: {self.max_concurrent}")

    async def get_embeddings(self, texts):
        """通过API获取文本嵌入，添加重试机制确保不跳过数据"""
        # 限制文本长度
        max_length = EmbeddingConfig.max_content_length
        processed_texts = []
        for text in texts:
            if len(text) > max_length:
                processed_text = text[:max_length]
                logger.debug(f"文本已截断至{max_length}字符")
                processed_texts.append(processed_text)
            else:
                processed_texts.append(text)

        retry_delay = 2   # 重试(秒)

        while True:  # 无限重试直到成功
            try:
                async with self.semaphore:  # 控制并发数量
                    async with aiohttp.ClientSession() as session:
                        payload = {
                            "input": processed_texts,  # 使用处理后的文本
                            "model": EmbeddingConfig.model_name
                        }
                        # headers = {"Content-Type": "application/json"}
                        headers = EmbeddingConfig.headers

                        async with session.post(
                            self.embedding_api_url,
                            json=payload,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=120)
                        ) as response:
                            response.raise_for_status()
                            result = await response.json()
                            # 提取嵌入向量，按 index 排序确保顺序一致
                            embeddings = [item['embedding']
                                          for item in sorted(result['data'], key=lambda x: x.get('index', 0))]
                            return embeddings
            except Exception as e:
                logger.error(f"获取嵌入向量失败: {e}")
                logger.info(f"等待 {retry_delay} 秒后重试...")
                await asyncio.sleep(retry_delay)

    async def batch_get_embeddings(self, texts_list: list, timeout: int = 120) -> list:
        """
        批量获取文本嵌入向量

        Args:
            texts_list: 文本列表的列表，每个子列表将作为一个批次发送
            timeout: 超时时间（秒）

        Returns:
            所有文本的嵌入向量列表
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for texts in texts_list:
                task = self._get_embeddings_with_semaphore(
                    session, texts, timeout)
                tasks.append(task)

            # 等待所有任务完成
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 处理结果
            embeddings = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"第{i}个批次获取嵌入向量失败: {str(result)}")
                    # 可以选择抛出异常或使用默认值
                    raise result
                else:
                    embeddings.extend(result)

            return embeddings

    async def _get_embeddings_with_semaphore(self, session, texts, timeout):
        """带信号量控制的获取嵌入向量方法"""
        async with self.semaphore:  # 控制并发数量
            try:
                payload = {
                    "input": texts,
                    "model": EmbeddingConfig.model_name
                }
                headers = {"Content-Type": "application/json"}

                async with session.post(
                    self.embedding_api_url,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    response.raise_for_status()
                    result = await response.json()
                    # 提取嵌入向量，按 index 排序确保顺序一致
                    embeddings = [item['embedding'] for item in sorted(result['data'], key=lambda x: x.get('index', 0))]
                    return embeddings
            except Exception as e:
                logger.error(f"获取嵌入向量失败: {e}")
                raise

    def create_collection(self):
        """创建集合和索引"""
        # 创建 schema
        schema = self.milvus_client.create_schema(
            auto_id=False,
            description="Knowledge base chunks with embeddings"
        )

        # 添加字段到 schema
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR,
                         dim=self.embedding_dim)
        schema.add_field("text", DataType.VARCHAR, max_length=65535)
        schema.add_field("items", DataType.VARCHAR, max_length=65535)
        schema.add_field("vectorizing_text", DataType.VARCHAR, max_length=65535)

        # 创建集合
        self.milvus_client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
            consistency_level="Strong"
        )

        # 创建向量索引
        index_params = self.milvus_client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            index_type="FLAT",
            metric_type="IP"
        )
        self.milvus_client.create_index(
            collection_name=self.collection_name,
            index_params=index_params
        )

        logger.info("已创建新集合和索引")
        
        self.load_collection()

    def has_collection(self):
        """检查集合是否存在"""
        return self.milvus_client.has_collection(self.collection_name)

    def drop_collection(self):
        """删除集合"""
        if self.has_collection():
            self.milvus_client.drop_collection(self.collection_name)
            logger.info("已删除现有集合")

    def insert_data(self, data):
        """插入数据到集合中"""
        # 检查集合是否存在，如果不存在则创建
        if not self.has_collection():
            logger.info(f"集合 {self.collection_name} 不存在，正在创建...")
            self.create_collection()

        result = self.milvus_client.insert(
            collection_name=self.collection_name,
            data=data
        )
        logger.info(f"成功插入 {len(result['ids'])} 条记录")
        return result

    def load_collection(self):
        """加载集合以供查询"""
        self.milvus_client.load_collection(
            collection_name=self.collection_name)
        logger.info("集合已加载，可以进行后续的查询操作")

    def get_collection_stats(self):
        """获取集合统计信息"""
        return self.milvus_client.get_collection_stats(self.collection_name)

    async def search(self, query_vectors, top_k=8, filter_expr=None):
        """
        在向量数据库中搜索相似向量（异步包装）
        :param query_vectors: 查询向量列表
        :param top_k: 返回最相似的前k个结果
        :param filter_expr: 过滤表达式，例如 "grade == '核心数据'"
        :return: 搜索结果
        """
        try:
            # 搜索参数
            search_params = {
                "metric_type": "IP",
                "params": {}
            }

            # 搜索
            result = self.milvus_client.search(
                collection_name=self.collection_name,
                data=query_vectors,
                limit=top_k,
                output_fields=["text", "items", "vectorizing_text"],
                filter=filter_expr,  # 应用过滤器
                search_params=search_params
            )
            return result
        except Exception as e:
            logger.error(f"向量搜索失败: {e}")
            raise
