#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据特征批量匹配服务
提供数据特征相似度匹配功能
"""

import logging
from typing import List, Tuple, Dict
import jieba
from rank_bm25 import BM25Okapi
import numpy as np
import asyncio
import hashlib
import json
import os
import time
from pathlib import Path
import threading
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import atexit

from interpretation_specification.schemas.spec_schema import (
    DataElementBatchMatchDto,
    DataElementBatchMatchResponse,
    DataElementBatchMatchRequest
)
from interpretation_specification.config.settings import DataElementMatchConfig, BM25Config, EmbeddingConfig, HttpStatus


from app.core.config import DataElementMatchConfig, BM25Config, EmbeddingConfig
from app.core.constants import HttpStatus

logger = logging.getLogger(__name__)


class VectorCacheManager:
    """向量缓存管理器，使用JSONL格式存储向量，并在24小时后自动删除"""
    
    def __init__(self, cache_dir: str = "./cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        # 使用当前时间戳创建唯一的缓存文件名
        timestamp = int(time.time())
        self.cache_file = self.cache_dir / f"vector_cache_{timestamp}.jsonl"
        self.cache_lock = threading.Lock()
        self._load_cache()
        
        # 启动定时清理线程
        self.cleanup_thread = threading.Thread(target=self._schedule_cleanup, daemon=True)
        self.cleanup_thread.start()
        
        # 注册程序退出时的清理函数
        atexit.register(self._cleanup_on_exit)
    
    def _load_cache(self):
        """从文件加载缓存到内存"""
        self.cache: Dict[str, List[float]] = {}
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            entry = json.loads(line.strip())
                            self.cache[entry['key']] = entry['vector']
            except Exception as e:
                logger.warning(f"加载缓存文件失败: {e}")
    
    def _save_to_cache(self, key: str, vector: List[float]):
        """将向量保存到缓存文件"""
        with self.cache_lock:
            self.cache[key] = vector
            try:
                with open(self.cache_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps({"key": key, "vector": vector}, separators=(',', ':')) + "\n")
            except Exception as e:
                logger.error(f"保存缓存失败: {e}")
    
    def get_cached_vector(self, key: str):
        """从缓存中获取向量"""
        with self.cache_lock:
            return self.cache.get(key)
    
    def is_cached(self, key: str) -> bool:
        """检查向量是否已在缓存中"""
        with self.cache_lock:
            return key in self.cache
    
    def _schedule_cleanup(self):
        """调度清理任务，在24小时后删除缓存文件"""
        time.sleep(24 * 3600)  # 24小时
        self._perform_cleanup()
    
    def _perform_cleanup(self):
        """执行清理操作"""
        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
                logger.info(f"已删除缓存文件: {self.cache_file}")
        except Exception as e:
            logger.error(f"删除缓存文件失败: {e}")
    
    def _cleanup_on_exit(self):
        """程序退出时清理缓存"""
        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
                logger.info(f"程序退出时已删除缓存文件: {self.cache_file}")
        except Exception as e:
            logger.error(f"程序退出时删除缓存文件失败: {e}")


class CachedEmbeddingSimilarityCalculator:
    """
    使用嵌入模型计算文本相似度的类，带缓存功能
    """
    
    def __init__(self, cache_manager: VectorCacheManager):
        """
        初始化相似度计算器
        """
        self.api_url = EmbeddingConfig.api_url
        self.model_name = EmbeddingConfig.model_name
        self.embedding_dim = EmbeddingConfig.embedding_dim
        # 添加最大重试次数和重试间隔
        self.max_retries = 5
        self.retry_delay = 1
        self.cache_manager = cache_manager

    @staticmethod
    def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
        """
        计算两个向量之间的余弦相似度

        Args:
            vec1: 第一个向量
            vec2: 第二个向量

        Returns:
            余弦相似度值 (0-1)
        """
        # 转换为numpy数组
        v1 = np.array(vec1)
        v2 = np.array(vec2)

        # 计算点积
        dot_product = np.dot(v1, v2)

        # 计算向量的模长
        norm_v1 = np.linalg.norm(v1)
        norm_v2 = np.linalg.norm(v2)

        # 避免除零错误
        if norm_v1 == 0 or norm_v2 == 0:
            return 0.0

        # 计算余弦相似度
        similarity = dot_product / (norm_v1 * norm_v2)

        # 确保结果在有效范围内 [-1, 1]
        return float(np.clip(similarity, -1.0, 1.0))

    async def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        获取文本的嵌入向量，优先从缓存获取

        Args:
            texts: 文本列表

        Returns:
            对应的嵌入向量列表
        """
        embeddings = []
        uncached_texts = []
        uncached_indices = []
        
        # 检查哪些文本已经在缓存中
        for i, text in enumerate(texts):
            text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
            cached_vector = self.cache_manager.get_cached_vector(text_hash)
            
            if cached_vector is not None:
                embeddings.append(cached_vector)
                logger.debug(f"从缓存中获取向量: {text}")
            else:
                embeddings.append(None)  # 占位符
                uncached_texts.append(text)
                uncached_indices.append(i)
        
        # 如果所有文本都在缓存中，直接返回
        if len(uncached_texts) == 0:
            return embeddings
        
        # 获取未缓存文本的向量
        uncached_embeddings = await self._get_uncached_embeddings(uncached_texts)
        
        # 更新缓存并将向量填回原位置
        for idx, (i, text) in enumerate(zip(uncached_indices, uncached_texts)):
            vector = uncached_embeddings[idx]
            embeddings[i] = vector
            # 将新获取的向量存入缓存
            text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
            self.cache_manager._save_to_cache(text_hash, vector)
        
        return embeddings

    async def _get_uncached_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        获取未缓存文本的嵌入向量

        Args:
            texts: 未缓存的文本列表

        Returns:
            对应的嵌入向量列表
        """
        # 构造请求数据
        payload = {
            "model": self.model_name,
            "input": texts
        }

        # 实现无限重试机制
        retry_count = 0
        while True:
            try:
                # 发送POST请求获取嵌入向量
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.api_url, json=payload) as response:
                        if response.status != 200:
                            raise RuntimeError(f"获取嵌入向量失败: {response.status}")
                        
                        result = await response.json()
                        # 提取嵌入向量
                        embeddings = [item["embedding"] for item in result["data"]]
                        return embeddings
            except aiohttp.ClientConnectorError as e:
                retry_count += 1
                logger.warning(f"获取嵌入向量连接失败 (第{retry_count}次尝试): {e}, "
                              f"将在{self.retry_delay}秒后重试...")
                await asyncio.sleep(self.retry_delay)
                continue
            except aiohttp.ClientError as e:
                retry_count += 1
                logger.warning(f"获取嵌入向量客户端错误 (第{retry_count}次尝试): {e}, "
                              f"将在{self.retry_delay}秒后重试...")
                await asyncio.sleep(self.retry_delay)
                continue
            except Exception as e:
                retry_count += 1
                logger.warning(f"获取嵌入向量未知错误 (第{retry_count}次尝试): {e}, "
                              f"将在{self.retry_delay}秒后重试...")
                await asyncio.sleep(self.retry_delay)
                continue

    async def calculate_similarity(self, text1: str, text2: str) -> float:
        """
        计算两个文本之间的相似度

        Args:
            text1: 第一个文本
            text2: 第二个文本

        Returns:
            文本相似度值 (0-1)
        """
        # 获取两个文本的嵌入向量
        embeddings = await self.get_embeddings([text1, text2])
        
        # 计算余弦相似度
        similarity = self.cosine_similarity(embeddings[0], embeddings[1])
        
        return similarity

    async def calculate_similarities(self, texts: List[str]) -> List[float]:
        """
        计算文本列表中每对文本之间的相似度

        Args:
            texts: 文本列表

        Returns:
            相似度值列表
        """
        if len(texts) < 2:
            return []

        # 获取所有文本的嵌入向量
        embeddings = await self.get_embeddings(texts)
        
        # 计算每对文本之间的相似度
        similarities = []
        for i in range(len(embeddings)):
            for j in range(i + 1, len(embeddings)):
                similarity = self.cosine_similarity(embeddings[i], embeddings[j])
                similarities.append(similarity)
        
        return similarities


class DataElementBatchMatchService:
    """数据特征批量匹配服务类"""

    def __init__(self):
        # 初始化缓存管理器
        self.cache_manager = VectorCacheManager("./cache")
        # 使用带缓存功能的计算器
        self.cached_calculator = CachedEmbeddingSimilarityCalculator(self.cache_manager)

    async def process_data_element_batch_match(self, request: DataElementBatchMatchRequest) -> DataElementBatchMatchResponse:
        """
        处理数据特征批量匹配请求
        
        Args:
            request: 数据特征批量匹配请求对象
            
        Returns:
            DataElementBatchMatchResponse: 数据特征批量匹配响应对象
        """
        try:
            # 清理输入数据，仅去除elementName的空格，保留elementNames的原始格式
            cleaned_element_name = request.elementName.replace(" ", "")
            original_element_names = request.elementNames  # 保存原始格式的元素名称列表
            cleaned_element_names = [name.replace(" ", "") for name in request.elementNames]
            
            logger.info(f"正在进行批量元素处理，当前元素: {cleaned_element_name}")
            logger.info(f"元素名称的数量有: {len(cleaned_element_names)}")
            logger.info(f"相似度阈值: {request.similarityThreshold}")
            logger.info(f"返回的最大数量: {request.maxResults}")
            
            # 使用带缓存功能的相似度计算器
            calculator = self.cached_calculator
            
            # 限制并发数以防止显存溢出
            semaphore = asyncio.Semaphore(5)  # 最多同时执行5个任务

            async def calculate_similarity_with_semaphore(element_name):
                async with semaphore:
                    return await calculator.calculate_similarity(cleaned_element_name, element_name)

            # 并发计算目标元素与所有候选元素之间的嵌入相似度，但限制并发数
            embedding_tasks = [
                calculate_similarity_with_semaphore(element_name)
                for element_name in cleaned_element_names
            ]
            embedding_results = await asyncio.gather(*embedding_tasks)
            embedding_similarities = list(zip(original_element_names, embedding_results))  # 使用原始格式的元素名称
            
            logger.info(f"相似度计算结果: {embedding_similarities}")
            
            # 如果只有一个候选元素，则只使用嵌入相似度
            if len(cleaned_element_names) == 1:
                combined_similarities = embedding_similarities
            else:
                # 计算BM25相似度 - 在内部使用清理后的版本进行计算，但保持原始格式用于输出
                bm25_similarities_raw = self._calculate_bm25_similarity(cleaned_element_name, cleaned_element_names)
                # 重新映射到原始格式
                bm25_similarities = list(zip(original_element_names, [score for _, score in bm25_similarities_raw]))
                
                logger.info(f"BM25 相似度计算结果: {bm25_similarities}")
                logger.info(f"词嵌入相似度计算结果: {embedding_similarities}")
                
                # 组合两种相似度得分
                combined_similarities = self._combine_similarities(embedding_similarities, bm25_similarities)
            
            logger.info(f"混合相似度计算结果: {combined_similarities}")
            
            # 根据阈值过滤
            threshold = request.similarityThreshold if request.similarityThreshold is not None else DataElementMatchConfig.default_similarity_threshold
            filtered_similarities = [(name, sim) for name, sim in combined_similarities if sim >= threshold]
            
            logger.info(f"过滤后的相似度 (阈值={threshold}): {filtered_similarities}")
            
            # 按相似度降序排序
            filtered_similarities.sort(key=lambda x: x[1], reverse=True)
            
            # 限制结果数量，默认返回前3个
            max_results = request.maxResults if request.maxResults is not None else DataElementMatchConfig.default_max_results
            top_results = filtered_similarities[:max_results]
            
            logger.info(f"前 {max_results} 个结果: {top_results}")
            
            # 转换为DTO对象
            match_results = [
                DataElementBatchMatchDto(
                    matchElementName=name,
                    similarity=sim
                ) for name, sim in top_results
            ]
            
            return DataElementBatchMatchResponse(
                success=True,
                code=HttpStatus.SUCCESS,
                msg="匹配成功！",
                data=match_results
            )
            
        except Exception as e:
            logger.error(f"Failed to process data element batch match: {str(e)}", exc_info=True)
            return DataElementBatchMatchResponse(
                success=False,
                code=HttpStatus.INTERNAL_SERVER_ERROR,
                msg=f"匹配失败: {str(e)}",
                data=[]
            )

    def _calculate_bm25_similarity(self, query: str, candidates: List[str]) -> List[Tuple[str, float]]:
        """
        使用BM25算法计算查询与候选元素之间的相似度
        
        Args:
            query: 查询文本
            candidates: 候选元素列表
            
        Returns:
            List[Tuple[str, float]]: 元素名称和相似度得分的元组列表
        """
        try:
            # 对所有候选元素进行分词
            tokenized_candidates = []
            valid_candidates = []  # 存储有效候选元素
            
            for candidate in candidates:
                tokens = list(jieba.cut(candidate))
                # 过滤掉空字符串和空白字符
                tokens = [token.strip() for token in tokens if token.strip()]
                logger.info(f"====================候选元素=================== {tokens}")
                if tokens:  # 只有非空的候选元素才加入
                    tokenized_candidates.append(tokens)
                    valid_candidates.append(candidate)
            
            if not tokenized_candidates:
                return [(candidate, 0.0) for candidate in candidates]
            
            # 构建BM25模型
            bm25 = BM25Okapi(tokenized_candidates)
            
            # 对查询进行分词
            query_tokens = list(jieba.cut(query))
            query_tokens = [token.strip() for token in query_tokens if token.strip()]
            logger.info(f"=====================待替换元素================== {query_tokens}")
            
            if not query_tokens:
                return [(candidate, 0.0) for candidate in candidates]
            
            # 计算BM25得分
            scores = bm25.get_scores(query_tokens)
            
            # 归一化得分到0-1范围
            if scores.size > 0:
                min_score = np.min(scores)
                max_score = np.max(scores)
                if max_score > min_score:
                    normalized_scores = (scores - min_score) / (max_score - min_score)
                else:
                    # 所有得分都相等，设为1.0
                    normalized_scores = np.ones_like(scores)
            else:
                normalized_scores = np.array([])
            
            # 构建结果列表
            result = []
            valid_idx = 0
            for candidate in candidates:
                if candidate in valid_candidates:
                    # 有效候选元素，使用计算得到的得分
                    score = float(normalized_scores[valid_idx]) if normalized_scores.size > valid_idx else 0.0
                    result.append((candidate, score))
                    valid_idx += 1
                else:
                    # 无效候选元素，得分为0
                    result.append((candidate, 0.0))
            
            return result
            
        except Exception as e:
            logger.error(f"BM25 相似度计算失败: {str(e)}", exc_info=True)
            # 如果BM25计算失败，返回所有候选元素得分为0
            return [(candidate, 0.0) for candidate in candidates]

    def _combine_similarities(self, embedding_similarities: List[Tuple[str, float]], 
                             bm25_similarities: List[Tuple[str, float]]) -> List[Tuple[str, float]]:
        """
        组合嵌入相似度和BM25相似度
        
        Args:
            embedding_similarities: 嵌入相似度列表
            bm25_similarities: BM25相似度列表
            
        Returns:
            List[Tuple[str, float]]: 组合后的相似度列表
        """
        try:
            # 获取权重配置
            embedding_weight, bm25_weight = BM25Config.default_weights
            
            # 创建字典以便快速查找
            bm25_dict = dict(bm25_similarities)
            
            # 组合相似度
            combined = []
            for name, emb_sim in embedding_similarities:
                bm25_sim = bm25_dict.get(name, 0.0)
                # 加权平均
                combined_sim = embedding_weight * emb_sim + bm25_weight * bm25_sim
                logger.info(f"超参数: 语义权重：{embedding_weight}，bm25权重：{bm25_weight}, 余弦相似度：{emb_sim}， bm25相似度：{bm25_sim}")
                combined.append((name, combined_sim))
            
            return combined
            
        except Exception as e:
            logger.error(f"混个检索相似度失败: {str(e)}", exc_info=True)
            # 如果组合失败，返回嵌入相似度
            return embedding_similarities


data_element_batch_match_service = DataElementBatchMatchService()