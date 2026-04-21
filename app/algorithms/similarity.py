# import numpy as np
# import aiohttp
# import asyncio
# from typing import List, Union
# from app.core.config import EmbeddingConfig


# class EmbeddingSimilarityCalculator:
#     """
#     使用嵌入模型计算文本相似度的类
#     """

#     def __init__(self):
#         """
#         初始化相似度计算器
#         """
#         self.api_url = EmbeddingConfig.api_url
#         self.model_name = EmbeddingConfig.model_name
#         self.embedding_dim = EmbeddingConfig.embedding_dim

#     @staticmethod
#     def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
#         """
#         计算两个向量之间的余弦相似度

#         Args:
#             vec1: 第一个向量
#             vec2: 第二个向量

#         Returns:
#             余弦相似度值 (0-1)
#         """
#         # 转换为numpy数组
#         v1 = np.array(vec1)
#         v2 = np.array(vec2)

#         # 计算点积
#         dot_product = np.dot(v1, v2)

#         # 计算向量的模长
#         norm_v1 = np.linalg.norm(v1)
#         norm_v2 = np.linalg.norm(v2)

#         # 避免除零错误
#         if norm_v1 == 0 or norm_v2 == 0:
#             return 0.0

#         # 计算余弦相似度
#         similarity = dot_product / (norm_v1 * norm_v2)

#         # 确保结果在有效范围内 [-1, 1]
#         return float(np.clip(similarity, -1.0, 1.0))

#     async def get_embeddings(self, texts: List[str]) -> List[List[float]]:
#         """
#         获取文本的嵌入向量

#         Args:
#             texts: 文本列表

#         Returns:
#             对应的嵌入向量列表
#         """
#         # 构造请求数据
#         payload = {
#             "model": self.model_name,
#             "input": texts
#         }

#         # 发送POST请求获取嵌入向量
#         async with aiohttp.ClientSession() as session:
#             async with session.post(self.api_url, json=payload) as response:
#                 if response.status != 200:
#                     raise RuntimeError(f"获取嵌入向量失败: {response.status}")
                
#                 result = await response.json()
#                 # 提取嵌入向量
#                 embeddings = [item["embedding"] for item in result["data"]]
#                 return embeddings

#     async def calculate_similarity(self, text1: str, text2: str) -> float:
#         """
#         计算两个文本之间的相似度

#         Args:
#             text1: 第一个文本
#             text2: 第二个文本

#         Returns:
#             文本相似度值 (0-1)
#         """
#         # 获取两个文本的嵌入向量
#         embeddings = await self.get_embeddings([text1, text2])
        
#         # 计算余弦相似度
#         similarity = self.cosine_similarity(embeddings[0], embeddings[1])
        
#         return similarity

#     async def calculate_similarities(self, texts: List[str]) -> List[float]:
#         """
#         计算文本列表中每对文本之间的相似度

#         Args:
#             texts: 文本列表

#         Returns:
#             相似度值列表
#         """
#         if len(texts) < 2:
#             return []

#         # 获取所有文本的嵌入向量
#         embeddings = await self.get_embeddings(texts)
        
#         # 计算每对文本之间的相似度
#         similarities = []
#         for i in range(len(embeddings)):
#             for j in range(i + 1, len(embeddings)):
#                 similarity = self.cosine_similarity(embeddings[i], embeddings[j])
#                 similarities.append(similarity)
        
#         return similarities



import numpy as np
import aiohttp
import asyncio
from typing import List, Union
from app.core.config import EmbeddingConfig
import time
import logging

logger = logging.getLogger(__name__)


class EmbeddingSimilarityCalculator:
    """
    使用嵌入模型计算文本相似度的类
    """

    def __init__(self):
        """
        初始化相似度计算器
        """
        self.api_url = EmbeddingConfig.api_url
        self.model_name = EmbeddingConfig.model_name
        self.embedding_dim = EmbeddingConfig.embedding_dim
        # 添加最大重试次数和重试间隔
        self.max_retries = 5
        self.retry_delay = 1

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
        获取文本的嵌入向量

        Args:
            texts: 文本列表

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