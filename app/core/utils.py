import asyncio
import re
import aiohttp
from typing import List, Dict, Any, Optional
import json
import logging

logger = logging.getLogger(__name__)


def remove_annotations(text):
    """
    移除文本中的注释和括号内容
    
    Args:
        text: 输入文本
        
    Returns:
        str: 清理后的文本
    """
    if not text or not isinstance(text, str):
        return text
        
    # 移除英文圆括号及其内容: (...)
    text = re.sub(r'\(.*?\)', '', text)
    
    # 移除中文圆括号及其内容: （...）
    text = re.sub(r'（.*?）', '', text)
    
    # 移除方括号及其内容: [...]
    text = re.sub(r'\[.*?\]', '', text)
    
    # 移除尖括号及其内容: <...>
    text = re.sub(r'<.*?>', '', text)
    
    # 移除花括号及其内容: {...}
    text = re.sub(r'\{.*?\}', '', text)
    
    # 移除中文方括号及其内容: 【...】
    text = re.sub(r'【.*?】', '', text)
    
    # 移除书名号及其内容: 《...》
    text = re.sub(r'《.*?》', '', text)
    
    # 移除破折号后的内容: —...
    text = re.sub(r'—.*$', '', text)
    
    # 移除冒号后的内容: :...
    text = re.sub(r':.*$', '', text)
    
    # 移除中文冒号后的内容: ：...
    text = re.sub(r'：.*$', '', text)
    
    # 移除多余的空格
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


class AsyncLLMClient:
    """
    异步LLM客户端，用于处理并发的LLM请求
    """

    def __init__(self, max_concurrent: int = 8):
        """
        初始化异步LLM客户端

        Args:
            max_concurrent: 最大并发请求数
        """
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def call_llm(self, session: aiohttp.ClientSession, url: str, headers: Dict[str, str],
                       request_data: Dict[str, Any], timeout: int = 2000) -> Dict[str, Any]:
        """
        异步调用LLM服务

        Args:
            session: aiohttp会话
            url: LLM服务URL
            headers: 请求头
            request_data: 请求数据
            timeout: 超时时间（秒）

        Returns:
            LLM响应结果
        """
        async with self.semaphore:  # 控制并发数量
            try:
                timeout_config = aiohttp.ClientTimeout(total=timeout)
                async with session.post(
                    url=url,
                    headers=headers,
                    json=request_data,
                    timeout=timeout_config
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise RuntimeError(
                            f"LLM服务返回错误状态码: {response.status}, 响应: {error_text}")

                    result = await response.json()
                    return result
            except asyncio.TimeoutError:
                raise RuntimeError(f"LLM请求超时 ({timeout}秒)")
            except Exception as e:
                raise RuntimeError(f"请求LLM服务时发生错误: {str(e)}")

    async def batch_call_llm(self, requests: List[Dict[str, Any]], timeout: int = 2000) -> List[Dict[str, Any]]:
        """
        批量异步调用LLM服务，保证响应与请求一一对应

        Args:
            requests: 请求列表，每个元素包含url, headers, request_data
            timeout: 超时时间（秒）

        Returns:
            LLM响应结果列表，与请求列表一一对应
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for req in requests:
                task = self.call_llm(
                    session=session,
                    url=req['url'],
                    headers=req['headers'],
                    request_data=req['request_data'],
                    timeout=timeout
                )
                tasks.append(task)

            # 等待所有任务完成，保持顺序
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 处理异常情况
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"第{i}个LLM请求出错: {str(result)}")
                    processed_results.append({"error": str(result)})
                else:
                    processed_results.append(result)

            return processed_results
