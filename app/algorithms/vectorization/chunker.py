#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文档分块器
使用LLM对Markdown文档进行智能分块，适用于RAG系统
"""

import json
import logging
import requests
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
from app.core.config import ChatLLMConfig, ChunkingConfig
from app.core.utils import AsyncLLMClient
from app.core.prompts.chunking import ChunkingPrompts

logger = logging.getLogger(__name__)


# 数据型文档分块
class MarkdownChunker:
    """Markdown文档分块器"""
    
    def __init__(self):
        """初始化分块器"""
        pass

    def chunk_document_with_llm(self, md_content: str, doc_id: Optional[str] = None) -> List[str]:
        """
        使用LLM对Markdown文档进行分块（同步版本）
        
        参数:
            md_content (str): Markdown文档内容
            doc_id (Optional[str]): 文档ID，用于日志记录
            
        返回:
            List[str]: 分块后的文档列表
        """
        # 限制文本长度
        if len(md_content) > ChunkingConfig.max_content_length:
            md_content = md_content[:ChunkingConfig.max_content_length]
            logger.debug(f"文档内容已截断至{ChunkingConfig.max_content_length}字符")
        
        # 构造完整的提示词
        prompt = ChunkingPrompts.get_chunking_prompt(md_content)
        
        # 构造消息
        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        # 获取请求数据
        request_data = ChatLLMConfig.get_request_data(messages)
        
        try:
            # 发送请求到LLM服务
            logger.debug(f"向LLM服务发送分块请求: {ChatLLMConfig.url}")
            if doc_id:
                logger.debug(f"处理文档ID: {doc_id}")
                
            response = requests.post(
                url=ChatLLMConfig.url,
                headers=ChatLLMConfig.headers,
                json=request_data,
                timeout=2000  # 5分钟超时
            )
            
            # 检查响应状态
            if response.status_code != 200:
                raise RuntimeError(f"LLM服务返回错误状态码: {response.status_code}, 响应: {response.text}")
            
            # 解析响应数据
            result = response.json()
            
            # 提取LLM的回复
            if "choices" in result and len(result["choices"]) > 0:
                msg = result["choices"][0]["message"]["content"]
                llm_response = msg.split("</think>\n\n")[1] if "</think>\n\n" in msg else msg

                logger.debug(f"LLM分块回复: {llm_response}")
                
                # 添加调试信息
                logger.info(f"LLM回复长度: {len(llm_response)} 字符")
                logger.info(f"LLM回复内容: {llm_response}")
                
                # 将回复按行分割成块
                chunks = llm_response.strip().split('\n')
                # 过滤掉空行
                chunks = [chunk for chunk in chunks if chunk.strip()]
                
                logger.info(f"过滤后得到 {len(chunks)} 个块")
                for i, chunk in enumerate(chunks):
                    logger.info(f"块 {i+1}: {chunk}")
                
                return chunks
            else:
                logger.error("LLM服务返回格式错误")
                logger.error(f"LLM完整响应: {result}")
                raise RuntimeError("LLM服务返回格式错误")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"请求LLM服务时发生网络错误: {str(e)}")
            raise RuntimeError(f"请求LLM服务时发生网络错误: {str(e)}")
        except Exception as e:
            logger.error(f"文档分块时发生未知错误: {str(e)}")
            raise RuntimeError(f"文档分块时发生未知错误: {str(e)}")

    async def chunk_document_with_llm_async(self, md_content: str, doc_id: Optional[str] = None) -> List[str]:
        """
        使用LLM对Markdown文档进行分块（异步版本）
        
        参数:
            md_content (str): Markdown文档内容
            doc_id (Optional[str]): 文档ID，用于日志记录
            
        返回:
            List[str]: 分类后的文档列表
        """
        # 限制文本长度
        if len(md_content) > ChunkingConfig.max_content_length:
            md_content = md_content[:ChunkingConfig.max_content_length]
            logger.debug(f"文档内容已截断至{ChunkingConfig.max_content_length}字符")
        
        # 构造完整的提示词
        prompt = ChunkingPrompts.get_chunking_prompt(md_content)
        
        # 构造消息
        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        # 获取请求数据
        request_data = ChatLLMConfig.get_request_data(messages)
        
        # 构造请求
        request_info = {
            'url': ChatLLMConfig.url,
            'headers': ChatLLMConfig.headers,
            'request_data': request_data
        }
        
        try:
            # 使用异步客户端发送请求
            client = AsyncLLMClient(max_concurrent=ChatLLMConfig.max_concurrent_requests)
            async with aiohttp.ClientSession() as session:
                response = await client.call_llm(
                    session=session,
                    url=request_info['url'],
                    headers=request_info['headers'],
                    request_data=request_info['request_data'],
                    timeout=2000
                )
            
            # 检查响应是否为异常
            if "error" in response:
                raise RuntimeError(f"LLM服务返回错误: {response['error']}")
            
            # 提取LLM的回复
            if "choices" in response and len(response["choices"]) > 0:
                msg = response["choices"][0]["message"]["content"]
                llm_response = msg.split("</think>\n\n")[1] if "</think>\n\n" in msg else msg
                
                logger.debug(f"LLM分块回复: {llm_response}")
                
                # 将回复按行分割成块
                chunks = llm_response.strip().split('\n')
                # 过滤掉空行
                chunks = [chunk for chunk in chunks if chunk.strip()]
                
                return chunks
            else:
                raise RuntimeError("LLM服务返回格式错误")
                
        except Exception as e:
            raise RuntimeError(f"文档分块时发生未知错误: {str(e)}")

    def chunk_document_file_with_llm(self, file_path: str) -> List[str]:
        """
        使用LLM对Markdown文档文件进行分块（同步版本）
        
        参数:
            file_path (str): Markdown文档文件路径
            
        返回:
            List[str]: 分块后的文档列表
        """
        import os
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文档文件不存在: {file_path}")
        
        # 读取文件内容
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            raise RuntimeError(f"读取文档文件时出错: {str(e)}")
        
        # 使用文件名作为文档ID
        doc_id = os.path.basename(file_path)
        
        # 调用LLM进行分块
        return self.chunk_document_with_llm(content, doc_id)

    async def chunk_document_file_with_llm_async(self, file_path: str) -> List[str]:
        """
        使用LLM对Markdown文档文件进行分块（异步版本）
        
        参数:
            file_path (str): Markdown文档文件路径
            
        返回:
            List[str]: 分块后的文档列表
        """
        import os
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文档文件不存在: {file_path}")
        
        # 读取文件内容
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            raise RuntimeError(f"读取文档文件时出错: {str(e)}")
        
        # 使用文件名作为文档ID
        doc_id = os.path.basename(file_path)
        
        # 调用LLM进行分块
        return await self.chunk_document_with_llm_async(content, doc_id)

    async def chunk_documents_with_llm_async(self, md_contents: List[str], doc_ids: Optional[List[str]] = None) -> List[List[str]]:
        """
        使用LLM对多个Markdown文档进行分块（异步批量版本）
        
        参数:
            md_contents (List[str]): Markdown文档内容列表
            doc_ids (Optional[List[str]]): 文档ID列表，用于日志记录
            
        返回:
            List[List[str]]: 每个文档分块后的列表
        """
        if doc_ids is None:
            doc_ids = [None] * len(md_contents)
            
        if len(md_contents) != len(doc_ids):
            raise ValueError("文档内容列表和文档ID列表长度必须相同")
        
        # 构造请求列表
        requests_data = []
        
        for i, md_content in enumerate(md_contents):
            # 限制文本长度
            processed_content = md_content
            if len(md_content) > ChunkingConfig.max_content_length:
                processed_content = md_content[:ChunkingConfig.max_content_length]
                logger.debug(f"文档内容已截断至{ChunkingConfig.max_content_length}字符")
            
            # 构造完整的提示词
            prompt = ChunkingPrompts.get_chunking_prompt(processed_content)
            
            # 构造消息
            messages = [
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            # 获取请求数据
            request_data = ChatLLMConfig.get_request_data(messages)
            
            # 添加到请求列表
            requests_data.append({
                'url': ChatLLMConfig.url,
                'headers': ChatLLMConfig.headers,
                'request_data': request_data
            })
        
        # 使用异步客户端发送请求
        client = AsyncLLMClient(max_concurrent=ChatLLMConfig.max_concurrent_requests)
        responses = await client.batch_call_llm(requests_data, timeout=2000)
        
        # 处理响应
        results = []
        for i, response in enumerate(responses):
            try:
                if "error" in response:
                    logger.error(f"文档 {doc_ids[i]} 分块出错: {response['error']}")
                    results.append([])
                    continue
                    
                # 提取LLM的回复
                if "choices" in response and len(response["choices"]) > 0:
                    msg = response["choices"][0]["message"]["content"]
                    llm_response = msg.split("</think>\n\n")[1] if "</think>\n\n" in msg else msg

                    logger.debug(f"LLM分块回复: {llm_response}")
                    
                    # 将回复按行分割成块
                    chunks = llm_response.strip().split('\n')
                    # 过滤掉空行
                    chunks = [chunk for chunk in chunks if chunk.strip()]
                    
                    results.append(chunks)
                else:
                    logger.error(f"文档 {doc_ids[i]} LLM服务返回格式错误")
                    results.append([])
            except Exception as e:
                logger.error(f"处理文档 {doc_ids[i]} 分块结果时发生错误: {str(e)}")
                results.append([])
        
        return results


# 为了向后兼容，也提供函数接口
def chunk_document_with_llm(md_content: str, doc_id: Optional[str] = None) -> List[str]:
    """
    使用LLM对Markdown文档进行分块（函数接口，同步版本）
    
    参数:
        md_content (str): Markdown文档内容
        doc_id (Optional[str]): 文档ID，用于日志记录
        
    返回:
        List[str]: 分块后的文档列表
    """
    chunker = MarkdownChunker()

    return chunker.chunk_document_with_llm(md_content, doc_id)


async def chunk_document_file_with_llm_async(file_path: str) -> List[str]:
    """
    使用LLM对Markdown文档文件进行分块（函数接口，异步版本）
    
    参数:
        file_path (str): Markdown文档文件路径
        
    返回:
        List[str]: 分块后的文档列表
    """
    chunker = MarkdownChunker()
    return await chunker.chunk_document_file_with_llm_async(file_path)


# 叙述型文档分块
class NarrativeMarkdownChunker:
    """
    叙述型Markdown文档分块器
    专门用于处理连贯表达型文本的分块，保持上下文连贯性
    """
    
    def __init__(self, max_chunk_size: int = 500):
        """
        初始化分块器
        
        参数:
            max_chunk_size (int): 每个块的最大字符数，默认500
        """
        self.max_chunk_size = max_chunk_size

    def chunk_document(self, md_content: str, doc_id: Optional[str] = None) -> List[str]:
        """
        对叙述型Markdown文档进行分块
        
        参数:
            md_content (str): Markdown文档内容
            doc_id (Optional[str]): 文档ID，用于日志记录
            
        返回:
            List[str]: 分块后的文档列表
        """
        import re
        
        lines = md_content.splitlines()
        chunks = []
        
        # 当前标题栈：[main_title, '# 标题1', '## 标题2', ...]
        title_stack = []
        main_title = None
        in_code_block = False
        main_title_set = False  # 标记主标题是否已设置

        def get_title_path():
            # 构建 "主标题:一级:二级:..." 路径
            parts = []
            if main_title is not None:
                parts.append(main_title)
            for t in title_stack:
                # 去掉井号和前后空格
                clean_t = re.sub(r'^#+\s*', '', t).strip()
                if clean_t:
                    parts.append(clean_t)
            return ':'.join(parts) if parts else ''

        i = 0
        while i < len(lines):
            line = lines[i]

            # 处理代码块标记
            if line.strip().startswith('```'):
                in_code_block = not in_code_block
                i += 1
                continue

            if in_code_block:
                i += 1
                continue

            # 检测是否是标题行（Markdown 标题）
            heading_match = re.match(r'^(#{1,6})\s+(.*)', line)
            if heading_match:
                level = len(heading_match.group(1))
                heading_text = heading_match.group(2).strip()

                # 如果是顶级标题（#）并且主标题尚未设置，则将其作为主标题
                if level == 1 and not main_title_set:
                    main_title = heading_text
                    main_title_set = True
                else:
                    # 更新标题栈：弹出比当前级别低或相等的标题（严格按层级）
                    while len(title_stack) >= level:
                        title_stack.pop()
                    title_stack.append(line)  # 保存原始行以便后续提取文本
                i += 1
                continue

            # 尝试设置主标题（仅一次，取第一个非空、非标题、非代码行）
            if not main_title_set and line.strip() and not heading_match and not line.strip().startswith('```'):
                main_title = line.strip()
                main_title_set = True

            # 收集连续的正文段落（直到下一个标题或文件结束）
            para_lines = []
            j = i
            while j < len(lines):
                current_line = lines[j]
                if current_line.strip().startswith('```'):
                    in_code_block = not in_code_block
                if in_code_block:
                    j += 1
                    continue

                # 检查是否是新标题
                if re.match(r'^#{1,6}\s+', current_line):
                    break
                para_lines.append(current_line)
                j += 1

            # 合并段落为一个字符串（保留换行或转为空格）
            paragraph = '\n'.join(para_lines).strip()
            if paragraph:
                # 替换多个空白为单个空格（可选，便于按字符切分）
                clean_para = re.sub(r'\s+', ' ', paragraph)

                title_path = get_title_path()
                prefix = title_path + ':' if title_path else ''

                # 按 max_chunk_size 分块
                start = 0
                while start < len(clean_para):
                    end = start + self.max_chunk_size
                    chunk_text = clean_para[start:end]
                    chunks.append(prefix + chunk_text)
                    start = end

            i = j  # 跳过已处理的正文行

        return chunks

    async def chunk_document_async(self, md_content: str, doc_id: Optional[str] = None) -> List[str]:
        """
        对叙述型Markdown文档进行分块（异步版本）
        
        参数:
            md_content (str): Markdown文档内容
            doc_id (Optional[str]): 文档ID，用于日志记录
            
        返回:
            List[str]: 分块后的文档列表
        """
        # 异步实现与同步一致，因为没有IO操作
        return self.chunk_document(md_content, doc_id)

    def chunk_document_file(self, file_path: str) -> List[str]:
        """
        对叙述型Markdown文档文件进行分块
        
        参数:
            file_path (str): Markdown文档文件路径
            
        返回:
            List[str]: 分块后的文档列表
        """
        import os
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文档文件不存在: {file_path}")
        
        # 读取文件内容
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            raise RuntimeError(f"读取文档文件时出错: {str(e)}")
        
        # 使用文件名作为文档ID
        doc_id = os.path.basename(file_path)
        
        # 调用分块方法
        return self.chunk_document(content, doc_id)

    async def chunk_document_file_async(self, file_path: str) -> List[str]:
        """
        对叙述型Markdown文档文件进行分块（异步版本）
        
        参数:
            file_path (str): Markdown文档文件路径
            
        返回:
            List[str]: 分块后的文档列表
        """
        # 异步实现与同步一致，因为没有IO操作
        return self.chunk_document_file(file_path)


# 为了向后兼容，也提供函数接口
def chunk_narrative_document(md_content: str, doc_id: Optional[str] = None, max_chunk_size: int = 300) -> List[str]:
    """
    对叙述型Markdown文档进行分块（函数接口）
    
    参数:
        md_content (str): Markdown文档内容
        doc_id (Optional[str]): 文档ID，用于日志记录
        max_chunk_size (int): 每个块的最大字符数，默认300
        
    返回:
        List[str]: 分块后的文档列表
    """
    chunker = NarrativeMarkdownChunker(max_chunk_size)
    return chunker.chunk_document(md_content, doc_id)


async def chunk_narrative_document_async(md_content: str, doc_id: Optional[str] = None, max_chunk_size: int = 300) -> List[str]:
    """
    对叙述型Markdown文档进行分块（函数接口，异步版本）
    
    参数:
        md_content (str): Markdown文档内容
        doc_id (Optional[str]): 文档ID，用于日志记录
        max_chunk_size (int): 每个块的最大字符数，默认300
        
    返回:
        List[str]: 分块后的文档列表
    """
    chunker = NarrativeMarkdownChunker(max_chunk_size)
    return await chunker.chunk_document_async(md_content, doc_id)


def chunk_narrative_document_file(file_path: str, max_chunk_size: int = 300) -> List[str]:
    """
    对叙述型Markdown文档文件进行分块（函数接口）
    
    参数:
        file_path (str): Markdown文档文件路径
        max_chunk_size (int): 每个块的最大字符数，默认300
        
    返回:
        List[str]: 分块后的文档列表
    """
    chunker = NarrativeMarkdownChunker(max_chunk_size)
    return chunker.chunk_document_file(file_path)


async def chunk_narrative_document_file_async(file_path: str, max_chunk_size: int = 300) -> List[str]:
    """
    对叙述型Markdown文档文件进行分块（函数接口，异步版本）
    
    参数:
        file_path (str): Markdown文档文件路径
        max_chunk_size (int): 每个块的最大字符数，默认300
        
    返回:
        List[str]: 分块后的文档列表
    """
    chunker = NarrativeMarkdownChunker(max_chunk_size)
    return await chunker.chunk_document_file_async(file_path)