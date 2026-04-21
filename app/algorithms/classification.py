#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文档分类算法
用于对处理后的Markdown文档进行分类
"""

import json
import logging
import requests
import asyncio
import re
from pathlib import Path
from typing import Union, Dict, Any, Optional, List, Tuple
from app.core.config import ChatLLMConfig, ClassificationConfig
from app.core.utils import AsyncLLMClient
from app.core.prompts.classification import ClassificationPrompts
from app.core.regex_matcher import extract_clean_name
from app.algorithms.similarity import EmbeddingSimilarityCalculator

# 配置日志
logger = logging.getLogger(__name__)

# 简单的内存存储，用于临时保存分类结果
classification_cache: Dict[str, Dict[str, Any]] = {}


def _get_persistent_storage_path() -> Path:
    """
    获取持久化存储路径
    
    返回:
        Path: 分类结果存储目录路径
    """
    # 默认存储在 data/processed/classifications 目录下
    storage_path = Path("data/processed/classifications")
    storage_path.mkdir(parents=True, exist_ok=True)
    return storage_path


def _save_classification_persistent(doc_id: str, result: Dict[str, Any]) -> None:
    """
    持久化保存分类结果到文件
    
    参数:
        doc_id (str): 文档ID
        result (Dict[str, Any]): 分类结果
    """
    storage_path = _get_persistent_storage_path()
    file_path = storage_path / f"{doc_id}.json"
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.debug(f"分类结果已持久化保存到: {file_path}")
    except Exception as e:
        logger.error(f"保存分类结果到文件时出错: {str(e)}")


def _load_classification_persistent(doc_id: str) -> Optional[Dict[str, Any]]:
    """
    从持久化存储中加载分类结果
    
    参数:
        doc_id (str): 文档ID
        
    返回:
        Optional[Dict[str, Any]]: 分类结果，如果未找到则返回None
    """
    storage_path = _get_persistent_storage_path()
    file_path = storage_path / f"{doc_id}.json"
    
    if not file_path.exists():
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            result = json.load(f)
        logger.debug(f"从持久化存储加载分类结果: {file_path}")
        return result
    except Exception as e:
        logger.error(f"从文件加载分类结果时出错: {str(e)}")
        return None


def extract_highest_level_categories(standard_file: str = None, specification_uid: str = None) -> List[str]:
    """
    从标准文件中提取最高级别的分类名称
    
    参数:
        standard_file (str): 标准分类文件路径
        specification_uid (str): 规范UId，用于构建标准文件路径
        
    返回:
        List[str]: 最高级别分类名称列表
    """
    # 如果没有指定标准文件路径，则根据specification_uid构建路径
    if standard_file is None:
        if specification_uid:
            specification_uid = specification_uid.replace("-", "_")
            standard_file = f"data/standards/{specification_uid}_standard.jsonl"
        else:
            standard_file = "data/standards/standard.jsonl"
    
    highest_level_names = []
    
    try:
        with open(standard_file, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                # 获取data字段
                data_dict = data.get("data", {})

                # 直接取二级分类（key "1"）
                highest_level_name = data_dict.get("1")

                if highest_level_name and isinstance(highest_level_name, str):
                    # 使用通用的去前缀方法提取纯名称
                    clean_name = extract_clean_name(highest_level_name)

                    # 添加到结果列表中（避免重复）
                    if clean_name and clean_name not in highest_level_names:
                        highest_level_names.append(clean_name)
                        
    except Exception as e:
        logger.error(f"读取标准分类文件时出错: {str(e)}")
        raise
    
    logger.info(f"提取到 {len(highest_level_names)} 个最高级别分类名称")
    return highest_level_names


async def match_folder_to_standard_category(folder_name: str, 
                                          standard_file: str = None,
                                          specification_uid: str = None) -> Tuple[str, float]:
    """
    将文件夹名称与标准分类进行匹配
    
    参数:
        folder_name (str): 文件夹名称
        standard_file (str): 标准分类文件路径
        specification_uid (str): 规范UId，用于构建标准文件路径
        
    返回:
        Tuple[str, float]: (匹配的分类名称, 相似度分数)
    """
    # 如果没有指定标准文件路径，则根据specification_uid构建路径
    if standard_file is None:
        if specification_uid:
            specification_uid = specification_uid.replace("-", "_")
            standard_file = f"data/standards/{specification_uid}_standard.jsonl"
        else:
            standard_file = "data/standards/standard.jsonl"
    
    # 提取标准分类名称
    standard_categories = extract_highest_level_categories(standard_file, specification_uid)
    
    # 使用嵌入相似度计算器
    calculator = EmbeddingSimilarityCalculator()
    
    # 计算文件夹名称与所有标准分类的相似度
    max_similarity = -1.0
    best_match = ""
    
    for category in standard_categories:
        try:
            similarity = await calculator.calculate_similarity(folder_name, category)
            if similarity > max_similarity:
                max_similarity = similarity
                best_match = category
        except Exception as e:
            logger.warning(f"计算 '{folder_name}' 与 '{category}' 的相似度时出错: {str(e)}")
            continue
    
    return (best_match, max_similarity)


def classify_document_by_llm(md_content: str, doc_id: Optional[str] = None) -> Dict[str, Any]:
    """
    使用LLM对文档进行分类（同步版本）
    
    参数:
        md_content (str): Markdown文档内容
        doc_id (Optional[str]): 文档ID，用于缓存分类结果
        
    返回:
        Dict[str, Any]: 分类结果，格式为 {"class": "1"} 或 {"class": "2"}
    """
    
    # 对内容进行长度限制，避免超出LLM处理能力
    max_length = ClassificationConfig.max_content_length
    if len(md_content) > max_length:
        md_content = md_content[:max_length]
        logger.debug(f"文档内容已截断至{max_length}字符")
    
    # 构造提示词
    prompt = ClassificationPrompts.get_document_classification_prompt(md_content)
    
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
        logger.debug(f"向LLM服务发送分类请求: {ChatLLMConfig.url}")
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
            logger.debug(f"LLM回复: {llm_response}")
            
            # 尝试解析LLM的JSON回复
            try:
                classification_result = json.loads(llm_response)
                # 缓存结果
                if doc_id:
                    # 根据配置决定存储方式
                    if ClassificationConfig.storage_mode == "temporary":
                        classification_cache[doc_id] = classification_result
                    elif ClassificationConfig.storage_mode == "persistent":
                        _save_classification_persistent(doc_id, classification_result)
                return classification_result
            except json.JSONDecodeError as e:
                logger.error(f"解析LLM回复的JSON时出错: {str(e)}, 回复内容: {llm_response}")
                raise RuntimeError(f"LLM回复格式错误: {llm_response}")
        else:
            raise RuntimeError("LLM服务返回格式错误")
            
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"请求LLM服务时发生网络错误: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"文档分类时发生未知错误: {str(e)}")


async def classify_documents_by_llm_async(md_contents: List[str], doc_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    使用LLM对多个文档进行分类（异步版本）
    
    参数:
        md_contents (List[str]): Markdown文档内容列表
        doc_ids (Optional[List[str]]): 文档ID列表，用于缓存分类结果
        
    返回:
        List[Dict[str, Any]]: 分类结果列表
    """
    if doc_ids is None:
        doc_ids = [None] * len(md_contents)
    
    if len(md_contents) != len(doc_ids):
        raise ValueError("文档内容列表和文档ID列表长度必须相同")
    
    # 构造请求列表
    requests_data = []
    
    for md_content in md_contents:
        # 对内容进行长度限制，避免超出LLM处理能力
        max_length = ClassificationConfig.max_content_length
        if len(md_content) > max_length:
            md_content = md_content[:max_length]
            logger.debug(f"文档内容已截断至{max_length}字符")
        
        # 构造提示词
        prompt = ClassificationPrompts.get_document_classification_prompt(md_content)
        
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
                results.append({"error": response["error"]})
                continue
                
            # 提取LLM的回复
            if "choices" in response and len(response["choices"]) > 0:
                llm_response = response["choices"][0]["message"]["content"]
                logger.debug(f"LLM回复: {llm_response}")
                
                # 尝试解析LLM的JSON回复
                try:
                    classification_result = json.loads(llm_response)
                    results.append(classification_result)
                    
                    # 缓存结果
                    if doc_ids[i]:
                        # 根据配置决定存储方式
                        if ClassificationConfig.storage_mode == "temporary":
                            classification_cache[doc_ids[i]] = classification_result
                        elif ClassificationConfig.storage_mode == "persistent":
                            _save_classification_persistent(doc_ids[i], classification_result)
                except json.JSONDecodeError as e:
                    logger.error(f"解析LLM回复的JSON时出错: {str(e)}, 回复内容: {llm_response}")
                    results.append({"error": f"LLM回复格式错误: {llm_response}"})
            else:
                results.append({"error": "LLM服务返回格式错误"})
        except Exception as e:
            logger.error(f"处理第{i}个文档分类结果时发生错误: {str(e)}")
            results.append({"error": f"处理文档分类结果时发生错误: {str(e)}"})
    
    return results


def classify_document_file(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    对文档文件进行分类
    
    参数:
        file_path (Union[str, Path]): 文档文件路径
        
    返回:
        Dict[str, Any]: 分类结果
    """
    file = Path(file_path)
    
    # 检查文件是否存在
    if not file.exists():
        raise FileNotFoundError(f"文档文件不存在: {file_path}")
    
    # 读取文件内容
    try:
        with open(file, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        raise RuntimeError(f"读取文档文件时出错: {str(e)}")
    
    # 使用文件名作为文档ID
    doc_id = file.stem
    
    # 调用LLM进行分类
    return classify_document_by_llm(content, doc_id)


async def classify_document_files_async(file_paths: List[Union[str, Path]]) -> List[Dict[str, Any]]:
    """
    对多个文档文件进行分类（异步版本）
    
    参数:
        file_paths (List[Union[str, Path]]): 文档文件路径列表
        
    返回:
        List[Dict[str, Any]]: 分类结果列表
    """
    # 读取所有文件内容
    md_contents = []
    doc_ids = []
    
    for file_path in file_paths:
        file = Path(file_path)
        
        # 检查文件是否存在
        if not file.exists():
            raise FileNotFoundError(f"文档文件不存在: {file_path}")
        
        # 读取文件内容
        try:
            with open(file, 'r', encoding='utf-8') as f:
                content = f.read()
            md_contents.append(content)
            doc_ids.append(file.stem)
        except Exception as e:
            raise RuntimeError(f"读取文档文件时出错: {str(e)}")
    
    # 调用LLM进行分类
    return await classify_documents_by_llm_async(md_contents, doc_ids)


def get_cached_classification(doc_id: str) -> Optional[Dict[str, Any]]:
    """
    获取缓存的分类结果
    
    参数:
        doc_id (str): 文档ID
        
    返回:
        Optional[Dict[str, Any]]: 分类结果，如果未找到则返回None
    """
    # 根据配置决定从哪里获取数据
    if ClassificationConfig.storage_mode == "temporary":
        return classification_cache.get(doc_id)
    elif ClassificationConfig.storage_mode == "persistent":
        # 先尝试从内存缓存获取，如果没有再从持久化存储获取
        if doc_id in classification_cache:
            return classification_cache[doc_id]
        else:
            result = _load_classification_persistent(doc_id)
            # 如果从持久化存储获取到了，也放到内存缓存中
            if result:
                classification_cache[doc_id] = result
            return result
    return None


def get_all_cached_classifications() -> Dict[str, Dict[str, Any]]:
    """
    获取所有缓存的分类结果
    
    返回:
        Dict[str, Dict[str, Any]]: 所有缓存的分类结果
    """
    if ClassificationConfig.storage_mode == "temporary":
        return classification_cache.copy()
    elif ClassificationConfig.storage_mode == "persistent":
        # 从持久化存储目录中获取所有分类结果
        storage_path = _get_persistent_storage_path()
        all_classifications = {}
        
        # 从内存缓存获取
        all_classifications.update(classification_cache)
        
        # 从文件系统获取
        try:
            for file_path in storage_path.glob("*.json"):
                doc_id = file_path.stem
                if doc_id not in all_classifications:  # 避免重复
                    result = _load_classification_persistent(doc_id)
                    if result:
                        all_classifications[doc_id] = result
        except Exception as e:
            logger.error(f"获取持久化存储的分类结果时出错: {str(e)}")
            
        return all_classifications
    return {}


def clear_classification_cache():
    """
    清空分类结果缓存
    """
    if ClassificationConfig.storage_mode == "temporary":
        classification_cache.clear()
    elif ClassificationConfig.storage_mode == "persistent":
        # 清空内存缓存
        classification_cache.clear()
        # 删除持久化存储文件
        storage_path = _get_persistent_storage_path()
        try:
            for file_path in storage_path.glob("*.json"):
                file_path.unlink()
            logger.info("已清空持久化存储的分类结果")
        except Exception as e:
            logger.error(f"清空持久化存储的分类结果时出错: {str(e)}")