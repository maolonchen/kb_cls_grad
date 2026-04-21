# !/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
知识库后处理服务
负责对预处理后的文件进行向量化处理
"""

import os
import jieba
import json
import logging
import re
from pathlib import Path
from typing import List, Dict, Any, Tuple
from app.core.vectoring import VectorClient
from app.core.regex_matcher import RegexMatcher
from app.core.config import VectorizedDataConfig
from app.algorithms.classification import get_cached_classification

logger = logging.getLogger(__name__)


class KnowledgePostprocessService:
    """知识库后处理服务类"""

    def __init__(self, chunks_dir: str = None, 
                 standard_file: str = None,
                 specification_uid: str = "default"):
        """
        初始化后处理服务
        
        Args:
            chunks_dir: 预处理文件目录路径
            standard_file: 标准分类文件路径
            specification_uid: 行业名ID（用户上传的specificationUId对应的值）
        """
        # 如果没有指定chunks_dir，则根据specification_uid构建路径
        if specification_uid:
            specification_uid = '_' + specification_uid if not specification_uid.startswith('_') else specification_uid
            specification_uid = specification_uid.replace("-", "_")
            
        if chunks_dir is None:
            chunks_dir = f"./data/processed/{specification_uid}_chunks"
            
        self.chunks_dir = Path(chunks_dir)
        
        # 如果没有指定标准文件路径，则根据specification_uid构建路径
        if standard_file is None:
            standard_file = f"./data/standards/{specification_uid}_standard.jsonl"
            
        self.standard_file = Path(standard_file)
        self.specification_uid = specification_uid
        self.vector_client = VectorClient()
        # self.regex_matcher = RegexMatcher(str(self.standard_file), specification_uid)
        self.regex_matcher = RegexMatcher(standard_file=str(self.standard_file), specification_uid=specification_uid)
        
        # 确保目录存在
        if not self.chunks_dir.exists():
            raise FileNotFoundError(f"Chunks目录不存在: {self.chunks_dir}")
            
        if not self.standard_file.exists():
            raise FileNotFoundError(f"标准分类文件不存在: {self.standard_file}")

    def tokenize_text(self, text: str) -> List[str]:
        """
        对文本进行jieba分词
        
        Args:
            text: 待分词的文本
            
        Returns:
            List[str]: 分词结果列表
        """
        return list(jieba.cut(text))

    def extract_standard_category_info(self, target_category: str) -> Dict[str, Any]:
        """
        从标准文件中提取特定分类的完整信息
        
        Args:
            target_category: 目标分类名称
            
        Returns:
            Dict[str, Any]: 分类的完整信息
        """
        return self.regex_matcher.get_category_info(target_category)

    def format_category_info_text(self, category_info: Dict[str, Any]) -> str:
        """
        将分类信息格式化为文本
        
        Args:
            category_info: 分类信息字典
            
        Returns:
            str: 格式化后的文本
        """
        if not category_info:
            return ""
            
        data_dict = category_info.get("data", {})
        
        # 动态处理所有键值对
        parts = []
        # 获取所有键并排序
        all_keys = list(data_dict.keys())
        try:
            # 尝试按数字排序
            sorted_keys = sorted([k for k in all_keys if k.isdigit()], key=int)
            # 添加非数字键
            non_numeric_keys = [k for k in all_keys if not k.isdigit()]
            sorted_keys.extend(sorted(non_numeric_keys))
        except:
            # 如果不能按数字排序，则按字母排序
            sorted_keys = sorted(all_keys)
        
        # 按排序后的键处理值
        for key in sorted_keys:
            value = data_dict[key]
            # 如果值是列表，将其转换为字符串
            if isinstance(value, list):
                if value:  # 只有非空列表才添加
                    parts.append(", ".join(str(v) for v in value))
            elif isinstance(value, (str, int, float)):
                str_value = str(value)
                if str_value:  # 只有非空字符串才添加
                    parts.append(str_value)
                
        return ", ".join(parts)

    def get_collection_name(self, is_general_knowledge: bool = False, is_narrative: bool = False) -> str:
        """
        获取集合名称
        
        Args:
            is_general_knowledge: 是否为通用知识
            is_narrative: 是否为叙述型分类知识
            
        Returns:
            str: 集合名称
        """
        # 将specification_uid中的连字符替换为下划线，以符合Milvus命名规范
        safe_specification_uid = self.specification_uid.replace("-", "_")
        
        # 确保集合名称以字母或下划线开头（Milvus要求）
        if safe_specification_uid and not safe_specification_uid[0].isalpha() and safe_specification_uid[0] != '_':
            safe_specification_uid = '_' + safe_specification_uid
        
        if is_general_knowledge:
            return f"{safe_specification_uid}_general_knowledge"
        elif is_narrative:
            return f"{safe_specification_uid}_narrative_classification"
        else:
            return f"{safe_specification_uid}_classification"

    async def process_general_knowledge_chunks(self) -> List[Dict[str, Any]]:
        """
        处理通用知识chunks目录下的所有markdown文件并进行向量化
        
        Returns:
            List[Dict[str, Any]]: 包含text、vector和items的列表
        """
        results = []
        general_knowledge_dir = self.chunks_dir / "general_knowledge"
        
        if not general_knowledge_dir.exists():
            logger.warning(f"通用知识目录不存在: {general_knowledge_dir}")
            return results
            
        logger.info(f"处理通用知识目录: {general_knowledge_dir}")
        
        # 查找所有markdown文件
        markdown_files = list(general_knowledge_dir.glob("*.md"))
        
        if not markdown_files:
            logger.warning(f"在目录 {general_knowledge_dir} 中未找到markdown文件")
            return results
            
        logger.info(f"找到 {len(markdown_files)} 个markdown文件待处理")
        
        # 收集所有文本行用于批量向量化
        all_lines = []
        line_metadata = []  # 保存每行的元数据
        
        # 读取所有markdown文件
        for md_file in markdown_files:
            logger.info(f"处理文件: {md_file.name}")
            
            try:
                with open(md_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    
                for line_num, line in enumerate(lines):
                    line = line.strip()
                    if line:  # 跳过空行
                        all_lines.append(line)
                        line_metadata.append({
                            'file': md_file.name,
                            'line_num': line_num + 1,
                            'original_text': line
                        })
                        
            except Exception as e:
                logger.error(f"读取文件 {md_file} 时出错: {e}")
                continue
        
        if not all_lines:
            logger.warning("未找到任何有效文本行")
            return results
            
        logger.info(f"总共收集到 {len(all_lines)} 行文本待向量化")
        
        # 批量获取向量化结果
        try:
            # 将文本分批处理，避免一次性处理太多文本
            batch_size = 100
            all_vectors = []
            for i in range(0, len(all_lines), batch_size):
                batch_lines = all_lines[i:i+batch_size]
                for idx, line in enumerate(batch_lines):
                    print(f"General knowledge vectorizing text[{i+idx}]: {line}")
                batch_vectors = await self.vector_client.get_embeddings(batch_lines)
                all_vectors.extend(batch_vectors)
                
        except Exception as e:
            logger.error(f"向量化过程中出错: {e}")
            raise
            
        # 处理结果
        for i, (line_data, vector) in enumerate(zip(line_metadata, all_vectors)):
            text = line_data['original_text']
            items = self.tokenize_text(text)
            
            result = {
                'text': text,
                'vector': vector,
                'items': items
            }
            
            results.append(result)
            
        logger.info(f"成功处理通用知识 {len(results)} 行文本")
        return results

    async def process_category_chunks(self) -> List[Dict[str, Any]]:
        """
        处理分类chunks目录下的所有markdown文件并进行向量化
        
        Returns:
            List[Dict[str, Any]]: 包含text、vector和items的列表
        """
        results = []
        
        # 遍历所有非general_knowledge的子目录
        for category_dir in self.chunks_dir.iterdir():
            if category_dir.is_dir() and category_dir.name != "general_knowledge":
                logger.info(f"处理分类目录: {category_dir.name}")
                
                # 递归遍历所有子目录，找到包含.md文件的最深层目录
                async def process_subdirectories(directory_path):
                    # 查找该目录下的所有markdown文件
                    markdown_files = list(directory_path.glob("*.md"))
                    
                    # 如果当前目录有.md文件，则处理这些文件
                    if markdown_files:
                        # 获取最深层的目录名称用于正则匹配
                        deepest_dir_name = directory_path.name
                        
                        # 通过正则表达式匹配获取所有可能匹配的标准分类
                        try:
                            matched_categories = self.regex_matcher.find_all_matches(deepest_dir_name, min_score=0.5)
                            logger.info(f"目录 '{deepest_dir_name}' 匹配到 {len(matched_categories)} 个标准分类")

                            if not matched_categories:
                                logger.warning(f"目录 '{deepest_dir_name}' 未匹配到任何标准分类")
                                return

                            # 只使用得分最高的最佳匹配（第一个）
                            category_info, score = matched_categories[0]
                            logger.info(f"使用最佳匹配: score={score:.3f}, 分类信息: {category_info.get('data', {}).get('1', 'UNKNOWN')}")

                            # 格式化分类信息为文本
                            category_info_text = self.format_category_info_text(category_info)

                            # 处理该目录下的所有markdown文件（只处理一次）
                            await process_markdown_files(directory_path, markdown_files, category_info, category_info_text)

                        except Exception as e:
                            logger.error(f"匹配目录 '{deepest_dir_name}' 到标准分类时出错: {e}")
                            return
                    
                    # 递归处理子目录
                    for sub_dir in directory_path.iterdir():
                        if sub_dir.is_dir():
                            await process_subdirectories(sub_dir)
                
                async def process_markdown_files(directory_path, markdown_files, category_info, category_info_text):
                    nonlocal results
                    
                    if not markdown_files:
                        logger.warning(f"在分类目录 {directory_path.name} 中未找到markdown文件")
                        return
                        
                    logger.info(f"在分类目录 {directory_path.name} 中找到 {len(markdown_files)} 个markdown文件")
                    
                    # 收集所有文本行用于批量向量化
                    all_lines = []
                    line_metadata = []  # 保存每行的元数据
                    
                    # 读取所有markdown文件
                    for md_file in markdown_files:
                        logger.debug(f"处理文件: {md_file.name}")
                        
                        try:
                            with open(md_file, 'r', encoding='utf-8') as f:
                                lines = f.readlines()
                                
                            for line_num, line in enumerate(lines):
                                line = line.strip()
                                if line:  # 跳过空行
                                    all_lines.append(line)
                                    # 获取文档ID以检查分类结果
                                    # 使用与预处理阶段相同的ID生成逻辑
                                    file_stem = md_file.stem
                                    # 尝试不同的ID格式来获取分类结果
                                    possible_doc_ids = [
                                        file_stem,  # 基本文档ID
                                        f"{self.specification_uid}_{file_stem}",  # 带规范UID的ID
                                        # 我们无法重建完整的ID，因为UUID部分是随机的
                                    ]
                                    
                                    classification_result = None
                                    doc_class = "2"  # 默认值
                                    
                                    # 在分类结果目录中查找匹配的文件
                                    classification_dir = Path("data/processed/classifications")
                                    if classification_dir.exists():
                                        for class_file in classification_dir.glob(f"*{file_stem}*.json"):
                                            try:
                                                classification_result = get_cached_classification(class_file.stem)
                                                if classification_result:
                                                    doc_class = classification_result.get("class", "1")
                                                    logger.debug(f"通过文件匹配找到分类结果: {class_file.name}, 分类: {doc_class}")
                                                    break
                                            except Exception as e:
                                                logger.warning(f"读取分类文件 {class_file} 时出错: {e}")
                                    
                                    # 如果通过文件名匹配没找到，尝试直接使用ID查找
                                    if not classification_result:
                                        for doc_id in possible_doc_ids:
                                            classification_result = get_cached_classification(doc_id)
                                            if classification_result:
                                                doc_class = classification_result.get("class", "1")
                                                logger.debug(f"通过ID匹配找到分类结果: {doc_id}, 分类: {doc_class}")
                                                break
                                    
                                    logger.debug(f"文件 {md_file.name} 的分类结果: {classification_result}, 分类类型: {doc_class}")
                                    
                                    line_metadata.append({
                                        'category': directory_path.name,
                                        'category_info': category_info,
                                        'category_info_text': category_info_text,
                                        'file': md_file.name,
                                        'line_num': line_num + 1,
                                        'original_text': line,
                                        'doc_class': doc_class  # 添加分类结果
                                    })
                                    
                        except Exception as e:
                            logger.error(f"读取文件 {md_file} 时出错: {e}")
                            continue
                    
                    if not all_lines:
                        logger.warning(f"分类目录 {directory_path.name} 中未找到任何有效文本行")
                        return
                        
                    logger.info(f"分类目录 {directory_path.name} 总共收集到 {len(all_lines)} 行文本待向量化")
                    
                    # 批量获取向量化结果
                    try:
                        # 将文本分批处理，避免一次性处理太多文本
                        batch_size = 100
                        all_vectors = []
                        for i in range(0, len(all_lines), batch_size):
                            batch_lines = all_lines[i:i+batch_size]
                            # 记录每个批次的文本来源（用于验证顺序）
                            batch_log_msg = []
                            for idx, line in enumerate(batch_lines):
                                # 尝试获取文本对应的分类信息
                                line_info = line_metadata[i+idx]
                                category_text = line_info.get('category_info', {}).get('data', {}).get('1', 'UNKNOWN')[:30]
                                batch_log_msg.append(f"line[{i+idx}]: {category_text}... | {line[:50]}")
                                print(f"Vectorizing text[{i+idx}]: {line[:50]}")

                            logger.debug(f"处理批次 {i//batch_size}: {' | '.join(batch_log_msg)}")

                            # 获取嵌入向量
                            batch_vectors = await self.vector_client.get_embeddings(batch_lines)
                            all_vectors.extend(batch_vectors)
                            
                    except Exception as e:
                        logger.error(f"分类目录 {directory_path.name} 向量化过程中出错: {e}")
                        return
                    
                    # 处理结果
                    for i, (line_data, vector) in enumerate(zip(line_metadata, all_vectors)):
                        # 保存原始的分类信息JSON字符串作为text字段
                        # 将分类信息转换为JSON字符串
                        text = json.dumps(line_data['category_info'], ensure_ascii=False)
                        # 原始行文本
                        original_text = line_data['original_text']
                        # 合并原始文本和分类信息文本用于分词
                        combined_text = original_text + " " + line_data['category_info_text']
                        items = self.tokenize_text(combined_text)

                        # 添加日志：检查 text 和 vectorizing_text 的分类一致性
                        try:
                            text_category = text.split('"data"')[1].split('"')[1] if '"data"' in text else "UNKNOWN"
                            if len(text_category) > 50:
                                text_category = text_category[:50] + "..."
                        except:
                            text_category = "EXTRACT_ERROR"

                        logger.debug(f"[分类一致性检查] 索引={i}")
                        logger.debug(f"  text分类信息: {text_category}")
                        logger.debug(f"  vectorizing_text前50字符: {original_text[:50]}")
                        logger.debug(f"  是否属于同一分类: {text_category in original_text or original_text in text_category}")

                        print("text----->", text)

                        # 1. 添加原始记录
                        result = {
                            'text': text,           # 使用原始分类信息的JSON字符串
                            'vector': vector,       # 原始行文本的向量化结果
                            'items': items,         # 原始行文本 + 标准分类信息的分词结果
                            'doc_class': line_data['doc_class'],  # 添加分类结果
                            'vectorizing_text': original_text
                        }
                        results.append(result)

                        # 2. 从 original_text 中提取分类值并生成额外的向量记录
                        try:
                            # 解析 original_text 的 JSON
                            original_json = json.loads(original_text)
                            header = original_json.get('header', {})
                            data = original_json.get('data', {})

                            # 找到"等级"对应的键
                            grade_key = None
                            for key, value in header.items():
                                if value == "等级":
                                    grade_key = key
                                    break

                            # 提取有效的分类值（排除空值、null、"[]"和等级值）
                            valid_values = []
                            for key, value in data.items():
                                # 跳过等级字段
                                if grade_key and key == grade_key:
                                    continue
                                # 跳过空值
                                if value is None or value == "" or value == "[]" or value == []:
                                    continue
                                # 转换为字符串
                                if isinstance(value, str):
                                    valid_values.append(value)
                                elif isinstance(value, list) and len(value) > 0:
                                    # 如果是非空列表，将每个元素作为单独的值
                                    valid_values.extend([str(v) for v in value if v])

                            # 为每个有效值生成新的向量记录
                            if valid_values:
                                logger.debug(f"从 original_text 中提取到 {len(valid_values)} 个有效分类值: {valid_values}")

                                # 批量获取这些值的向量
                                additional_vectors = await self.vector_client.get_embeddings(valid_values)

                                for value, additional_vector in zip(valid_values, additional_vectors):
                                    # 合并单个分类值和分类信息文本用于分词
                                    combined_text_additional = value + " " + line_data['category_info_text']
                                    items_additional = self.tokenize_text(combined_text_additional)

                                    additional_result = {
                                        'text': text,  # text 保持不变
                                        'vector': additional_vector,  # 新的向量
                                        'items': items_additional,  # 重新分词
                                        'doc_class': line_data['doc_class'],
                                        'vectorizing_text': value  # 单个分类值
                                    }
                                    results.append(additional_result)

                                # 生成拼接值的向量记录
                                if len(valid_values) > 1:
                                    concatenated_text = "、".join(valid_values)
                                    concatenated_vector = (await self.vector_client.get_embeddings([concatenated_text]))[0]
                                    combined_text_concat = concatenated_text + " " + line_data['category_info_text']
                                    items_concat = self.tokenize_text(combined_text_concat)

                                    concat_result = {
                                        'text': text,
                                        'vector': concatenated_vector,
                                        'items': items_concat,
                                        'doc_class': line_data['doc_class'],
                                        'vectorizing_text': concatenated_text
                                    }
                                    results.append(concat_result)

                        except json.JSONDecodeError:
                            # 如果 original_text 不是 JSON 格式，跳过额外处理
                            logger.debug(f"original_text 不是有效的 JSON，跳过拆分处理")
                        except Exception as e:
                            logger.warning(f"处理额外向量记录时出错: {e}")

                    # 统计分类不匹配的数量
                    mismatch_count = 0
                    for i, result in enumerate(results):
                        try:
                            text_data = json.loads(result['text'])
                            text_category = text_data.get('data', {}).get('1', 'UNKNOWN')
                            vectorizing_text_sub = result['vectorizing_text'][:50]

                            if text_category not in result['vectorizing_text'] and result['vectorizing_text'] not in text_category:
                                mismatch_count += 1
                                if mismatch_count <= 5:  # 只记录前5个不匹配的详细日志
                                    logger.warning(f"分类不匹配[#{mismatch_count}]: text_category={text_category}, vectorizing_text包含={vectorizing_text_sub}...")
                        except:
                            pass

                    logger.info(f"成功处理分类目录 {directory_path.name} 共 {len(results)} 行文本，其中 {mismatch_count} 个存在分类不匹配")
                
                # 开始处理目录
                await process_subdirectories(category_dir)
        
        return results

    async def process_all_chunks_and_insert_to_milvus(self) -> Dict[str, int]:
        """
        处理所有chunks目录下的文件并插入到Milvus数据库
        
        Returns:
            Dict[str, int]: 各个集合插入的记录数
        """
        import time
        import hashlib
        result_stats = {}
        # 处理通用知识
        logger.info("开始处理通用知识并插入到Milvus")
        general_results = await self.process_general_knowledge_chunks()
        general_collection_name = self.get_collection_name(is_general_knowledge=True)
        
        if general_results:
            # 创建或使用通用知识集合
            temp_client = VectorClient()
            temp_client.collection_name = general_collection_name
            
            if not temp_client.has_collection():
                temp_client.create_collection()
                logger.info(f"创建通用知识集合: {general_collection_name}")
            else:
                # 清空现有集合中的所有数据
                temp_client.drop_collection()
                temp_client.create_collection()
                logger.info(f"清空并重新创建通用知识集合: {general_collection_name}")
            
            temp_client.load_collection()
            
            # 为每条记录生成唯一内容哈希
            general_data = []
            timestamp_base = int(time.time() * 1000000) % 100000000  # 取时间戳的一部分作为基础
            existing_hashes = set()
            
            # 查询现有记录的内容哈希
            try:
                existing_records = temp_client.milvus_client.query(
                    collection_name=general_collection_name,
                    filter="id >= 0",
                    output_fields=["text"]
                )
                for record in existing_records:
                    content_hash = hashlib.md5(record['text'].encode('utf-8')).hexdigest()
                    existing_hashes.add(content_hash)
            except Exception as e:
                logger.warning(f"查询现有记录时出错: {e}")
            
            # 为每条记录生成唯一内容哈希
            new_records_count = 0
            for i, result in enumerate(general_results):
                # 为内容生成哈希以检查重复
                # 添加唯一标识符以避免误判
                content_hash = hashlib.md5((result['text'] + str(timestamp_base + i)).encode('utf-8')).hexdigest()
                if content_hash not in existing_hashes:
                    record = {
                        "id": timestamp_base + new_records_count,
                        "text": result['text'],
                        "vector": result['vector'],
                        "items": ", ".join(result['items']),
                        "vectorizing_text": result.get('vectorizing_text', result['text'])
                    }
                    general_data.append(record)
                    new_records_count += 1
            
            # 插入新数据
            if general_data:
                insert_result = temp_client.insert_data(general_data)
                
                # 确保数据写入磁盘
                try:
                    temp_client.milvus_client.flush(collection_name=general_collection_name)
                    logger.info(f"集合 {general_collection_name} 数据已刷新到磁盘")
                except Exception as e:
                    logger.warning(f"刷新集合 {general_collection_name} 数据时出错: {e}")
                
                result_stats[general_collection_name] = len(insert_result['ids'])
                logger.info(f"成功向集合 {general_collection_name} 插入 {len(insert_result['ids'])} 条新记录")
            else:
                result_stats[general_collection_name] = 0
                logger.info(f"通用知识处理结果中没有新记录，集合 {general_collection_name} 未插入任何记录")
        else:
            # 即使没有通用知识结果，也需要确保集合存在并可能被清空
            temp_client = VectorClient()
            temp_client.collection_name = general_collection_name
            
            if not temp_client.has_collection():
                temp_client.create_collection()
                logger.info(f"创建通用知识集合: {general_collection_name}")
            else:
                # 清空现有集合中的所有数据（即使没有新的数据要插入）
                temp_client.drop_collection()
                temp_client.create_collection()
                logger.info(f"清空并重新创建通用知识集合: {general_collection_name}")
            
            result_stats[general_collection_name] = 0
            logger.info(f"通用知识处理结果为空，集合 {general_collection_name} 已被清空并重新创建")
        
        # 处理分类知识
        logger.info("开始处理分类知识并插入到Milvus")
        category_results = await self.process_category_chunks()
        
        logger.info(f"分类结果总数: {len(category_results)}")
        for i, result in enumerate(category_results):
            logger.debug(f"分类结果 {i}: {result}")
        
        # 分离不同类型的分类结果
        narrative_results = [r for r in category_results if str(r.get('doc_class', '')) == '1']
        data_results = [r for r in category_results if str(r.get('doc_class', '')) == '2']
        
        logger.info(f"叙述型结果数量 (class=1): {len(narrative_results)}")
        logger.info(f"数据型结果数量 (class=2): {len(data_results)}")
        
        # 处理叙述型分类（class=1）的结果
        if narrative_results:
            narrative_collection_name = self.get_collection_name(is_narrative=True)
            
            # 创建或使用叙述型分类知识集合
            temp_client = VectorClient()
            temp_client.collection_name = narrative_collection_name
            
            if not temp_client.has_collection():
                temp_client.create_collection()
                logger.info(f"创建叙述型分类知识集合: {narrative_collection_name}")
            else:
                # 清空现有集合中的所有数据
                temp_client.drop_collection()
                temp_client.create_collection()
                logger.info(f"清空并重新创建叙述型分类知识集合: {narrative_collection_name}")
            
            temp_client.load_collection()
            
            # 为每条记录生成唯一内容哈希
            narrative_data = []
            timestamp_base = int(time.time() * 1000000) % 100000000 + 200000000
            existing_hashes = set()
            
            # 查询现有记录的内容哈希
            try:
                existing_records = temp_client.milvus_client.query(
                    collection_name=narrative_collection_name,
                    filter="id >= 0",
                    output_fields=["text"]
                )
                for record in existing_records:
                    content_hash = hashlib.md5(record['text'].encode('utf-8')).hexdigest()
                    existing_hashes.add(content_hash)
            except Exception as e:
                logger.warning(f"查询现有记录时出错: {e}")
            
            # 为每条记录生成唯一内容哈希
            new_records_count = 0
            for i, result in enumerate(narrative_results):
                # 为内容生成哈希以检查重复
                content_hash = hashlib.md5((result['text'] + ", ".join(result['items'])).encode('utf-8')).hexdigest()
                if content_hash not in existing_hashes:
                    record = {
                        "id": timestamp_base + new_records_count,
                        "text": result['text'],
                        "vector": result['vector'],
                        "items": ", ".join(result['items']),
                        "vectorizing_text": result.get('vectorizing_text', result['text'])
                    }
                    narrative_data.append(record)
                    new_records_count += 1
            
            # 插入新数据
            if narrative_data:
                insert_result = temp_client.insert_data(narrative_data)
                
                # 确保数据写入磁盘
                try:
                    temp_client.milvus_client.flush(collection_name=narrative_collection_name)
                    logger.info(f"集合 {narrative_collection_name} 数据已刷新到磁盘")
                except Exception as e:
                    logger.warning(f"刷新集合 {narrative_collection_name} 数据时出错: {e}")
                
                result_stats[narrative_collection_name] = len(insert_result['ids'])
                logger.info(f"成功向集合 {narrative_collection_name} 插入 {len(insert_result['ids'])} 条新记录")
            else:
                result_stats[narrative_collection_name] = 0
                logger.info(f"叙述型分类知识处理结果中没有新记录，集合 {narrative_collection_name} 未插入任何记录")
        else:
            narrative_collection_name = self.get_collection_name(is_narrative=True)
            result_stats[narrative_collection_name] = 0
            logger.info(f"叙述型分类知识处理结果为空，集合 {narrative_collection_name} 未插入任何记录")
        
        # 处理数据型分类（class=2）的结果
        if data_results:
            category_collection_name = self.get_collection_name()
            
            # 创建或使用分类知识集合
            temp_client = VectorClient()
            temp_client.collection_name = category_collection_name
            
            if not temp_client.has_collection():
                temp_client.create_collection()
                logger.info(f"创建分类知识集合: {category_collection_name}")
            else:
                # 清空现有集合中的所有数据
                temp_client.drop_collection()
                temp_client.create_collection()
                logger.info(f"清空并重新创建分类知识集合: {category_collection_name}")
            
            temp_client.load_collection()
            
            # 为每条记录生成唯一内容哈希
            category_data = []
            timestamp_base = int(time.time() * 1000000) % 100000000 + 100000000
            existing_hashes = set()
            
            # 查询现有记录的内容哈希
            try:
                existing_records = temp_client.milvus_client.query(
                    collection_name=category_collection_name,
                    filter="id >= 0",
                    output_fields=["text"]
                )
                for record in existing_records:
                    content_hash = hashlib.md5(record['text'].encode('utf-8')).hexdigest()
                    existing_hashes.add(content_hash)
            except Exception as e:
                logger.warning(f"查询现有记录时出错: {e}")
            
            # 准备新记录数据
            new_records_count = 0
            for i, result in enumerate(data_results):
                # 为每条记录生成唯一内容哈希
                # 使用text和items的组合作为hash的基础，避免同一分类下的不同记录被认为是重复的
                # 注意：items包含原始文本和分类信息的分词结果，能够区分不同的原始文本
                content_hash = hashlib.md5((result['text'] + ", ".join(result['items'])).encode('utf-8')).hexdigest()
                if content_hash not in existing_hashes:
                    record = {
                        "id": timestamp_base + new_records_count,
                        "text": result['text'],
                        "vector": result['vector'],
                        "items": ", ".join(result['items']),
                        "vectorizing_text": result.get('vectorizing_text', result['text'])
                    }
                    category_data.append(record)
                    new_records_count += 1
            
            # 插入新数据
            if category_data:
                insert_result = temp_client.insert_data(category_data)
                
                # 确保数据写入磁盘
                try:
                    temp_client.milvus_client.flush(collection_name=category_collection_name)
                    logger.info(f"集合 {category_collection_name} 数据已刷新到磁盘")
                except Exception as e:
                    logger.warning(f"刷新集合 {category_collection_name} 数据时出错: {e}")
                
                result_stats[category_collection_name] = len(insert_result['ids'])
                logger.info(f"成功向集合 {category_collection_name} 插入 {len(insert_result['ids'])} 条新记录")
            else:
                result_stats[category_collection_name] = 0
                logger.info(f"分类知识处理结果中没有新记录，集合 {category_collection_name} 未插入任何记录")
        else:
            category_collection_name = self.get_collection_name()
            result_stats[category_collection_name] = 0
            logger.info(f"分类知识处理结果为空，集合 {category_collection_name} 未插入任何记录")
        
        logger.info(f"总共处理完成，各集合插入记录数: {result_stats}")
        return result_stats

    async def process_all_chunks(self) -> List[Dict[str, Any]]:
        """
        处理所有chunks目录下的文件并进行向量化
        
        Returns:
            List[Dict[str, Any]]: 包含text、vector和items的列表
        """
        # 处理通用知识
        general_results = await self.process_general_knowledge_chunks()
        
        # 处理分类知识
        category_results = await self.process_category_chunks()
        
        # 合并结果
        all_results = general_results + category_results
        
        logger.info(f"总共处理了 {len(all_results)} 行文本")
        return all_results

    def save_vectorized_data(self, data: List[Dict[str, Any]], output_file: str = VectorizedDataConfig.output_file):
        """
        保存向量化后的数据到文件
        
        Args:
            data: 向量化后的数据
            output_file: 输出文件路径
        """
        # 确保输出目录存在
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"向量化数据已保存到: {output_file}")
        except Exception as e:
            logger.error(f"保存向量化数据时出错: {e}")
            raise


async def process_all_chunks_and_insert_to_milvus(specification_uid: str = "default", task_uid: str = None):
    """
    处理所有chunks并插入到Milvus向量数据库
    
    Args:
        specification_uid: 规范UID
        task_uid: 任务唯一标识，用于发送状态更新
        
    Returns:
        dict: 插入统计信息
    """
    # 创建KnowledgePostprocessService实例并处理chunks
    service = KnowledgePostprocessService(specification_uid=specification_uid)
    insertion_stats = await service.process_all_chunks_and_insert_to_milvus()
    
    # 返回实际的插入统计信息
    return {
        "inserted_count": sum(insertion_stats.values()),
        "processed_files": len(insertion_stats),
        "specification_uid": specification_uid,
        "collection_stats": insertion_stats
    }

async def process_all_chunks(specification_uid: str = "default") -> List[Dict[str, Any]]:
    """
    处理所有chunks目录下的文件并进行向量化
    
    Args:
        specification_uid: 行业名ID（用户上传的specificationUId对应的值）
        
    Returns:
        List[Dict[str, Any]]: 包含text、vector和items的列表
    """
    service = KnowledgePostprocessService(specification_uid=specification_uid)
    return await service.process_all_chunks()


def tokenize_text(text: str) -> List[str]:
    """
    对文本进行jieba分词（函数接口）
    
    Args:
        text: 待分词的文本
        
    Returns:
        List[str]: 分词结果列表
    """
    return list(jieba.cut(text))