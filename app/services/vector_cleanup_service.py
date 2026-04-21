#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
向量数据库清理服务
处理向量数据库中非叶节点数据的清理操作
"""

import json
import logging
from typing import List, Dict, Any, Tuple
from pymilvus import MilvusClient
import os
import time

logger = logging.getLogger(__name__)


class VectorCleanupService:
    """向量数据库清理服务类"""

    def __init__(self, db_path: str = None):
        """
        初始化清理服务
        
        Args:
            db_path: Milvus数据库路径
        """
        if db_path is None:
            # 使用与VectorClient相同的配置，以确保连接到同一个数据库
            from app.core.config import DatabaseConfig
            db_path = DatabaseConfig.uri
        self.db_path = db_path

    def connect_to_milvus(self) -> MilvusClient:
        """
        连接到Milvus数据库
        
        Returns:
            MilvusClient: Milvus客户端实例
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                milvus_client = MilvusClient(self.db_path)
                return milvus_client
            except Exception as e:
                logger.warning(f"尝试 {attempt + 1} 连接失败: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    logger.error("无法连接到Milvus数据库")
                    raise e

    def extract_classification_path(self, data_dict: Dict[str, Any]) -> Tuple[List[str], str]:
        """
        从记录中提取分类路径
        
        Args:
            data_dict: 包含header和data的字典
            
        Returns:
            tuple: (分类值列表, 路径字符串)
        """
        try:
            data = data_dict.get("data", {})
            header = data_dict.get("header", {})
            
            if not data or not header:
                return [], ""
                
            # 计算分类字段数量
            classification_fields = []
            for key, value in header.items():
                if "分类" in str(value):
                    classification_fields.append(int(key))
            
            # 按数字顺序排序
            classification_fields.sort()
            
            # 提取分类值
            path_values = []
            for field_index in classification_fields:
                key = str(field_index)
                value = data.get(key, None)
                # 如果遇到None或空值，停止构建路径
                if value is None or not str(value).strip():
                    break
                path_values.append(str(value).strip())
                
            # 构建路径字符串用于比较
            path_string = "->".join(path_values)
            return path_values, path_string
            
        except Exception as e:
            logger.error(f"提取分类路径时出错: {e}")
            return [], ""

    def identify_non_leaf_records(self, records: List[Dict[str, Any]]) -> List[int]:
        """
        识别非叶节点记录
        基于以下原则：如果存在更深层次的分类记录，则较浅的分类记录为非叶节点
        
        Args:
            records: 记录列表
            
        Returns:
            List[int]: 非叶节点记录的ID列表
        """
        # 首先提取所有记录的分类路径
        record_paths = {}  # id -> (path_values, path_string)
        path_depths = {}   # path_string -> depth
        
        for record in records:
            try:
                # 解析text字段中的JSON数据
                text_data = record.get('text', '{}')
                if isinstance(text_data, str):
                    data_dict = json.loads(text_data)
                else:
                    data_dict = text_data
                    
                path_values, path_string = self.extract_classification_path(data_dict)
                
                if path_values:  # 只处理有分类路径的记录
                    record_paths[record['id']] = (path_values, path_string)
                    path_depths[path_string] = len(path_values)
                    
            except json.JSONDecodeError:
                logger.warning(f"记录 {record.get('id')} JSON解析失败")
            except Exception as e:
                logger.error(f"处理记录 {record.get('id')} 时出错: {e}")
        
        # 识别非叶节点
        non_leaf_ids = set()
        
        # 对于每条记录，检查是否存在比它更深的扩展路径
        for record_id, (path_values, path_string) in record_paths.items():
            depth = len(path_values)
            
            # 检查是否存在任何以当前路径为前缀且更深的路径
            is_extended = False
            for other_path_string, other_depth in path_depths.items():
                if other_path_string != path_string and other_depth > depth:
                    # 检查other_path_string是否以path_string为前缀
                    if other_path_string.startswith(path_string + "->"):
                        is_extended = True
                        break
            
            # 如果存在更深层次的扩展路径，则当前记录为非叶节点
            if is_extended:
                non_leaf_ids.add(record_id)
        
        return list(non_leaf_ids)

    def clean_collection_non_leaf_nodes(self, collection_name: str) -> int:
        """
        清理指定集合中的非叶节点数据
        
        Args:
            collection_name: 集合名称
            
        Returns:
            int: 删除的记录数量
        """
        logger.info(f"开始清理集合 {collection_name} 中的非叶节点数据...")
        
        try:
            # 连接到Milvus
            client = self.connect_to_milvus()
            
            # 检查集合是否存在
            collections = client.list_collections()
            # print("============================================================================", collections)
            if collection_name not in collections:
                logger.warning(f"集合 {collection_name} 不存在")
                
                # 等待一段时间再重试，给Milvus一些时间来完成集合创建
                import time
                max_retries = 5
                retry_count = 0
                
                while retry_count < max_retries:
                    time.sleep(3)  # 等待3秒
                    collections = client.list_collections()
                    if collection_name in collections:
                        logger.info(f"集合 {collection_name} 现在可用，继续清理操作")
                        break
                    else:
                        retry_count += 1
                        logger.warning(f"集合 {collection_name} 仍然不存在，第 {retry_count} 次重试...")
                
                if collection_name not in collections:
                    logger.warning(f"经过 {max_retries} 次重试后，集合 {collection_name} 仍然不存在")
                    return 0
            
            # 加载集合到内存
            logger.info("正在加载集合...")
            client.load_collection(collection_name=collection_name)
            
            # 获取集合统计信息
            stats = client.get_collection_stats(collection_name)
            row_count = stats.get('row_count', 0)
            logger.info(f"集合统计信息: {stats}")
            
            if row_count == 0:
                logger.info("集合为空，无需清理")
                return 0
            
            # 分批获取所有记录，避免一次获取太多数据
            all_records = []
            batch_size = 100
            offset = 0
            
            while offset < row_count:
                # 使用id >= 0作为过滤器，配合limit和offset来分批获取数据
                batch_records = client.query(
                    collection_name=collection_name,
                    filter="id >= 0",
                    limit=batch_size,
                    offset=offset,
                    output_fields=["id", "text"]
                )
                
                if not batch_records:
                    break
                    
                all_records.extend(batch_records)
                offset += len(batch_records)
                
                # 如果返回的记录数少于请求的数量，说明已经获取完所有记录
                if len(batch_records) < batch_size:
                    break
            
            logger.info(f"总共找到 {len(all_records)} 条记录")
            
            # 识别非叶节点记录
            non_leaf_ids = self.identify_non_leaf_records(all_records)
            logger.info(f"识别出 {len(non_leaf_ids)} 条非叶节点记录需要删除")
            
            # 显示将要删除的记录详情（最多显示5条）
            displayed_count = 0
            for record in all_records:
                if record['id'] in non_leaf_ids and displayed_count < 5:
                    try:
                        text_data = record.get('text', '{}')
                        if isinstance(text_data, str):
                            data_dict = json.loads(text_data)
                        else:
                            data_dict = text_data
                            
                        path_values, path_string = self.extract_classification_path(data_dict)
                        path_display = " -> ".join(path_values)
                        
                        logger.info(f"  将删除 ID: {record['id']}, 路径: {path_display}")
                    except:
                        text_preview = str(record.get('text', ''))[:50] + "..." if len(str(record.get('text', ''))) > 50 else str(record.get('text', ''))
                        logger.info(f"  将删除 ID: {record['id']}, TEXT预览: {text_preview}")
                    displayed_count += 1
                    
            if len(non_leaf_ids) > 5:
                logger.info(f"  ... 还有 {len(non_leaf_ids) - 5} 条记录")
            
            # 执行删除操作
            deleted_count = 0
            if non_leaf_ids:
                # 分批删除，避免一次性删除太多记录
                delete_batch_size = 50
                
                for i in range(0, len(non_leaf_ids), delete_batch_size):
                    batch_ids = non_leaf_ids[i:i + delete_batch_size]
                    # Milvus中通过ID删除记录
                    expr = f"id in {batch_ids}"
                    client.delete(
                        collection_name=collection_name,
                        filter=expr
                    )
                    deleted_count += len(batch_ids)
                    logger.info(f"已删除批次，包含 {len(batch_ids)} 条记录")
                
                logger.info(f"集合 {collection_name} 清理完成，总共删除了 {deleted_count} 条记录")
            else:
                logger.info("没有发现需要删除的非叶节点记录")
                
            return deleted_count
            
        except Exception as e:
            logger.error(f"清理集合 {collection_name} 时出错: {e}", exc_info=True)
            return 0

    # async def cleanup_classification_collections(self, specification_uid: str) -> Dict[str, int]:
    #     """
    #     清理指定规范的分类集合中的非叶节点数据
        
    #     Args:
    #         specification_uid: 规范UID
            
    #     Returns:
    #         Dict[str, int]: 各集合的清理结果
    #     """
    #     logger.info(f"开始清理规范 {specification_uid} 的分类集合中的非叶节点数据")
        
    #     try:
    #         # 确定要清理的集合名称
    #         # 确保使用与KnowledgePostprocessService中相同的命名规则
    #         safe_specification_uid = specification_uid.replace("-", "_")
            
    #         collection_names = [
    #             f"{safe_specification_uid}_classification"
    #         ]
    async def cleanup_classification_collections(self, specification_uid: str) -> Dict[str, int]:
        """
        清理指定规范的分类集合中的非叶节点数据
        
        Args:
            specification_uid: 规范UID
            
        Returns:
            Dict[str, int]: 各集合的清理结果
        """
        logger.info(f"开始清理规范 {specification_uid} 的分类集合中的非叶节点数据")
        
        try:
            # 确定要清理的集合名称
            # 确保使用与KnowledgePostprocessService中相同的命名规则
            safe_specification_uid = specification_uid.replace("-", "_")
            
            # 确保集合名称以字母或下划线开头（Milvus要求）
            if safe_specification_uid and not safe_specification_uid[0].isalpha() and safe_specification_uid[0] != '_':
                safe_specification_uid = '_' + safe_specification_uid
            
            collection_names = [
                f"{safe_specification_uid}_classification"
            ]
            
            # 清理每个集合
            results = {}
            for collection_name in collection_names:
                deleted_count = self.clean_collection_non_leaf_nodes(collection_name)
                results[collection_name] = deleted_count
                
            logger.info(f"规范 {specification_uid} 的分类集合清理完成: {results}")
            return results
            
        except Exception as e:
            logger.error(f"清理规范 {specification_uid} 的分类集合时出错: {e}", exc_info=True)
            return {}


# 创建全局实例
vector_cleanup_service = VectorCleanupService()