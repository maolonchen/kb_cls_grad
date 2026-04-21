#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
向量数据库重构服务
处理向量数据库的删除和重构操作
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from app.core.vectoring import VectorClient
from app.algorithms.similarity import EmbeddingSimilarityCalculator
from app.core.config import DatabaseConfig
from pymilvus import MilvusClient
from app.schemas.knowledge_base import KnowledgeBaseUploadResponse
from app.services.knowledge_postprocess_service import process_all_chunks_and_insert_to_milvus
from app.services.vector_cleanup_service import vector_cleanup_service
import json
import shutil


logger = logging.getLogger(__name__)


class VectorRebuildService:
    """向量数据库重构服务类"""
    
    def __init__(self):
        """初始化服务类"""
        pass
    


    async def delete_knowledge_file_with_classification(self, specification_uid: str, file_classification: str, file_path: str) -> KnowledgeBaseUploadResponse:
        """
        删除行业知识库文件（支持分类参数）
        
        Args:
            specification_uid: 行业ID
            file_classification: 知识库文件所属类别（|分隔）
            file_path: 相对于知识库根目录的文件路径
            
        Returns:
            KnowledgeBaseUploadResponse: 删除结果响应
        """
        try:
            # 构建基于行业ID的chunks目录路径
            chunks_base_dir = Path(f"data/processed/{specification_uid}_chunks")
            
            # 检查chunks目录是否存在
            if not chunks_base_dir.exists():
                return KnowledgeBaseUploadResponse(
                    success=False,
                    code=404,
                    msg="行业目录不存在"
                )
            
            target_file_path = None
            
            if file_classification:
                # 如果提供了分类参数，按指定路径查找文件
                # 分割分类参数，获取所有分类层级
                categories = file_classification.split("|")
                # 构建完整路径，包含所有分类层级
                target_file_path = chunks_base_dir
                for category in categories:
                    target_file_path = target_file_path / category
                target_file_path = target_file_path / file_path
            else:
                # 如果没有提供分类参数，默认在general_knowledge目录下查找
                target_file_path = chunks_base_dir / "general_knowledge" / file_path
            
            # 检查目标文件是否存在
            if target_file_path is None or not target_file_path.exists():
                return KnowledgeBaseUploadResponse(
                    success=False,
                    code=404,
                    msg="文件不存在"
                )
            
            # 删除目标文件
            target_file_path.unlink()
            logger.info(f"成功删除文件: {target_file_path}")
            
            # 查找并删除对应的JSON文件
            classifications_dir = Path("data/processed/classifications")
            if classifications_dir.exists():
                # 构造基本文件名（不含扩展名）
                base_filename = os.path.splitext(os.path.basename(file_path))[0]
                
                # 在classifications目录中查找匹配的JSON文件
                for json_file in classifications_dir.glob(f"{specification_uid}*{base_filename}*.json"):
                    json_file.unlink()
                    logger.info(f"成功删除关联的JSON文件: {json_file}")
            
            # 检查并删除空目录
            if file_classification:
                # 获取文件所在的目录
                file_directory = target_file_path.parent
                # 递归检查并删除空目录
                self._remove_empty_dirs(chunks_base_dir, file_directory)
            else:
                # 对于默认的general_knowledge目录，也检查是否为空，但不要删除该目录本身
                general_knowledge_dir = chunks_base_dir / "general_knowledge"
                if general_knowledge_dir.exists() and not any(general_knowledge_dir.iterdir()):
                    logger.info(f"清空了general_knowledge目录，但保留目录本身: {general_knowledge_dir}")
            
            return KnowledgeBaseUploadResponse(
                success=True,
                code=200,
                msg="删除成功"
            )
            
        except Exception as e:
            logger.error(f"删除文件时出错: {str(e)}")
            return KnowledgeBaseUploadResponse(
                success=False,
                code=500,
                msg=f"删除文件时出错: {str(e)}"
            )
    
    def _remove_empty_dirs(self, base_dir: Path, current_dir: Path):
        """
        递归删除空目录，但保留general_knowledge目录
        
        Args:
            base_dir: 基础目录，不会被删除
            current_dir: 当前检查的目录
        """
        try:
            # 如果当前目录是基础目录或不在基础目录下，则停止
            # 或者当前目录是general_knowledge目录，也不删除
            if (current_dir == base_dir or 
                not str(current_dir).startswith(str(base_dir)) or
                current_dir.name == "general_knowledge"):
                return
            
            # 检查目录是否为空
            if current_dir.is_dir() and not any(current_dir.iterdir()):
                current_dir.rmdir()
                logger.info(f"删除空目录: {current_dir}")
                # 递归检查父目录
                self._remove_empty_dirs(base_dir, current_dir.parent)
        except Exception as e:
            logger.warning(f"删除空目录时出错: {str(e)}")

    async def delete_knowledge_file(self, specification_uid: str, file_path: str) -> KnowledgeBaseUploadResponse:
        """
        删除行业知识库文件（兼容旧版方法）
        
        Args:
            specification_uid: 行业ID
            file_path: 相对于知识库根目录的文件路径
            
        Returns:
            KnowledgeBaseUploadResponse: 删除结果响应
        """
        return await self.delete_knowledge_file_with_classification(specification_uid, None, file_path)
    
    async def rebuild_vector_database(self, specification_uid: str) -> KnowledgeBaseUploadResponse:
        """
        重构行业向量数据库
        直接重构该行业的所有知识，不是只删除某些向量数据
        
        Args:
            specification_uid: 行业ID
            
        Returns:
            KnowledgeBaseUploadResponse: 重构结果响应
        """
        try:
            # 检查行业分类结果文件是否存在
            classifications_dir = Path("data/processed/classifications")
            if not classifications_dir.exists():
                # 如果分类结果目录不存在，仍然继续处理，因为可能存在通用知识
                logger.info(f"分类结果目录不存在，但仍需处理通用知识: {classifications_dir}")
            # 检查是否有该行业的分类结果文件
            industry_files = list(classifications_dir.glob(f"{specification_uid}_*.json"))
            if not industry_files:
                # 不直接返回错误，而是继续处理，因为可能只有通用知识需要处理
                logger.info(f"未找到行业 {specification_uid} 的分类结果文件，但仍需处理通用知识")
            
            # 使用knowledge_postprocess_service重构向量数据库
            from app.services.knowledge_postprocess_service import process_all_chunks_and_insert_to_milvus
            insertion_stats = await process_all_chunks_and_insert_to_milvus(specification_uid=specification_uid)
            
            logger.info(f"行业 {specification_uid} 的向量数据库重建完成，插入统计: {insertion_stats}")
            
            return KnowledgeBaseUploadResponse(
                success=True,
                code=200,
                msg="向量知识库重建成功"
            )
            
        except Exception as e:
            logger.error(f"重构向量数据库时出错: {str(e)}")
            return KnowledgeBaseUploadResponse(
                success=False,
                code=500,
                msg=f"重构向量数据库时出错: {str(e)}"
            )
    
    async def handle_classification_change(self, spec_uid: str, classifications: List[Dict[str, Any]]) -> bool:
        """
        处理分类信息变更后的操作
        
        Args:
            spec_uid: 规范UId
            classifications: 分类信息列表
            
        Returns:
            bool: 操作是否成功
        """
        try:
            # 检查是否包含任何操作
            has_any_operation = any(cls.get("action") in ["create", "update", "delete"] for cls in classifications)
            
            if not has_any_operation:
                logger.info(f"分类信息变更中不包含任何操作，跳过向量数据库重建: {spec_uid}")
                return True
            
            # 处理delete操作 - 删除对应的文件
            for classification in classifications:
                if classification.get("action") == "delete":
                    await self._delete_classification_files(spec_uid, classification)
            
            # 执行向量数据库重建
            logger.info(f"开始重建向量数据库: {spec_uid}")
            await process_all_chunks_and_insert_to_milvus(specification_uid=spec_uid)
            logger.info(f"向量数据库重建完成: {spec_uid}")
            
            # 等待数据完全写入Milvus，并验证集合存在
            logger.info(f"等待数据写入完成并验证集合: {spec_uid}")
            await self._verify_collections_exist(spec_uid)
            
            # 清理向量数据库中的非叶节点数据
            logger.info(f"开始清理向量数据库中的非叶节点数据: {spec_uid}")
            await vector_cleanup_service.cleanup_classification_collections(spec_uid)
            logger.info(f"向量数据库非叶节点数据清理完成: {spec_uid}")
            
            return True
        except Exception as e:
            logger.error(f"处理分类信息变更失败: {str(e)}", exc_info=True)
            return False
    
    async def _delete_classification_files(self, spec_uid: str, classification_item: Dict[str, Any]) -> bool:
        """
        删除分类对应的文件
        
        Args:
            spec_uid: 规范UId
            classification_item: 分类项数据
            
        Returns:
            bool: 删除是否成功
        """
        try:
            # 解析分类路径键值对，按键排序以确保顺序正确
            digit_keys = [k for k in classification_item.keys() if str(k).isdigit()]
            sorted_keys = sorted(digit_keys, key=lambda x: int(x))
            
            # 构建分类路径
            classification_path_parts = []
            for key in sorted_keys:
                classification_path_parts.append(classification_item[key])
            
            if not classification_path_parts:
                logger.warning(f"分类项中没有有效的分类路径: {classification_item}")
                return False
            
            # 构建文件路径
            chunks_dir = Path(f"data/processed/{spec_uid}_chunks")
            target_dir = chunks_dir
            for part in classification_path_parts:
                target_dir = target_dir / part
            
            # 删除目录及其中的文件
            if target_dir.exists() and target_dir.is_dir():
                # 收集要删除的文件名，用于清理分类信息文件
                deleted_files = []
                for file in target_dir.glob("*.md"):
                    deleted_files.append(file.stem)
                
                shutil.rmtree(target_dir)
                logger.info(f"成功删除分类目录: {target_dir}")
                
                # 删除对应的分类信息文件
                await self._delete_classification_info_files(deleted_files, spec_uid)
                
                # 检查并删除空的父目录
                self._remove_empty_parent_dirs(chunks_dir, target_dir.parent)
                return True
            else:
                logger.warning(f"分类目录不存在: {target_dir}")
                return False
                
        except Exception as e:
            logger.error(f"删除分类文件失败: {str(e)}", exc_info=True)
            return False

    async def _delete_classification_info_files(self, file_stems: List[str], spec_uid: str):
        """
        删除分类信息文件
        
        Args:
            file_stems: 文件名stem列表（不含扩展名）
            spec_uid: 规范UId
        """
        try:
            classifications_dir = Path("data/processed/classifications")
            if not classifications_dir.exists():
                return
            
            for file_stem in file_stems:
                # 查找并删除匹配的分类信息文件
                for class_file in classifications_dir.glob(f"{spec_uid}_{file_stem}*.json"):
                    class_file.unlink()
                    logger.info(f"删除分类信息文件: {class_file}")
        except Exception as e:
            logger.error(f"删除分类信息文件失败: {str(e)}", exc_info=True)
    
    def _remove_empty_parent_dirs(self, base_dir: Path, current_dir: Path):
        """
        递归删除空的父目录
        
        Args:
            base_dir: 基础目录，不会被删除
            current_dir: 当前检查的目录
        """
        try:
            # 如果当前目录是基础目录或不在基础目录下，则停止
            if current_dir == base_dir or not str(current_dir).startswith(str(base_dir)):
                return
            
            # 检查目录是否为空
            if current_dir.is_dir() and not any(current_dir.iterdir()):
                current_dir.rmdir()
                logger.info(f"删除空目录: {current_dir}")
                # 递归检查父目录
                self._remove_empty_parent_dirs(base_dir, current_dir.parent)
        except Exception as e:
            logger.warning(f"删除空父目录时出错: {str(e)}")

    async def _verify_collections_exist(self, specification_uid: str) -> bool:
        """
        验证指定规范的向量集合是否已创建完成
        
        Args:
            specification_uid: 规范UID
            
        Returns:
            bool: 集合是否存在
        """
        import time
        from app.core.vectoring import VectorClient
        
        # 确保使用与KnowledgePostprocessService中相同的命名规则
        safe_specification_uid = specification_uid.replace("-", "_")
        
        # 确保集合名称以字母或下划线开头（Milvus要求）
        if safe_specification_uid and not safe_specification_uid[0].isalpha() and safe_specification_uid[0] != '_':
            safe_specification_uid = '_' + safe_specification_uid
        
        collection_names = [
            f"{safe_specification_uid}_classification",
            f"{safe_specification_uid}_narrative_classification",
            f"{safe_specification_uid}_general_knowledge"
        ]
        
        max_attempts = 10  # 最多尝试10次
        attempt = 0
        
        while attempt < max_attempts:
            all_collections_exist = True
            existing_collections = []
            
            for collection_name in collection_names:
                try:
                    temp_client = VectorClient()
                    temp_client.collection_name = collection_name
                    if temp_client.has_collection():
                        # 尝试加载集合并查询统计信息以确保集合完全可用
                        try:
                            # 加载集合到内存
                            temp_client.load_collection()
                            
                            # 获取统计信息
                            stats = temp_client.get_collection_stats()
                            logger.debug(f"集合 {collection_name} 状态正常: {stats}")
                            
                            # 尝试简单查询以确保集合可访问
                            results = temp_client.milvus_client.query(
                                collection_name=collection_name,
                                filter="id >= 0",
                                limit=1,
                                output_fields=["id", "text"]
                            )
                            logger.debug(f"集合 {collection_name} 查询成功，至少有一条记录可访问")
                            
                            existing_collections.append(collection_name)
                        except Exception as query_err:
                            logger.warning(f"集合 {collection_name} 存在但无法访问: {query_err}")
                            all_collections_exist = False
                    else:
                        all_collections_exist = False
                        logger.debug(f"集合 {collection_name} 尚不存在，继续等待...")
                except Exception as e:
                    logger.warning(f"检查集合 {collection_name} 时出错: {e}")
                    all_collections_exist = False
            
            if all_collections_exist:
                logger.info(f"所有集合已确认存在且完全可用: {existing_collections}")
                return True
            else:
                logger.info(f"第 {attempt+1} 次尝试: 部分集合尚未创建完成或不可用，等待5秒后重试...")
                time.sleep(5)
                attempt += 1
        
        logger.warning(f"经过 {max_attempts} 次尝试后仍有部分集合未找到或不可用，但将继续后续操作...")
        return False

vector_rebuilding_service = VectorRebuildService()