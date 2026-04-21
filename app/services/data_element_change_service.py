#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据元素更换服务
提供数据元素替换功能
"""

import asyncio
import logging
import json
from pathlib import Path
from typing import List
import ast
from app.schemas.knowledge_base import DataElementChangeRequest, DataElementChangeResponse
from app.services.knowledge_postprocess_service import process_all_chunks_and_insert_to_milvus

logger = logging.getLogger(__name__)


class DataElementChangeService:
    """数据元素更换服务类"""

    async def process_data_element_change(self, request: DataElementChangeRequest) -> DataElementChangeResponse:
        """
        处理数据元素更换请求
        
        Args:
            request: 数据元素更换请求对象
            
        Returns:
            DataElementChangeResponse: 数据元素更换响应对象
        """
        try:
            specificationUId = request.specificationUId.replace("-", "_")
            prefixed_specification_uid = f"_{specificationUId}"
            
            logger.info(f"Processing data element change for specification: {request.specificationUId}")
            logger.info(f"Origin element: {request.originElement}, Replace element: {request.replaceElement}")
            
            # 构建标准文件路径
            standards_dir = Path("data/standards")
            file_path = standards_dir / f"{prefixed_specification_uid}_standard.jsonl"
            
            # 检查文件是否存在
            if not file_path.exists():
                logger.warning(f"Standard file does not exist: {file_path}")
                raise FileNotFoundError(f"找不到规范 {request.specificationUId} 的标准文件")
            
            # 读取文件内容
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            # 替换所有匹配的元素
            updated_lines = []
            elements_replaced_count = 0
            
            for line in lines:
                try:
                    data = json.loads(line.strip())
                    header = data.get("header", {})
                    data_content = data.get("data", {})
                    
                    # 查找"真实数据"字段的键
                    real_data_key = None
                    for k, v in header.items():
                        if v == "真实数据":
                            real_data_key = k
                            break
                    
                    # 如果找到了"真实数据"字段
                    if real_data_key is not None:
                        # 获取现有的真实数据
                        existing_real_data = data_content.get(real_data_key, [])
                        if isinstance(existing_real_data, str):
                            try:
                                # 安全地解析现有数据列表
                                existing_real_data = ast.literal_eval(existing_real_data) if existing_real_data else []
                                if not isinstance(existing_real_data, list):
                                    existing_real_data = []
                            except:
                                existing_real_data = []
                        
                        # 确保existing_real_data是一个列表
                        if not isinstance(existing_real_data, list):
                            existing_real_data = []
                        
                        # 替换匹配的元素
                        replaced_in_line = False
                        for i, element in enumerate(existing_real_data):
                            if element == request.originElement:
                                existing_real_data[i] = request.replaceElement
                                elements_replaced_count += 1
                                replaced_in_line = True
                        
                        # 如果有替换发生，更新数据
                        if replaced_in_line:
                            data_content[real_data_key] = existing_real_data
                            updated_record = {
                                "header": header,
                                "data": data_content
                            }
                            updated_lines.append(json.dumps(updated_record, ensure_ascii=False) + "\n")
                        else:
                            # 未替换的行保持原样
                            updated_lines.append(line)
                    else:
                        # 没有"真实数据"字段的行保持原样
                        updated_lines.append(line)
                except json.JSONDecodeError:
                    # 无法解析的行保持原样
                    updated_lines.append(line)
            
            # 写入更新后的内容
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(updated_lines)
            
            logger.info(f"Replaced {elements_replaced_count} occurrences of '{request.originElement}' with '{request.replaceElement}' in spec {request.specificationUId}")
            
            # 异步启动向量数据库更新任务，不等待完成
            logger.info(f"Starting background task to update vector database for specification: {request.specificationUId}")
            asyncio.create_task(process_all_chunks_and_insert_to_milvus(specification_uid=request.specificationUId))
            logger.info(f"Background task started to update vector database for specification: {request.specificationUId}")
            
            return DataElementChangeResponse(
                success=True,
                code=200,
                msg=f"元素更换成功！共替换 {elements_replaced_count} 个元素。"
            )
            
        except Exception as e:
            logger.error(f"Failed to process data element change: {str(e)}", exc_info=True)
            return DataElementChangeResponse(
                success=False,
                code=500,
                msg=f"处理数据元素更换失败: {str(e)}"
            )


# 创建服务实例
data_element_change_service = DataElementChangeService()