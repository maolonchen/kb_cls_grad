#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据特征操作服务
提供数据特征增删改功能
"""

import asyncio
import logging
import json
from pathlib import Path
from typing import List
import ast
from app.schemas.knowledge_base import DataElementRequest, DataElementResponse, DataElementItem
from app.services.knowledge_postprocess_service import process_all_chunks_and_insert_to_milvus

logger = logging.getLogger(__name__)


class DataElementOperateService:
    """数据特征操作服务类"""

    async def process_data_elements(self, request: DataElementRequest) -> DataElementResponse:
        """
        处理数据特征信息请求
        
        Args:
            request: 数据特征信息请求对象
            
        Returns:
            DataElementResponse: 数据特征信息响应对象
        """
        results = []  # 存储每个数据元素项的处理结果
        
        try:
            specificationUId = request.specificationUId.replace("-", "_")
            prefixed_specification_uid = f"_{specificationUId}"
            
            logger.info(f"正在处理规范名称为 {request.specificationName} 的数据元素...")
            logger.info(f"规范的 UID: {request.specificationUId}")
            logger.info(f"需要操作的项数: {len(request.dataElements)}")
            
            # 处理每个数据元素项
            for i, element in enumerate(request.dataElements):
                logger.info(f"正在处理第 {i} 项数据: 动作 {element.action}, "
                           f"类别为 {element.classification}, 元素数量为 {len(element.element)}")
                
                try:
                    # 根据操作类型处理数据元素
                    if element.action == "add":
                        await self._add_data_element(prefixed_specification_uid, element)
                        results.append(f"第{i+1}项添加成功")
                    elif element.action == "update":
                        await self._update_data_element(prefixed_specification_uid, element)
                        results.append(f"第{i+1}项更新成功")
                    elif element.action == "delete":
                        await self._delete_data_element(prefixed_specification_uid, element)
                        results.append(f"第{i+1}项删除成功")
                    else:
                        error_msg = f"第{i+1}项操作失败：未知的操作类型 '{element.action}'"
                        logger.warning(error_msg)
                        results.append(error_msg)
                except Exception as e:
                    error_msg = f"第{i+1}项{element.action}操作失败：{str(e)}"
                    logger.error(error_msg, exc_info=True)
                    results.append(error_msg)
            
            # 检查是否有任何失败的操作
            failed_operations = [r for r in results if "失败" in r]
            if failed_operations:
                response = DataElementResponse(
                    success=False,
                    code=500,
                    msg="部分操作失败：" + "；".join(failed_operations)
                )
            else:
                response = DataElementResponse(
                    success=True,
                    code=200,
                    msg="所有数据特征信息处理成功：" + "；".join(results)
                )
            
            # 处理完所有数据元素操作后，首先保存原始数据再启动向量数据库更新任务
            logger.info(f"正在保存规范 {request.specificationUId} 的原始数据...")
            await self._save_origin_data_to_processed_dir(prefixed_specification_uid)
            
            # 异步启动向量数据库更新任务，不等待完成
            logger.info(f"正在后台更新规范 {request.specificationUId} 的向量数据库...")
            asyncio.create_task(process_all_chunks_and_insert_to_milvus(specification_uid=prefixed_specification_uid))
            logger.info(f"已启动后台任务更新规范 {request.specificationUId} 的向量数据库")
            
            return response
            
        except Exception as e:
            logger.error(f"处理数据元素失败: {str(e)}", exc_info=True)
            return DataElementResponse(
                success=False,
                code=500,
                msg=f"处理数据特征信息失败: {str(e)}"
            )

    async def _add_data_element(self, spec_uid: str, element: DataElementItem) -> None:
        """
        添加数据元素
        
        Args:
            spec_uid: 规范UID
            element: 数据元素项
        """
        logger.info(f"正在添加数据元素 {element} 到规范 {spec_uid} 中")
        
        # 构建标准文件路径
        standards_dir = Path("data/standards")
        file_path = standards_dir / f"{spec_uid}_standard.jsonl"
        
        # 检查文件是否存在
        if not file_path.exists():
            logger.warning(f"标准规范文件 {file_path} 不存在!")
            raise FileNotFoundError(f"找不到规范 {spec_uid} 的标准文件")
        
        # 读取文件内容
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        # 查找匹配的分类行
        updated_lines = []
        found_matching_classification = False
        
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
                
                # 查找所有分类字段，看是否有匹配element.classification的值
                classification_match = False
                for k, v in header.items():
                    # 检查是否是分类字段（以"分类"结尾但不是"真实数据"）
                    if v.endswith("分类") and v != "真实数据":
                        # 检查该分类字段的值是否匹配
                        if data_content.get(k) == element.classification:
                            classification_match = True
                            break
                
                # 如果找到了匹配的分类
                if classification_match and real_data_key is not None:
                    found_matching_classification = True
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
                    
                    # 添加新元素（避免重复）
                    for new_element in element.element:
                        if new_element not in existing_real_data:
                            existing_real_data.append(new_element)
                    
                    # 更新数据
                    data_content[real_data_key] = existing_real_data
                    
                    # 重新构建记录
                    updated_record = {
                        "header": header,
                        "data": data_content
                    }
                    updated_lines.append(json.dumps(updated_record, ensure_ascii=False) + "\n")
                else:
                    # 未匹配的行保持原样
                    updated_lines.append(line)
            except json.JSONDecodeError:
                # 无法解析的行保持原样
                updated_lines.append(line)
        
        # 如果找到了匹配的分类，则写入更新后的内容
        if found_matching_classification:
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(updated_lines)
            logger.info(f"向规范 {spec_uid} 添加数据元素成功")
        else:
            logger.warning(f"在规范 {spec_uid} 中没有找到与 {element.classification} 的匹配!")
            raise ValueError(f"未找到匹配的分类: {element.classification}")

    async def _update_data_element(self, spec_uid: str, element: DataElementItem) -> None:
        """
        更新数据元素
        
        Args:
            spec_uid: 规范UID
            element: 数据元素项
        """
        logger.info(f"正在更新规范 {spec_uid} 中的数据元素: {element}")
        
        # 构建标准文件路径
        standards_dir = Path("data/standards")
        file_path = standards_dir / f"{spec_uid}_standard.jsonl"
        
        # 检查文件是否存在
        if not file_path.exists():
            logger.warning(f"标准文件 {file_path} 不存在!")
            raise FileNotFoundError(f"找不到规范 {spec_uid} 的标准文件")
        
        # 读取文件内容
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        # 查找匹配的分类行
        updated_lines = []
        found_matching_classification = False
        
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
                
                # 查找所有分类字段，看是否有匹配element.classification的值
                classification_match = False
                for k, v in header.items():
                    # 检查是否是分类字段（以"分类"结尾但不是"真实数据"）
                    if v.endswith("分类") and v != "真实数据":
                        # 检查该分类字段的值是否匹配
                        if data_content.get(k) == element.classification:
                            classification_match = True
                            break
                
                # 如果找到了匹配的分类
                if classification_match and real_data_key is not None:
                    found_matching_classification = True
                    # 直接使用用户提供的新元素列表替换原有数据
                    data_content[real_data_key] = element.element
                    
                    # 重新构建记录
                    updated_record = {
                        "header": header,
                        "data": data_content
                    }
                    updated_lines.append(json.dumps(updated_record, ensure_ascii=False) + "\n")
                else:
                    # 未匹配的行保持原样
                    updated_lines.append(line)
            except json.JSONDecodeError:
                # 无法解析的行保持原样
                updated_lines.append(line)
        
        # 如果找到了匹配的分类，则写入更新后的内容
        if found_matching_classification:
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(updated_lines)
            logger.info(f"规范 {spec_uid} 数据元素更新成功")
        else:
            logger.warning(f"在规范 {spec_uid} 中没有找到与 {element.classification} 的匹配!")
            raise ValueError(f"未找到匹配的分类: {element.classification}")

    async def _delete_data_element(self, spec_uid: str, element: DataElementItem) -> None:
        """
        删除数据元素
        
        Args:
            spec_uid: 规范UID
            element: 数据元素项
        """
        logger.info(f"正在删除规范 {spec_uid} 的数据元素: {element}")
        
        # 构建标准文件路径
        standards_dir = Path("data/standards")
        file_path = standards_dir / f"{spec_uid}_standard.jsonl"
        
        # 检查文件是否存在
        if not file_path.exists():
            logger.warning(f"标准文件 {file_path} 不存在!")
            raise FileNotFoundError(f"找不到规范 {spec_uid} 的标准文件")
        
        # 读取文件内容
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        # 查找匹配的分类行
        updated_lines = []
        found_matching_classification = False
        
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
                
                # 查找所有分类字段，看是否有匹配element.classification的值
                classification_match = False
                for k, v in header.items():
                    # 检查是否是分类字段（以"分类"结尾但不是"真实数据"）
                    if v.endswith("分类") and v != "真实数据":
                        # 检查该分类字段的值是否匹配
                        if data_content.get(k) == element.classification:
                            classification_match = True
                            break
                
                # 如果找到了匹配的分类
                if classification_match and real_data_key is not None:
                    found_matching_classification = True
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
                    
                    # 从现有数据中删除用户指定的元素
                    for delete_element in element.element:
                        if delete_element in existing_real_data:
                            existing_real_data.remove(delete_element)
                    
                    # 更新数据
                    data_content[real_data_key] = existing_real_data
                    
                    # 重新构建记录
                    updated_record = {
                        "header": header,
                        "data": data_content
                    }
                    updated_lines.append(json.dumps(updated_record, ensure_ascii=False) + "\n")
                else:
                    # 未匹配的行保持原样
                    updated_lines.append(line)
            except json.JSONDecodeError:
                # 无法解析的行保持原样
                updated_lines.append(line)
        
        # 如果找到了匹配的分类，则写入更新后的内容
        if found_matching_classification:
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(updated_lines)
            logger.info(f"成功删除规范 {spec_uid} 的数据元素")
        else:
            logger.warning(f"未在规范 {spec_uid} 中找到匹配的分类: {element.classification} !")
            raise ValueError(f"未找到匹配的分类: {element.classification}")

    async def _save_origin_data_to_processed_dir(self, spec_uid: str) -> None:
        """
        将更新后的.jsonl文件数据保存到data/processed目录下的origin_data.md文件中
        
        Args:
            spec_uid: 规范UId
        """
        try:
            # 构建标准文件路径
            standards_dir = Path("data/standards")
            file_path = standards_dir / f"{spec_uid}_standard.jsonl"
            
            # 检查文件是否存在
            if not file_path.exists():
                logger.warning(f"标准规范文件 {file_path} 不存在!")
                return
            
            # 读取文件内容
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            # 处理每一行数据
            for line in lines:
                try:
                    data = json.loads(line.strip())
                    header = data.get("header", {})
                    data_content = data.get("data", {})
                    
                    # 构建分类路径
                    classification_path_parts = []
                    for k, v in sorted(header.items(), key=lambda x: x[0]):
                        if v.endswith("分类"):
                            classification_path_parts.append(str(data_content.get(k, "")))
                    
                    if classification_path_parts:
                        # 构建目标目录路径
                        processed_dir = Path(f"data/processed/{spec_uid}_chunks")
                        target_dir = processed_dir
                        for part in classification_path_parts:
                            target_dir = target_dir / part
                        
                        # 确保目录存在
                        target_dir.mkdir(parents=True, exist_ok=True)
                        
                        # 构建origin_data.md文件路径
                        origin_data_file = target_dir / "origin_data.md"
                        
                        # 直接将原始行数据写入文件，保持原始格式
                        with open(origin_data_file, "w", encoding="utf-8") as f:
                            f.write(line)
                        
                        logger.info(f"已将原始分类数据保存到: {origin_data_file}")
                except json.JSONDecodeError:
                    logger.warning(f"无法解析JSON行: {line}")
                    continue
                except Exception as e:
                    logger.error(f"保存分类数据时出错: {str(e)}", exc_info=True)
                    continue
                    
        except Exception as e:
            logger.error(f"处理原始数据保存时出错: {str(e)}", exc_info=True)
            

# 创建服务实例
data_element_operate_service = DataElementOperateService()