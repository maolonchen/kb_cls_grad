import ast
import asyncio
from typing import List, Dict, Any
import logging
import json
from pathlib import Path
from app.schemas.knowledge_base import ClassificationRequest, ClassificationResponse
from app.services.vector_rebuild_service import vector_rebuilding_service


logger = logging.getLogger(__name__)


class KnowledgeClassificationService:
    """
    知识库分类信息服务类
    """

    async def process_classifications(self, request: ClassificationRequest) -> ClassificationResponse:
        """
        处理分类信息请求

        Args:
            request: 分类信息请求对象

        Returns:
            ClassificationResponse: 分类信息响应对象
        """
        results = []  # 存储每个分类项的处理结果
        has_any_operation = False  # 标记是否有任何操作

        try:
            specificationUId = request.specificationUId.replace("-", "_")
            prefixed_specification_uid = f"_{specificationUId}"
            
            # 记录请求信息
            logger.info(f"正在处理规范为{request.specificationUId}的类别请求")
            logger.info(f"需处理的类别数量: {len(request.classifications)}")

            # 检查是否包含任何操作
            has_any_operation = any(
                cls.action in ["create", "update", "delete"] for cls in request.classifications)

            # 处理每个分类项
            for i, classification in enumerate(request.classifications):
                logger.info(f"正在处理需修改类别的第{i}条: {classification}")
                # 根据操作类型处理分类信息
                # 使用model_dump()方法来获取所有字段，包括动态字段
                classification_dict = classification.model_dump()
                logger.info(f"需处理的类别的字典: {classification_dict}")

                try:
                    if classification.action == "create":
                        await self._create_classification(prefixed_specification_uid, classification_dict)
                        results.append(f"第{i+1}项创建成功")
                    elif classification.action == "update":
                        await self._update_classification(prefixed_specification_uid, classification_dict)
                        results.append(f"第{i+1}项更新成功")
                    elif classification.action == "delete":
                        await self._delete_classification(prefixed_specification_uid, classification_dict)
                        results.append(f"第{i+1}项删除成功")
                    else:
                        error_msg = f"第{i+1}项操作失败：未知的操作类型 '{classification.action}'"
                        logger.warning(error_msg)
                        results.append(error_msg)
                except Exception as e:
                    error_msg = f"第{i+1}项{classification.action}操作失败：{str(e)}"
                    logger.error(error_msg, exc_info=True)
                    results.append(error_msg)

            # 检查是否有任何失败的操作
            failed_operations = [r for r in results if "失败" in r]
            if failed_operations:
                response = ClassificationResponse(
                    success=False,
                    code=500,
                    msg="部分操作失败：" + "；".join(failed_operations)
                )
            else:
                response = ClassificationResponse(
                    success=True,
                    code=200,
                    msg="所有分类信息处理成功：" + "；".join(results)
                )

            # 如果有任何操作，启动后台任务处理分类变更，但不等待其完成
            if has_any_operation:
                logger.info(
                    f"检测到操作，开始后台处理分类变更: {request.specificationUId}")
                
                # 首先将更新后的.jsonl文件数据保存到origin_data.md
                await self._save_origin_data_to_processed_dir(prefixed_specification_uid)
                
                # 异步启动向量数据库更新任务，不等待完成
                asyncio.create_task(vector_rebuilding_service.handle_classification_change(
                    prefixed_specification_uid,
                    [cls.model_dump() for cls in request.classifications]
                ))
                logger.info(f"已启动后台任务处理分类变更: {request.specificationUId}")
            
            return response

        except Exception as e:
            logger.error(f"处理分类信息失败: {str(e)}", exc_info=True)
            return ClassificationResponse(
                success=False,
                code=500,
                msg=f"处理分类信息失败: {str(e)}"
            )

    async def _create_classification(self, spec_uid: str, classification_item: Dict[str, Any]) -> None:
        """
        创建分类信息

        Args:
            spec_uid: 规范UId
            classification_item: 分类项数据
        """
        logger.info(f"正在创建规范UID为 {spec_uid} 的类别项")
        logger.info(f"其中的第 {classification_item} 数据")

        # 解析分类路径键值对，按键排序以确保顺序正确
        # 需要将字符串键转换为整数进行排序，然后再转回字符串
        digit_keys = [k for k in classification_item.keys()
                      if str(k).isdigit()]
        logger.info(f"发现'数字'型键: {digit_keys}")
        sorted_keys = sorted(digit_keys, key=lambda x: int(x))
        logger.info(f"排序后的键顺序: {sorted_keys}")
        print("=================================", digit_keys)

        # 构建分类层级
        classification_levels = {}
        header_levels = {}

        # 添加各个级别的分类
        for i, key in enumerate(sorted_keys):
            level_index = str(i)
            classification_levels[level_index] = classification_item[key]
            header_levels[level_index] = f"{['一','二','三','四','五','六','七','八','九','十'][i]}级分类"

        # 获取最大的索引值
        max_index = len(sorted_keys) - 1 if sorted_keys else -1
        logger.info(f"'N 级分类'中最高的等级: {max_index}")

        # 添加特征和等级信息（即使用户没有提供或为空也要添加）
        next_index = max_index + 1

        # 添加特征（无论是否存在且非空）
        feature_index = str(next_index)
        classification_levels[feature_index] = classification_item.get(
            "feature", "")
        header_levels[feature_index] = "对应特征"
        next_index += 1

        # 添加等级（无论是否存在）
        grading_index = str(next_index)
        classification_levels[grading_index] = classification_item.get(
            "grading", "")
        header_levels[grading_index] = "等级"
        next_index += 1

        # 添加条件和真实数据（保持为空）
        condition_index = str(next_index)
        classification_levels[condition_index] = ""
        header_levels[condition_index] = "条件"

        data_index = str(next_index + 1)
        classification_levels[data_index] = "[]"
        header_levels[data_index] = "真实数据"

        # 构建最终的数据结构
        result_data = {
            "header": header_levels,
            "data": classification_levels
        }

        logger.info(f"最终结果数据: {result_data}")

        # 确保目录存在
        standards_dir = Path("data/standards")
        standards_dir.mkdir(parents=True, exist_ok=True)

        # 保存到文件，文件名为 {specificationUId}_standard.jsonl
        file_path = standards_dir / f"{spec_uid}_standard.jsonl"

        # 检查是否已存在相同分类（仅比较分类路径部分）
        # 构建当前分类路径用于比较
        current_classification_path = {}
        for i, key in enumerate(sorted_keys):
            level_index = str(i)
            current_classification_path[level_index] = classification_item[key]

        # 如果文件存在，检查是否已经有相同的分类路径
        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        existing_data = json.loads(line.strip())
                        existing_classification = {}

                        # 提取已有数据的分类路径部分
                        data = existing_data.get("data", {})
                        header = existing_data.get("header", {})

                        # 提取分类路径
                        for k, v in header.items():
                            if v.endswith("分类"):
                                existing_classification[k] = data.get(k, "")

                        # 比较分类路径是否相同
                        if existing_classification == current_classification_path:
                            logger.info(f"类别已存在，请先delete再create: {file_path}")
                            raise Exception("已存在，请先delete再create")
                    except json.JSONDecodeError:
                        continue  # 忽略无效行

        # 读取现有文件内容
        existing_lines = []
        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                existing_lines = f.readlines()

        # 检查是否已存在相同分类（仅比较分类路径部分）
        updated_lines = []
        is_updated = False
        is_duplicate = False

        if existing_lines:
            # 构建当前分类路径用于比较
            current_feature = classification_item.get("feature", "")
            current_grading = classification_item.get("grading", "")

            # 检查每一行是否匹配
            for line in existing_lines:
                try:
                    existing_data = json.loads(line.strip())
                    existing_classification = {}

                    # 提取已有数据的分类路径部分
                    data = existing_data.get("data", {})
                    header = existing_data.get("header", {})

                    # 分离分类路径和特征/等级数据
                    existing_feature = ""
                    existing_grading = ""

                    # 提取分类路径和特征/等级
                    for k, v in header.items():
                        if v.endswith("分类"):
                            existing_classification[k] = data.get(k, "")
                        elif v == "对应特征":
                            existing_feature = data.get(k, "")
                        elif v == "等级":
                            existing_grading = data.get(k, "")

                    # 比较分类路径是否相同
                    if existing_classification == current_classification_path:
                        is_duplicate = True
                        # 检查feature或grading是否有变化
                        # 只有当新的feature或grading非空且与现有值不同时才更新
                        should_update = False

                        # 构建更新后的classification_levels
                        updated_classification_levels = classification_levels.copy()

                        # 处理feature更新逻辑
                        if current_feature and existing_feature != current_feature:
                            # 只有当新feature非空且与现有值不同时才更新
                            should_update = True
                        elif not current_feature and existing_feature:
                            # 如果新feature为空但存在旧feature，则保留旧feature
                            # 找到对应特征的键
                            for k, v in header.items():
                                if v == "对应特征":
                                    updated_classification_levels[k] = existing_feature
                                    break

                        # 处理grading更新逻辑
                        if current_grading and existing_grading != current_grading:
                            # 只有当新grading非空且与现有值不同时才更新
                            should_update = True
                        elif not current_grading and existing_grading:
                            # 如果新grading为空但存在旧grading，则保留旧grading
                            # 找到对应等级的键
                            for k, v in header.items():
                                if v == "等级":
                                    updated_classification_levels[k] = existing_grading
                                    break

                        if should_update:
                            # 更新该行数据
                            updated_result_data = {
                                "header": header_levels,
                                "data": updated_classification_levels
                            }
                            updated_lines.append(json.dumps(
                                updated_result_data, ensure_ascii=False) + "\n")
                            is_updated = True
                        else:
                            # 保持原样
                            updated_lines.append(line)
                    else:
                        # 不匹配的行保持原样
                        updated_lines.append(line)
                except json.JSONDecodeError:
                    # 无效行保持原样
                    updated_lines.append(line)

        # 如果是重复但已更新，则重写整个文件
        if is_duplicate and is_updated:
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(updated_lines)
            logger.info(f"类别更新保存在: {file_path}")
        # 如果不是重复的分类，则追加到文件
        elif not is_duplicate:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(result_data, ensure_ascii=False) + "\n")
            logger.info(f"新的类别已创建，保存在: {file_path}")
        else:
            logger.info(
                f"类别已存在，包含有相同的feature/grading，跳过 {file_path} 的create操作")

    async def _update_classification(self, spec_uid: str, classification_item: Dict[str, Any]) -> None:
        """
        更新分类信息

        Args:
            spec_uid: 规范UId
            classification_item: 分类项数据
        """
        logger.info(f"正在更新 {spec_uid} 的类别...")

        # 解析分类路径键值对，按键排序以确保顺序正确
        digit_keys = [k for k in classification_item.keys()
                      if str(k).isdigit()]
        sorted_keys = sorted(digit_keys, key=lambda x: int(x))

        # 构建要匹配的分类路径
        target_classification_path = {}
        for i, key in enumerate(sorted_keys):
            level_index = str(i)
            target_classification_path[level_index] = classification_item[key]

        # 获取要更新的feature和grading
        new_feature = classification_item.get("feature", "")
        new_grading = classification_item.get("grading", "")

        # 文件路径
        standards_dir = Path("data/standards")
        file_path = standards_dir / f"{spec_uid}_standard.jsonl"

        # 检查文件是否存在
        if not file_path.exists():
            logger.warning(f"警告: 正在进行 update 操作，但是文件 {file_path} 不存在!")
            raise Exception(f"更新失败：找不到规范 {spec_uid} 的分类数据文件")

        # 读取文件内容
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # 查找并更新匹配的分类
        updated_lines = []
        found_match = False

        for line in lines:
            try:
                existing_data = json.loads(line.strip())
                existing_header = existing_data.get("header", {})
                existing_data_content = existing_data.get("data", {})

                # 提取现有分类路径用于比较
                existing_classification_path = {}
                for k, v in existing_header.items():
                    if v.endswith("分类"):
                        existing_classification_path[k] = existing_data_content.get(
                            k, "")

                # 检查分类路径是否匹配
                if existing_classification_path == target_classification_path:
                    found_match = True
                    # 更新feature和grading，保留其他字段包括"真实数据"
                    updated_data_content = existing_data_content.copy()
                    updated_header = existing_header.copy()

                    # 查找并更新feature和grading字段
                    # 只有当新值非空时才更新
                    for k, v in existing_header.items():
                        if v == "对应特征" and new_feature:
                            updated_data_content[k] = new_feature
                        elif v == "等级" and new_grading:
                            updated_data_content[k] = new_grading
                        # "真实数据"和其他字段保持不变

                    # 构建更新后的数据
                    updated_record = {
                        "header": updated_header,
                        "data": updated_data_content
                    }
                    updated_lines.append(json.dumps(
                        updated_record, ensure_ascii=False) + "\n")
                    logger.info(f"规范 {spec_uid} 类别记录已更新")
                else:
                    # 未匹配的行保持不变
                    updated_lines.append(line)
            except json.JSONDecodeError:
                # 无法解析的行保持不变
                updated_lines.append(line)

        # 如果找到了匹配项，则写入更新后的内容
        if found_match:
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(updated_lines)
            logger.info(f"成功更新 {file_path} 中的类别")
        else:
            error_msg = f"更新失败：找不到匹配的类别，无法更新"
            logger.warning(f"未在 {file_path} 中匹配到可 update 的类别!")
            raise Exception(error_msg)

    async def _delete_classification(self, spec_uid: str, classification_item: Dict[str, Any]) -> None:
        """
        删除分类信息

        Args:
            spec_uid: 规范UId
            classification_item: 分类项数据
        """
        logger.info(f"正在删除规范 {spec_uid} 中的类别...")

        # 解析分类路径键值对，按键排序以确保顺序正确
        digit_keys = [k for k in classification_item.keys()
                      if str(k).isdigit()]
        sorted_keys = sorted(digit_keys, key=lambda x: int(x))

        # 构建要匹配的分类路径
        target_classification_path = {}
        for i, key in enumerate(sorted_keys):
            level_index = str(i)
            target_classification_path[level_index] = classification_item[key]

        # 文件路径
        standards_dir = Path("data/standards")
        file_path = standards_dir / f"{spec_uid}_standard.jsonl"

        # 检查文件是否存在
        if not file_path.exists():
            logger.warning(f"正在执行 delete 操作，但文件 {file_path} 不存在!")
            raise Exception(f"删除失败：找不到规范 {spec_uid} 的分类数据文件")

        # 读取文件内容
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # 查找并删除匹配的分类
        remaining_lines = []
        found_match = False

        for line in lines:
            try:
                existing_data = json.loads(line.strip())
                existing_header = existing_data.get("header", {})
                existing_data_content = existing_data.get("data", {})

                # 提取现有分类路径用于比较
                existing_classification_path = {}
                for k, v in existing_header.items():
                    if v.endswith("分类"):
                        existing_classification_path[k] = existing_data_content.get(
                            k, "")

                # 检查分类路径是否匹配
                if existing_classification_path == target_classification_path:
                    found_match = True
                    logger.info(f"规范 {spec_uid} 中的类别记录已删除")
                    # 不将匹配的行添加到remaining_lines中，实现删除效果
                else:
                    # 未匹配的行保留在文件中
                    remaining_lines.append(line)
            except json.JSONDecodeError:
                # 无法解析的行保持不变
                remaining_lines.append(line)

        # 如果找到了匹配项，则写入剩余的内容（实现删除效果）
        if found_match:
            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(remaining_lines)
            logger.info(f"成功删除 {file_path} 中的类别")
        else:
            error_msg = f"删除失败：找不到匹配的类别"
            logger.warning(f"删除失败，在 {file_path} 中找不到匹配的类别!")
            raise Exception(error_msg)


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
knowledge_classification_service = KnowledgeClassificationService()
