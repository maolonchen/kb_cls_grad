from fastapi import APIRouter, Query, UploadFile, File, Form, HTTPException, Depends
from typing import List, Optional, Dict, Any
import uuid
import os
import json
import logging
from pathlib import Path
import asyncio
from pydantic import BaseModel
from datetime import datetime

from app.schemas.knowledge_base import (
    KnowledgeBaseUploadResponse,
    DataRecognitionRequest,
    DataRecognitionResponse,
    TableAIScanResultDto,
    FileRecognitionRequest,
    FileRecognitionResponse,
    ClassificationRequest,
    ClassificationResponse,
    DataElementRequest,
    DataElementResponse,
    DataElementChangeRequest,
    DataElementChangeResponse,
    DataElementBatchMatchRequest,
    DataElementBatchMatchResponse,
    KnowledgeBaseDeleteRequest,
    KnowledgeBaseSizeInfoResponse
)

from app.services.knowledge_preprocess_service import knowledge_preprocessing_service
from app.services.knowledge_postprocess_service import process_all_chunks_and_insert_to_milvus
from app.services.vector_rebuild_service import vector_rebuilding_service
from app.services.data_recognition_service import data_recognizing_service
from app.services.file_recognition_service import file_recognition_service
from app.services.classification_grading_feature_operate_service import knowledge_classification_service
from app.services.data_element_operate_service import data_element_operate_service
from app.services.data_element_change_service import data_element_change_service
from app.services.data_element_batch_match_service import data_element_batch_match_service
from app.services.knowledge_size_service import knowledge_size_service
from app.services.kafka_service import kafka_service
from app.services.health_check_service import health_check_service
from app.services.interface_tracking_service import interface_tracking_service
from app.services.standard_compared_deletion_service import standard_compared_deletion_service
from app.core.task_manager import task_manager


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["知识库管理"])

# 健康检查响应模型
class HealthCheckResponse(BaseModel):
    status: str
    services: Dict[str, str]
    timestamp: str

@router.get("/health", response_model=HealthCheckResponse, summary="模型服务健康检查")
async def health_check():
    """
    检查各个AI模型服务的健康状态
    
    Returns:
        HealthCheckResponse: 包含各服务状态的响应
    """
    return await health_check_service.check_all_services()


@router.post("/specification/knowledgeBase", response_model=KnowledgeBaseUploadResponse, summary="向行业知识库添加知识")
async def add_knowledge(
    specificationUId: str = Form(..., description="规范UId唯一标识"),
    specificationName: str = Form(..., description="规范名称"),
    fileClassification: Optional[str] = Form(
        None, description="知识库文件所属类别（|分隔）"),
    files: Optional[List[UploadFile]] = File(
        None, description="知识库文件（PDF/Word/Excel/txt/csv）"),
    overrideExisting: Optional[bool] = Form(
        False, description="覆盖已有版本(默认false)")
):
    """
    向行业知识库添加知识接口

    Args:
        specificationUId: 规范UId唯一标识
        specificationName: 规范名称
        fileClassification: 知识库文件所属类别（|分隔）
        files: 知识库文件（PDF/Word/Excel/txt/csv）
        overrideExisting: 覆盖已有版本(默认false)

    Returns:
        dict: 包含success, code, msg的响应字典
    """
    # 为specificationUId添加前缀以满足Milvus命名规范（不支持数字开头）
    specificationUId = specificationUId.replace("-", "_")
    prefixed_specification_uid = f"_{specificationUId}"

    # 首先检查规范ID是否存在（检查data/processed目录下的文件夹名称，去掉_chunks后缀）
    processed_dir = Path("data/processed")
    if not processed_dir.exists():
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )

    # 获取所有以_chunks结尾的目录，并检查是否匹配
    existing_chunks_dirs = [d.name[:-7] for d in processed_dir.iterdir() if d.is_dir() and d.name.endswith("_chunks")]

    # 检查原ID或带前缀的ID是否存在于目录列表中
    if specificationUId not in existing_chunks_dirs and prefixed_specification_uid not in existing_chunks_dirs:
        logger.warning(f"规范不存在: {specificationUId}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )

    # 首先检查模型服务的健康状态
    health_status = await health_check_service.check_all_services()
    
    # 记录健康检查结果
    logger.info(f"向行业知识库添加知识接口，健康检查结果: {health_status}")
    
    # 如果模型不健康，返回错误信息
    if health_status["status"] != "healthy":
        logger.warning(f"模型服务不健康，拒绝处理请求: {health_status}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=500,
            msg="算力服务异常"
        )

    # 当fileClassification为空时，在data/standards目录下创建对应的空jsonl文件
    if not fileClassification:
        # 构建文件路径
        # standards_dir = Path("data/standards")
        # standard_file_path = standards_dir / f"{specificationUId}_standard.jsonl"
        standards_dir = Path("data/standards")
        standard_file_path = standards_dir / f"{prefixed_specification_uid}_standard.jsonl"
        
        # 创建空的标准文件，如果它不存在的话
        if not standard_file_path.exists():
            # 确保目录存在
            standards_dir.mkdir(parents=True, exist_ok=True)
            
            with open(standard_file_path, "w", encoding="utf-8") as f:
                pass
            
            logger.info(f"已为specificationUId '{specificationUId}' 创建空标准文件: {standard_file_path}")

    # 生成任务UID
    task_uid = str(uuid.uuid4())

    # 发送"待执行"状态消息
    kafka_service.send_task_status_message(task_uid, 1)

    existing_version = False  # 假设这里检测到已存在同名版本

    if existing_version and not overrideExisting:
        # 发送"任务失败"状态消息
        kafka_service.send_task_status_message(
            task_uid, 4, f"版本冲突！{specificationName}已存在，可设置override_existing=true")

        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg=f"版本冲突！{specificationName}已存在，可设置override_existing=true"
        )

    # 发送"执行中"状态消息
    kafka_service.send_task_status_message(task_uid, 2)

    # 检查fileClassification是否存在且匹配度低
    if fileClassification:
        # 导入RegexMatcher用于检查分类匹配度
        from app.core.regex_matcher import RegexMatcher
        regex_matcher = RegexMatcher(specification_uid=prefixed_specification_uid)

        # 获取分类的最后一级作为匹配文本
        category_parts = fileClassification.split("|")
        deepest_category = category_parts[-1] if category_parts else fileClassification

        # 进行匹配并获取匹配度
        matched_category, overlap_score = regex_matcher.find_best_match(
            deepest_category)

        logger.info(
            f"分类匹配检查: 输入分类='{deepest_category}', 匹配到标准分类='{matched_category}', 匹配度={overlap_score}")

        # 如果匹配度小于0.9，则返回特定的响应且不处理文件
        if overlap_score < 0.9:
            logger.info(
                f"分类匹配度低于阈值(0.9)，终止文件处理: 输入分类='{deepest_category}', 匹配到标准分类='{matched_category}', 匹配度={overlap_score}")

            # 发送"任务失败"状态消息
            kafka_service.send_task_status_message(
                task_uid,
                4,
                "知识库上传失败 1 个文件，构建失败，请检查'类别'及'等级'是否已预先上传！"
            )

            return KnowledgeBaseUploadResponse(
                success=False,
                code=400,
                msg="知识库上传失败 1 个文件，构建失败，请检查'类别'及'等级'是否已预先上传！"
            )

    # 处理上传的文件
    saved_files = []
    processing_tasks = []

    if files:
        for file in files:
            try:
                # 获取文件扩展名
                file_extension = file.filename.split(
                    '.')[-1].lower() if '.' in file.filename else ''

                # 验证文件类型
                if file_extension not in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'csv']:
                    # 发送"任务失败"状态消息
                    kafka_service.send_task_status_message(
                        task_uid, 4, f"不支持的文件类型: {file.filename}")
                    raise HTTPException(
                        status_code=400, detail=f"不支持的文件类型: {file.filename}")

                # 使用原始文件名而不是生成唯一文件名
                original_filename = file.filename
                file_path = knowledge_preprocessing_service.upload_dir / original_filename

                # 如果文件已存在，先删除
                if file_path.exists():
                    file_path.unlink()

                # 保存文件
                content = await file.read()
                with open(file_path, "wb") as buffer:
                    buffer.write(content)
                    saved_files.append(str(file_path))

                # 异步启动文件处理流程，不等待处理完成
                task = asyncio.create_task(
                    knowledge_preprocessing_service.process_file_async(
                        file_path,
                        file.filename,
                        prefixed_specification_uid,
                        fileClassification
                    )
                )
                processing_tasks.append(task)
                
            except Exception as e:
                logger.error(f"处理文件 {file.filename} 时出错: {str(e)}")
                # 发送"任务失败"状态消息
                kafka_service.send_task_status_message(
                    task_uid, 4, f"处理文件 {file.filename} 时出错: {str(e)}")
                raise HTTPException(
                    status_code=500, detail=f"处理文件 {file.filename} 时出错: {str(e)}")

    # 异步启动向量构建流程（在预处理完成后）
    if saved_files:
        # 使用任务管理器调度向量化任务，确保同一规范ID的任务串行执行
        # 并取消任何之前的待执行任务
        asyncio.create_task(
            process_vector_building_async(prefixed_specification_uid, task_uid)
        )
        
    else:
        # 如果没有文件需要处理，直接发送完成消息
        kafka_service.send_task_status_message(task_uid, 3)

    # 实际业务逻辑应该在这里实现
    logger.info(f"知识库添加成功，共保存了 {len(saved_files)} 个文件")

    # 立即返回上传成功，不等待处理完成
    return KnowledgeBaseUploadResponse(
        success=True,
        code=200,
        msg=f"知识库添加成功，共保存了 {len(saved_files)} 个文件" if files else "知识库信息已记录，未上传文件"
    )


async def process_vector_building_async(specification_uid: str, task_uid: str = None):
    """
    异步处理向量构建流程

    Args:
        specification_uid: 规范UId唯一标识
        task_uid: 任务唯一标识
    """
    # # 为specification_uid添加前缀以满足Milvus命名规范（不支持数字开头）
    # prefixed_specification_uid = f"_{specification_uid}"
    
    try:
        logger.info(f"开始异步处理规范 {specification_uid} 的向量构建")

        # 使用任务管理器调度向量化任务，确保同一规范ID的任务串行执行
        # 并取消任何之前的待执行任务
        # 定义一个包装函数来正确传递参数
        async def vector_build_wrapper(uid):
            return await process_all_chunks_and_insert_to_milvus(specification_uid=uid)

        insertion_stats = await task_manager.schedule_vector_task(
            specification_uid,
            vector_build_wrapper,
            specification_uid
        )

        logger.info(f"规范 {specification_uid} 的向量构建完成，插入统计: {insertion_stats}")

        # 发送"任务完成"状态消息
        if task_uid:
            kafka_service.send_task_status_message(task_uid, 3)

        # 可以在这里添加构建完成的通知逻辑，如更新数据库状态等

    except Exception as e:
        logger.error(f"规范 {specification_uid} 的向量构建失败: {e}", exc_info=True)
        # 发送"任务失败"状态消息
        if task_uid:
            kafka_service.send_task_status_message(
                task_uid, 4, f"向量构建失败: {str(e)}")


@router.post("/specification/knowledgeBase/delete", response_model=KnowledgeBaseUploadResponse, summary="删除行业知识库文件")
async def delete_knowledge(
    request: KnowledgeBaseDeleteRequest
):
    """
    删除行业知识库文件接口

    Args:
        request: 删除知识库文件请求参数

    Returns:
        dict: 包含success, code, msg的响应字典
    """
    
    # 为specificationUId添加前缀以满足Milvus命名规范（不支持数字开头）
    specificationUId = request.specificationUId.strip()
    specificationUId = specificationUId.replace("-", "_")
    prefixed_specification_uid = f"_{specificationUId}"

    # 首先检查规范ID是否存在（检查data/processed目录下的文件夹名称，去掉_chunks后缀）
    processed_dir = Path("data/processed")
    if not processed_dir.exists():
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )

    # 获取所有以_chunks结尾的目录，并检查是否匹配
    existing_chunks_dirs = [d.name[:-7] for d in processed_dir.iterdir() if d.is_dir() and d.name.endswith("_chunks")]

    # 检查原ID或带前缀的ID是否存在于目录列表中
    if specificationUId not in existing_chunks_dirs and prefixed_specification_uid not in existing_chunks_dirs:
        logger.warning(f"规范不存在: {specificationUId}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )
    
    # 首先检查模型服务的健康状态
    health_status = await health_check_service.check_all_services()
    
    # 记录健康检查结果
    logger.info(f"删除行业知识库文件接口，健康检查结果: {health_status}")
    
    # 如果模型不健康，返回错误信息
    if health_status["status"] != "healthy":
        logger.warning(f"模型服务不健康，拒绝处理请求: {health_status}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=500,
            msg="算力服务异常"
        )
        
    # 将文件名后缀替换为.md
    import os
    base_name = os.path.splitext(request.fileName)[0]
    md_file_name = base_name + ".md"
    
    # 删除data/raw目录下相同文件名的所有文件以及 *_fix.* 格式的文件
    raw_dir = Path("data/raw")
    if raw_dir.exists():
        for file_path in raw_dir.iterdir():
            if file_path.is_file():
                file_stem = os.path.splitext(file_path.name)[0]
                # 检查是否匹配原文件名或者 *_fix 格式
                if file_stem == base_name or (file_stem.endswith('_fix') and file_stem[:-4] == base_name):
                    try:
                        file_path.unlink()
                        logger.info(f"成功删除原始文件: {file_path}")
                    except Exception as e:
                        logger.warning(f"删除原始文件失败: {file_path}, 错误: {str(e)}")
    
    # 调用删除文件服务
    result = await vector_rebuilding_service.delete_knowledge_file_with_classification(
        prefixed_specification_uid, 
        request.fileClassification, 
        md_file_name
    )
    
    # 如果删除成功，使用任务管理器调度向量数据库重构任务，确保同一规范ID的任务串行执行
    # 并取消任何之前的待执行任务
    if result.success:
        logger.info(f"文件删除成功，开始后台调用向量数据库重构接口: {request.specificationUId}")
        import asyncio
        
        # 定义一个异步任务包装器，以捕获和记录可能的异常
        async def rebuild_wrapper():
            try:
                await vector_rebuilding_service.rebuild_vector_database(prefixed_specification_uid)
                logger.info(f"向量数据库重构任务完成: {request.specificationUId}")
            except Exception as e:
                logger.error(f"向量数据库重构任务执行失败: {request.specificationUId}, 错误: {str(e)}", exc_info=True)
        
        asyncio.create_task(rebuild_wrapper())
        logger.info(f"已启动后台任务调用重构接口: {request.specificationUId}")
    
    return result


@router.post("/specification/knowledgeBase/rebuild", response_model=KnowledgeBaseUploadResponse, summary="重构行业向量数据库")
async def reconstruct_vector_db(
    specificationUId: str = Form(..., description="行业ID")
):
    """
    重构行业向量数据库接口
    将data/processed/{specificationUId}_deleted目录下的文件进行向量化，
    并与现有向量数据库中的数据进行比较，删除相似度为1的数据

    Args:
        specificationUId: 行业ID

    Returns:
        dict: 包含success, code, msg的响应字典
    """
    
    # 为specificationUId添加前缀以满足Milvus命名规范（不支持数字开头）
    specificationUId = specificationUId.replace("-", "_")
    prefixed_specification_uid = f"_{specificationUId}"
    
    # 首先检查规范ID是否存在（检查data/processed目录下的文件夹名称，去掉_chunks后缀）
    processed_dir = Path("data/processed")
    if not processed_dir.exists():
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )

    # 获取所有以_chunks结尾的目录，并检查是否匹配
    existing_chunks_dirs = [d.name[:-7] for d in processed_dir.iterdir() if d.is_dir() and d.name.endswith("_chunks")]

    # 检查原ID或带前缀的ID是否存在于目录列表中
    if specificationUId not in existing_chunks_dirs and prefixed_specification_uid not in existing_chunks_dirs:
        logger.warning(f"规范不存在: {specificationUId}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )
    
    # 首先检查模型服务的健康状态
    health_status = await health_check_service.check_all_services()
    
    # 记录健康检查结果
    logger.info(f"重构行业向量数据库接口，健康检查结果: {health_status}")
    
    # 如果模型不健康，返回错误信息
    if health_status["status"] != "healthy":
        logger.warning(f"模型服务不健康，拒绝处理请求: {health_status}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=500,
            msg="算力服务异常"
        )
        
    return await vector_rebuilding_service.rebuild_vector_database(prefixed_specification_uid)


async def _save_recognition_result(data_result: TableAIScanResultDto, request: DataRecognitionRequest):
    """异步保存数据识别结果到 cache/ 目录"""
    try:
        # 提取表级别字段
        save_data = {
            "dbName": data_result.dbName,
            "schemaName": data_result.schemaName,
            "tableName": data_result.tableName,
            "tableAnnotate": data_result.tableAnnotate,
            "tableClassification": data_result.tableClassification,
            "tableGrade": data_result.tableGrade,
            "fields": []
        }

        # 提取字段级别信息
        if data_result.fields:
            for field in data_result.fields:
                save_data["fields"].append({
                    "fieldName": field.fieldName,
                    "fieldAnnotate": field.fieldAnnotate,
                    "classification": field.classification,
                    "grade": field.grade,
                    "element": field.element,
                    "reason": field.reason,
                })

        # 构造文件名: dbName_tableName_规范UIDs_时间戳.json
        parts = [request.dbName or ""]
        parts.append(request.tableName or "")
        for uid in [request.senSpecificationUId, request.impSpecificationUId, request.coreSpecificationUId]:
            if uid:
                parts.append(uid)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        parts.append(timestamp)
        filename = "_".join(p for p in parts if p) + ".json"

        # 写入文件
        cache_dir = Path("cache")
        cache_dir.mkdir(exist_ok=True)
        file_path = cache_dir / filename

        await asyncio.to_thread(
            lambda: file_path.write_text(json.dumps(save_data, ensure_ascii=False, indent=4), encoding="utf-8")
        )
        logger.info(f"识别结果已保存: {file_path}")
    except Exception as e:
        logger.error(f"保存识别结果失败: {e}")


@router.post("/dataRecognition", response_model=DataRecognitionResponse, summary="AI数据识别接口")
async def data_recognition(
    request: DataRecognitionRequest
):
    """
    AI数据识别接口

    Args:
        request: 数据识别请求参数

    Returns:
        DataRecognitionResponse: 数据识别结果
    """

    senSpecificationUId = getattr(request, 'senSpecificationUId', None)
    impSpecificationUId = getattr(request, 'impSpecificationUId', None)
    coreSpecificationUId = getattr(request, 'coreSpecificationUId', None)
    
    # 处理并添加下划线前缀
    if senSpecificationUId:
        senSpecificationUId = senSpecificationUId.replace("-", "_")
        senSpecificationUId = f"_{senSpecificationUId}"
        
    if impSpecificationUId:
        impSpecificationUId = impSpecificationUId.replace("-", "_")
        impSpecificationUId = f"_{impSpecificationUId}"
        
    if coreSpecificationUId:
        coreSpecificationUId = coreSpecificationUId.replace("-", "_")
        coreSpecificationUId = f"_{coreSpecificationUId}"
        

    # 收集所有非空的规范ID用于检查
    specification_ids = []
    if request.senSpecificationUId:
        specification_ids.append(senSpecificationUId)
    if request.impSpecificationUId:
        specification_ids.append(impSpecificationUId)
    if request.coreSpecificationUId:
        specification_ids.append(coreSpecificationUId)

    # 如果没有任何规范ID，则返回错误
    if not specification_ids:
        return DataRecognitionResponse(
            success=False,
            code=400,
            msg="至少需要提供一个有效的规范ID（senSpecificationUId、impSpecificationUId或coreSpecificationUId）"
        )

    # 验证所有提供的规范ID是否存在
    processed_dir = Path("data/processed")
    if not processed_dir.exists():
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )

    # 获取所有以_chunks结尾的目录，并检查是否匹配
    existing_chunks_dirs = [d.name[:-7] for d in processed_dir.iterdir() if d.is_dir() and d.name.endswith("_chunks")]
    print("=========================================================================", "existing_chunks_dirs:", existing_chunks_dirs)
    # ['_1res_001']

    # 检查每个规范ID（包括带前缀的版本）是否存在于目录列表中
    print("=========================================================================", "specification_ids:", specification_ids)

    found_any_spec = False
    for spec_id in specification_ids:
        if spec_id in existing_chunks_dirs:
            found_any_spec = True
            break
    
    # 只有当所有规范ID都不存在或者是空列表时，才返回错误
    if not found_any_spec:
        logger.warning(f"规范不存在: {specification_ids}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg=f"规范不存在: {specification_ids}，请先建立规范"
        )
    
    # 首先检查模型服务的健康状态
    health_status = await health_check_service.check_all_services()
    
    # 记录健康检查结果
    logger.info(f"AI数据识别接口，健康检查结果: {health_status}")
    
    # 如果模型不健康，返回错误信息
    if health_status["status"] != "healthy":
        logger.warning(f"模型服务不健康，拒绝处理请求: {health_status}")
        return DataRecognitionResponse(
            success=False,
            code=500,
            msg="算力服务异常",
            data=None
        )
        
    # 使用数据识别服务处理请求
    result = await data_recognizing_service.recognize_data(request)
    
    # 如果结果是列表且至少有一个元素，则取出第一个元素
    if isinstance(result, list) and len(result) > 0:
        data_result = result[0]
    elif isinstance(result, list) and len(result) == 0:
        # 如果结果是空列表，创建一个默认的空对象
        data_result = TableAIScanResultDto(
            dbName=request.dbName,
            schemaName=getattr(request, 'schemaName', None),
            tableName=getattr(request, 'tableName', "") or "",
            tableAnnotate="",
            tableClassification="",
            tableGrade="",
            tableElement=[],
            fields=[]
        )
    else:
        # 如果结果已经是单个对象，直接使用
        data_result = result
    
    # 异步保存识别结果到 cache/
    asyncio.create_task(_save_recognition_result(data_result, request))

    # 返回包装后的结果
    return DataRecognitionResponse(
        success=True,
        code=200,
        msg="处理成功",
        data=data_result
    )

@router.post("/fileRecognition", summary="AI文件识别接口")
async def file_recognition(
    fileName: str = Form(..., description="文件名"),
    file: UploadFile = File(...,
                            description="被识别文件（PDF/Word/Excel/md/csv/txt）"),
    senSpecificationUId: Optional[str] = Form(None, description="一般数据识别规范标识"),
    impSpecificationUId: Optional[str] = Form(None, description="重要数据识别规范标识"),
    coreSpecificationUId: Optional[str] = Form(None, description="核心数据识别规范标识"),
    systemType: Optional[str] = Form(None, description="业务系统类型"),
    systemName: Optional[str] = Form(None, description="业务系统名称")
):
    """
    AI文件识别接口

    Args:
        senSpecificationUId: 一般数据识别规范标识
        impSpecificationUId: 重要数据识别规范标识
        coreSpecificationUId: 核心数据识别规范标识
        fileName: 文件名
        file: 被识别文件（PDF/Word/Excel/md/csv/txt）
        systemType: 业务系统类型
        systemName: 业务系统名称

    Returns:
        dict: 包含success, code, msg和data字段的响应
    """
    
    # 收集所有非空的规范ID用于检查
    if senSpecificationUId:
        senSpecificationUId = senSpecificationUId.replace("-", "_")
        senSpecificationUId = f"_{senSpecificationUId}"
    if impSpecificationUId:
        impSpecificationUId = impSpecificationUId.replace("-", "_")
        impSpecificationUId = f"_{impSpecificationUId}"
    if coreSpecificationUId:
        coreSpecificationUId = coreSpecificationUId.replace("-", "_")
        coreSpecificationUId = f"_{coreSpecificationUId}"

    specification_ids = []
    if senSpecificationUId:
        specification_ids.append(senSpecificationUId.strip())
    if impSpecificationUId:
        specification_ids.append(impSpecificationUId.strip())
    if coreSpecificationUId:
        specification_ids.append(coreSpecificationUId.strip())

    # 如果没有任何规范ID，则返回错误
    if not specification_ids:
        return {
            "success": False,
            "code": 400,
            "msg": "至少需要提供一个有效的规范ID（senSpecificationUId、impSpecificationUId或coreSpecificationUId）",
            "data": {
                "fileName": fileName,
                "fileClassification": "",
                "fileGrade": "",
                "reason": "至少需要提供一个有效的规范ID"
            }
        }

    # 验证所有提供的规范ID是否存在
    processed_dir = Path("data/processed")
    if not processed_dir.exists():
        return {
            "success": False,
            "code": 400,
            "msg": "规范不存在，请先建立规范",
            "data": {
                "fileName": fileName,
                "fileClassification": "",
                "fileGrade": "",
                "reason": "规范不存在，请先建立规范"
            }
        }

    # 获取所有以_chunks结尾的目录，并检查是否匹配
    existing_chunks_dirs = [d.name[:-7] for d in processed_dir.iterdir() if d.is_dir() and d.name.endswith("_chunks")]
    
    print("=========================================================================", "existing_chunks_dirs:", existing_chunks_dirs)
    # ['_1res_001']

    # 检查每个规范ID（包括带前缀的版本）是否存在于目录列表中
    print("=========================================================================", "specification_ids:", specification_ids)
    # ['_', '_1res_001', '_1res_001']
    
    found_any_spec = False
    for spec_id in specification_ids:
        if spec_id in existing_chunks_dirs:
            found_any_spec = True
            break
    
    # 只有当所有规范ID都不存在或者是空列表时，才返回错误
    if not found_any_spec:
        logger.warning(f"规范不存在: {specification_ids}")
        return {
            "success": False,
            "code": 400,
            "msg": f"规范不存在: {specification_ids}，请先建立规范",
            "data": {
                "fileName": fileName,
                "fileClassification": "",
                "fileGrade": "",
                "reason": f"规范不存在: {specification_ids}，请先建立规范"
            }
        }
    
    # 首先检查模型服务的健康状态
    health_status = await health_check_service.check_all_services()
    
    # 记录健康检查结果
    logger.info(f"AI文件识别接口，健康检查结果: {health_status}")
    
    # 如果模型不健康，返回错误信息
    if health_status["status"] != "healthy":
        logger.warning(f"模型服务不健康，拒绝处理请求: {health_status}")
        return {
            "success": False,
            "code": 500,
            "msg": "算力服务异常",
            "data": {
                "fileName": fileName,
                "fileClassification": "",
                "fileGrade": "",
                "reason": "模型服务不健康"
            }
        }
        
    try:
        # 保存上传的文件到临时位置
        temp_file_path = knowledge_preprocessing_service.upload_dir / \
            f"temp_{file.filename}"
        content = await file.read()
        with open(temp_file_path, "wb") as buffer:
            buffer.write(content)

        # 使用文件识别服务处理请求
        result = await file_recognition_service.recognize_file(
            file_name=fileName,
            file_path=str(temp_file_path),
            system_type=systemType,
            system_name=systemName,
            sen_spec_uid=senSpecificationUId,
            imp_spec_uid=impSpecificationUId,
            core_spec_uid=coreSpecificationUId
        )

        # 清理临时文件
        if temp_file_path.exists():
            temp_file_path.unlink()

        # 包装符合规范的响应格式，添加data字段
        return {
            "success": True,
            "code": 200,
            "msg": "处理成功",
            "data": result
        }

    except Exception as e:
        logger.error(f"文件识别失败: {e}", exc_info=True)
        # 清理临时文件
        temp_file_path = knowledge_preprocessing_service.upload_dir / \
            f"temp_{file.filename}"
        if temp_file_path.exists():
            temp_file_path.unlink()

        return {
            "success": False,
            "code": 500,
            "msg": f"文件识别失败: {str(e)}",
            "data": {
                "fileName": fileName,
                "fileClassification": "",
                "fileGrade": "",
                "reason": str(e)
            }
        }


@router.post(
    "/specification/knowledgeBase/classification",
    response_model=ClassificationResponse,
    summary="数据分类信息接口"
)
async def classification_info(request: ClassificationRequest):
    """
    数据分类信息接口

    Args:
        request: 分类信息请求参数

    Returns:
        ClassificationResponse: 分类信息处理结果
    """
    # 首先检查模型服务的健康状态
    health_status = await health_check_service.check_all_services()
    
    # 记录健康检查结果
    logger.info(f"数据分类信息接口，健康检查结果: {health_status}")
    
    # 如果模型不健康，返回错误信息
    if health_status["status"] != "healthy":
        logger.warning(f"模型服务不健康，拒绝处理请求: {health_status}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=500,
            msg="算力服务异常"
        )
        
    try:
        # 使用分类信息服务处理请求
        result = await knowledge_classification_service.process_classifications(request)
        return result
    except Exception as e:
        logger.error(f"处理分类信息失败: {e}", exc_info=True)
        return ClassificationResponse(
            success=False,
            code=500,
            msg=f"处理分类信息失败: {str(e)}"
        )


@router.post(
    "/specification/knowledgeBase/dataElement",
    response_model=DataElementResponse,
    summary="数据特征信息接口"
)
async def data_element_info(request: DataElementRequest):
    """
    数据特征信息接口

    Args:
        request: 数据特征信息请求参数

    Returns:
        DataElementResponse: 数据特征信息处理结果
    """
    # 首先检查模型服务的健康状态
    health_status = await health_check_service.check_all_services()
    
    # 记录健康检查结果
    logger.info(f"数据特征信息接口，健康检查结果: {health_status}")
    
    # 如果模型不健康，返回错误信息
    if health_status["status"] != "healthy":
        logger.warning(f"模型服务不健康，拒绝处理请求: {health_status}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=500,
            msg="算力服务异常"
        )
        
    try:
        # 使用数据特征信息服务处理请求
        result = await data_element_operate_service.process_data_elements(request)
        return result
    except Exception as e:
        logger.error(f"处理数据特征信息失败: {e}", exc_info=True)
        return DataElementResponse(
            success=False,
            code=500,
            msg=f"处理数据特征信息失败: {str(e)}"
        )


@router.post(
    "/specification/knowledgeBase/restClassification",
    response_model=ClassificationResponse,
    summary="数据分类信息接口"
)
async def rest_classification_info(request: ClassificationRequest):
    """
    数据分类信息接口

    Args:
        request: 分类信息请求参数

    Returns:
        ClassificationResponse: 分类信息处理结果
    """
    
    # 为specificationUId添加前缀以满足Milvus命名规范（不支持数字开头）
    specificationUId = request.specificationUId
    specificationUId = specificationUId.replace("-", "_")
    prefixed_specification_uid = f"_{specificationUId}"
    
    # 首先检查模型服务的健康状态
    health_status = await health_check_service.check_all_services()
    
    # 记录健康检查结果
    logger.info(f"数据分类信息接口，健康检查结果: {health_status}")
    
    # 如果模型不健康，返回错误信息
    if health_status["status"] != "healthy":
        logger.warning(f"模型服务不健康，拒绝处理请求: {health_status}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=500,
            msg="算力服务异常"
        )
        
    try:
        # 首先清空标准文件
        clear_result = standard_compared_deletion_service.clear_standard_file(prefixed_specification_uid)
        if not clear_result["success"]:
            logger.warning(f"清空标准文件失败: {clear_result['msg']}")
            
        # 使用分类信息服务处理请求
        result = await knowledge_classification_service.process_classifications(request)
        
        # 记录接口调用，以便后续处理标准比较删除
        interface_tracking_service.record_interface_call(prefixed_specification_uid, 'classification')
        
        return result
    except Exception as e:
        logger.error(f"处理分类信息失败: {e}", exc_info=True)
        return ClassificationResponse(
            success=False,
            code=500,
            msg=f"处理分类信息失败: {str(e)}"
        )


@router.post(
    "/specification/knowledgeBase/restDataElement",
    response_model=DataElementResponse,
    summary="数据特征信息接口"
)
async def rest_data_element_info(request: DataElementRequest):
    """
    数据特征信息接口

    Args:
        request: 数据特征信息请求参数

    Returns:
        DataElementResponse: 数据特征信息处理结果
    """
    specificationUId = request.specificationUId
    specificationUId = specificationUId.replace("-", "_")
    prefixed_specification_uid = f"_{specificationUId}"
    
    # 首先检查模型服务的健康状态
    health_status = await health_check_service.check_all_services()
    
    # 记录健康检查结果
    logger.info(f"数据特征信息接口，健康检查结果: {health_status}")
    
    # 如果模型不健康，返回错误信息
    if health_status["status"] != "healthy":
        logger.warning(f"模型服务不健康，拒绝处理请求: {health_status}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=500,
            msg="算力服务异常"
        )
        
    try:
        # 使用数据特征信息服务处理请求
        result = await data_element_operate_service.process_data_elements(request)
        
        # 记录接口调用，以便后续处理标准比较删除
        interface_tracking_service.record_interface_call(prefixed_specification_uid, 'data_element')
        
        return result
    except Exception as e:
        logger.error(f"处理数据特征信息失败: {e}", exc_info=True)
        return DataElementResponse(
            success=False,
            code=500,
            msg=f"处理数据特征信息失败: {str(e)}"
        )

@router.post(
    "/specification/knowledgeBase/dataElement/change",
    response_model=DataElementChangeResponse,
    summary="数据元素更换接口"
)
async def data_element_change(request: DataElementChangeRequest):
    """
    数据元素更换接口

    Args:
        request: 数据元素更换请求参数

    Returns:
        DataElementChangeResponse: 数据元素更换处理结果
    """
    
    # 为specificationUId添加前缀以满足Milvus命名规范（不支持数字开头）
    specificationUId = request.specificationUId
    specificationUId = specificationUId.replace("-", "_")
    prefixed_specification_uid = f"_{specificationUId}"

    # 首先检查规范ID是否存在（检查data/processed目录下的文件夹名称，去掉_chunks后缀）
    processed_dir = Path("data/processed")
    if not processed_dir.exists():
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )

    # 获取所有以_chunks结尾的目录，并检查是否匹配
    existing_chunks_dirs = [d.name[:-7] for d in processed_dir.iterdir() if d.is_dir() and d.name.endswith("_chunks")]

    # 检查原ID或带前缀的ID是否存在于目录列表中
    if specificationUId not in existing_chunks_dirs and prefixed_specification_uid not in existing_chunks_dirs:
        logger.warning(f"规范不存在: {specificationUId}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )
    
    # 首先检查模型服务的健康状态
    health_status = await health_check_service.check_all_services()
    
    # 记录健康检查结果
    logger.info(f"数据元素更换接口，健康检查结果: {health_status}")
    
    # 如果模型不健康，返回错误信息
    if health_status["status"] != "healthy":
        logger.warning(f"模型服务不健康，拒绝处理请求: {health_status}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=500,
            msg="算力服务异常"
        )
        
    try:
        # 使用数据元素更换服务处理请求
        result = await data_element_change_service.process_data_element_change(request)
        return result
    except Exception as e:
        logger.error(f"处理数据元素更换失败: {e}", exc_info=True)
        return DataElementChangeResponse(
            success=False,
            code=500,
            msg=f"处理数据元素更换失败: {str(e)}"
        )


# @router.post(
#     "/dataElements/match",
#     response_model=DataElementBatchMatchResponse,
#     summary="匹配数据特征一致性接口"
# )
# async def data_element_batch_match(request: DataElementBatchMatchRequest):
#     """
#     匹配数据特征一致性接口

#     Args:
#         request: 数据特征批量匹配请求参数

#     Returns:
#         DataElementBatchMatchResponse: 数据特征批量匹配处理结果
#     """
#     # 首先检查模型服务的健康状态
#     health_status = await health_check_service.check_all_services()
    
#     # 记录健康检查结果
#     logger.info(f"匹配数据特征一致性接口，健康检查结果: {health_status}")
    
#     # 如果编码服务不健康，返回错误信息
#     if health_status["services"]["编码服务"] != "healthy":
#         logger.warning(f"编码服务不健康，拒绝处理请求: {health_status}")
#         return KnowledgeBaseUploadResponse(
#             success=False,
#             code=500,
#             msg="算力服务异常"
#         )
        
#     try:
#         # 使用数据特征批量匹配服务处理请求
#         result = await data_element_batch_match_service.process_data_element_batch_match(request)
#         return result
#     except Exception as e:
#         logger.error(f"处理数据特征匹配失败: {e}", exc_info=True)
#         return DataElementBatchMatchResponse(
#             success=False,
#             code=500,
#             msg=f"处理数据特征匹配失败: {str(e)}",
#             data=[]
#         )


@router.get(
    "/specification/knowledgeBase/sizeInformation",
    response_model=KnowledgeBaseSizeInfoResponse,
    summary="获取知识库大小信息接口"
)
async def get_knowledge_base_size_info(specificationUId: str = Query(...)):
    """
    获取知识库大小信息接口
    统计指定规范ID下的原始文件大小、解析后的知识大小和向量数据大小

    Args:
        specificationUId: 规范UId唯一标识

    Returns:
        KnowledgeBaseSizeInfoResponse: 包含各种数据大小信息的响应
    """
    
    # 为specificationUId添加前缀以满足Milvus命名规范（不支持数字开头）
    specificationUId = specificationUId.replace("-", "_")
    prefixed_specification_uid = f"_{specificationUId}"

    # 首先检查规范ID是否存在（检查data/processed目录下的文件夹名称，去掉_chunks后缀）
    processed_dir = Path("data/processed")
    if not processed_dir.exists():
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )

    # 获取所有以_chunks结尾的目录，并检查是否匹配
    existing_chunks_dirs = [d.name[:-7] for d in processed_dir.iterdir() if d.is_dir() and d.name.endswith("_chunks")]

    # 检查原ID或带前缀的ID是否存在于目录列表中
    if specificationUId not in existing_chunks_dirs and prefixed_specification_uid not in existing_chunks_dirs:
        logger.warning(f"规范不存在: {specificationUId}")
        return KnowledgeBaseUploadResponse(
            success=False,
            code=400,
            msg="规范不存在，请先建立规范"
        )
    
    # 使用服务类处理逻辑
    result = await knowledge_size_service.get_knowledge_base_size_info(prefixed_specification_uid)
    
    if result is None:
        return KnowledgeBaseSizeInfoResponse(
            success=False,
            code=404,
            msg=f"未找到行业 {specificationUId} 的分类结果文件"
        )

    # 返回结果
    return KnowledgeBaseSizeInfoResponse(
        success=True,
        code=200,
        msg="操作成功",
        data=result
    )