from enum import Enum
from pydantic import BaseModel, ConfigDict
from typing import Dict, Optional, List, Union
from fastapi import UploadFile


class KnowledgeBaseCreateRequest(BaseModel):
    """
    创建知识库请求模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    specificationUId: str
    specificationName: Optional[str] = None
    fileClassification: Optional[str] = None
    overrideExisting: Optional[bool] = False


class KnowledgeBaseCreateResponse(BaseModel):
    """
    创建知识库响应模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    success: bool
    code: int
    msg: str


class KnowledgeBaseUploadRequest(BaseModel):
    """
    知识库上传文件请求模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    specificationUId: str
    specificationName: Optional[str] = None
    fileClassification: Optional[str] = None
    overrideExisting: Optional[bool] = False
    

class KnowledgeBaseUploadResponse(BaseModel):
    """
    知识库上传文件响应模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    success: bool
    code: int
    msg: str


class FieldDataDto(BaseModel):
    """
    字段数据传输对象
    """
    model_config = ConfigDict(populate_by_name=True)
    
    fieldName: str
    fieldComment: Optional[str] = None
    sampleValue: Optional[List[str]] = None


class DataRecognitionRequest(BaseModel):
    """
    数据识别请求模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    senSpecificationUId: Optional[str] = None
    impSpecificationUId: Optional[str] = None
    coreSpecificationUId: Optional[str] = None
    senSpecificationName: Optional[str] = None
    impSpecificationName: Optional[str] = None
    coreSpecificationName: Optional[str] = None
    dbName: str
    schemaName: Optional[str] = None
    tableName: Optional[str] = None
    tableComment: Optional[str] = None
    tableRows: Optional[int] = None
    systemType: Optional[str] = None
    systemName: Optional[str] = None
    fields: Optional[List[FieldDataDto]] = None


class FileRecognitionRequest(BaseModel):
    """
    文件识别请求模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    senSpecificationUId: Optional[str] = None
    impSpecificationUId: Optional[str] = None
    coreSpecificationUId: Optional[str] = None
    senSpecificationName: Optional[str] = None
    impSpecificationName: Optional[str] = None
    coreSpecificationName: Optional[str] = None
    fileName: str
    systemType: Optional[str] = None
    systemName: Optional[str] = None


class FileRecognitionResponse(BaseModel):
    """
    文件识别响应模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    fileName: str
    fileClassification: str
    fileGrade: str
    reason: str


class FieldAIScanResultDto(BaseModel):
    """
    字段AI识别结果传输对象
    """
    model_config = ConfigDict(populate_by_name=True)
    
    fieldName: str
    fieldAnnotate: Optional[str] = None
    classification: Optional[str] = None
    grade: Optional[str] = None
    element: Optional[str] = None
    reason: Optional[str] = None


class TableAIScanResultDto(BaseModel):
    """
    表AI识别结果传输对象
    """
    model_config = ConfigDict(populate_by_name=True)
    
    dbName: str
    schemaName: Optional[str] = None
    tableName: str
    tableAnnotate: Optional[str] = None
    tableClassification: Optional[str] = None
    tableGrade: Optional[str] = None
    tableElement: Optional[List[str]] = None
    fields: List[FieldAIScanResultDto]


class DataRecognitionResponse(BaseModel):
    """
    数据识别响应模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    success: bool
    code: int
    msg: Optional[str] = None
    data: Optional[TableAIScanResultDto] = None


class ClassificationItem(BaseModel):
    """
    分类信息项模型
    """
    model_config = ConfigDict(extra='allow', populate_by_name=True)

    
    action: str  # 只针对分类的操作，取值"create"、"update"、"delete"
    grading: Optional[str] = None  # 数据分级结果，例如 "第3级"
    feature: Optional[str] = None  # 类别特征描述
    
    # # 使用字典来接收动态键值对，表示分类路径（如 "0", "1", "2"），值为分类描述文本
    # __pydantic_extra__: Dict[str, str]


class ClassificationRequest(BaseModel):
    """
    数据分类信息请求模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    specificationUId: str
    specificationName: Optional[str] = None
    classifications: List[ClassificationItem]


class ClassificationResponse(BaseModel):
    """
    数据分类信息响应模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    success: bool
    code: int
    msg: Optional[str] = None


class DataElementAction(str, Enum):
    """
    数据元素操作类型枚举
    """
    model_config = ConfigDict(populate_by_name=True)
    
    ADD = "add"
    UPDATE = "update"
    DELETE = "delete"


class DataElementItem(BaseModel):
    """
    数据元素项模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    action: DataElementAction
    classification: str
    element: List[str]


class DataElementRequest(BaseModel):
    """
    数据特征信息请求模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    specificationUId: str
    specificationName: Optional[str] = None
    dataElements: List[DataElementItem]


class DataElementResponse(BaseModel):
    """
    数据特征信息响应模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    success: bool
    code: int
    msg: Optional[str] = None
    
    
class DataElementChangeRequest(BaseModel):
    """
    数据元素更换请求模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    specificationUId: str
    specificationName: Optional[str] = None
    originElement: str
    replaceElement: str


class DataElementChangeResponse(BaseModel):
    """
    数据元素更换响应模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    success: bool
    code: int
    msg: Optional[str] = None
    
    
class DataElementBatchMatchRequest(BaseModel):
    """
    数据特征批量匹配请求模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    elementName: str
    elementNames: List[str]
    similarityThreshold: Optional[float] = None
    maxResults: Optional[int] = None


class DataElementBatchMatchDto(BaseModel):
    """
    数据特征批量匹配结果传输对象
    """
    model_config = ConfigDict(populate_by_name=True)
    
    matchElementName: str
    similarity: float


class DataElementBatchMatchResponse(BaseModel):
    """
    数据特征批量匹配响应模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    success: bool
    code: int
    msg: Optional[str] = None
    data: Optional[List[DataElementBatchMatchDto]] = None
    
    
class KnowledgeBaseDeleteRequest(BaseModel):
    """
    删除知识库文件请求模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    specificationUId: str
    fileName: str
    fileClassification: Optional[str] = None
    
    
class KnowledgeBaseSizeInfoResponse(BaseModel):
    """
    知识库大小信息响应模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    success: bool
    code: int
    msg: str
    data: Optional[Dict[str, float]] = None