from pydantic import BaseModel, ConfigDict
from typing import Dict, Optional, List, Union


class DataElementBatchMatchDto(BaseModel):
    """
    数据特征批量匹配结果传输对象
    """
    model_config = ConfigDict(populate_by_name=True)
    
    matchElementName: str
    similarity: float



class DataElementBatchMatchRequest(BaseModel):
    """
    数据特征批量匹配请求模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    elementName: str
    elementNames: List[str]
    similarityThreshold: Optional[float] = None
    maxResults: Optional[int] = None


class DataElementBatchMatchResponse(BaseModel):
    """
    数据特征批量匹配响应模型
    """
    model_config = ConfigDict(populate_by_name=True)
    
    success: bool
    code: int
    msg: Optional[str] = None
    data: Optional[List[DataElementBatchMatchDto]] = None
