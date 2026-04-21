# -*- coding: utf-8 -*-
"""
项目配置文件
"""

import os
from dataclasses import dataclass
from typing import Any, ClassVar, Dict

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 数据路径配置 - 修复路径，使用项目根目录的data目录
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')

# 原始Excel文件路径
XLS_ORI_PATH = os.path.join(RAW_DATA_DIR, 'origin_data.xlsx')

# 处理后文件路径
XLS_UNCOMBINE_PATH = os.path.join(PROCESSED_DATA_DIR, 'uncombine_data.xlsx')
JSON_ORI_PATH = os.path.join(PROCESSED_DATA_DIR, 'ori_json.json')
MULTI_TREE_PATH = os.path.join(PROCESSED_DATA_DIR, 'multi_tree.txt')
GRAD_ENTITY_PATH = os.path.join(PROCESSED_DATA_DIR, 'output_sheet3.json')
SIMILAR_COMPARE_PATH = os.path.join(PROCESSED_DATA_DIR, 'grad_results.txt')
LLM_POST_SIMILAR_PATH = os.path.join(PROCESSED_DATA_DIR, 'llm_similar_post.txt')
FINAL_PATH = os.path.join(PROCESSED_DATA_DIR, 'processed_results.jsonl')
POST_FINAL_PATH = os.path.join(PROCESSED_DATA_DIR, 'final.jsonl')
KAFKA_DATA_PATH = os.path.join(PROCESSED_DATA_DIR, 'kafka_output_data.json')
KAFKA_GRAD_PATH = os.path.join(PROCESSED_DATA_DIR, 'kafka_output_grad.json')
KAFKA_feat_PATH = os.path.join(PROCESSED_DATA_DIR, 'kafka_output_feat.json')

# Kafka开关配置 - 控制是否启用Kafka消息发送功能
ENABLE_KAFKA = os.getenv("ENABLE_KAFKA", "true").lower() == "true"

def get_dynamic_xls_path(specification_u_id=None):
    """根据规范ID获取Excel文件路径"""
    if specification_u_id:
        excel_dir = os.path.join(
            PROJECT_ROOT, 
            f'classification_and_grading/standard_{specification_u_id}/01_raw_documents/excel'
        )
        # 检查目录是否存在
        if os.path.exists(excel_dir):
            # 查找目录中的xlsx文件
            for file in os.listdir(excel_dir):
                if file.endswith('.xlsx'):
                    return os.path.join(excel_dir, file)
        # 如果没有找到xlsx文件，则使用默认名称
        return os.path.join(excel_dir, 'oriAI_rules.xlsx')
    return XLS_ORI_PATH

@dataclass
class EmbeddingConfig:
    """嵌入服务配置类"""
    api_url: ClassVar[str] = os.getenv(
        "EMBEDDING_API_URL", "http://192.168.101.113:9998/v1/embeddings")
    model: ClassVar[str] = os.getenv("EMBEDDING_MODEL", "qwen3-embedding-8b")

# 数据库配置
# DB_PATH = os.path.join(PROJECT_ROOT, 'db/milvus_excel_grading.db')
DB_HOST = "192.168.10.15"  # Milvus服务器地址
DB_PORT = "19530"          # Milvus gRPC端口
DB_PATH = f"http://{DB_HOST}:{DB_PORT}"  # Standalone Milvus连接地址
GRAD_COLLECTION_NAME = 'excel_grading_collection'

# LLM配置
# LLM_API_URL = "http://192.168.101.113:8000/v1/chat/completions"
# MODEL_NAME = "qwen3-32b"

LLM_API_URL = "http://192.168.101.113:11434/v1/chat/completions"
MODEL_NAME = "qwen2.5-7b"

# LLM_API_URL = "https://u343777-b730-b26a2498.westx.seetacloud.com:8443/v1/chat/completions"
# MODEL_NAME = "/root/autodl-tmp/model/Qwen/Qwen3-8B"

HEADERS = {
    "Content-Type": "application/json",
}

def API_PAYLOAD(model_name, prompt):
    """生成API请求载荷"""
    return {
        "model": model_name,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "top_p": 0.8,
        "top_k": 20,
        "max_tokens": 4096,
        "presence_penalty": 1.2,
        "chat_template_kwargs": {"enable_thinking": True},
        "stream": False
    }
    

class DataElementMatchConfig:
    # 数据元素匹配配置
    # 默认相似度阈值
    default_similarity_threshold: ClassVar[float] = 0.0
    # 默认最大返回结果数
    default_max_results: ClassVar[int] = 1
    
    
class BM25Config:
    # BM25配置
    # 默认top_k值
    default_top_k: ClassVar[int] = 10
    # 默认权重分配 (嵌入相似度权重, BM25权重)
    default_weights: ClassVar[tuple] = (1.0, 0.0)
    
    
class EmbeddingConfig:
    # 嵌入模型配置
    api_url: ClassVar[str] = "http://192.168.101.113:9998/v1/embeddings"  # siweicn
    headers: ClassVar[Dict[str, Any]] = {
        "Content-Type": "application/json",
        # "Authorization": f"Bearer ???"
    }
    model: ClassVar[str] = "qwen3-embedding-8b"
    embedding_dim: ClassVar[int] = 4096
    # 文本最大长度（字符数）
    max_content_length: ClassVar[int] = 3000
    
    
class HttpStatus:
    """HTTP状态码常量"""
    SUCCESS = 200
    CREATED = 201
    ACCEPTED = 202
    NO_CONTENT = 204
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    REQUEST_TIMEOUT = 408
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503