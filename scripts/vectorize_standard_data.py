#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
向量化标准数据脚本
将 data/standards/*_standard.jsonl 中每行数据的"真实数据"字段中的每个值单独向量化
并将结果存储在名为 "element_vector" 的 collection 中
"""

import asyncio
import json
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.core.vectoring import VectorClient
from app.core.config import DatabaseConfig, EmbeddingConfig
from pymilvus import MilvusClient, DataType
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElementVectorClient(VectorClient):
    """专门用于元素向量化的客户端"""
    
    def __init__(self, max_concurrent: int = None):
        # 确保数据库目录存在
        db_path = Path(DatabaseConfig.uri)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 使用API而不是本地模型
        # self.milvus_client = MilvusClient(DatabaseConfig.path)
        self.milvus_client = MilvusClient(uri=DatabaseConfig.uri)
        self.collection_name = "element_vector"
        self.embedding_api_url = EmbeddingConfig.api_url
        
        # 维度
        self.embedding_dim = EmbeddingConfig.embedding_dim
        
        # 并发控制
        from app.core.config import AsyncEmbeddingConfig
        if max_concurrent is None:
            max_concurrent = AsyncEmbeddingConfig.max_concurrent_requests
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        logger.info(f"向量维度: {self.embedding_dim}, 最大并发数: {self.max_concurrent}")

    def create_collection(self):
        """创建element_vector集合和索引"""
        # 创建 schema
        schema = self.milvus_client.create_schema(
            auto_id=False,
            description="Element vectors with embeddings for standard data"
        )
        
        # 添加字段到 schema
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=self.embedding_dim)
        schema.add_field("text", DataType.VARCHAR, max_length=65535)
        schema.add_field("items", DataType.VARCHAR, max_length=65535)
        schema.add_field("classification", DataType.VARCHAR, max_length=1024)  # 一级分类
        schema.add_field("subcategory", DataType.VARCHAR, max_length=1024)     # 二级分类
        schema.add_field("source_line", DataType.INT64)                        # 源数据行号
        
        # 创建集合
        self.milvus_client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
            consistency_level="Strong"
        )
        
        # 创建向量索引
        index_params = self.milvus_client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            index_type="FLAT",
            metric_type="IP"
        )
        self.milvus_client.create_index(
            collection_name=self.collection_name,
            index_params=index_params
        )
        
        logger.info("已创建 element_vector 集合和索引")
        
        self.load_collection()
    
    def insert_data(self, data):
        """插入数据到集合中"""
        # 检查集合是否存在，如果不存在则创建
        if not self.has_collection():
            logger.info(f"集合 {self.collection_name} 不存在，正在创建...")
            self.create_collection()
        
        result = self.milvus_client.insert(
            collection_name=self.collection_name,
            data=data
        )
        logger.info(f"成功插入 {len(result['ids'])} 条记录")
        return result


async def vectorize_standard_data(specification_uid: str = None):
    """向量化标准数据文件中的每个真实数据项"""
    print("=== 开始向量化标准数据 ===")
    
    # 初始化向量客户端
    vector_client = ElementVectorClient()
    
    # 确定标准数据文件路径
    if specification_uid:
        specification_uid = specification_uid.replace("-", "_")
        standard_file_path = project_root / "data" / "standards" / f"{specification_uid}_standard.jsonl"
    else:
        standard_file_path = project_root / "data" / "standards" / "standard.jsonl"
    
    # 确保集合存在
    if not vector_client.has_collection():
        print("element_vector 集合不存在，正在创建...")
        vector_client.create_collection()
    
    # 加载集合
    vector_client.load_collection()
    
    if not standard_file_path.exists():
        print(f"标准数据文件不存在: {standard_file_path}")
        return
    
    # 读取并处理标准数据文件
    data_items = []
    with open(standard_file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                record = json.loads(line.strip())
                # 获取真实数据字段
                real_data_key = None
                header = record.get("header", {})
                
                # 查找"真实数据"列对应的键
                for key, value in header.items():
                    if value == "真实数据":
                        real_data_key = key
                        break
                
                if real_data_key is None:
                    print(f"警告: 第{line_num}行未找到'真实数据'字段")
                    continue
                
                # 获取真实数据值
                real_data_values = record.get("data", {}).get(real_data_key, [])
                
                # 如果是字符串形式的数组，则需要解析
                if isinstance(real_data_values, str):
                    try:
                        real_data_values = json.loads(real_data_values)
                    except json.JSONDecodeError:
                        # 如果不是JSON格式，可能是逗号分隔的字符串
                        real_data_values = [item.strip() for item in real_data_values.split(',')]
                
                # 添加到数据项列表
                if isinstance(real_data_values, list):
                    for item in real_data_values:
                        if item:  # 忽略空项
                            data_items.append({
                                "text": item,
                                "source_line": line_num,
                                "classification": record.get("data", {}).get("0", ""),  # 一级分类
                                "subcategory": record.get("data", {}).get("1", "") if "1" in record.get("data", {}) else ""      # 二级分类
                            })
                else:
                    print(f"警告: 第{line_num}行的真实数据格式不正确")
                    
            except json.JSONDecodeError as e:
                print(f"警告: 第{line_num}行JSON解析错误: {e}")
            except Exception as e:
                print(f"警告: 处理第{line_num}行时发生错误: {e}")
    
    print(f"总共找到 {len(data_items)} 个数据项需要向量化")
    
    if not data_items:
        print("没有找到需要向量化的数据项")
        return
    
    # 提取文本列表
    texts = [item["text"] for item in data_items]
    
    # 获取嵌入向量
    print("正在获取嵌入向量...")
    try:
        embeddings = await vector_client.get_embeddings(texts)
        print(f"成功获取 {len(embeddings)} 个嵌入向量")
    except Exception as e:
        print(f"获取嵌入向量失败: {e}")
        return
    
    # 准备插入数据
    insert_data = [
        {
            "id": i + 1,  # ID从1开始递增
            "vector": embedding,
            "text": text,
            "items": f"{item['classification']}|{item['subcategory']}|{item['text']}",
            "classification": item["classification"],
            "subcategory": item["subcategory"],
            "source_line": item["source_line"]
        }
        for i, (embedding, text, item) in enumerate(zip(embeddings, texts, data_items))
    ]
    
    # 插入数据到向量数据库
    print("正在插入数据到 element_vector 集合...")
    try:
        result = vector_client.insert_data(insert_data)
        print(f"成功插入 {len(result['ids'])} 条记录到 element_vector 集合")
    except Exception as e:
        print(f"插入数据到向量数据库失败: {e}")
        return
    
    print("=== 向量化标准数据完成 ===")


async def main(specification_uid: str = None):
    """主函数"""
    await vectorize_standard_data(specification_uid)


if __name__ == "__main__":
    asyncio.run(main())