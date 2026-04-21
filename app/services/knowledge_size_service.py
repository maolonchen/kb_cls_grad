import os
from pathlib import Path
from typing import Dict, Optional


class KnowledgeSizeService:
    """知识库大小信息服务类"""

    def __init__(self):
        pass

    async def get_knowledge_base_size_info(self, specification_uid: str) -> Optional[Dict]:
        """
        获取知识库大小信息
        统计指定规范ID下的原始文件大小、解析后的知识大小和向量数据大小

        Args:
            specification_uid: 规范UId唯一标识

        Returns:
            dict: 包含各种数据大小信息的字典，如果specification_uid不存在则返回None
        """
        # 检查是否已存在该规范对应的处理结果
        processed_dir = Path(f"data/processed/{specification_uid}_chunks")
        if not processed_dir.exists():
            return None

        # 计算原始文件大小（data/raw 目录下的文件）
        raw_data_size = self._calculate_raw_data_size(specification_uid)

        # 计算知识数据大小（data/processed/{specification_uid}_chunks 目录下的所有.md文件）
        knowledge_data_size = self._calculate_knowledge_data_size(processed_dir)

        # 计算向量数据大小（data/db/milvus_data.db）
        vector_data_size = self._calculate_vector_data_size()

        # 转换为KB单位
        raw_data_size_kb = raw_data_size / 1024.0
        knowledge_data_size_kb = knowledge_data_size / 1024.0
        vector_data_size_kb = vector_data_size / 1024.0

        # 返回结果
        return {
            "rawDataSize": raw_data_size_kb,
            "knowledgeDataSize": knowledge_data_size_kb,
            "vectorDataSize": vector_data_size_kb
        }

    def _calculate_raw_data_size(self, specification_uid: str) -> float:
        """
        计算原始数据大小
        计算data/raw目录下与specification_uid相关的原始文件（*.原始后缀，不含_fix.*和.md文件）
        """
        raw_data_size = 0
        raw_dir = Path("data/raw")
        if raw_dir.exists():
            for file_path in raw_dir.iterdir():
                if file_path.is_file():
                    # 获取文件的扩展名和基本名
                    file_ext = file_path.suffix.lower()
                    file_stem = file_path.stem
                    
                    # 跳过_fix.md文件和普通的.md文件
                    if file_ext == '.md':
                        if file_stem.endswith('_fix'):
                            continue  # 跳过_fix.md文件
                        else:
                            continue  # 跳过普通.md文件
                    elif file_ext in ['.doc', '.docx', '.pdf', '.txt', '.csv', '.xls', '.xlsx']:
                        # 这是原始文件（如pdf, docx等），直接计算大小
                        raw_data_size += file_path.stat().st_size

        return float(raw_data_size)

    def _calculate_knowledge_data_size(self, processed_dir: Path) -> float:
        """
        计算知识数据大小
        计算指定目录下所有.md文件的大小
        """
        knowledge_data_size = 0
        for file_path in processed_dir.rglob("*.md"):  # 使用rglob递归搜索所有子目录中的.md文件
            if file_path.is_file():
                knowledge_data_size += file_path.stat().st_size
        return float(knowledge_data_size)

    def _calculate_vector_data_size(self) -> float:
        """
        计算向量数据大小
        """
        vector_data_size = 0
        milvus_db_path = Path("data/db/milvus_data.db")
        if milvus_db_path.exists():
            vector_data_size = milvus_db_path.stat().st_size
        return float(vector_data_size)


# 创建全局实例
knowledge_size_service = KnowledgeSizeService()