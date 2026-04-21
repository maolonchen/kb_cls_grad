#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
知识预处理服务类
处理知识库文档预处理的核心业务逻辑
"""

import asyncio
import logging
import uuid
from pathlib import Path
from typing import List, Optional

from app.processors.file_processor import process_file
from app.processors.md_fix_processor import extract_and_filter_toc, process_markdown_file
from app.algorithms.classification import classify_document_by_llm
from app.algorithms.vectorization.chunker import MarkdownChunker, NarrativeMarkdownChunker

logger = logging.getLogger(__name__)


class KnowledgePreprocessingService:
    """知识预处理服务类"""
    
    def __init__(self):
        """初始化服务类"""
        self.upload_dir = Path("data/raw")
        self.upload_dir.mkdir(parents=True, exist_ok=True)
    
    async def process_file_async(self, file_path, file_filename, specification_uid, file_classification=None):
        """
        异步处理单个文件的完整流程
        
        Args:
            file_path: 文件路径
            file_filename: 文件名
            specification_uid: 规范UID
            file_classification: 文件分类
        """
        try:
            # 处理流程1: 文件转MD格式
            md_file_path = process_file(file_path)
            
            # 处理流程2: 修复MD文件（完整处理流程，生成_fix.md文件）
            process_markdown_file(md_file_path)
            fixed_md_file_path = md_file_path.replace('.md', '_fix.md')
            
            # 读取修复后的MD文件内容
            with open(fixed_md_file_path, 'r', encoding='utf-8') as f:
                md_content = f.read()
            
            # 为每个文件生成唯一的文档ID，确保分类结果能够正确区分
            # 使用原始文件名（不含扩展名）+ UUID 作为唯一标识
            file_stem = Path(file_filename).stem
            unique_doc_id = f"{specification_uid}_{file_stem}_{uuid.uuid4().hex[:8]}"
            
            # 根据是否有fileClassification决定处理流程
            if not file_classification:
                # 没有指定分类，使用NarrativeMarkdownChunker进行分块（连贯文本文档处理方式）
                chunker = NarrativeMarkdownChunker()
                chunks = chunker.chunk_document(md_content, unique_doc_id)
                
                # 保存到general_knowledge目录
                self.save_chunks_to_file(chunks, "general_knowledge", file_filename, specification_uid)
            else:
                # 指定了分类，需要进行分类处理
                # 使用整个file_classification来构建多级文件夹，不同级的文件夹用"|"隔开
                category = file_classification
                
                # 处理流程3: 对修复的MD进行分类
                classification_result = classify_document_by_llm(md_content, unique_doc_id)
                
                # 根据分类结果选择不同的分块方式
                doc_class = classification_result.get("class", "1")  # 默认为1
                print(">>> 分类结果:", classification_result, "\n", ">>> 分类数字:", doc_class)
                
                if doc_class == "2":
                    # 类别1：使用MarkdownChunker进行分块
                    chunker = MarkdownChunker()
                    chunks = chunker.chunk_document_with_llm(md_content, unique_doc_id)
                elif doc_class == "1":
                    # 类别2：使用NarrativeMarkdownChunker进行分块
                    chunker = NarrativeMarkdownChunker()
                    chunks = chunker.chunk_document(md_content, unique_doc_id)
                else:
                    # 其他情况默认使用MarkdownChunker
                    chunker = MarkdownChunker()
                    chunks = chunker.chunk_document_with_llm(md_content, unique_doc_id)
                # 保存到指定类别目录
                self.save_chunks_to_file(chunks, category, file_filename, specification_uid)
            
            logger.info(f"文件 {file_path} 处理完成")
            
        except Exception as e:
            logger.error(f"异步处理文件 {file_path} 时出错: {str(e)}")
            raise
    
    def save_chunks_to_file(self, chunks, category, filename, specification_uid=None):
        """
        将分块结果保存到文件中，每行一个块
        
        Args:
            chunks (List[str]): 分块结果列表
            category (str): 分类目录名
            filename (str): 原始文件名
            specification_uid (str, optional): 行业ID，用于创建行业特定目录
        """
        # 确保目录存在，支持多级目录结构
        if specification_uid:
            specification_uid = specification_uid.replace("-", "_")
            chunks_dir = Path(f"data/processed/{specification_uid}_chunks")
        else:
            chunks_dir = Path("data/processed/chunks")
        
        # 将category按"|"分割并创建多级目录
        category_parts = category.split("|")
        for part in category_parts:
            chunks_dir = chunks_dir / part
            
        chunks_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成目标文件路径
        target_file = chunks_dir / f"{Path(filename).stem}.md"
        
        # 写入文件，每行一个块
        with open(target_file, 'w', encoding='utf-8') as f:
            for chunk in chunks:
                f.write(chunk + '\n')
        
        logger.info(f"已将 {len(chunks)} 个块保存到 {target_file}")

# 创建服务实例
knowledge_preprocessing_service = KnowledgePreprocessingService()