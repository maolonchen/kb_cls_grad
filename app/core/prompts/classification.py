#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
核心提示词模板
用于定义各种AI任务的提示词
"""

from typing import Dict, Any


class ClassificationPrompts:
    """文档分类任务的提示词"""
    
    @staticmethod
    def get_document_classification_prompt(md_content: str) -> str:
        """
        获取文档分类提示词
        
        Args:
            md_content (str): Markdown文档内容
            
        Returns:
            str: 格式化的提示词
        """
        return f"""
类型1：连贯文本文档
- 特征：篇幅大，整篇文档由连贯的段落、章节组成，具有逻辑叙事性（如文章、报告、说明等），可能包含多种类型的数据。
类型2：数据形文档
- 特征：篇幅短，主要是某行业信息数据，几乎无逻辑文章描述。

· 回复要求：
- 其中1代表连贯文本文档，2代表数据形文档。
- 仅回复一个JSON格式的字符串，不要回答其他解释性内容。

· 请以以下JSON格式返回结果：
{{"class": "1"}} 或 {{"class": "2"}}

请根据以上特征判断规则，以下文档属于哪个json，开始：

{md_content}
"""


# 向后兼容，提供函数形式的接口
def get_document_classification_prompt(md_content: str) -> str:
    """
    获取文档分类提示词（函数接口）
    
    Args:
        md_content (str): Markdown文档内容
        
    Returns:
        str: 格式化的提示词
    """
    return ClassificationPrompts.get_document_classification_prompt(md_content)