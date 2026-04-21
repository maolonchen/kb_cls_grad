#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
分块任务提示词
用于定义文档分块相关的提示词模板
"""

from typing import Dict, Any


class ChunkingPrompts:
    """文档分块任务的提示词"""
    
    CHUNKING_PROMPT = """角色：你是一名专业的AI数据处理助手，专门负责为RAG系统进行高质量的文档预处理和分块。

任务：请严格根据以下规则，对用户提供的文档内容进行分块。

一、处理规则（Markdown格式）
- 操作：
·必须提取带"#"的完整标题路径，并用英文逗号连接。
·必须将数据区内的每一条数据作为一个独立的块。
·必须将标题路径与每条数据记录拼接成最终块。
·数据格式可能是JSONL、JSON数组或其他形式，必须将其拆分为单条记录。

二、输出格式：每个块单独一行，格式为：标题路径,数据记录。禁止任何额外输出。

核心示例：
输入：

# 通信业类

## 公共资源类数据

json

{"column0": "xx县办公楼（超级基站）一楼无线机房/ZHJJ02-09-A-DDF01"}

{"column0": "枢纽9楼传输机房A-3-xx"}

输出：
通信业类,公共资源类数据,{"column0": "xx县办公楼（超级基站）一楼无线机房/ZHJJ02-09-A-DDF01"}
通信业类,公共资源类数据,{"column0": "枢纽9楼传输机房A-3-xx"}
请严格遵循以上规则和格式进行处理。

三、注意
1.每条数据都要保留原有的层级标题作为块的元数据或标题。
2.只回答分块的内容，不要回答其他的解释性描述。

开始："""

    @classmethod
    def get_chunking_prompt(cls, md_content: str) -> str:
        """
        获取文档分块提示词
        
        Args:
            md_content (str): Markdown文档内容
            
        Returns:
            str: 格式化的提示词
        """
        return cls.CHUNKING_PROMPT + "\n" + md_content


# 为了向后兼容，也可以提供函数形式的接口
def get_chunking_prompt(md_content: str) -> str:
    """
    获取文档分块提示词（函数接口）
    
    Args:
        md_content (str): Markdown文档内容
        
    Returns:
        str: 格式化的提示词
    """
    return ChunkingPrompts.get_chunking_prompt(md_content)