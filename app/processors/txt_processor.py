#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TXT文件处理器
用于将.txt文件转换为.md文件格式
"""

import logging
from pathlib import Path
from typing import Union

# 配置日志
logger = logging.getLogger(__name__)


def convert_txt_to_md(txt_file_path: Union[str, Path], output_dir: Union[str, Path] = None) -> str:
    """
    将单个TXT文件转换为MD格式文件

    参数:
        txt_file_path (Union[str, Path]): TXT文件路径
        output_dir (Union[str, Path], optional): 输出目录路径，默认为None表示与源文件同目录

    返回:
        str: 生成的MD文件路径

    异常:
        FileNotFoundError: 如果TXT文件不存在
        RuntimeError: 如果转换失败
    """
    txt_file = Path(txt_file_path)
    
    # 检查文件是否存在
    if not txt_file.exists():
        raise FileNotFoundError(f"TXT文件不存在: {txt_file_path}")
    
    # 检查文件扩展名
    if txt_file.suffix.lower() != '.txt':
        raise ValueError(f"不支持的文件格式: {txt_file.suffix}，仅支持.txt文件")
    
    # 确定输出目录
    if output_dir is None:
        output_dir = txt_file.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # 读取TXT文件内容
        # 尝试多种编码格式
        encodings = ['utf-8', 'gbk', 'latin-1']
        content = None
        for encoding in encodings:
            try:
                with open(txt_file, 'r', encoding=encoding) as f:
                    content = f.read()
                logger.debug(f"使用编码 '{encoding}' 成功读取 {txt_file}")
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            raise RuntimeError(f"无法使用常见编码读取文件: {txt_file}")
        
        # 构造MD文件路径
        md_file_path = output_dir / f"{txt_file.stem}.md"
        
        # 写入MD文件
        with open(md_file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"已转换: {txt_file} -> {md_file_path}")
        return str(md_file_path)
        
    except Exception as e:
        logger.error(f"转换文件 {txt_file} 时出错: {e}", exc_info=True)
        raise RuntimeError(f"转换失败: {str(e)}")


def batch_convert_txt_to_md(txt_files: list, output_dir: Union[str, Path] = None) -> dict:
    """
    批量将TXT文件转换为MD格式

    参数:
        txt_files (list): TXT文件路径列表
        output_dir (Union[str, Path], optional): 输出目录路径

    返回:
        dict: 转换结果字典，包含成功和失败的文件信息
    """
    results = {
        "success": [],
        "failed": []
    }
    
    for txt_file in txt_files:
        try:
            md_path = convert_txt_to_md(txt_file, output_dir)
            results["success"].append({
                "source": txt_file,
                "output": md_path
            })
        except Exception as e:
            results["failed"].append({
                "source": txt_file,
                "error": str(e)
            })
            logger.error(f"转换失败 {txt_file}: {str(e)}")
    
    return results


def add_markdown_formatting(txt_file_path: Union[str, Path], output_dir: Union[str, Path] = None) -> str:
    """
    将TXT文件转换为带有基本Markdown格式的MD文件

    参数:
        txt_file_path (Union[str, Path]): TXT文件路径
        output_dir (Union[str, Path], optional): 输出目录路径，默认为None表示与源文件同目录

    返回:
        str: 生成的MD文件路径
    """
    txt_file = Path(txt_file_path)
    
    # 检查文件是否存在
    if not txt_file.exists():
        raise FileNotFoundError(f"TXT文件不存在: {txt_file_path}")
    
    # 检查文件扩展名
    if txt_file.suffix.lower() != '.txt':
        raise ValueError(f"不支持的文件格式: {txt_file.suffix}，仅支持.txt文件")
    
    # 确定输出目录
    if output_dir is None:
        output_dir = txt_file.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # 读取TXT文件内容
        encodings = ['utf-8', 'gbk', 'latin-1']
        content = None
        for encoding in encodings:
            try:
                with open(txt_file, 'r', encoding=encoding) as f:
                    content = f.read()
                logger.debug(f"使用编码 '{encoding}' 成功读取 {txt_file}")
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            raise RuntimeError(f"无法使用常见编码读取文件: {txt_file}")
        
        # 添加基本的Markdown格式
        # 将连续的换行符替换为段落分隔
        lines = content.splitlines()
        formatted_lines = []
        
        for line in lines:
            # 空行表示段落分隔
            if not line.strip():
                formatted_lines.append("")
            else:
                # 普通文本行
                formatted_lines.append(line)
        
        # 重新组合内容
        formatted_content = "\n".join(formatted_lines)
        
        # 添加标题（使用文件名作为标题）
        title = txt_file.stem
        markdown_content = f"# {title}\n\n{formatted_content}"
        
        # 构造MD文件路径
        md_file_path = output_dir / f"{txt_file.stem}.md"
        
        # 写入MD文件
        with open(md_file_path, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        logger.info(f"已转换并添加格式: {txt_file} -> {md_file_path}")
        return str(md_file_path)
        
    except Exception as e:
        logger.error(f"转换文件 {txt_file} 时出错: {e}", exc_info=True)
        raise RuntimeError(f"转换失败: {str(e)}")