#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
统一文件处理器
用于根据文件类型调用相应的处理器进行处理
"""

import logging
from pathlib import Path
from typing import Union

# 配置日志
logger = logging.getLogger(__name__)

# 导入各种处理器
from app.processors.txt_processor import convert_txt_to_md
from app.processors.word_processor import convert_word_to_pdf
from app.processors.pdf_processor import extract_text_from_pdf
from app.processors.csv_processor import convert_csv_to_excel
from app.processors.excel_processor import process_excel_to_markdown


def process_file(file_path: Union[str, Path], output_dir: Union[str, Path] = None) -> str:
    """
    根据文件类型调用相应的处理器进行处理

    参数:
        file_path (Union[str, Path]): 文件路径
        output_dir (Union[str, Path], optional): 输出目录路径，默认为None表示与源文件同目录

    返回:
        str: 生成的MD文件路径

    异常:
        FileNotFoundError: 如果文件不存在
        ValueError: 如果文件类型不支持
        RuntimeError: 如果处理失败
    """
    file = Path(file_path)
    
    # 检查文件是否存在
    if not file.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    # 确定输出目录
    if output_dir is None:
        output_dir = file.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # 根据文件扩展名选择处理方式
    suffix = file.suffix.lower()
    
    try:
        if suffix == '.txt':
            # TXT文件直接转为MD格式
            md_file_path = convert_txt_to_md(file_path, output_dir)
            logger.info(f"已处理TXT文件: {file_path} -> {md_file_path}")
            return md_file_path
            
        elif suffix in ['.doc', '.docx']:
            # Word文件先转为PDF，再使用MinerU处理得到MD
            pdf_file_path = convert_word_to_pdf(file_path, output_dir)
            # 使用MinerU处理PDF得到MD内容
            md_content = extract_text_from_pdf(pdf_file_path)
            # 保存为MD文件
            md_file_path = output_dir / f"{file.stem}.md"
            with open(md_file_path, 'w', encoding='utf-8') as f:
                f.write(md_content)
            logger.info(f"已处理Word文件: {file_path} -> {md_file_path}")
            return str(md_file_path)
            
        elif suffix == '.pdf':
            # PDF文件直接使用MinerU处理得到MD
            md_content = extract_text_from_pdf(file_path)
            # 保存为MD文件
            md_file_path = output_dir / f"{file.stem}.md"
            with open(md_file_path, 'w', encoding='utf-8') as f:
                f.write(md_content)
            logger.info(f"已处理PDF文件: {file_path} -> {md_file_path}")
            return str(md_file_path)
            
        elif suffix == '.csv':
            # CSV文件先转为Excel，再处理为MD
            excel_file_path = convert_csv_to_excel(file_path)
            md_file_path = process_excel_to_markdown(excel_file_path, output_dir / f"{file.stem}.md")
            logger.info(f"已处理CSV文件: {file_path} -> {md_file_path}")
            return str(md_file_path)
            
        elif suffix in ['.xls', '.xlsx']:
            # Excel文件直接处理为MD
            md_file_path = process_excel_to_markdown(file_path, output_dir / f"{file.stem}.md")
            logger.info(f"已处理Excel文件: {file_path} -> {md_file_path}")
            return str(md_file_path)
            
        else:
            # 不支持的文件类型
            raise ValueError(f"不支持的文件类型: {suffix}")
            
    except Exception as e:
        logger.error(f"处理文件 {file_path} 时出错: {e}", exc_info=True)
        raise RuntimeError(f"文件处理失败: {str(e)}")


def batch_process_files(file_paths: list, output_dir: Union[str, Path] = None) -> dict:
    """
    批量处理文件

    参数:
        file_paths (list): 文件路径列表
        output_dir (Union[str, Path], optional): 输出目录路径

    返回:
        dict: 处理结果字典，包含成功和失败的文件信息
    """
    results = {
        "success": [],
        "failed": []
    }
    
    for file_path in file_paths:
        try:
            md_path = process_file(file_path, output_dir)
            results["success"].append({
                "source": file_path,
                "output": md_path
            })
        except Exception as e:
            results["failed"].append({
                "source": file_path,
                "error": str(e)
            })
            logger.error(f"批量处理文件 {file_path} 失败: {e}", exc_info=True)
    
    return results