#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Word文件处理器
用于将.doc和.docx文件转换为PDF格式
"""

import logging
import subprocess
import os
from pathlib import Path
from typing import Union

# 配置日志
logger = logging.getLogger(__name__)


def convert_word_to_pdf(word_file_path: Union[str, Path], output_dir: Union[str, Path] = None) -> str:
    """
    使用LibreOffice将Word文件(.doc/.docx)转换为PDF格式

    参数:
        word_file_path (Union[str, Path]): Word文件路径
        output_dir (Union[str, Path], optional): 输出目录路径，默认为None表示与源文件同目录

    返回:
        str: 生成的PDF文件路径

    异常:
        FileNotFoundError: 如果Word文件不存在
        RuntimeError: 如果转换失败
    """
    word_file = Path(word_file_path)
    
    # 检查文件是否存在
    if not word_file.exists():
        raise FileNotFoundError(f"Word文件不存在: {word_file_path}")
    
    # 检查文件扩展名
    if word_file.suffix.lower() not in ['.doc', '.docx']:
        raise ValueError(f"不支持的文件格式: {word_file.suffix}，仅支持.doc和.docx文件")
    
    # 确定输出目录
    if output_dir is None:
        output_dir = word_file.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # 使用LibreOffice进行转换
        cmd = [
            'libreoffice',
            '--headless',
            '--convert-to',
            'pdf',
            '--outdir',
            str(output_dir),
            str(word_file)
        ]
        
        logger.debug(f"执行命令: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=2000  # 5分钟超时
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"LibreOffice转换失败: {result.stderr}")
        
        # 确定生成的PDF文件路径
        pdf_file_path = output_dir / f"{word_file.stem}.pdf"
        
        if not pdf_file_path.exists():
            raise RuntimeError("PDF文件未生成，请检查LibreOffice是否正确安装")
        
        logger.info(f"成功转换: {word_file} -> {pdf_file_path}")
        return str(pdf_file_path)
        
    except subprocess.TimeoutExpired:
        raise RuntimeError("LibreOffice转换超时")
    except Exception as e:
        raise RuntimeError(f"转换过程中发生错误: {str(e)}")


def batch_convert_word_to_pdf(word_files: list, output_dir: Union[str, Path] = None) -> dict:
    """
    批量将Word文件转换为PDF格式

    参数:
        word_files (list): Word文件路径列表
        output_dir (Union[str, Path], optional): 输出目录路径

    返回:
        dict: 转换结果字典，包含成功和失败的文件信息
    """
    results = {
        "success": [],
        "failed": []
    }
    
    for word_file in word_files:
        try:
            pdf_path = convert_word_to_pdf(word_file, output_dir)
            results["success"].append({
                "source": word_file,
                "output": pdf_path
            })
        except Exception as e:
            results["failed"].append({
                "source": word_file,
                "error": str(e)
            })
            logger.error(f"转换失败 {word_file}: {str(e)}")
    
    return results