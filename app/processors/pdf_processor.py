#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PDF文件处理器
用于处理PDF文件，结合MinerU服务进行文档解析
"""

import logging
import requests
import json
import re
from pathlib import Path
from typing import Union, Dict, Any, Optional
from app.core.config import MinerUConfig
from bs4 import BeautifulSoup

# 配置日志
logger = logging.getLogger(__name__)

def parse_pdf_with_mineru(pdf_file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    使用MinerU服务解析PDF文件

    参数:
        pdf_file_path (Union[str, Path]): PDF文件路径

    返回:
        Dict[str, Any]: 解析结果

    异常:
        FileNotFoundError: 如果PDF文件不存在
        RuntimeError: 如果解析失败
    """
    pdf_file = Path(pdf_file_path)
    
    # 检查文件是否存在
    if not pdf_file.exists():
        raise FileNotFoundError(f"PDF文件不存在: {pdf_file_path}")
    
    # 检查文件扩展名
    if pdf_file.suffix.lower() != '.pdf':
        raise ValueError(f"不支持的文件格式: {pdf_file.suffix}，仅支持.pdf文件")
    
    try:
        # 准备请求数据
        request_data = MinerUConfig.get_request_data()
        # 移除request_data中的output_dir，因为这可能不是通过表单数据传递的
        request_data.pop("output_dir", None)
        
        # 准备文件数据
        files = {
            'files': (pdf_file.name, open(pdf_file, 'rb'), 'application/pdf')
        }
        
        # 发送POST请求到MinerU服务
        logger.debug(f"向MinerU服务发送请求: {MinerUConfig.url}")
        response = requests.post(
            url=MinerUConfig.url,
            files=files,
            data=request_data,
            timeout=2000  # 7分钟超时
        )
        
        # 关闭文件
        files['files'][1].close()
        
        # 检查响应状态
        if response.status_code != 200:
            raise RuntimeError(f"MinerU服务返回错误状态码: {response.status_code}, 响应: {response.text}")
        
        # 解析响应数据
        result = response.json()
        logger.info(f"成功解析PDF文件: {pdf_file_path}")
        return result
        
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"请求MinerU服务时发生网络错误: {str(e)}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"解析MinerU服务响应时发生JSON解码错误: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"解析PDF文件时发生未知错误: {str(e)}")


def expand_merged_cells(html_content):
    """
    展开HTML表格中的合并单元格
    """
    # 解析HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')
    
    if not table:
        return []
    
    # 提取表格数据到二维列表
    data = []
    for row in table.find_all('tr'):
        row_data = []
        for cell in row.find_all(['td', 'th']):
            rowspan = int(cell.get('rowspan', 1))
            colspan = int(cell.get('colspan', 1))
            text = cell.get_text(strip=True)
            row_data.append((text, rowspan, colspan))
        data.append(row_data)
    
    if not data:
        return []
    
    # 创建网格填充器
    max_cols = max((sum(cell[2] for cell in row) for row in data), default=0)
    if max_cols == 0:
        return []
        
    grid = [[''] * max_cols for _ in range(len(data))]
    
    # 填充网格
    for i, row in enumerate(data):
        col_idx = 0
        for cell in row:
            text, rowspan, colspan = cell
            # 跳过已填充的列
            while col_idx < max_cols and grid[i][col_idx]:
                col_idx += 1
            # 填充主单元格和合并区域
            for r in range(i, min(i + rowspan, len(grid))):  # 防止超出范围
                for c in range(col_idx, min(col_idx + colspan, max_cols)):
                    grid[r][c] = text
            col_idx += colspan
    
    return grid


def build_tree_from_grid(grid):
    """
    从网格数据构建树形结构
    返回一个包含所有行数据的列表
    """
    if not grid or len(grid) < 1:
        return []
    
    # 获取表头列名（第一行）
    header_names = [cell.strip() for cell in grid[0]]
    
    # 收集所有行数据
    rows_data = []
    
    # 处理数据行（从第二行开始）
    for row in grid[1:]:
        # 跳过空行
        if not any(cell.strip() for cell in row):
            continue
        
        # 构建当前行的数据字典
        row_data = {}
        for i, header in enumerate(header_names):
            if i < len(row):
                row_data[header] = row[i].strip()
            else:
                row_data[header] = ""
        
        rows_data.append(row_data)
    
    return rows_data


def clean_data(data):
    """
    递归清理数据中的空格：
    - 所有键的空格去除；
    - 所有字符串值的空格去除；
    - 支持嵌套字典和列表。
    """
    if isinstance(data, dict):
        return {
            key.replace(" ", ""): clean_data(value)
            for key, value in data.items()
        }
    elif isinstance(data, list):
        return [clean_data(value) for value in data]
    elif isinstance(data, str):
        return data.replace(" ", "")
    else:
        return data


def html_to_jsonl(html_content):
    """
    将HTML表格转换为JSONL格式
    """
    try:
        # 处理表格数据
        grid = expand_merged_cells(html_content)
        
        if not grid:
            return None
            
        # 构建树形结构
        rows_data = build_tree_from_grid(grid)
        
        if not rows_data:
            return None
            
        # 转换为JSONL格式
        jsonl_lines = []
        for row in rows_data:
            # 清理整个树结构中的空格
            cleaned_row = clean_data(row)
            # 转换为JSON行
            jsonl_lines.append(json.dumps(cleaned_row, ensure_ascii=False))
        
        # 用换行符连接所有行
        return '\n'.join(jsonl_lines)
    except Exception as e:
        logger.error(f"转换HTML到JSONL时出错: {str(e)}")
        return None


def extract_and_merge_html_divs(content):
    """
    从markdown内容中提取并合并HTML块（包括table、div和html元素）。
    连续的HTML块如果之间只有空白字符则会被合并。
    返回合并后的块和它们在原始文件中的位置信息。
    """
    # 查找所有HTML块（包括table、div和html元素）
    html_pattern = r'(<(table|div|html)[^>]*>.*?</\2>)'
    html_blocks = re.findall(html_pattern, content, re.DOTALL)
    
    # 提取完整的HTML块（包含开始和结束标签）
    full_html_blocks = [block[0] for block in html_blocks]
    
    # 查找所有匹配项及其位置
    html_matches = list(re.finditer(html_pattern, content, re.DOTALL))
    
    merged_blocks = []
    merged_positions = []
    i = 0
    
    while i < len(html_matches):
        current_block = html_matches[i].group(1)  # 获取完整匹配（第一个捕获组）
        current_start = html_matches[i].start()
        current_end = html_matches[i].end()
        
        # 记录第一个HTML块的起始位置
        merge_start = current_start
        
        # 检查是否有更多HTML块可能需要合并
        j = i + 1
        while j < len(html_matches):
            next_start = html_matches[j].start()
            # 检查当前HTML块和下一个HTML块之间是否只有空白字符
            between_text = content[current_end:next_start]
            if re.search(r'\S', between_text):  # 如果有非空白字符内容
                break
            
            # 用换行符分隔符合合并的块
            current_block += '\n' + html_matches[j].group(1)
            current_end = html_matches[j].end()
            j += 1
        
        # 记录合并块的结束位置
        merge_end = current_end
        
        merged_blocks.append(current_block)
        merged_positions.append((merge_start, merge_end))
        i = j if j > i + 1 else i + 1
    
    return content, merged_blocks, merged_positions


def process_html_tables_in_markdown(markdown_content: str) -> str:
    """
    处理Markdown内容中的HTML表格，将其转换为JSONL格式
    
    参数:
        markdown_content (str): 包含HTML表格的Markdown内容
        
    返回:
        str: 处理后的Markdown内容，其中HTML表格已被替换为JSONL格式
    """
    try:
        # 提取并合并HTML块
        content, html_blocks, positions = extract_and_merge_html_divs(markdown_content)
        
        if not html_blocks:
            return markdown_content
            
        # 从后往前替换，避免位置偏移
        result_content = content
        for i in range(len(positions) - 1, -1, -1):
            start, end = positions[i]
            html_block = html_blocks[i]
            
            # 将HTML块转换为JSONL
            jsonl_content = html_to_jsonl(html_block)
            
            if jsonl_content:
                # 替换原HTML块为JSONL内容
                result_content = result_content[:start] + jsonl_content + result_content[end:]
                
        return result_content
    except Exception as e:
        logger.error(f"处理Markdown中的HTML表格时出错: {str(e)}")
        # 出错时返回原始内容
        return markdown_content


def extract_text_from_pdf(pdf_file_path: Union[str, Path]) -> str:
    """
    从PDF文件中提取文本内容

    参数:
        pdf_file_path (Union[str, Path]): PDF文件路径

    返回:
        str: 提取的文本内容
    """
    try:
        result = parse_pdf_with_mineru(pdf_file_path)
        
        # 从结果中提取文本内容
        # 根据MinerU的返回格式调整这部分逻辑
        if isinstance(result, dict) and "results" in result:
            # 获取第一个键的值，通常是文件名
            first_key = next(iter(result["results"]))
            if "md_content" in result["results"][first_key]:
                md_content = result["results"][first_key]["md_content"]
                # 处理HTML表格
                processed_content = process_html_tables_in_markdown(md_content)
                return processed_content
        elif "markdown" in result:
            md_content = result["markdown"]
            # 处理HTML表格
            processed_content = process_html_tables_in_markdown(md_content)
            return processed_content
        elif "content" in result:
            md_content = result["content"]
            # 处理HTML表格
            processed_content = process_html_tables_in_markdown(md_content)
            return processed_content
        else:
            # 如果没有预定义的字段，返回整个结果的字符串表示
            return json.dumps(result, ensure_ascii=False, indent=2)
            
    except Exception as e:
        logger.error(f"从PDF提取文本时发生错误: {str(e)}")
        raise


def extract_metadata_from_pdf(pdf_file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    从PDF文件中提取元数据

    参数:
        pdf_file_path (Union[str, Path]): PDF文件路径

    返回:
        Dict[str, Any]: 提取的元数据
    """
    try:
        result = parse_pdf_with_mineru(pdf_file_path)
        
        # 提取元数据
        metadata_keys = ["title", "author", "subject", "creator", "producer", "creation_date", "modification_date"]
        metadata = {}
        
        for key in metadata_keys:
            if key in result:
                metadata[key] = result[key]
        
        return metadata
        
    except Exception as e:
        logger.error(f"从PDF提取元数据时发生错误: {str(e)}")
        raise