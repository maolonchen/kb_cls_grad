#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MD文件修复处理器
用于移除目录行，修复并格式化不同等级的markdown标题
"""

import re
import os
from difflib import SequenceMatcher
from pathlib import Path
from typing import List, Union
import logging

# 配置日志
logger = logging.getLogger(__name__)

def extract_and_filter_toc(file_path):
    """
    从markdown文件中移除目录行 - 使用新的基于相邻行规则的方法
    
    Args:
        file_path (str): 需要处理的Markdown文件路径
        
    Returns:
        str: 清理后的文件路径
    """
    # 使用新方法处理TOC行
    cleaned_file_path = file_path.replace('.md', '_cleaned.md')
    remove_toc_by_neighbors_strip_blanks(file_path, cleaned_file_path)
    return cleaned_file_path

def normalize_for_check(line: str) -> str:
    """
    标准化行内容以便检查
    
    Args:
        line (str): 原始行内容
        
    Returns:
        str: 去除空白字符后的行内容
    """
    return re.sub(r'\s+', '', line)

def has_two_dots_before_number(norm: str) -> bool:
    """
    检查标准化后的行是否在数字前包含两个或更多点
    
    Args:
        norm (str): 标准化后的行内容
        
    Returns:
        bool: 如果匹配返回True，否则返回False
    """
    return bool(re.search(r'([\.．…]{2,})(\d+)$', norm))

def ends_with_number(norm: str) -> bool:
    """
    检查标准化后的行是否以数字结尾
    
    Args:
        norm (str): 标准化后的行内容
        
    Returns:
        bool: 如果以数字结尾返回True，否则返回False
    """
    return bool(re.search(r'\d+$', norm))

def mark_toc_lines(lines):
    """
    标记目录行
    
    Args:
        lines (list): 文件行内容列表
        
    Returns:
        list: 布尔值列表，标识每行是否为目录行
    """
    n = len(lines)
    condition2 = [False]*n
    norms = [normalize_for_check(l) for l in lines]
    for i, norm in enumerate(norms):
        if not norm:
            continue
        if ends_with_number(norm) and has_two_dots_before_number(norm):
            condition2[i] = True
    is_toc = [False]*n
    for i in range(n):
        if not condition2[i]:
            continue
        if (i-1 >= 0 and condition2[i-1]) or (i+1 < n and condition2[i+1]):
            is_toc[i] = True
    return is_toc

def remove_toc_by_neighbors_strip_blanks(in_path, out_path):
    """
    移除相邻的目录行并去除空白行
    
    Args:
        in_path (str): 输入文件路径
        out_path (str): 输出文件路径
    """
    with open(in_path, 'r', encoding='utf-8') as f:
        raw_lines = f.read().splitlines()
    # 1) 先去掉所有空白行（只含空白字符的行）
    compact_lines = [ln for ln in raw_lines if ln.strip() != '']
    # 2) 用紧缩后的列表做判定
    is_toc = mark_toc_lines(compact_lines)
    # 3) 生成输出：跳过被标记为目录的行；输出中不会包含原文件的空白行
    out_lines = [ln for ln, flag in zip(compact_lines, is_toc) if not flag]
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(out_lines))
    removed = sum(1 for flag in is_toc if flag)
    logger.info(f'Processed {in_path} -> {out_path}. Removed {removed} TOC lines; blank lines were removed before processing.')
    
def calculate_similarity(line1, line2):
    """
    计算两个字符串之间的相似度
    
    Args:
        line1 (str): 第一个字符串
        line2 (str): 第二个字符串
        
    Returns:
        float: 相似度值，范围在0到1之间
    """
    return SequenceMatcher(None, line1.strip(), line2.strip()).ratio()

def clean_markdown(file_path, toc_lines):
    """
    从markdown文件中移除与toc_lines最相似的行
    并将结果保存到cleaned.md
    
    Args:
        file_path (str): Markdown文件路径
        toc_lines (list): 目录行列表
    """
    # 读取原始文件内容
    with open(file_path, 'r', encoding='utf-8') as file:
        original_lines = file.readlines()
    
    # 创建标记数组来指示保留哪些行
    keep_lines = [True] * len(original_lines)
    
    # 对于每个TOC行，找到最相似的原始行并标记为移除
    for toc_line in toc_lines:
        best_match_index = -1
        best_similarity = 0
        
        # 遍历所有原始行以找到最佳匹配
        for i, original_line in enumerate(original_lines):
            if keep_lines[i]:  # 只考虑尚未标记为移除的行
                similarity = calculate_similarity(toc_line, original_line)
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match_index = i
        
        # 如果找到足够相似的行（相似度阈值设为0.8），则标记为移除
        if best_match_index != -1 and best_similarity > 0.8:
            keep_lines[best_match_index] = False
            logger.info(f"移除行（相似度: {best_similarity:.2f}）: {original_lines[best_match_index].strip()}")
    
    # 将保留的行写入新文件，移除所有空白行
    cleaned_file_path = file_path.replace('.md', '_cleaned.md')
    with open(cleaned_file_path, 'w', encoding='utf-8') as file:
        for i, line in enumerate(original_lines):
            if keep_lines[i]:
                # 只写入非空白行
                if line.strip():
                    file.write(line)
    
    logger.info(f"清理后的markdown保存到: {cleaned_file_path}")

def fix_markdown_headings_two_pass(file_path):
    """
    两遍方法修复markdown标题:
    1. 应用现有逻辑修复基本标题结构
    2. 处理正确编号的父标题之间的中间标题
    
    Args:
        file_path (str): 需要修复的Markdown文件路径
        
    Returns:
        str: 修复后的文件路径
    """
    
    # 第一遍: 应用现有逻辑
    first_pass_content = fix_markdown_headings_first_pass(file_path)
    
    # 第二遍: 处理中间标题
    second_pass_content = fix_markdown_headings_second_pass(first_pass_content)
    
    # 生成新文件名
    base_name = os.path.splitext(file_path)[0]
    new_file_path = f"{base_name}_fixed_two_pass.md"
    
    # 写入最终结果
    with open(new_file_path, 'w', encoding='utf-8') as file:
        file.write(second_pass_content)
    
    logger.info(f"\n两遍处理完成。最终文件保存为: {new_file_path}")
    return new_file_path

def fix_markdown_headings_first_pass(file_path):
    """
    第一遍: 应用现有标题修复逻辑
    
    Args:
        file_path (str): 需要修复的Markdown文件路径
        
    Returns:
        str: 修复后的内容字符串
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    lines = content.splitlines()
    new_lines = []
    
    # 跟踪变量
    heading_stack = []
    last_major_heading = None
    in_interrupted_section = False
    interrupted_parent = None
    subheading_counter = 1
    expected_next_major = 1  # 预期的下一个主要标题编号
    
    for line in lines:
        match = re.match(r'^(#{1,6})\s+(.*)$', line)
        if match:
            hashes = match.group(1)
            title_text = match.group(2)
            
            number_match = re.match(r'^(\d+(?:\.\d+)*)', title_text)
            if number_match:
                number_str = number_match.group(1)
                number_parts = number_str.split('.')
                
                if len(number_parts) == 1:
                    current_major = int(number_parts[0])
                    
                    # 检查这是否是预期的下一个主要标题
                    if current_major == expected_next_major:
                        # 正常的主要标题
                        last_major_heading = number_str
                        heading_stack = number_parts
                        level = 1
                        in_interrupted_section = False
                        interrupted_parent = None
                        subheading_counter = 1
                        expected_next_major = current_major + 1  # 更新预期的下一个主要标题
                    elif in_interrupted_section and interrupted_parent and current_major < int(last_major_heading):
                        # 只有当当前主要编号小于最后主要标题时才视为子标题
                        # 这意味着它是中断部分的一部分，而不是新的主要标题
                        new_number = f"{interrupted_parent}.{subheading_counter}"
                        subheading_counter += 1
                        
                        # 确定正确的标题级别
                        level = len(interrupted_parent.split('.')) + 1
                        level = max(1, min(6, level))
                        
                        # 生成正确数量的井号
                        correct_hashes = '#' * level
                        
                        # 重建标题行
                        fixed_line = f"{correct_hashes} {new_number} {title_text[len(number_str):].strip()}"
                        new_lines.append(fixed_line)
                        logger.info(f"修复中断标题: '{line}' -> '{fixed_line}'")
                        continue
                    else:
                        # 如果不是预期的下一个主要标题且不在中断部分，则重置状态
                        # 这意味着它是新的主要标题（如13, 14等）
                        last_major_heading = number_str
                        heading_stack = number_parts
                        level = 1
                        in_interrupted_section = False
                        interrupted_parent = None
                        subheading_counter = 1
                        expected_next_major = current_major + 1
                else:
                    # 检查是否属于当前主要标题
                    if last_major_heading and number_parts[0] == last_major_heading:
                        level = len(number_parts)
                        if len(number_parts) <= len(heading_stack):
                            # 同级或更高级标题
                            heading_stack = number_parts
                            in_interrupted_section = False
                            interrupted_parent = None
                            subheading_counter = 1
                        else:
                            # 子标题
                            heading_stack = number_parts
                            in_interrupted_section = False
                            interrupted_parent = None
                            subheading_counter = 1
                    else:
                        # 检查这是否是预期的中断模式（如12.3.2后跟1,2,3，然后是12.3.3）
                        if (last_major_heading and 
                            len(number_parts) == 3 and 
                            number_parts[0] == last_major_heading and
                            len(heading_stack) > 1 and
                            number_parts[1] == heading_stack[1] and
                            int(number_parts[2]) == int(heading_stack[2]) + 1):
                            
                            # 检测到中断结束，回到正常序列
                            in_interrupted_section = False
                            interrupted_parent = None
                            subheading_counter = 1
                            heading_stack = number_parts
                            level = len(number_parts)
                            # 更新预期的下一个主要标题
                            expected_next_major = int(number_parts[0]) + 1
                        else:
                            # 开始中断部分
                            in_interrupted_section = True
                            interrupted_parent = '.'.join(heading_stack)
                            subheading_counter = 1
                            level = len(number_parts)
                            heading_stack = number_parts
                
                # 确保级别在1-6范围内
                level = max(1, min(6, level))
                
                # 生成正确数量的井号
                correct_hashes = '#' * level
                
                # 重建标题行
                fixed_line = f"{correct_hashes} {title_text}"
                new_lines.append(fixed_line)
                logger.info(f"修复: '{line}' -> '{fixed_line}'")
            else:
                # 非编号标题，保持不变
                new_lines.append(line)
        else:
            new_lines.append(line)
    
    return '\n'.join(new_lines)

def fix_markdown_headings_second_pass(content):
    """
    第二遍: 处理父标题之间的中间标题
    
    Args:
        content (str): 第一遍处理后的内容
        
    Returns:
        str: 第二遍处理后的内容
    """
    lines = content.splitlines()
    parsed_headings = []
    
    # 解析所有带行号的标题
    for i, line in enumerate(lines):
        match = re.match(r'^(#{1,6})\s+(.*)$', line)
        if match:
            hashes = match.group(1)
            title_text = match.group(2)
            level = len(hashes)
            
            # 添加所有标题到parsed_headings，无论是否编号
            number_match = re.match(r'^(\d+(?:\.\d+)*)', title_text)
            if number_match:
                number_str = number_match.group(1)
                number_parts = number_str.split('.')
            else:
                # 对于非编号标题，使用None或特殊标记
                number_str = None
                number_parts = []
            
            parsed_headings.append({
                'line_index': i,
                'level': level,
                'number_str': number_str,
                'number_parts': number_parts,
                'title_text': title_text,
                'original_line': line,
                'is_numbered': number_str is not None  # 标记是否为编号标题
            })
    
    # 处理标题以识别父子关系
    new_lines = lines.copy()
    
    for i in range(len(parsed_headings)):
        current = parsed_headings[i]
        
        # 检查这是否是潜在的父标题（如12.3.2）
        # 修改条件: 只处理编号标题
        if current['is_numbered'] and len(current['number_parts']) >= 2:
            # 查找同级的下一个兄弟标题（如12.3.3）
            next_sibling = None
            for j in range(i + 1, len(parsed_headings)):
                potential_sibling = parsed_headings[j]
                # 只比较编号标题
                if (potential_sibling['is_numbered'] and
                    len(potential_sibling['number_parts']) == len(current['number_parts']) and
                    potential_sibling['number_parts'][:-1] == current['number_parts'][:-1] and
                    int(potential_sibling['number_parts'][-1]) == int(current['number_parts'][-1]) + 1):
                    next_sibling = potential_sibling
                    break
            
            # 如果找到下一个兄弟标题，则处理中间标题
            if next_sibling:
                # 找到当前标题和下一个兄弟标题之间的所有标题
                intermediate_headings = []
                for k in range(i + 1, len(parsed_headings)):
                    if parsed_headings[k]['line_index'] < next_sibling['line_index']:
                        intermediate_headings.append(parsed_headings[k])
                    else:
                        break
                
                # 调整中间标题级别使其成为当前标题的子标题
                parent_number = current['number_str']
                parent_level = current['level']
                child_level = min(6, parent_level + 1)
                child_hashes = '#' * child_level
                
                for idx, intermediate in enumerate(intermediate_headings):
                    # 处理简单编号标题（1, 2, 3等）和非编号标题
                    if (intermediate['is_numbered'] and len(intermediate['number_parts']) == 1) or \
                       (not intermediate['is_numbered']):
                        # 对于编号标题，保持现有逻辑
                        if intermediate['is_numbered']:
                            child_number = f"{parent_number}.{idx + 1}"
                            # 从标题文本中移除旧编号
                            old_number = intermediate['number_parts'][0]
                            title_without_number = intermediate['title_text'][len(old_number):].strip()
                            # 创建新标题行
                            new_line = f"{child_hashes} {child_number} {title_without_number}"
                        else:
                            # 对于非编号标题，直接作为子标题处理
                            child_number = f"{parent_number}.{idx + 1}"
                            new_line = f"{child_hashes} {child_number} {intermediate['title_text']}"
                        
                        new_lines[intermediate['line_index']] = new_line
                        logger.info(f"重新归属: '{intermediate['original_line']}' -> '{new_line}'")
    
    return '\n'.join(new_lines)

def rename_final_file(file_path):
    """
    将最终文件重命名为具有"_final_fix.md"后缀并移除中间文件
    
    Args:
        file_path (str): 最终文件路径
        
    Returns:
        str: 重命名后的文件路径
    """
    # 获取目录和基本名称
    directory = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    
    # 创建带有_fix.md后缀的新文件名
    final_file_name = base_name.replace('_fixed_two_pass', '') + '_fix.md'
    final_file_path = os.path.join(directory, final_file_name)
    
    # 重命名文件
    os.rename(file_path, final_file_path)
    logger.info(f"最终文件重命名为: {final_file_path}")
    
    return final_file_path

def remove_intermediate_files(original_file_path, cleaned_file_path):
    """
    移除中间markdown文件
    
    Args:
        original_file_path (str): 原始文件路径
        cleaned_file_path (str): 清理后的文件路径
    """
    # 移除清理后的文件（中间文件）
    if os.path.exists(cleaned_file_path):
        os.remove(cleaned_file_path)
        logger.info(f"移除中间文件: {cleaned_file_path}")

def get_latest_timestamp_folder(base_path: Union[str, Path]) -> Union[Path, None]:
    """
    获取最新的时间戳文件夹
    
    Args:
        base_path: 基础路径
        
    Returns:
        最新的时间戳文件夹路径，如果未找到则返回None
    """
    base_path = Path(base_path)
    if not base_path.exists():
        return None

    timestamp_dirs = []
    for item in base_path.iterdir():
        if item.is_dir():
            parts = item.name.split('_')
            if len(parts) >= 2:
                timestamp_str = parts[-1]
                try:
                    if len(timestamp_str) == 14 and timestamp_str.isdigit():
                        timestamp_dirs.append((item, timestamp_str))
                except ValueError:
                    continue

    if timestamp_dirs:
        timestamp_dirs.sort(key=lambda x: x[1], reverse=True)
        return timestamp_dirs[0][0]
    
    return None

def get_preferred_md_file(files: List[Path]) -> List[Path]:
    """
    从同一基础名称的多个文件中选择首选文件
    
    Args:
        files: 同一基础名称的文件列表
        
    Returns:
        选中的文件列表
    """
    # 优先级顺序：_final.md > _with_html.md > .md
    final_files = [f for f in files if f.name.endswith('_final.md')]
    if final_files:
        return final_files
    
    html_files = [f for f in files if f.name.endswith('_with_html.md')]
    if html_files:
        return html_files
    
    # 如果只有普通.md文件，返回所有
    return [f for f in files if f.name.endswith('.md') and not f.name.endswith(('_final.md', '_with_html.md'))]

def _process_category_folders(base_path: Union[str, Path]) -> List[Path]:
    """
    处理类别文件夹，选择合适的文件

    Args:
        base_path: 基础路径

    Returns:
        选中的文件列表
    """
    try:
        latest_folder = get_latest_timestamp_folder(base_path)
        if not latest_folder:
            logger.warning("未找到时间戳文件夹")
            return []
    except Exception as e:
        logger.error(f"获取最新时间戳文件夹时出错: {e}")
        raise

    logger.info(f"正在处理文件夹: {latest_folder}")

    # 遍历每个类别文件夹
    try:
        category_folders = [f for f in latest_folder.iterdir() if f.is_dir()]
    except Exception as e:
        logger.error(f"遍历类别文件夹时出错: {e}")
        raise

    selected_files = []

    for category in category_folders:
        try:
            # 获取此类别中的所有 .md 文件
            md_files = list(category.glob('*.md'))
            if not md_files:
                continue

            # 按基本名称（不包括扩展名）对文件进行分组
            file_groups = {}
            for file in md_files:
                # 通过删除所有已知后缀提取基本名称
                base_name = file.name
                if base_name.endswith('_final.md'):
                    base_name = base_name[:-9]  # 删除 '_final.md'
                elif base_name.endswith('_with_html.md'):
                    base_name = base_name[:-13]  # 删除 '_with_html.md'
                else:
                    base_name = base_name[:-3]  # 删除 '.md'

                if base_name not in file_groups:
                    file_groups[base_name] = []
                file_groups[base_name].append(file)

            # 对于每个组，选择首选文件
            for base_name, files in file_groups.items():
                preferred_files = get_preferred_md_file(files)
                selected_files.extend(preferred_files)
        except Exception as e:
            logger.error(f"处理类别 {category} 时出错: {e}")
            raise

    return selected_files

def process_markdown_file(file_path):
    """
    处理单个markdown文件
    
    Args:
        file_path (str): 需要处理的Markdown文件路径
        
    Returns:
        bool: 处理成功返回True，否则返回False
    """
    logger.info(f"\n=== Processing file: {file_path} ===")
    
    try:
        # 生成固定的输出文件路径
        base_name = os.path.splitext(file_path)[0]
        output_file_path = f"{base_name}_fix.md"
        
        # Step 1: Execute tt.py logic (TOC removal)
        logger.info("=== Step 1: Removing TOC lines ===")
        cleaned_file_path = extract_and_filter_toc(file_path)
        
        # Step 2: Execute tt1.py logic (heading fix)
        logger.info("\n=== Step 2: Fixing markdown headings ===")
        fixed_file_path = fix_markdown_headings_two_pass(cleaned_file_path)
        
        # Step 3: Finalize files - 重命名为固定的输出文件名
        logger.info("\n=== Step 3: Finalizing files ===")
        # 如果输出文件已存在，先删除它
        if os.path.exists(output_file_path):
            os.remove(output_file_path)
        
        # 重命名最终文件为固定名称
        os.rename(fixed_file_path, output_file_path)
        
        # 新增步骤：在修复后的文件第一行添加 "# 文件名" 标题
        with open(output_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        filename = os.path.basename(file_path)
        # 移除文件扩展名
        if filename.endswith('.md'):
            filename = filename[:-3]
        
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write(f"# {filename}\n")
            f.write(content)
        
        # 清理中间文件
        remove_intermediate_files(file_path, cleaned_file_path)
        
        logger.info(f"Successfully processed: {file_path} -> {output_file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        return False

def fix_md_file(file_path: Union[str, Path], output_dir: Union[str, Path] = None) -> str:
    """
    修复MD文件的主入口函数，符合项目处理器规范
    
    参数:
        file_path (Union[str, Path]): MD文件路径
        output_dir (Union[str, Path], optional): 输出目录路径，默认为None表示与源文件同目录

    返回:
        str: 生成的修复后的MD文件路径

    异常:
        FileNotFoundError: 如果文件不存在
        RuntimeError: 如果处理失败
    """
    file = Path(file_path)
    
    # 检查文件是否存在
    if not file.exists():
        raise FileNotFoundError(f"MD文件不存在: {file_path}")
    
    # 检查文件扩展名
    if file.suffix.lower() != '.md':
        raise ValueError(f"不支持的文件格式: {file.suffix}，仅支持.md文件")
    
    # 确定输出目录
    if output_dir is None:
        output_dir = file.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # 处理MD文件
        success = process_markdown_file(str(file_path))
        if not success:
            raise RuntimeError(f"处理MD文件失败: {file_path}")
        
        # 生成输出文件路径
        output_file_path = str(output_dir / f"{file.stem}_fix.md")
        logger.info(f"已处理MD文件: {file_path} -> {output_file_path}")
        return output_file_path
        
    except Exception as e:
        raise RuntimeError(f"处理MD文件时发生未知错误: {str(e)}")