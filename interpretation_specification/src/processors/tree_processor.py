# -*- coding: utf-8 -*-
"""
多树结构处理模块

本模块用于从JSON格式的Excel树形结构中提取特定工作表数据，
并将每行数据转换为独立的字典结构，便于后续处理。
"""

import os
import json
from rich import print
from pathlib import Path
from config.settings import JSON_ORI_PATH, MULTI_TREE_PATH


def find_sheet_in_tree(tree, sheet_name):
    """
    在树形结构中递归查找指定名称的工作表
    
    参数:
        tree: 树形结构数据
        sheet_name: 要查找的工作表名称
        
    返回:
        找到的工作表节点，或None
    """
    if isinstance(tree, dict):
        # 检查当前节点是否是目标工作表
        if tree.get('type') == 'sheet' and tree.get('name') == sheet_name:
            return tree
            
        # 递归检查子节点
        if 'children' in tree:
            for child in tree['children']:
                found = find_sheet_in_tree(child, sheet_name)
                if found:
                    return found
    return None

def load_and_extract_sheet(file_path, sheet_name):
    """
    加载JSON文件并提取指定工作表数据
    
    参数:
        file_path: JSON文件路径
        sheet_name: 要提取的工作表名称
    
    返回:
        包含原始数据和表头的字典
    """
    try:
        # 读取JSON文件
        with open(file_path, "r", encoding="utf-8") as f:
            tree = json.load(f)
        
        # 查找目标工作表
        sheet_node = find_sheet_in_tree(tree, sheet_name)
        if not sheet_node:
            # 尝试获取所有可用工作表名称
            available_sheets = []
            def collect_sheets(node):
                if isinstance(node, dict):
                    if node.get('type') == 'sheet':
                        available_sheets.append(node.get('name', ''))
                    if 'children' in node:
                        for child in node['children']:
                            collect_sheets(child)
            collect_sheets(tree)
            raise ValueError(f"未找到工作表 '{sheet_name}'。可用工作表: {available_sheets}")
        
        # 提取工作表数据
        sheet_data = sheet_node.get('info', {}).get('data', [])
        
        return {
            "raw_data": sheet_data,
            "headers": sheet_data[0] if sheet_data else {}
        }
    
    except FileNotFoundError:
        raise ValueError(f"文件不存在: {file_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON解析错误: {str(e)}")
    except Exception as e:
        raise ValueError(f"处理过程中出错: {str(e)}")

def main():
    """
    主函数：执行工作表数据提取和处理流程
    """
    file_path = JSON_ORI_PATH
    sheet_name = "Sheet1"
    
    try:
        # 提取工作表数据
        result = load_and_extract_sheet(file_path, sheet_name)
        print(f"成功找到工作表 '{sheet_name}'，共 {len(result['raw_data'])} 行数据（含表头行）")
        
        # 提取表头行
        header_row = result["headers"]
        
        # 处理每个数据行（从第二行开始）
        processed_results = []
        
        # 跳过表头行，从第一个数据行开始
        for data_row in result["raw_data"][1:]:
            # 为每个数据行创建独立的字典结构
            data_dict = {
                "data": [
                    header_row,  # 表头行保持原样
                    data_row     # 当前数据行
                ]
            }
            processed_results.append(data_dict)
        
        # 打印并返回所有处理结果
        if not processed_results:
            print("没有需要处理的数据行")
            return []
            
        print(f"\n成功处理了 {len(processed_results)} 个数据行")
        
        # 打印前几个处理结果
        print("\n==== 示例数据结构 ====")
        for i, result_dict in enumerate(processed_results[:2]):  # 只显示前两个
            print(f"\n结构 {i+1}:")
            print(json.dumps(result_dict, ensure_ascii=False, indent=2))
        
        print("\n==== 正在写入所有数据结构 ====")
        with open(MULTI_TREE_PATH, "w", encoding="utf-8") as f:
            for i, result_dict in enumerate(processed_results):
                f.write(json.dumps(result_dict, ensure_ascii=False) + "\n")
        print(f"所有数据已成功写入: {MULTI_TREE_PATH}")
        # return processed_results
        return True
    
    # except ValueError as e:
    #     print(f"错误: {str(e)}")
    #     return []
    except ValueError as e:
        print(f"错误: {str(e)}")
        return False
    except Exception as e:
        print(f"未预期的错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
