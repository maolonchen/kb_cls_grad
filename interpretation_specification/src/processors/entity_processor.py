# -*- coding: utf-8 -*-
"""
梯度到实体转换模块

本模块用于处理Excel中的Sheet2数据，提取子类和类别信息，
并生成新的工作表，将处理后的数据保存为JSON格式。
"""

import json
import os
from rich import print
from config.settings import JSON_ORI_PATH, GRAD_ENTITY_PATH

def find_sheet(tree, sheet_name):
    """
    在树结构中递归查找指定名称的工作表节点
    
    参数:
        tree: 树形结构数据
        sheet_name: 要查找的工作表名称
    
    返回:
        找到的工作表节点，或None
    """
    # 当前节点是目标工作表
    if tree.get('type') == 'sheet' and tree.get('name') == sheet_name:
        return tree
    
    # 如果当前节点有子节点，递归查找
    if 'children' in tree:
        for child in tree['children']:
            found = find_sheet(child, sheet_name)
            if found:
                return found
    return None

def find_table_for_sheet(tree, sheet_node):
    """
    找到包含指定工作表节点的表格节点
    
    参数:
        tree: 树形结构数据
        sheet_node: 目标工作表节点
    
    返回:
        包含目标工作表的表格节点，或None
    """
    if 'children' in tree:
        # 如果当前节点是表格节点，且包含目标工作表
        if any(child is sheet_node for child in tree.get('children', [])):
            return tree
        
        # 递归查找子节点
        for child in tree['children']:
            table = find_table_for_sheet(child, sheet_node)
            if table:
                return table
    return None

def safe_split(text):
    """
    智能分割文本，完整保留括号外内容并保护括号内的顿号
    
    参数:
        text: 需要分割的文本字符串
    
    返回:
        分割后的文本片段列表
    """
    if not text.strip():
        return []
    
    parts = []
    buffer = []
    depth = 0  # 括号嵌套深度
    
    for char in text:
        if char in ['(', '（']:
            depth += 1
            buffer.append(char)
        elif char in [')', '）']:
            if depth > 0:
                depth -= 1
            buffer.append(char)
        elif char == '、' and depth == 0:
            # 只在顶层分割顿号
            if buffer:  # 确保不会添加空片段
                parts.append(''.join(buffer))
                buffer = []
        else:
            buffer.append(char)
    
    # 添加最后一个片段
    if buffer:
        parts.append(''.join(buffer))
    
    return parts

def main():
    """
    主函数：处理Excel中的Sheet2数据，提取子类和类别信息，
    并生成新的Sheet3工作表，将处理后的数据保存为JSON格式。
    """
    try:
        input_file = JSON_ORI_PATH
        output_file = GRAD_ENTITY_PATH

        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
                print(f"已创建输出目录: {output_dir}")
            except Exception as e:
                print(f"创建输出目录失败: {str(e)}")
                return False

        with open(input_file, 'r', encoding='utf-8') as f:
            tree = json.load(f)  # 现在读取的是树形结构的字典

        # 2. 查找Sheet2节点
        sheet2 = find_sheet(tree, 'Sheet2')
        if not sheet2:
            print("错误: 未找到Sheet2")
            return False

        # 3. 提取Sheet2数据
        try:
            sheet2_rows = sheet2['info']['data']
            print(f"Sheet2有 {len(sheet2_rows)} 行数据")
            
            # 打印Sheet2数据以验证
            print("\nSheet2前5行数据:")
            for i, row in enumerate(sheet2_rows[1:6]):  # 跳过表头行
                row_data = row  # 由于是纯JSON，row已经是字典
                # 获取列索引值
                col0_value = next((v for k, v in row_data.items() if int(k) == 0), "")
                col1_value = next((v for k, v in row_data.items() if int(k) == 1), "")
                
                print(f"行 {i+1}: 类别='{col0_value}', 子类范围='{col1_value}'")
                
        except Exception as e:
            print(f"提取Sheet2数据失败: {str(e)}")
            return False

        # 4. 处理子类及范围数据
        category_mappings = []

        for row in sheet2_rows[1:]:
            col0_value = next((v for k, v in row.items() if int(k) == 0), "")
            col1_value = next((v for k, v in row.items() if int(k) == 1), "")
            
            if not col0_value or not col1_value:
                continue
                
            print(f"处理: {col0_value} | {col1_value}")
            
            # 使用改进的分割方法
            items = safe_split(col1_value)
            
            for item in items:
                if not item.strip():
                    continue
                    
                cleaned_item = item.strip()
                
                # 移除结尾的"等"
                if cleaned_item.endswith('等'):
                    cleaned_item = cleaned_item[:-1].strip()
                
                # 处理不完整括号
                if '(' in cleaned_item and ')' not in cleaned_item:
                    cleaned_item += ')'
                if '（' in cleaned_item and '）' not in cleaned_item:
                    cleaned_item += '）'
                
                # 过滤空项
                if cleaned_item and cleaned_item not in ['）', ')']:
                    # 添加到映射列表
                    category_mappings.append({
                        "0": cleaned_item,  # 子类项
                        "1": col0_value     # 类别
                    })
                    print(f"  添加映射: {cleaned_item} => {col0_value}")

        # 5. 创建新的Sheet3节点
        # 找到Sheet2所在的表格节点
        table_node = find_table_for_sheet(tree, sheet2)
        if not table_node:
            print("错误: 未找到包含Sheet2的表格节点")
            return False

        # 创建Sheet3节点
        sheet3 = {
            'type': 'sheet',
            'name': 'Sheet3',
            'path': f"{table_node['path']}/Sheet3",
            'info': {
                'data': [
                    # 表头 - 使用数字键名与前两列一致
                    {"0": "子类项", "1": "类别"}
                ],
                'merged_cells': []
            }
        }

        # 添加映射数据
        for mapping in category_mappings:
            sheet3['info']['data'].append({
                "0": mapping["0"],  # 子类项
                "1": mapping["1"]   # 类别
            })

        print(f"创建Sheet3，包含 {len(sheet3['info']['data'])} 行数据")

        # 6. 在表格节点中添加Sheet3
        # 检查表格节点中是否已存在Sheet3
        existing_sheet3 = next((s for s in table_node['children'] 
                               if s.get('name') == 'Sheet3'), None)
                               
        if existing_sheet3:
            # 如果已存在，更新信息
            existing_sheet3.update(sheet3)
            print("已更新现有的Sheet3")
        else:
            # 添加新的Sheet3
            if 'children' not in table_node:
                table_node['children'] = []
            table_node['children'].append(sheet3)
            print("已添加新的Sheet3")

        # 7. 保存结果
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(tree, f, ensure_ascii=False, indent=2)

        print(f"结果已保存到: {output_file}")
        print(f"Sheet3包含 {len(sheet3['info']['data'])} 行数据 (包括表头)")

        # 打印Sheet3的前5行验证
        print("\nSheet3前5行数据:")
        for i, row in enumerate(sheet3['info']['data'][:5]):
            # 获取列值
            col0_value = next((v for k, v in row.items() if int(k) == 0), "")
            col1_value = next((v for k, v in row.items() if int(k) == 1), "")
            
            print(f"行 {i}: 子类项='{col0_value}', 类别='{col1_value}'")
        
        return True
        
    except Exception as e:
        print(f"处理过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)