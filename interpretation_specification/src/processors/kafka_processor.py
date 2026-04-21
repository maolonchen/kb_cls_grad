# -*- coding: utf-8 -*-
"""
Kafka数据处理器

该模块负责将处理后的数据转换为适合发送到Kafka的格式，
包括构建分类树结构、处理等级信息和特征映射等。
"""

import json
import os
import time
import requests
from collections import defaultdict
from rich import print

from src.prompts.llm_prompts import PRIORITY_SORT_PROMPT
from config.settings import (
    LLM_API_URL, 
    MODEL_NAME, 
    API_PAYLOAD, 
    HEADERS, 
    KAFKA_DATA_PATH,
    PROJECT_ROOT, 
    KAFKA_feat_PATH, 
    KAFKA_GRAD_PATH, 
    POST_FINAL_PATH
)


def parse_jsonl_entry(line):
    """
    解析JSONL行数据
    
    参数:
        line: JSONL格式的行数据
    
    返回:
        tuple: (entry, header, data) 或 (None, None, None) 如果解析失败
    """
    try:
        entry = json.loads(line)
        header = entry['header']
        data = entry['data']
        return entry, header, data
    except Exception as e:
        print(f"解析行时出错: {line}")
        print(f"错误信息: {str(e)}")
        return None, None, None


def find_level_and_feature_keys(header):
    """
    查找等级和特征字段在header中的键
    
    参数:
        header: 表头信息字典
    
    返回:
        tuple: (level_key, feature_key)
    """
    level_key = None
    feature_key = None
    
    # 查找"等级"字段在header中的键
    for key, value in header.items():
        if value == '等级':
            level_key = key
            break
            
    # 查找"对应特征"字段在header中的键
    for key, value in header.items():
        if value == '对应特征':
            feature_key = key
            break
            
    return level_key, feature_key


def extract_classification_path(header, data):
    """
    提取分类层级路径
    
    参数:
        header: 表头信息字典
        data: 数据字典
    
    返回:
        tuple: (path, classification_key)
    """
    path = []
    classification_key = None
    
    # 获取所有数字键并排序，用于构建分类路径
    keys = [k for k in header.keys() if k.isdigit()]
    sorted_keys = sorted(keys, key=int)
    
    # 构建分类路径并识别关键字段
    for key in sorted_keys:
        header_val = header[key]
        if key in data:
            data_val = data[key]
            
            if '级分类' in header_val:
                path.append(data_val)
                # 记录最后一级分类作为分类标识
                classification_key = key
    
    return path, classification_key


# def collect_grade_info(grades_set, level):
#     """
#     收集等级信息
    
#     参数:
#         grades_set: 等级集合
#         level: 等级信息
#     """
#     if not level:
#         return
        
#     # 处理等级信息
#     if "/" in level:
#         # 处理多个等级的情况，如"第2级/第3级"
#         levels = level.split("/")
#         for l in levels:
#             l = l.strip()
#             # 提取核心数据、重要数据或第N级等标准等级格式
#             if "核心数据" in l:
#                 grades_set.add("核心数据")
#             elif "重要数据" in l:
#                 grades_set.add("重要数据")
#             elif l.startswith("第") and l.endswith("级"):
#                 grades_set.add(l)
#     else:
#         # 单个等级的情况
#         level = level.strip()
#         # 提取核心数据、重要数据或第N级等标准等级格式
#         if "核心数据" in level:
#             grades_set.add("核心数据")
#         elif "重要数据" in level:
#             grades_set.add("重要数据")
#         elif level.startswith("第") and level.endswith("级"):
#             grades_set.add(level)
def collect_grade_info(grades_set, level):
    """
    收集等级信息
    
    参数:
        grades_set: 等级集合
        level: 等级信息
    """
    if not level:
        return
        
    # 处理等级信息
    if "/" in level:
        # 处理多个等级的情况，如"第2级/第3级" 或 "L1/L5" 或 "核心数据/第2级"
        levels = level.split("/")
        for l in levels:
            l = l.strip()
            if l:  # 确保不是空字符串
                # 保留对核心数据和重要数据的特殊处理
                if "核心数据" in l:
                    grades_set.add("核心数据")
                elif "重要数据" in l:
                    grades_set.add("重要数据")
                elif l:  # 其他任何形式的等级都添加原值
                    grades_set.add(l)
    else:
        # 单个等级的情况
        level = level.strip()
        if level:  # 确保不是空字符串
            # 保留对核心数据和重要数据的特殊处理
            if "核心数据" in level:
                grades_set.add("核心数据")
            elif "重要数据" in level:
                grades_set.add("重要数据")
            elif level:  # 其他任何形式的等级都添加原值
                grades_set.add(level)


def collect_feature_mappings(feature_mappings, feature, classification, data, classification_key, real_data_key):
    """
    收集特征映射信息
    
    参数:
        feature_mappings: 特征映射列表
        feature: 特征信息
        classification: 分类信息
        data: 数据字典
        classification_key: 分类键
        real_data_key: 真实数据键
    """
    # 使用真实数据而不是特征描述来构建DataElement
    if real_data_key and real_data_key in data and classification_key and classification_key in data:
        real_data = data[real_data_key]
        classification = data[classification_key]
        
        # 如果真实数据是列表，为每个元素创建映射
        if isinstance(real_data, list):
            for element in real_data:
                feature_mappings.append({
                    "DataElement": element,
                    "DataClassification": classification
                })
        # 如果真实数据是字符串，创建单个映射
        elif isinstance(real_data, str):
            feature_mappings.append({
                "DataElement": real_data,
                "DataClassification": classification
            })


def build_tree_structure(tree, path, level, feature):
    """
    构建树结构
    
    参数:
        tree: 树结构字典
        path: 分类路径
        level: 等级信息
        feature: 特征信息
    """
    # 构建树结构
    current = tree
    for i, node_name in enumerate(path[:-1]):
        if node_name not in current:
            current[node_name] = defaultdict(dict)
        current = current[node_name]
    
    # 处理最后一级节点
    last_node = path[-1]
    if last_node not in current:
        current[last_node] = {
            "DataClassification": last_node,
            "DataGrading": level,
            "Annotate": feature,
            "SubDataClassifications": []
        }
    else:
        # 更新现有节点
        current[last_node]["DataGrading"] = level
        current[last_node]["Annotate"] = feature


def convert_to_template(node):
    """
    将嵌套字典转换为模板格式的递归函数
    
    参数:
        node: 当前处理的节点
        
    返回:
        转换后的节点结构
    """
    def extract_lowest_grade(grade_str):
        """
        从可能包含多个等级的字符串中提取最低等级（数字最大的等级）
        
        参数:
            grade_str: 等级字符串，可能包含多个等级，如"L2/L3"或"第4级/第2级/第3级等"
            
        返回:
            最低优先级的等级（数字最大的等级，对于相同类型；"核心数据"和"重要数据"有特殊优先级）
        """
        if not grade_str:
            return grade_str
            
        # 分割多个等级
        grades = [g.strip() for g in grade_str.split("/") if g.strip()]
        
        # 如果只有一个等级，直接返回
        if len(grades) <= 1:
            return grade_str
        
        # 定义特殊等级的优先级映射（数值越小优先级越高）
        special_priority_map = {
            "核心数据": 1,
            "重要数据": 2
        }
        
        # 分离特殊等级和其他等级
        special_grades = []
        numeric_grades = []
        
        for grade in grades:
            if grade in special_priority_map:
                special_grades.append(grade)
            else:
                # 提取等级中的数字部分
                import re
                numbers = re.findall(r'\d+', grade)  # 提取所有数字
                if numbers:
                    # 取最大的数字作为该等级的数值
                    max_number = max(int(num) for num in numbers)
                    numeric_grades.append((max_number, grade))
                else:
                    # 如果无法提取数字，将其视为等级0
                    numeric_grades.append((0, grade))
        
        # 如果有特殊等级（核心数据或重要数据），返回优先级最高的（数值最小的）
        if special_grades:
            highest_priority_special = min(special_grades, key=lambda x: special_priority_map[x])
            return highest_priority_special
        
        # 对于数字等级，返回数字最大的（即最低等级）
        if numeric_grades:
            highest_number_grade = max(numeric_grades, key=lambda x: x[0])
            return highest_number_grade[1]
        
        # 如果既没有特殊等级也没有可解析的数字等级，返回第一个
        return grades[0]
    
    if isinstance(node, dict):
        result = []
        for key, value in node.items():
            if isinstance(value, dict) and "DataClassification" in value:
                # 这是叶节点
                # 提取最低等级
                original_grade = value["DataGrading"]
                lowest_grade = extract_lowest_grade(original_grade)
                
                result.append({
                    "DataClassification": value["DataClassification"],
                    "DataGrading": lowest_grade,
                    "Annotate": value["Annotate"],
                    "SubDataClassifications": convert_to_template(value.get("SubDataClassifications", []))
                })
            else:
                # 这是中间节点
                result.append({
                    "DataClassification": key,
                    "DataGrading": None,
                    "Annotate": None,
                    "SubDataClassifications": convert_to_template(value)
                })
        return result
    return None


def build_classification_tree(jsonl_lines):
    """
    从JSONL行数据构建分类树结构，并收集等级和特征信息
    
    参数:
        jsonl_lines: JSONL格式的行列表
    
    返回:
        tuple: (分类树字典, 等级列表, 特征映射列表)
    """
    # 创建嵌套字典结构
    tree = defaultdict(lambda: defaultdict(dict))
    grades_set = set()  # 收集所有等级
    feature_mappings = []  # 收集特征映射
    
    for line in jsonl_lines:
        # 解析JSON行
        entry, header, data = parse_jsonl_entry(line)
        if not entry:
            continue
            
        # 确定分类层级路径
        path, classification_key = extract_classification_path(header, data)
        
        # 如果路径为空，跳过该行
        if not path:
            continue
            
        # 查找等级、特征和真实数据字段键
        level_key, feature_key = find_level_and_feature_keys(header)
        
        # 查找真实数据键
        real_data_key = None
        for key, value in header.items():
            if value == '真实数据':
                real_data_key = key
                break
        
        # 获取等级、特征和真实数据值
        level = data.get(level_key, None) if level_key else None
        feature = data.get(feature_key, None) if feature_key else None
        real_data = data.get(real_data_key, None) if real_data_key else None
        
        # 获取分类值
        classification = data.get(classification_key, None) if classification_key else None
        
        # 收集等级信息
        collect_grade_info(grades_set, level)
        
        # 收集特征映射信息
        collect_feature_mappings(feature_mappings, feature, classification, data, classification_key, real_data_key)
        
        # 构建树结构
        build_tree_structure(tree, path, level, feature)
    
    # 转换树结构
    tree_converted = convert_to_template(tree)
    
    # 获取所有有效的等级
    valid_grades = list(grades_set)
    for grade in valid_grades:
        print(f"处理等级: {grade}")
    
    # 使用LLM对等级进行排序
    print(f"处理等级: {valid_grades}")
    grades_list = sort_grades_with_llm(valid_grades)
    
    return tree_converted, grades_list, feature_mappings


def sort_grades_with_llm(grades):
    """
    使用LLM对等级进行排序
    
    参数:
        grades: 等级列表
    
    返回:
        排序后的等级列表，包含等级和对应的优先级
    """
    # 构建提示词
    prompt = PRIORITY_SORT_PROMPT(grades)
    
    # 持续重试直到成功
    while True:
        try:
            # 使用settings.py中的配置创建payload
            payload = API_PAYLOAD(MODEL_NAME, prompt)
            
            # 调用LLM API
            response = requests.post(LLM_API_URL, headers=HEADERS, json=payload, timeout=300)
            response.raise_for_status()  # 检查HTTP错误
            
            # 提取LLM的回复
            result = response.json()
            llm_output = result["choices"][0]["message"]["content"]
            llm_output = llm_output.split("</think>\n\n")[1] if "</think>\n\n" in llm_output else llm_output

            # 验证返回结果的结构
            if is_valid_json_format(llm_output):
                grades_list = json.loads(llm_output)
                # 验证内容是否符合要求
                if is_valid_grade_list(grades_list, grades):
                    return grades_list
                else:
                    print("LLM返回的内容不符合要求，重新请求...")
            else:
                print("LLM返回的不是有效的JSON格式，重新请求...")
                
        except requests.exceptions.Timeout:
            print("LLM请求超时，重新连接...")
            time.sleep(1)  # 等待5秒后重试
        except requests.exceptions.RequestException as e:
            print(f"LLM请求失败: {str(e)}，重新连接...")
            time.sleep(1)  # 等待5秒后重试
        except Exception as e:
            print(f"处理LLM响应时出错: {str(e)}，重新连接...")
            time.sleep(5)  # 等待5秒后重试


def is_valid_json_format(text):
    """
    验证文本是否为有效的JSON格式
    
    参数:
        text: 待验证的文本
    
    返回:
        bool: 是否为有效的JSON格式
    """
    try:
        json.loads(text)
        return True
    except json.JSONDecodeError:
        return False


def is_valid_grade_list(grades_list, expected_grades):
    """
    验证等级列表是否符合要求
    
    参数:
        grades_list: LLM返回的等级列表
        expected_grades: 期望的等级列表
    
    返回:
        bool: 是否符合要求
    """
    try:
        # 检查是否为列表
        if not isinstance(grades_list, list):
            return False
            
        # 检查每个元素
        expected_set = set(expected_grades)
        actual_set = set()
        
        for item in grades_list:
            # 检查每个元素是否为字典
            if not isinstance(item, dict):
                return False
                
            # 检查必需的键
            if "DataGrading" not in item or "Priority" not in item:
                return False
                
            # 检查DataGrading是否在期望的等级中
            if item["DataGrading"] not in expected_set:
                return False
                
            # 检查Priority是否为整数
            if not isinstance(item["Priority"], int):
                return False
                
            actual_set.add(item["DataGrading"])
            
        # 检查是否包含了所有期望的等级
        if actual_set != expected_set:
            return False
            
        # 检查优先级是否唯一
        priorities = [item["Priority"] for item in grades_list]
        if len(priorities) != len(set(priorities)):
            return False
            
        return True
    except Exception:
        return False


def resolve_duplicate_features_with_llm(feature_mappings):
    """
    使用LLM解决特征映射中的重复DataElement问题
    
    参数:
        feature_mappings: 原始特征映射列表
    
    返回:
        list: 解决重复问题后的特征映射列表
    """
    from collections import defaultdict
    
    # 按DataElement分组
    element_groups = defaultdict(list)
    for idx, item in enumerate(feature_mappings):
        element_groups[item["DataElement"]].append((idx, item))
    
    # 找出有重复的DataElement
    duplicates = {key: value for key, value in element_groups.items() if len(value) > 1}
    
    if not duplicates:
        print("没有发现重复的DataElement")
        return feature_mappings
    
    print(f"发现 {len(duplicates)} 个重复的DataElement，开始使用LLM解决...")
    
    # 存储要保留的项目
    final_mappings = []
    processed_elements = set()
    
    for data_element, items_with_idx in element_groups.items():
        if len(items_with_idx) == 1:
            # 没有重复，直接添加
            final_mappings.append(items_with_idx[0][1])
        else:
            # 有重复，使用LLM选择最佳分类
            print(f"\n正在使用LLM判断'{data_element}'的最佳分类...")
            
            # 获取所有可能的分类
            classifications = [item[1]["DataClassification"] for item in items_with_idx]
            
            # 构建提示词
            prompt = f"""
请判断以下数据元素"{data_element}"更适合属于哪个分类：

选项：
"""
            for i, classification in enumerate(classifications):
                prompt += f"{i+1}. {classification}\n"
            
            prompt += f"""
仅回复数字即可，表示最适合的分类编号，回复格式：<数字>
"""
            
            # 构建API请求
            payload = API_PAYLOAD(MODEL_NAME, prompt)
            
            try:
                # 调用LLM API
                response = requests.post(LLM_API_URL, headers=HEADERS, json=payload, timeout=300)
                response.raise_for_status()
                
                # 提取LLM的回复
                result = response.json()
                llm_output = result["choices"][0]["message"]["content"]
                
                # 从回复中提取数字
                import re
                numbers = re.findall(r'<(\d+)>', llm_output)
                if numbers:
                    # 获取最佳分类的索引（减去1，因为用户选择从1开始，但列表索引从0开始）
                    best_index = int(numbers[0]) - 1
                    
                    if 0 <= best_index < len(items_with_idx):
                        # 添加LLM选择的分类
                        final_mappings.append(items_with_idx[best_index][1])
                        print(f"LLM选择: 选项 {best_index + 1} -> '{classifications[best_index]}'")
                    else:
                        # 如果索引超出范围，使用第一个
                        final_mappings.append(items_with_idx[0][1])
                        print(f"LLM返回的索引超出范围，使用默认选择: '{classifications[0]}'")
                else:
                    # 如果没有找到匹配的数字格式，使用第一个
                    final_mappings.append(items_with_idx[0][1])
                    print(f"警告: 无法从LLM回复中提取数字，使用默认选择。LLM回复: {llm_output}")
                    
            except Exception as e:
                print(f"调用LLM时出错: {str(e)}，使用默认选择")
                # 发生错误时使用第一个分类
                final_mappings.append(items_with_idx[0][1])
    
    print(f"处理完成，保留 {len(final_mappings)} 个特征映射项")
    return final_mappings


def main(specification_u_id=None):
    """
    主函数：读取JSONL文件，构建分类树，收集等级和特征信息，并保存结果
    
    参数:
        specification_u_id: 规范唯一标识符（可选）
    """
    try:
        # 输入文件路径
        input_file = POST_FINAL_PATH
        
        # 输出文件路径
        kafka_data_path = KAFKA_DATA_PATH
        kafka_grad_path = KAFKA_GRAD_PATH
        kafka_feat_path = KAFKA_feat_PATH
        
        print(f"开始处理文件: {input_file}")
        
        # 读取JSONL文件
        with open(input_file, 'r') as f:
            jsonl_lines = f.readlines()
        
        print(f"成功读取 {len(jsonl_lines)} 行数据")
        
        # 构建分类树并收集信息
        print("开始构建分类树并收集信息...")
        classification_tree, grades_list, feature_mappings = build_classification_tree(jsonl_lines)
        print("分类树构建完成")
        
        # 保存分类树结果
        print(f"将分类树结果保存到: {kafka_data_path}")
        with open(kafka_data_path, 'w', encoding='utf-8') as f:
            json.dump(classification_tree, f, ensure_ascii=False, indent=2)
        
        # 保存等级信息
        print(f"将等级信息保存到: {kafka_grad_path}")
        with open(kafka_grad_path, 'w', encoding='utf-8') as f:
            json.dump(grades_list, f, ensure_ascii=False, indent=2)
        
        # 准备特征映射数据结构
        feat_data = feature_mappings
        
        # 保存特征映射信息
        print(f"将特征映射信息保存到: {kafka_feat_path}")
        with open(kafka_feat_path, 'w', encoding='utf-8') as f:
            json.dump(feat_data, f, ensure_ascii=False, indent=2)
        
        # 如果提供了specification_u_id，则尝试进行DataClassification值的反向映射
        if specification_u_id:
            # 构造映射文件路径 - 避免重复添加spec_
            mapping_file_path = os.path.join(PROJECT_ROOT, 'src', 'other', f'mapping_{specification_u_id}.json')
            
            if os.path.exists(mapping_file_path):
                print(f"找到映射文件: {mapping_file_path}，开始进行DataClassification值的反向映射...")
                
                # 加载映射文件
                with open(mapping_file_path, 'r', encoding='utf-8') as f:
                    mapping_data = json.load(f)
                
                # 创建反向映射：从处理后的值映射到原始键
                reverse_mapping = {}
                for original_key, processed_value in mapping_data.items():
                    reverse_mapping[processed_value] = original_key
                
                # 读取已保存的kafka_output_data.json文件
                with open(kafka_data_path, 'r', encoding='utf-8') as f:
                    saved_data = json.load(f)
                
                # 递归函数来更新DataClassification值
                def update_classification_values(data):
                    if isinstance(data, list):
                        for item in data:
                            update_classification_values(item)
                    elif isinstance(data, dict):
                        if "DataClassification" in data:
                            current_value = data["DataClassification"]
                            if current_value in reverse_mapping:
                                data["DataClassification"] = reverse_mapping[current_value]
                                print(f"将DataClassification值 '{current_value}' 映射回 '{reverse_mapping[current_value]}'")
                        if "SubDataClassifications" in data and data["SubDataClassifications"]:
                            update_classification_values(data["SubDataClassifications"])
                
                # 应用反向映射到kafka_output_data.json
                update_classification_values(saved_data)
                
                # 保存更新后的kafka_output_data.json
                with open(kafka_data_path, 'w', encoding='utf-8') as f:
                    json.dump(saved_data, f, ensure_ascii=False, indent=2)
                
                # 读取已保存的kafka_output_feat.json文件
                with open(kafka_feat_path, 'r', encoding='utf-8') as f:
                    saved_feat_data = json.load(f)
                
                # 更新kafka_output_feat.json中的DataClassification值
                for item in saved_feat_data:
                    if "DataClassification" in item:
                        current_value = item["DataClassification"]
                        if current_value in reverse_mapping:
                            item["DataClassification"] = reverse_mapping[current_value]
                            print(f"将特征映射中的DataClassification值 '{current_value}' 映射回 '{reverse_mapping[current_value]}'")
                
                # 保存更新后的kafka_output_feat.json
                with open(kafka_feat_path, 'w', encoding='utf-8') as f:
                    json.dump(saved_feat_data, f, ensure_ascii=False, indent=2)
                
                print("DataClassification值的反向映射完成")
            else:
                print(f"映射文件不存在: {mapping_file_path}，跳过反向映射")
        
        print("处理完成，所有结果已保存")
        return True
    
    except FileNotFoundError:
        print(f"错误: 文件未找到 - {input_file}")
        return False
    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {str(e)}")
        return False
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
        return False


if __name__ == "__main__":
    main()