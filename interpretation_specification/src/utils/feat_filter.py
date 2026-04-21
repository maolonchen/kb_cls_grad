# -*- coding: utf-8 -*-
"""
特征过滤器

该模块负责查找和打印kafka_output_feat.json文件中重复的DataElement值
并使用LLM判断应该保留哪个分类
"""

import json
from collections import defaultdict
import requests


LLM_API_URL = "http://192.168.101.113:11434/v1/chat/completions"
MODEL_NAME = "qwen2.5-7b"

# LLM_API_URL = "https://u343777-b730-b26a2498.westx.seetacloud.com:8443/v1/chat/completions"
# MODEL_NAME = "/root/autodl-tmp/model/Qwen/Qwen3-8B"

HEADERS = {
    "Content-Type": "application/json",
}

def API_PAYLOAD(model_name, prompt):
    """生成API请求载荷"""
    return {
        "model": model_name,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "top_p": 0.8,
        "top_k": 20,
        "max_tokens": 4096,
        "presence_penalty": 1.7,
        "chat_template_kwargs": {"enable_thinking": True},
        "stream": False
    }

def find_duplicate_data_elements(file_path):
    """
    查找JSON文件中重复的DataElement值
    
    参数:
        file_path: JSON文件路径
    
    返回:
        dict: 包含重复DataElement及其出现次数和详情的字典
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 使用字典存储每个DataElement的出现次数和详情
    element_count = defaultdict(list)
    
    for index, item in enumerate(data):
        data_element = item.get('DataElement')
        if data_element:
            element_count[data_element].append({
                'index': index,
                'item': item
            })
    
    # 找出重复的DataElement（出现次数大于1）
    duplicates = {key: value for key, value in element_count.items() if len(value) > 1}
    
    return duplicates


def print_duplicates(duplicates):
    """
    打印重复的DataElement信息
    
    参数:
        duplicates: 包含重复DataElement的字典
    """
    if not duplicates:
        print("没有发现重复的DataElement")
        return
    
    print(f"共发现 {len(duplicates)} 个重复的DataElement:")
    print("-" * 50)
    
    for data_element, occurrences in duplicates.items():
        print(f"\nDataElement: '{data_element}' 出现了 {len(occurrences)} 次:")
        for occurrence in occurrences:
            index = occurrence['index']
            item = occurrence['item']
            print(f"  - 位置 {index}: {item}")
    
    return duplicates


def query_llm_for_best_classification(data_element, classifications):
    """
    使用LLM判断给定数据元素最适合的分类
    
    参数:
        data_element: 数据元素名称
        classifications: 可能的分类列表
    
    返回:
        int: 最佳分类在列表中的索引
    """
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
        response = requests.post(LLM_API_URL, headers=HEADERS, json=payload, timeout=120)
        response.raise_for_status()
        
        # 提取LLM的回复
        result = response.json()
        llm_output = result["choices"][0]["message"]["content"]
        
        # 从回复中提取数字
        import re
        numbers = re.findall(r'<(\d+)>', llm_output)
        if numbers:
            # 返回索引（减去1，因为用户选择从1开始，但列表索引从0开始）
            return int(numbers[0]) - 1
        else:
            # 如果没有找到匹配的数字格式，默认返回第一个
            print(f"警告: 无法从LLM回复中提取数字，使用默认选择。LLM回复: {llm_output}")
            return 0
            
    except Exception as e:
        print(f"调用LLM时出错: {str(e)}，使用默认选择")
        return 0


def resolve_duplicates_with_llm(duplicates):
    """
    使用LLM解决重复的DataElement问题
    
    参数:
        duplicates: 包含重复DataElement的字典
    
    返回:
        dict: 包含解决结果的字典
    """
    resolved_results = {}
    
    for data_element, occurrences in duplicates.items():
        print(f"\n正在使用LLM判断'{data_element}'的最佳分类...")
        
        # 获取所有可能的分类
        classifications = [occurrence['item']['DataClassification'] for occurrence in occurrences]
        
        # 使用LLM判断最佳分类
        best_index = query_llm_for_best_classification(data_element, classifications)
        
        # 确保索引有效
        if 0 <= best_index < len(occurrences):
            best_occurrence = occurrences[best_index]
            resolved_results[data_element] = {
                'best_item': best_occurrence['item'],
                'best_index': best_occurrence['index'],
                'all_occurrences': occurrences,
                'selected_by_llm': best_index
            }
            print(f"LLM选择: 选项 {best_index + 1} -> '{classifications[best_index]}'")
        else:
            # 如果LLM返回的索引无效，使用第一个
            resolved_results[data_element] = {
                'best_item': occurrences[0]['item'],
                'best_index': occurrences[0]['index'],
                'all_occurrences': occurrences,
                'selected_by_llm': 0
            }
            print(f"LLM返回的索引无效，使用默认选择: '{classifications[0]}'")
    
    return resolved_results


def print_resolved_results(resolved_results):
    """
    打印LLM解决重复问题的结果
    
    参数:
        resolved_results: 包含解决结果的字典
    """
    print("\n" + "="*60)
    print("LLM解决重复DataElement的结果:")
    print("="*60)
    
    for data_element, result in resolved_results.items():
        print(f"\n数据元素: '{data_element}'")
        print(f"保留的分类: {result['best_item']}")
        print(f"原始所有分类选项:")
        for i, occurrence in enumerate(result['all_occurrences']):
            marker = " >> 选择" if i == result['selected_by_llm'] else ""
            print(f"  {i+1}. {occurrence['item']}{marker}")


def main():
    """
    主函数：查找重复的DataElement并使用LLM决定保留哪个
    """
    file_path = 'interpretation_specification/data/processed/kafka_output_feat.json'
    
    try:
        print("正在查找重复的DataElement...")
        duplicates = find_duplicate_data_elements(file_path)
        
        if not duplicates:
            print("没有发现重复的DataElement")
            return
        
        # 打印初始重复情况
        print_duplicates(duplicates)
        
        # 使用LLM解决重复问题
        resolved_results = resolve_duplicates_with_llm(duplicates)
        
        # 打印解决结果
        print_resolved_results(resolved_results)
        
    except FileNotFoundError:
        print(f"错误: 文件未找到 - {file_path}")
    except json.JSONDecodeError:
        print(f"错误: 文件不是有效的JSON格式 - {file_path}")
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")


if __name__ == "__main__":
    main()