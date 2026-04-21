# -*- coding: utf-8 -*-
"""
LLM后处理相似度分析模块

本模块用于将相似度比较结果通过大语言模型进行进一步分析，
提取数据等级信息，并将结果与原始数据合并后保存。
"""

import json
import requests
import time
import threading
from tqdm import tqdm
from rich import print
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.prompts.llm_prompts import LLM_POST_SIMILAR_PROMPT
from config.settings import SIMILAR_COMPARE_PATH, MULTI_TREE_PATH, LLM_POST_SIMILAR_PATH, LLM_API_URL, MODEL_NAME, HEADERS, API_PAYLOAD


# 文件路径常量
INPUT_FILE = SIMILAR_COMPARE_PATH
OUTPUT_FILE = LLM_POST_SIMILAR_PATH
ORI_DATA_PATH = MULTI_TREE_PATH

# API配置
API_URL = LLM_API_URL

# 请求头
headers = HEADERS

# 定义锁
file_lock = threading.Lock()
retry_lock = threading.Lock()

def persistent_api_request(payload):
    """
    持续尝试发送API请求直到成功，无论等多久
    
    参数:
        payload: 发送给API的JSON数据载荷
    
    返回:
        response: 成功的HTTP响应对象
    """
    while True:
        try:
            response = requests.post(
                API_URL,
                headers=headers,
                json=payload,
                timeout=120
            )
            response.raise_for_status()
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            # 仅记录重试信息但不写入错误文件
            with retry_lock:
                print(f"请求超时或连接错误，正在重试...")
            time.sleep(5)  # 等待5秒后重试
        except Exception as e:
            # 其他错误立即抛出
            raise e

def merge_data(data1_str, data2_str):
    """
    合并两个数据源
    
    参数:
        data1_str: 第一个数据源的JSON字符串（原始数据）
        data2_str: 第二个数据源的JSON字符串（分析结果）
    
    返回:
        合并后的数据字典
    """
    # 解析原始数据
    data1_dict = json.loads(data1_str)
    data2_list = json.loads(data2_str)
    
    # 提取主数据集
    main_data = data1_dict["data"]
    if not main_data:
        return json.dumps({"data": []}, ensure_ascii=False)
    
    # 确定最大列索引
    header = main_data[0]
    max_key = max((int(k) for k in header.keys() if k.isdigit()), default=-1)
    
    # 准备新列键名 (自动递增)
    next_keys = [str(max_key + 1 + i) for i in range(len(data2_list[0]))]
    
    # 添加表头
    for i, key in enumerate(next_keys):
        header[key] = list(data2_list[0].keys())[i]  # 新列的标题
    
    # 添加数据行
    for i in range(1, len(main_data)):
        if i-1 < len(data2_list):  # 确保有对应的data2数据
            row_data = data2_list[i-1]
            for j, key in enumerate(next_keys):
                main_data[i][key] = list(row_data.values())[j]
        else:  # 缺失数据的填充
            for key in next_keys:
                main_data[i][key] = ""
    
    return data1_dict

def process_line(line1, line2):
    """
    处理单行数据的函数

    参数:
        line1: 相似度比较结果行
        line2: 原始数据行

    返回:
        tuple: (处理结果字符串, 是否成功标志)
    """
    try:
        # 解析JSON行数据
        json_line = json.loads(line1)
        ori_data = json_line["ori_data"]
        similar_data = json_line["similar_data"]

        # 占位行（只有一级分类、无匹配标识符），跳过LLM调用，用默认值合并
        if not ori_data:
            default_result = json.dumps([{"等级": "", "条件": ""}], ensure_ascii=False)
            merged_dict = merge_data(line2, default_result)
            merged_str = json.dumps(merged_dict, ensure_ascii=False)
            return merged_str, True

        # 构建分析提示
        prompt = LLM_POST_SIMILAR_PROMPT(ori_data, similar_data)
        
        # 构建请求载荷
        payload = API_PAYLOAD(MODEL_NAME, prompt)
        
        # 发送API请求
        response = persistent_api_request(payload)
        
        # 解析响应
        response_data = response.json()
        model_output = response_data["choices"][0]["message"]["content"]
        
        # 提取有效内容
        analysis_result = model_output.split("</think>\n\n")[1] if "</think>\n\n" in model_output else model_output
        print("ori_data===>", ori_data)
        print("similar_data===>", similar_data)
        print(f"=========================步骤5，llm分析结果：===========================\n", analysis_result)

        # 合并数据
        merged_dict = merge_data(line2, analysis_result)
        
        # 将合并结果转换为字符串
        merged_str = json.dumps(merged_dict, ensure_ascii=False)
        
        return merged_str, True
    except Exception as e:
        error_str = f"ERROR processing: {str(e)}"
        return error_str, False

def main():
    """主函数"""
    # 清空输出文件
    with open(OUTPUT_FILE, "w", encoding="utf-8") as output_f:
        pass
    
    # 读取所有数据行
    with open(INPUT_FILE, "r", encoding="utf-8") as f1, \
         open(ORI_DATA_PATH, "r", encoding="utf-8") as f2:
        lines1 = f1.readlines()
        lines2 = f2.readlines()
    
    total_lines = len(lines1)
    print(f"开始处理 {total_lines} 行数据...")
    
    # 存储结果，以便最后按顺序写入
    results = [None] * total_lines
    
    # 使用线程池并发处理
    with ThreadPoolExecutor(max_workers=8) as executor:
        # 创建进度条
        with tqdm(total=total_lines, desc="处理进度", unit="行") as pbar:
            futures = {}
            for idx, (line1, line2) in enumerate(zip(lines1, lines2)):
                # 提交任务并保存行号
                future = executor.submit(process_line, line1.strip(), line2.strip())
                futures[future] = idx
            
            # 处理完成的任务并更新进度
            for future in as_completed(futures):
                line_idx = futures[future]
                result_str, success = future.result()
                # 存储结果（按行号索引）
                results[line_idx] = result_str
                if success:
                    pbar.update(1)  # 更新进度条
    
    # 所有任务完成后，按顺序写入结果
    with file_lock:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as output_f:
            for result in results:
                if result:  # 确保结果不为空
                    output_f.write(result + "\n")
    
    print(f"处理完成！结果已保存到 {OUTPUT_FILE}")

if __name__ == "__main__":
    main()