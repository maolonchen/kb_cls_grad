# -*- coding: utf-8 -*-
"""
特征到实体转换模块

本模块用于从处理后的数据中提取"对应特征"字段，
通过大语言模型分析，将特征描述转换为具体的可管理数据单元名称列表。
"""

import ast
import os
import re
import json
import time
import random
import concurrent.futures
import sys
import requests
from config.settings import LLM_API_URL, MODEL_NAME, API_PAYLOAD, HEADERS, LLM_POST_SIMILAR_PATH, FINAL_PATH
from src.prompts.llm_prompts import FEATURE_ENTITY_PROMPT


# 配置路径
path = LLM_POST_SIMILAR_PATH
output_path = FINAL_PATH

def generate_response(content, 
                      api_url=LLM_API_URL,
                      model_name=MODEL_NAME,
                      temperature=0.1,
                      top_p=0.8,
                      top_k=20,
                      max_tokens=5096,
                      presence_penalty=1.7,
                      chat_template_kwargs={"enable_thinking": True},
                      stream=False,
                      timeout=180):

    payload = API_PAYLOAD(model_name, content)
    # 更新特定参数
    payload.update({
        "temperature": temperature,
        "top_p": top_p,
        "top_k": top_k,
        "max_tokens": max_tokens,
        "presence_penalty": presence_penalty,
        "chat_template_kwargs": chat_template_kwargs,
        "stream": stream
    })
    
    try:
        response = requests.post(api_url, headers=HEADERS, json=payload, timeout=timeout)
        response.raise_for_status()  # 检查HTTP错误
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"API请求失败: {e}")
        return None

def process_feature(feature_data):
    """处理单个特征数据并获取LLM的响应"""
    
    content = FEATURE_ENTITY_PROMPT(feature_data)

    for attempt in range(5):
        try:
            response = generate_response(
                content,
                api_url=LLM_API_URL,
                model_name=MODEL_NAME,
                timeout=180
            )
            
            if response:
                response_str = response["choices"][0]["message"]["content"]
                response_str = response_str.split("</think>\n\n")[1]
                print(f"模型原始响应: {response_str}")
                
                # 关键处理：进行分割后再解析
                if "```json" in response_str:
                    # 提取代码块中的内容
                    json_str = response_str.split("```json")[1].split("```")[0]
                    return json_str.strip()
                elif response_str.startswith("[") and response_str.endswith("]"):
                    # 直接是数组格式
                    return response_str
                else:
                    # 尝试提取响应中的JSON部分
                    # 查找第一个[和最后一个]之间的内容
                    start = response_str.find('[')
                    end = response_str.rfind(']')
                    if start != -1 and end != -1 and start < end:
                        json_candidate = response_str[start:end+1]
                        # 验证是否为有效的JSON
                        try:
                            ast.literal_eval(json_candidate)
                            return json_candidate
                        except:
                            pass
                    
                    # 如果没有找到有效的JSON格式，返回原始响应供进一步处理
                    return response_str
                
        except Exception as e:
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            print(f"特征处理失败 (尝试 {attempt+1}/5): {str(e)}")
            time.sleep(wait_time)
    
    print(f"特征处理失败: {feature_data}")
    return "[]"

def extract_json_from_response(response_str):
    """从LLM响应中提取JSON数组"""
    # 情况1: 已经是有效的JSON数组
    if response_str.startswith("[") and response_str.endswith("]"):
        try:
            ast.literal_eval(response_str)
            return response_str
        except:
            pass
    
    # 情况2: 包含在代码块中
    if "```json" in response_str:
        json_str = response_str.split("```json")[1].split("```")[0]
        try:
            ast.literal_eval(json_str.strip())
            return json_str.strip()
        except:
            pass
    
    # 情况3: 在响应的某一行中
    lines = response_str.split("\n")
    for line in lines:
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            try:
                ast.literal_eval(line)
                return line
            except:
                pass
    
    # 情况4: 手动提取并构建JSON数组
    # 查找第一个[和最后一个]之间的内容
    start = response_str.find('[')
    end = response_str.rfind(']')
    if start != -1 and end != -1 and start < end:
        json_candidate = response_str[start:end+1]
        try:
            ast.literal_eval(json_candidate)
            return json_candidate
        except:
            pass
    
    # 如果所有尝试都失败，返回空数组
    return "[]"

def process_single_line(line, index):
    """处理单行数据，确保结果与原始行严格对应"""
    # 处理错误行（以ERROR开头的行）
    if line.startswith("ERROR"):
        print(f"检测到错误行 [行 {index}]: {line[:50]}...")
        # 尝试从错误行中提取原始数据
        try:
            # 假设错误行格式为 "ERROR processing: {error_msg}\n原始内容: {original_json}"
            # 或者 "ERROR processing: {error_msg}"
            if "原始内容:" in line:
                original_content = line.split("原始内容:")[1].strip()
                line = original_content
                print(f"从错误行中提取原始内容 [行 {index}]: {line[:50]}...")
            else:
                # 如果无法提取原始内容，跳过该行
                print(f"无法提取原始内容，跳过错误行 [行 {index}]")
                return index, None
        except:
            print(f"解析错误行失败，跳过 [行 {index}]")
            return index, None
    
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            # 解析JSON
            try:
                line_data = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"JSON解析失败 [行 {index}] (尝试 {attempt+1}/{max_attempts}): {str(e)}\n原始内容: {line[:100]}")
                if attempt < max_attempts - 1:
                    time.sleep(1)
                    continue
                else:
                    return index, None
            
            # 验证数据结构
            if "data" not in line_data or len(line_data["data"]) < 2:
                print(f"无效数据格式 [行 {index}]: {str(line)[:50]}...")
                return index, None
            
            header_ori_dict = line_data["data"][0]
            data_ori_dict = line_data["data"][1]
            
            # 查找"对应特征"键
            feature_key = None
            for key, value in header_ori_dict.items():
                if str(value) == "对应特征":
                    feature_key = key
                    break
                    
            if not feature_key:
                print(f"未找到'对应特征'字段 [行 {index}]: {str(line)[:50]}...")
                feature_key = " "  # 如果没有找到对应特征字段，设置为空字符串
            else:
                feature = data_ori_dict.get(feature_key)
                if not feature:
                    print(f"'对应特征'字段值为空 [行 {index}], 设置为空字符串")
                    feature = " "  # 如果对应特征为空，设置为空字符串
                
            print(f"处理特征 [行 {index}]: {feature[:50]}...")
            
            # 处理特征并返回结果
            extracted_fields_response = process_feature(feature)
            print(f"提取响应 [行 {index}]: {extracted_fields_response}")
            
            # 从响应中提取JSON
            extracted_fields_str = extract_json_from_response(extracted_fields_response)
            print(f"提取的JSON字符串 [行 {index}]: {extracted_fields_str}")
            
            # 使用更安全的解析方法
            try:
                extracted_fields = ast.literal_eval(extracted_fields_str)
            except:
                # 如果ast.literal_eval失败，尝试json.loads
                try:
                    extracted_fields = json.loads(extracted_fields_str)
                except Exception as parse_error:
                    print(f"解析LLM响应失败 [行 {index}]: {str(parse_error)}")
                    if attempt < max_attempts - 1:
                        time.sleep(2)
                        continue
                    else:
                        extracted_fields = []
            
            # 添加新的键值对
            keys = list(header_ori_dict.keys())
            try:
                if keys:
                    # 尝试将键转换为数字
                    numeric_keys = [int(k) for k in keys if k.isdigit()]
                    if numeric_keys:
                        next_key = str(max(numeric_keys) + 1)
                    else:
                        next_key = str(int(max(keys)) + 1)  # 字符串键的后继
                else:
                    next_key = "0"
            except:
                next_key = str(len(keys))
                
            header_ori_dict[next_key] = '真实数据'
            data_ori_dict[next_key] = extracted_fields
            
            # 构造完整数据结构
            return index, {
                "header": header_ori_dict,
                "data": data_ori_dict
            }
            
        except Exception as e:
            print(f"处理失败 [行 {index}] (尝试 {attempt+1}/{max_attempts}): {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(2)
                continue
            else:
                import traceback
                traceback.print_exc()
                return index, None

def main():
    """主处理函数，返回执行状态码"""
    # 确保输出文件存在
    try:
        if not os.path.exists(output_path):
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("")
    except Exception as e:
        print(f"创建输出文件失败: {str(e)}")
        return 1  # 返回错误状态码
    
    # 读取输入文件
    try:
        with open(path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"读取输入文件失败: {str(e)}")
        return 1  # 返回错误状态码
    
    # 创建固定大小的结果列表
    results = [None] * len(lines)
    processed_count = 0
    failed_count = 0
    
    # 并发处理
    try:
        # 控制并发数量
        MAX_CONCURRENT = 4
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
            # 提交所有任务并保留索引
            futures = {executor.submit(process_single_line, line, idx): idx for idx, line in enumerate(lines)}
            
            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                try:
                    result_index, result = future.result()
                    # 确保索引匹配
                    if result_index == idx and result is not None:
                        results[idx] = result
                        processed_count += 1
                        print(f"成功处理 [行 {idx}] - 进度: {processed_count}/{len(lines)}")
                    else:
                        failed_count += 1
                        print(f"处理失败 [行 {idx}] - 结果不匹配")
                except Exception as e:
                    failed_count += 1
                    print(f"处理异常 [行 {idx}]: {str(e)}")
    
    except Exception as e:
        print(f"并发处理失败: {str(e)}")
        return 1  # 返回错误状态码
    
    # 写入结果
    try:
        print(f"开始写入结果...")
        with open(output_path, 'w', encoding='utf-8') as out_file:
            for i, result in enumerate(results):
                if result is not None:
                    try:
                        # 确保结构一致
                        if "data" not in result:
                            result = {"data": result}
                        out_file.write(json.dumps(result, ensure_ascii=False) + '\n')
                        print(f"已写入 [行 {i}]")
                    except Exception as e:
                        failed_count += 1
                        print(f"写入失败 [行 {i}]: {str(e)}")
                else:
                    failed_count += 1
                    print(f"跳过空结果 [行 {i}]")
    except Exception as e:
        print(f"写入输出文件失败: {str(e)}")
        return 1  # 返回错误状态码
    
    # 打印统计信息并返回状态码
    print(f"处理完成! 总行数: {len(lines)}, 成功: {processed_count}, 失败: {failed_count}")
    if failed_count > 0:
        print("警告：部分数据处理失败")
        return 2  # 部分失败状态码
    return 0  # 完全成功状态码

if __name__ == "__main__":
    # 执行主函数并退出，返回状态码
    exit_code = main()
    sys.exit(exit_code)