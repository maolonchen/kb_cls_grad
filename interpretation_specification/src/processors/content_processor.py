import json
import re
import ast
import asyncio
import aiohttp
from rich import print
from src.prompts.llm_prompts import CONTENT_TO_GRAD_PROMPT
from config.settings import FINAL_PATH, POST_FINAL_PATH, LLM_API_URL, HEADERS, API_PAYLOAD, MODEL_NAME
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


# 模型API配置
API_URL = LLM_API_URL

# 输入输出文件路径
input_file = FINAL_PATH
output_file = POST_FINAL_PATH

# 并发控制参数
CONCURRENCY_LIMIT = 10  # 最大并发请求数
REQUEST_TIMEOUT = 120   # 请求超时时间（秒）
MAX_RETRIES = 10        # 最大重试次数

def get_chinese_number(n):
    """将数字转换为中文数字表示"""
    chinese_numbers = ["零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十"]
    if n <= 10:
        return chinese_numbers[n]
    elif n <= 99:
        # 处理11-99的中文表示
        tens = n // 10
        units = n % 10
        if tens == 1:
            return f"十{chinese_numbers[units]}" if units > 0 else "十"
        else:
            return f"{chinese_numbers[tens]}十{chinese_numbers[units]}" if units > 0 else f"{chinese_numbers[tens]}十"
    else:
        return str(n)  # 超过99直接使用阿拉伯数字

def transform_json(original_json, new_str):
    # 解析原始JSON
    original = json.loads(original_json)
    
    # 找到"对应特征"的位置
    header = original["header"]
    feature_index = None
    for key, value in header.items():
        if value == "对应特征":
            feature_index = int(key)
            break
    
    if feature_index is None:
        raise ValueError("原始JSON中未找到'对应特征'字段")
    
    # 获取前一个分类字段的名称
    prev_key = str(feature_index - 1)
    prev_class_name = header.get(prev_key, "")
    
    # 提取前一个分类的层级数字
    match = re.search(r'([一二三四五六七八九十]+)级分类', prev_class_name)
    if match:
        # 从中文数字转换为阿拉伯数字
        chinese_num = match.group(1)
        chinese_to_num = {"一":1, "二":2, "三":3, "四":4, "五":5, "六":6, "七":7, "八":8, "九":9, "十":10, "十一":11, "十二":12}
        prev_level = chinese_to_num.get(chinese_num, feature_index)
        new_level = prev_level + 1
        new_level_name = f"{get_chinese_number(new_level)}级分类"
    else:
        # 如果没有匹配到，使用位置推断
        new_level = feature_index + 1
        new_level_name = f"{get_chinese_number(new_level)}级分类"
    
    # 创建新的header结构
    new_header = {}
    new_data = {}
    
    # 重建header和data
    new_key_index = 0
    for key in sorted(header.keys(), key=int):
        key_int = int(key)
        
        # 在"对应特征"前插入新分类
        if key_int == feature_index:
            new_header[str(new_key_index)] = new_level_name
            new_data[str(new_key_index)] = new_str
            new_key_index += 1
        
        # 复制原有字段
        new_header[str(new_key_index)] = header[key]
        new_data[str(new_key_index)] = original["data"][key]
        new_key_index += 1
    
    return json.dumps({"header": new_header, "data": new_data}, ensure_ascii=False)

@retry(
    retry=retry_if_exception(lambda e: isinstance(e, (aiohttp.ClientError, asyncio.TimeoutError))),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    before_sleep=lambda retry_state: print(f"重试 {retry_state.attempt_number}/{MAX_RETRIES} 次..."),
    reraise=True
)


async def call_model_api(session, prompt):
    """异步调用模型API并返回结果"""
    payload = API_PAYLOAD(MODEL_NAME, prompt)
    
    try:
        async with session.post(
            API_URL, 
            headers=HEADERS, 
            json=payload,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        ) as response:
            response.raise_for_status()
            data = await response.json()
            return data['choices'][0]['message']['content']
    except Exception as e:
        print(f"API调用失败: {str(e)}")
        raise

async def process_line(session, semaphore, line, index):
    """处理单行数据"""
    async with semaphore:
        try:
            data = json.loads(line)
            # 查找"对应特征"列
            feature_key = None
            for key, value in data['header'].items():
                if value == '对应特征':
                    feature_key = key
                    break
            
            if not feature_key:
                print(f"行 {index}: 未找到'对应特征'列")
                return index, [line]  # 返回列表以保持统一格式
            
            post_data = data['data'].get(feature_key, "")
            print(f"[green]行 {index}: ============================================================[/green]")
            print(post_data)
            prompt = CONTENT_TO_GRAD_PROMPT(post_data)
            
            # 调用模型API
            model_response = await call_model_api(session, prompt)
            model_response = model_response.split('</think>\n\n')[1]
            # 处理可能的额外前缀
            if '```json' in model_response:
                model_response = model_response.split('```json')[1].split('```')[0].strip()
            elif '```' in model_response:
                model_response = model_response.split('```')[1].strip()
            
            if not model_response:
                return index, [line]  # 返回列表以保持统一格式
            
            # 解析模型响应
            try:
                # 尝试处理可能的额外前缀
                if '```json' in model_response:
                    json_part = model_response.split('```json')[1].split('```')[0].strip()
                    data_ = json.loads(json_part)
                else:
                    data_ = json.loads(model_response)
                
                # 如果返回的是字符串形式的字典
                if isinstance(data_, str):
                    data_ = json.loads(data_)
            except (json.JSONDecodeError, SyntaxError):
                # 尝试使用ast安全解析
                try:
                    data_ = ast.literal_eval(model_response)
                except:
                    print(f"行 {index}: 无法解析模型响应: {model_response}")
                    return index, [line]  # 返回列表以保持统一格式
            
            print(f"行 {index}: 模型返回: {data_}")
            
            if list(data_.keys())[0] == "1":
                # 处理多个分类的情况
                class_strs = data_["1"].split(';')
                processed_lines = []
                
                # 保存原始行数据，用于后续处理
                original_data = json.loads(line)
                
                # 获取原始特征字段的值
                feature_key_orig = None
                for key, value in original_data['header'].items():
                    if value == '对应特征':
                        feature_key_orig = key
                        break
                
                if feature_key_orig is None:
                    print(f"行 {index}: 未找到'对应特征'列")
                    return index, [line]
                
                feature_value_orig = original_data['data'].get(feature_key_orig, "")
                
                # 移除所有分类标识
                # 使用正则表达式匹配分类标识（如G6-5-1, 2-1-4-5-1等）
                cleaned_feature_value = re.sub(r'[A-Za-z0-9\-]+(?:\s|:|：|;|；)?', '', feature_value_orig)
                
                # 清理多余的分号和空格
                cleaned_feature_value = re.sub(r';\s*;', ';', cleaned_feature_value)  # 处理连续分号
                cleaned_feature_value = re.sub(r'^\s*;\s*', '', cleaned_feature_value)  # 移除开头的分号
                cleaned_feature_value = re.sub(r'\s*;\s*$', '', cleaned_feature_value)  # 移除结尾的分号
                cleaned_feature_value = re.sub(r'\s+', ' ', cleaned_feature_value)  # 合并多个空格
                
                for class_str in class_strs:
                    class_str = class_str.strip()
                    print(f"\n1.class_str:", class_str)
                    if class_str:  # 确保分类字符串非空
                        try:
                            # 初始化class_id变量
                            class_id = None
                            
                            # 提取分类标识（如"G6-5-1"或"2-1-4-5-1"）
                            # 匹配字母数字和横线组成的模式
                            match = re.search(r'([A-Za-z0-9\-]+)\s', class_str)
                            if match:
                                class_id = match.group(1)
                                print(f"\n2.class_id:", class_id)
                                # 只保留分类标识和简短描述
                                short_desc = class_str.split(maxsplit=1)[1] if ' ' in class_str else class_id
                                short_desc = re.split(r'[：:；;]', short_desc)[0].strip()
                                class_str = f"{class_id} {short_desc}"
                                print(f"\n3.short_desc:", short_desc)
                                print(f"\n4.class_str:", class_str)
                            
                            # 转换JSON，创建新行
                            tradata = transform_json(line, class_str)
                            print(f"\n5.tradata:", tradata)
                            tradata_dict = json.loads(tradata)
                            print(f"\n6.tradata_dict:", tradata_dict)
                            
                            # 找到"对应特征"字段的键
                            feature_key = None
                            for key, value in tradata_dict['header'].items():
                                if value == '对应特征':
                                    feature_key = key
                                    break
                            
                            # 如果找到对应特征字段，设置清理后的值
                            if feature_key:
                                tradata_dict['data'][feature_key] = cleaned_feature_value
                                print(f"\n7.cleaned_feature_value:", cleaned_feature_value)
                            
                            # 重新序列化为JSON字符串
                            tradata = json.dumps(tradata_dict, ensure_ascii=False)
                            print(f"行 {index}: 转换后的数据: {tradata}")
                            processed_lines.append(tradata + '\n')
                        except Exception as e:
                            print(f"行 {index}: 转换出错: {str(e)}")
                            processed_lines.append(line)  # 出错时保留原行
                
                # 如果没有有效分类，保留原行
                if not processed_lines:
                    processed_lines.append(line)
                
                return index, processed_lines
            else:
                return index, [line]  # 返回列表以保持统一格式
                
        except Exception as e:
            print(f"行 {index}: 处理出错: {str(e)}")
            return index, [line]  # 返回列表以保持统一格式

async def process_data():
    """异步处理JSONL文件数据"""
    # 读取所有行
    with open(input_file, 'r') as infile:
        lines = infile.readlines()
    
    # 创建信号量控制并发
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    # 创建HTTP会话
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i, line in enumerate(lines):
            task = asyncio.create_task(process_line(session, semaphore, line, i))
            tasks.append(task)
        
        # 等待所有任务完成
        results = await asyncio.gather(*tasks)
    
    # 按原始顺序排序结果
    results.sort(key=lambda x: x[0])
    
    # 展开所有行（处理一行变多行的情况）
    all_lines = []
    for _, lines_list in results:
        all_lines.extend(lines_list)
    
    # 写入输出文件
    with open(output_file, 'w') as outfile:
        for processed_line in all_lines:
            outfile.write(processed_line)

def main():
    asyncio.run(process_data())
    print("数据处理完成！")

if __name__ == "__main__":
    main()