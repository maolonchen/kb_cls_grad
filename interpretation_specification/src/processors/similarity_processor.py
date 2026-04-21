# -*- coding: utf-8 -*-
"""
相似度比较模块

本模块用于对处理后的数据进行语义相似度比较，
通过向量嵌入和Milvus数据库搜索，找出与输入数据最相似的内容。
"""

import re
import json
import asyncio
from rich import print
from pymilvus import MilvusClient
from src.utils.api_utils import EmbeddingAPI
from config.settings import GRAD_COLLECTION_NAME, DB_PATH, MULTI_TREE_PATH, SIMILAR_COMPARE_PATH


# 初始化全局变量
# milvus_client = MilvusClient(DB_PATH)
milvus_client = MilvusClient(uri=DB_PATH)
collection_name = GRAD_COLLECTION_NAME

# def match_identifier(text):
#     """匹配标识符，优先匹配带括号的模式，如果括号内非空则直接匹配整个括号"""
#     # # 首先尝试匹配各种括号形式（英文圆括号、中文圆括号、方括号）
#     # # 优先匹配带括号的模式，确保括号内有内容
#     # bracket_pattern = r'(\([^)]+\)|（[^）]+）|\[[^\]]+\])\s*(.*)'
#     # match = re.search(bracket_pattern, text.strip())
#     # if match:
#     #     identifier = match.group(1)
#     #     content = match.group(2).strip()
#     #     # 验证括号内不只是空格
#     #     bracket_content = identifier[1:-1].strip()  # 去掉括号，提取中间内容
#     #     if bracket_content:  # 如果括号内有内容
#     #         return identifier, content
    
#     # 如果没有匹配到带括号的模式，尝试匹配其他模式
#     # 注意：匹配顺序很重要，需要从更具体的模式开始
#     patterns = [
#         # 匹配 A1-2 这种格式
#         r'([A-Z]\d+(?:-\d+)*)\s*(.*)',
#         # # 匹配 1.2.3 这种格式（多个数字点）
#         # r'(\d+(?:\.\d+)+[、.]?)\s*(.*)',
#         # # 匹配 A1. 这种格式
#         # r'([A-Za-z0-9IVX\u2160-\u216F\u2170-\u217F]+\.+)\s*(.*)',
#         # # 匹配 A1) 这种格式
#         # r'([A-Za-z0-9IVX\u2160-\u216F\u2170-\u217F]+\))\s*(.*)',
#         # # 匹配 A1 这种格式（字母+数字）
#         # r'([A-Z][A-Z0-9\u2160-\u216F\u2170-\u217F]*)\s*(.*)',
#         # # 匹配 1. 2. 这种格式
#         # r'(\d+[、.])\s*(.*)',
#         # # 匹配 ①②③等编号
#         # r'([\u2460-\u2473\u2474-\u2487])\s*(.*)',
#         # # 匹配 一、二、等中文编号
#         # r'([一二三四五六七八九十]+[、.])\s*(.*)',
#         # # 匹配最后剩下的数字
#         # r'(\d+)\s*(.*)'
#     ]
    
#     for pattern in patterns:
#         match = re.search(pattern, text.strip())
#         if match:
#             identifier = match.group(1).rstrip('、.')  # 去除结尾的标点
#             content = match.group(2).strip()
#             return identifier, content
    
#     return None, None

# 字符数限制和批处理大小
char_count = 9
BATCH_SIZE = 8
OUTPUT_FILE = SIMILAR_COMPARE_PATH

pattern = re.compile(r'^(?:[A-Z]\s+)?([A-Z]\d+(?:-\d+)*)(.*)')

def extract_and_combine_line(line, char_count):
    """
    提取该行所有匹配项，并拼接成一句话
    
    参数:
        line: JSON格式的行数据
        char_count: 每个部分保留的字符数
    
    返回:
        tuple: (原始列表, 拼接后的查询文本, 标识符列表) 或 (None, None, None)
    """
    try:
        data = json.loads(line.strip())
        items = data.get("data", [])
        if len(items) < 2:
            return None, None, None

        target_dict = items[1]
        parts = []
        identifiers = []
        for value in target_dict.values():
            match = pattern.match(value.strip())
            if match:
                code = match.group(1)
                text_part = match.group(2).strip()[:char_count]
                combined = f"{code} {text_part}".strip()
                if combined:
                    parts.append(combined)
                    identifiers.append(code)

        if not parts:
            return None, None, None

        query_text = "；".join(parts)
        return parts, query_text, identifiers

    except Exception as e:
        print(f"解析错误: {e}")
        return None, None, None

async def process_search_results(search_results, batch_infos, output_file):
    """
    处理搜索结果并写入文件
    
    参数:
        search_results: Milvus搜索结果
        batch_infos: 批处理元数据
        output_file: 输出文件对象
    """
    for idx, (line_num, orig_line, orig_list, q_text, ori_ids) in enumerate(batch_infos):
        print(f"\n=== 第 {line_num} 行解析结果 ===")
        print(f"原始行内容: {orig_line}")
        print(f"提取数据: {orig_list}")
        print(f"查询文本: '{q_text}'")
        ori_ids = ori_ids[-1:] if len(ori_ids) > 1 else ori_ids
        print(f"提取标识: {ori_ids}")
        
        if idx < len(search_results):
            print("相似内容:")
            similar_items = extract_similar_items(search_results[idx])
            
            # 准备输出数据
            result = {
                "ori_data": ori_ids,
                "similar_data": similar_items
            }
            
            # 写入输出文件
            json.dump(result, output_file, ensure_ascii=False)
            output_file.write("\n")
        else:
            print("  未返回搜索结果")

def extract_similar_items(search_result):
    """
    从搜索结果中提取相似项目
    
    参数:
        search_result: Milvus搜索结果
        
    返回:
        相似项目列表
    """
    similar_items = []
    if search_result:
        for item in search_result:
            similar_items.append({
                "id": item.get("id"),
                "distance": item.get("distance"),
                "text": re.sub(r'\b(L\d+)\b', r'等级是：\1', item.get("entity", {}).get("text"))
            })
    return similar_items

async def process_batch(batch_lines, line_numbers):
    """
    处理一批数据
    
    参数:
        batch_lines: 批量行数据
        line_numbers: 行号列表
        
    返回:
        处理结果
    """
    batch_infos = []
    query_texts = []
    
    # 提取查询文本
    for line_num, line in zip(line_numbers, batch_lines):
        orig_list, q_text, ori_ids = extract_and_combine_line(line, char_count)
        if q_text:
            batch_infos.append((line_num, line, orig_list, q_text, ori_ids))
            query_texts.append(q_text)
    # print("查询文本:", query_texts)
    if not query_texts:
        print("----------------------未找到查询文本")
        return [], []
    
    # 获取嵌入向量
    async with EmbeddingAPI() as embedding_api:
        query_embeddings = await embedding_api.get_embeddings_batch(query_texts)
    
    # 在Milvus中搜索
    search_results = milvus_client.search(
        collection_name=collection_name,
        data=query_embeddings,
        limit=4,
        output_fields=["text", "id"]
    )
    
    return search_results, batch_infos

async def main():
    """
    主函数：处理数据并进行相似度比较
    """
    # 读取数据文件
    try:
        with open(MULTI_TREE_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"错误: 文件不存在: {MULTI_TREE_PATH}")
        return

    # 打开输出文件
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as output_file:
        # 分批处理数据
        for i in range(0, len(lines), BATCH_SIZE):
            batch_lines = lines[i:i + BATCH_SIZE]
            line_numbers = list(range(i + 1, i + len(batch_lines) + 1))

            # 对批次中每一行提取信息，记录匹配状态
            line_infos = []  # (line_num, line, orig_list, q_text, ori_ids) 或 None（不匹配）
            query_texts = []
            query_indices = []  # 在 line_infos 中的索引，用于对齐搜索结果

            for idx, (line_num, line) in enumerate(zip(line_numbers, batch_lines)):
                orig_list, q_text, ori_ids = extract_and_combine_line(line, char_count)
                if q_text:
                    line_infos.append((line_num, line, orig_list, q_text, ori_ids))
                    query_texts.append(q_text)
                    query_indices.append(idx)
                else:
                    line_infos.append(None)

            # 仅对匹配行做嵌入和搜索
            search_results = []
            if query_texts:
                async with EmbeddingAPI() as embedding_api:
                    query_embeddings = await embedding_api.get_embeddings_batch(query_texts)
                search_results = milvus_client.search(
                    collection_name=collection_name,
                    data=query_embeddings,
                    limit=4,
                    output_fields=["text", "id"]
                )

            # 按原始顺序写入所有行的结果
            search_idx = 0
            for info in line_infos:
                if info is not None:
                    line_num, orig_line, orig_list, q_text, ori_ids = info
                    ori_ids = ori_ids[-1:] if len(ori_ids) > 1 else ori_ids
                    print(f"\n=== 第 {line_num} 行解析结果 ===")
                    print(f"原始行内容: {orig_line}")
                    print(f"提取数据: {orig_list}")
                    print(f"查询文本: '{q_text}'")
                    print(f"提取标识: {ori_ids}")

                    similar_items = []
                    if search_idx < len(search_results):
                        print("相似内容:")
                        similar_items = extract_similar_items(search_results[search_idx])
                    else:
                        print("  未返回搜索结果")
                    search_idx += 1

                    result = {"ori_data": ori_ids, "similar_data": similar_items}
                else:
                    # 不匹配的行写入占位结果，保持与 multi_tree.txt 行对齐
                    result = {"ori_data": [], "similar_data": []}

                json.dump(result, output_file, ensure_ascii=False)
                output_file.write("\n")

            print(f"已处理 {min(i + BATCH_SIZE, len(lines))}/{len(lines)} 行")

if __name__ == "__main__":
    asyncio.run(main())
