#!/usr/bin/env python3
# view_vector_db.py

import os
import sys
from pymilvus import MilvusClient
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.core.config import DatabaseConfig

# 获取数据库路径，优先使用环境变量中的用户数据库路径
DB_PATH = os.getenv(
    'USER_DB_PATH', DatabaseConfig.uri)

# 添加重试机制
max_retries = 3
n = 30  # 查询的数量
for attempt in range(max_retries):
    try:
        milvus_client = MilvusClient(DB_PATH)
        break
    except Exception as e:
        print(f"尝试 {attempt + 1} 连接失败: {e}")
        if attempt < max_retries - 1:
            time.sleep(2)
        else:
            print("无法连接到Milvus数据库，可能正在被其他进程使用")
            exit(1)

# 打印数据库中的所有集合
print("测试数据库连接...")
try:
    collections = milvus_client.list_collections()
    print("所有集合:", collections)

    # bbbbb_general_knowledge   bbbbb_classification    bbbbb_narrative_classification    aaaaa_classification    aaaaa_narrative_classification
    collection_name = '_2_classification'

    if collection_name in collections:
        print(f"\n查询集合 {collection_name}...")

        # 加载集合到内存
        print("正在加载集合...")
        milvus_client.load_collection(collection_name=collection_name)
        
        # 获取统计信息
        stats = milvus_client.get_collection_stats(collection_name)
        print("统计信息:", stats)

        # 尝试简单的ID查询
        try:
            results = milvus_client.query(
                collection_name=collection_name,
                filter="id >= 0",
                limit=n,
                output_fields=["id", "text", "items", "vectorizing_text"]  # 还有vector
            )
            print("ID查询结果:", results)
        except Exception as e:
            print("ID查询失败:", e)

        # 尝试查询所有字段
        try:
            results = milvus_client.query(
                collection_name=collection_name,
                filter="",
                limit=n,
                output_fields=["*"]  # 查询所有字段包括vector和items
            )
            print(f"\n前{n}条完整记录:")
            for i, result in enumerate(results):
                print(f"记录 {i+1}:")
                print(f"  ID: {result.get('id', 'N/A')}")
                print(f"  TEXT: {result.get('text', 'N/A')[:1000]}...")
                items_str = result.get('items', 'N/A')
                print(f"  ITEMS: {items_str[:30]}..." if len(
                    items_str) > 30 else f"  ITEMS: {items_str}")
                print(f"  vectorizing_text: {result.get('vectorizing_text', 'N/A')}")
                vector_data = result.get('vector', [])
                print(f"  VECTOR: 长度 {len(vector_data)}" +
                      (f", 前10个元素: {vector_data[:30]}" if vector_data else ""))
                print()

            # 新增：搜索包含"G6-5-2"的记录
            print("\n搜索包含'G6-5-2'的记录:")
            try:
                results_with_keyword = milvus_client.query(
                    collection_name=collection_name,
                    filter="text like '%G6-5-2%'",
                    limit=100,
                    output_fields=["*"]
                )
                
                if results_with_keyword:
                    print(f"找到 {len(results_with_keyword)} 条包含'G6-5-2'的记录:")
                    for i, result in enumerate(results_with_keyword):
                        print(f"匹配记录 {i+1}:")
                        print(f"  ID: {result.get('id', 'N/A')}")
                        print(f"  TEXT: {result.get('text', 'N/A')}")
                        print(f"  ITEMS: {result.get('items', 'N/A')}")
                        print(f"  vectorizing_text: {result.get('vectorizing_text', 'N/A')}")
                        print()
                else:
                    print("未找到包含'G6-5-2'的记录")
            except Exception as e:
                print(f"搜索包含'G6-5-2'的记录时出错: {e}")
                # 如果LIKE查询不支持，尝试获取所有记录并用Python过滤
                print("尝试在已有记录中搜索'G6-5-2'...")
                count = 0
                for i, result in enumerate(results):
                    text_content = result.get('text', '')
                    if 'G6-5-2' in text_content:
                        print(f"包含'G6-5-2'的记录 {i+1}:")
                        print(f"  ID: {result.get('id', 'N/A')}")
                        print(f"  TEXT: {text_content[:1000]}...")
                        print(f"  ITEMS: {result.get('items', 'N/A')}")
                        count += 1
                if count == 0:
                    print("在前{n}条记录中未找到包含'G6-5-2'的记录")
        except Exception as e:
            print("查询所有字段失败:", e)
    else:
        print(f"集合 {collection_name} 不存在")

except Exception as e:
    print(f"查询过程中出错: {e}")

# 注意：使用完毕后卸载集合以释放资源
try:
    milvus_client.release_collection(collection_name=collection_name)
    print(f"\n已释放集合 {collection_name}")
except:
    pass