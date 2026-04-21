#!/usr/bin/env python3
# clean_milvus_non_leaf.py

import os
import json
from pymilvus import MilvusClient
import time
from app.core.config import DatabaseConfig

# 获取数据库路径，优先使用环境变量中的用户数据库路径
DB_PATH = os.getenv(
    'USER_DB_PATH', DatabaseConfig.uri)

def connect_to_milvus():
    """连接到Milvus数据库"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            milvus_client = MilvusClient(uri=DB_PATH)
            return milvus_client
        except Exception as e:
            print(f"尝试 {attempt + 1} 连接失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print("无法连接到Milvus数据库，可能正在被其他进程使用")
                raise e

def extract_classification_path(data_dict):
    """
    从记录中提取分类路径
    返回分类值列表和完整路径字符串
    """
    try:
        data = data_dict.get("data", {})
        header = data_dict.get("header", {})
        
        if not data or not header:
            return [], ""
            
        # 计算分类字段数量
        classification_fields = []
        for key, value in header.items():
            if "分类" in str(value):
                classification_fields.append(int(key))
        
        # 按数字顺序排序
        classification_fields.sort()
        
        # 提取分类值
        path_values = []
        for field_index in classification_fields:
            key = str(field_index)
            value = data.get(key, None)
            # 如果遇到None或空值，停止构建路径
            if value is None or not str(value).strip():
                break
            path_values.append(str(value).strip())
            
        # 构建路径字符串用于比较
        path_string = "->".join(path_values)
        return path_values, path_string
        
    except Exception as e:
        print(f"提取分类路径时出错: {e}")
        return [], ""

def identify_non_leaf_records(records):
    """
    识别非叶节点记录
    基于以下原则：如果存在更深层次的分类记录，则较浅的分类记录为非叶节点
    """
    # 首先提取所有记录的分类路径
    record_paths = {}  # id -> (path_values, path_string)
    path_depths = {}   # path_string -> depth
    
    for record in records:
        try:
            # 解析text字段中的JSON数据
            text_data = record.get('text', '{}')
            if isinstance(text_data, str):
                data_dict = json.loads(text_data)
            else:
                data_dict = text_data
                
            path_values, path_string = extract_classification_path(data_dict)
            
            if path_values:  # 只处理有分类路径的记录
                record_paths[record['id']] = (path_values, path_string)
                path_depths[path_string] = len(path_values)
                
        except json.JSONDecodeError:
            print(f"记录 {record.get('id')} JSON解析失败")
        except Exception as e:
            print(f"处理记录 {record.get('id')} 时出错: {e}")
    
    # 识别非叶节点
    non_leaf_ids = set()
    
    # 对于每条记录，检查是否存在比它更深的扩展路径
    for record_id, (path_values, path_string) in record_paths.items():
        depth = len(path_values)
        
        # 检查是否存在任何以当前路径为前缀且更深的路径
        is_extended = False
        for other_path_string, other_depth in path_depths.items():
            if other_path_string != path_string and other_depth > depth:
                # 检查other_path_string是否以path_string为前缀
                if other_path_string.startswith(path_string + "->"):
                    is_extended = True
                    break
        
        # 如果存在更深层次的扩展路径，则当前记录为非叶节点
        if is_extended:
            non_leaf_ids.add(record_id)
    
    return list(non_leaf_ids)

def clean_collection(client, collection_name):
    """清理指定集合中的非叶节点数据"""
    print(f"开始清理集合 {collection_name} 中的非叶节点数据...")
    
    try:
        client.load_collection(collection_name=collection_name)
        
        # 获取集合统计信息
        stats = client.get_collection_stats(collection_name)
        row_count = stats.get('row_count', 0)
        print(f"集合统计信息: {stats}")
        
        if row_count == 0:
            print("集合为空，无需清理")
            return
        
        # 分批获取所有记录，避免一次获取太多数据
        all_records = []
        batch_size = 100
        offset = 0
        
        while offset < row_count:
            # 使用id >= 0作为过滤器，配合limit和offset来分批获取数据
            batch_records = client.query(
                collection_name=collection_name,
                filter="id >= 0",
                limit=batch_size,
                offset=offset,
                output_fields=["id", "text"]
            )
            
            if not batch_records:
                break
                
            all_records.extend(batch_records)
            offset += len(batch_records)
            
            # 如果返回的记录数少于请求的数量，说明已经获取完所有记录
            if len(batch_records) < batch_size:
                break
        
        print(f"总共找到 {len(all_records)} 条记录")
        
        # 显示一些样本数据以便分析
        print("\n样本数据分析:")
        sample_count = min(10, len(all_records))
        for i in range(sample_count):  # 显示前10条记录
            record = all_records[i]
            try:
                text_data = record.get('text', '{}')
                if isinstance(text_data, str):
                    data_dict = json.loads(text_data)
                else:
                    data_dict = text_data
                    
                path_values, path_string = extract_classification_path(data_dict)
                
                print(f"  记录 {i+1} (ID: {record['id']}):")
                print(f"    分类路径: {' -> '.join(path_values)}")
            except Exception as e:
                print(f"    记录 {i+1} 解析失败: {e}")
        print()
        
        # 识别非叶节点记录
        non_leaf_ids = identify_non_leaf_records(all_records)
        print(f"识别出 {len(non_leaf_ids)} 条非叶节点记录需要删除")
        
        # 显示将要删除的记录详情（最多显示10条）
        displayed_count = 0
        for record in all_records:
            if record['id'] in non_leaf_ids and displayed_count < 10:
                try:
                    text_data = record.get('text', '{}')
                    if isinstance(text_data, str):
                        data_dict = json.loads(text_data)
                    else:
                        data_dict = text_data
                        
                    path_values, path_string = extract_classification_path(data_dict)
                    path_display = " -> ".join(path_values)
                    
                    print(f"  将删除 ID: {record['id']}, 路径: {path_display}")
                except:
                    text_preview = str(record.get('text', ''))[:100] + "..." if len(str(record.get('text', ''))) > 100 else str(record.get('text', ''))
                    print(f"  将删除 ID: {record['id']}, TEXT预览: {text_preview}")
                displayed_count += 1
                
        if len(non_leaf_ids) > 10:
            print(f"  ... 还有 {len(non_leaf_ids) - 10} 条记录")
        
        # 执行删除操作
        if non_leaf_ids:
            # 分批删除，避免一次性删除太多记录
            delete_batch_size = 50
            deleted_count = 0
            
            for i in range(0, len(non_leaf_ids), delete_batch_size):
                batch_ids = non_leaf_ids[i:i + delete_batch_size]
                # Milvus中通过ID删除记录
                expr = f"id in {batch_ids}"
                delete_result = client.delete(
                    collection_name=collection_name,
                    filter=expr
                )
                deleted_count += len(batch_ids)
                print(f"已删除批次，包含 {len(batch_ids)} 条记录")
            
            print(f"删除成功，总共删除了 {deleted_count} 条记录")
        else:
            print("没有发现需要删除的非叶节点记录")
            
    except Exception as e:
        print(f"清理集合 {collection_name} 时出错: {e}")
        import traceback
        traceback.print_exc()

def main():
    """主函数"""
    print("开始连接到Milvus数据库...")
    try:
        client = connect_to_milvus()
        print("成功连接到Milvus数据库")
        
        # 获取所有集合
        collections = client.list_collections()
        print(f"所有集合: {collections}")
        
        # 处理指定的集合
        target_collections = [c for c in collections if '_classification' in c]
        if not target_collections:
            target_collections = collections
            
        for collection_name in target_collections:
            print(f"\n{'='*50}")
            clean_collection(client, collection_name)
            
        print(f"\n{'='*50}")
        print("所有集合清理完成")
        
    except Exception as e:
        print(f"执行过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()