# scripts/copy_collection.py (最终修复版 - 使用 search 提取向量)
import sys
import os
import traceback
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, utility

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MILVUS_HOST = "192.168.10.15"
MILVUS_PORT = "19530"

SOURCE_COLLECTION_NAME = "res_001_classification"
TARGET_COLLECTION_NAME = "collection1"
BATCH_SIZE = 1000

print(f"尝试连接到 Milvus: {MILVUS_HOST}:{MILVUS_PORT}")
connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)

try:
    utility.list_collections()
    print("✅ 连接成功!")
except Exception as e:
    print(f"❌ 连接失败: {e}")
    sys.exit(1)

if not utility.has_collection(SOURCE_COLLECTION_NAME):
    print(f"❌ 源集合不存在")
    sys.exit(1)

source_collection = Collection(name=SOURCE_COLLECTION_NAME)
source_schema = source_collection.schema
print(f"源集合结构: {source_schema}")

# 获取 consistency_level 和索引
source_desc = source_collection.describe()
source_consistency = source_desc.get("consistency_level", "Bounded")
print(f"源一致性级别: {source_consistency}")

vector_field_name = None
for field in source_schema.fields:
    if field.dtype.name == "FLOAT_VECTOR":
        vector_field_name = field.name
        break

if not vector_field_name:
    print("❌ 未找到向量字段")
    sys.exit(1)

# 删除目标集合
if utility.has_collection(TARGET_COLLECTION_NAME):
    utility.drop_collection(TARGET_COLLECTION_NAME)

# 构建 schema
sink_fields = []
for field in source_schema.fields:
    kwargs = {
        "name": field.name,
        "dtype": field.dtype,
        "is_primary": field.is_primary,
        "auto_id": field.auto_id,
        "description": field.description,
    }
    if hasattr(field, 'params') and isinstance(field.params, dict):
        kwargs.update(field.params)
    sink_fields.append(FieldSchema(**kwargs))

sink_schema = CollectionSchema(
    fields=sink_fields,
    description=source_schema.description,
    enable_dynamic_field=source_schema.enable_dynamic_field
)

# 创建目标集合
sink_collection = Collection(
    name=TARGET_COLLECTION_NAME,
    schema=sink_schema,
    consistency_level=source_consistency
)
print(f"✅ 目标集合 '{TARGET_COLLECTION_NAME}' 创建成功")

# ========== 关键修复：使用 search() 提取向量 ==========
print("加载源集合...")
source_collection.load()

# 获取总数量
source_collection.flush()
total_count = source_collection.num_entities
print(f"源集合总实体数: {total_count}")

if total_count == 0:
    print("⚠️ 源集合为空，跳过迁移")
else:
    # 构造一个虚拟查询向量（维度必须匹配）
    dummy_vector = [0.0] * 4096  # 替换为实际 dim

    # 全量搜索（获取所有向量）
    print("正在通过 search() 提取所有向量...")
    search_params = {"metric_type": "IP", "params": {"nprobe": 1}}
    results = source_collection.search(
        data=[dummy_vector],
        anns_field=vector_field_name,
        param=search_params,
        limit=total_count,
        output_fields=[f.name for f in source_schema.fields]
    )

    if not results or len(results[0]) == 0:
        print("❌ 搜索返回空结果")
        sys.exit(1)

    hits = results[0]  # TopK 结果列表

    # 转换为列式数据
    field_names_in_order = [field.name for field in source_schema.fields]
    column_data = [[] for _ in field_names_in_order]

    for hit in hits:
        entity = hit.entity
        for i, field_name in enumerate(field_names_in_order):
            column_data[i].append(entity.get(field_name))

    # 插入目标集合
    sink_collection.insert(column_data)
    print(f"✅ 成功插入 {len(hits)} 条记录")

# 刷新
sink_collection.flush()
final_count = sink_collection.num_entities
print(f"目标集合实体总数: {final_count}")

# 创建索引
if vector_field_name:
    # 尝试复用源索引
    try:
        idx_info = source_collection.indexes[0]
        index_params = idx_info.params
    except:
        index_params = {"index_type": "FLAT", "metric_type": "IP", "params": {}}
    
    print(f"创建索引: {index_params}")
    sink_collection.create_index(vector_field_name, index_params)
    print("✅ 索引创建成功")

print("🎉 复制完成！现在向量应该存在了。")