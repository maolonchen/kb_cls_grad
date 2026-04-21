# scripts/test_milvus_connection.py
import sys
import os
# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymilvus import MilvusClient

# 手动定义连接参数
MILVUS_HOST = "192.168.10.15"
MILVUS_PORT = "19530"
MILVUS_URI = f"http://{MILVUS_HOST}:{MILVUS_PORT}"

def test_connection():
    try:
        print(f"尝试连接到Milvus: {MILVUS_URI}")
        client = MilvusClient(uri=MILVUS_URI)
        collections = client.list_collections()
        print(f"连接成功! 当前集合: {collections}")
        return True
    except Exception as e:
        print(f"连接失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_connection()