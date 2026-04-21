# """
# 数据识别服务
# 处理AI数据识别逻辑
# """

# import logging
# import asyncio
# import re
# from typing import List, Optional
# from pymilvus import MilvusClient
# from app.schemas.knowledge_base import (
#     DataRecognitionRequest,
#     TableAIScanResultDto,
#     FieldAIScanResultDto,
#     FieldDataDto
# )
# from app.core.config import DatabaseConfig
# from app.core.vectoring import VectorClient

# logger = logging.getLogger(__name__)


# class DataRecognitionService:
#     """数据识别服务类"""

#     def __init__(self):
#         """初始化服务"""
#         self.db_path = DatabaseConfig.path
#         self.milvus_client = MilvusClient(self.db_path)

#     # def _find_relevant_collections(self) -> dict:
#     #     """
#     #     查找所有相关的集合，包括 *_classification（排除 *_narrative_classification）和 *_general_knowledge

#     #     Returns:
#     #         dict: 包含分类集合和通用知识集合的字典
#     #     """
#     #     try:
#     #         collections = self.milvus_client.list_collections()
#     #     except Exception as e:
#     #         logger.error(f"获取集合列表失败: {e}")
#     #         collections = []

#     #     classification_collections = []
#     #     general_knowledge_collections = []

#     #     for collection in collections:
#     #         # 匹配 *_classification 但排除 *_narrative_classification
#     #         if (collection.endswith('_classification') and
#     #             not collection.endswith('_narrative_classification')):
#     #             classification_collections.append(collection)
#     #         # 匹配 *_general_knowledge
#     #         elif collection.endswith('_general_knowledge'):
#     #             general_knowledge_collections.append(collection)

#     #     return {
#     #         'classification': classification_collections,
#     #         'general_knowledge': general_knowledge_collections
#     #     }
#     def _find_relevant_collections(self, specification_uid: str = None) -> dict:
#         """
#         查找相关的集合，包括 *_classification（排除 *_narrative_classification）和 *_general_knowledge
#         如果提供了specification_uid，则只返回与该规范相关的集合

#         Args:
#             specification_uid: 规范UID，如果提供则只查找相关集合

#         Returns:
#             dict: 包含分类集合和通用知识集合的字典
#         """
#         try:
#             collections = self.milvus_client.list_collections()
#         except Exception as e:
#             logger.error(f"获取集合列表失败: {e}")
#             collections = []

#         classification_collections = []
#         general_knowledge_collections = []

#         # 如果提供了specification_uid，则只查找相关集合
#         # if specification_uid:
#         #     specification_uid = specification_uid.replace("-", "_")
#         #     classification_collection = f"{specification_uid}_classification"
#         #     general_knowledge_collection = f"{specification_uid}_general_knowledge"
#         if specification_uid:
#             specification_uid = specification_uid.replace("-", "_")
#             # 确保集合名称以字母或下划线开头（Milvus要求）
#             if specification_uid and not specification_uid[0].isalpha() and specification_uid[0] != '_':
#                 specification_uid = '_' + specification_uid
                
#             classification_collection = f"{specification_uid}_classification"
#             general_knowledge_collection = f"{specification_uid}_general_knowledge"
            
            
            
            

#             if classification_collection in collections:
#                 classification_collections.append(classification_collection)
#             if general_knowledge_collection in collections:
#                 general_knowledge_collections.append(
#                     general_knowledge_collection)
#         else:
#             # 否则查找所有集合（原有逻辑）
#             for collection in collections:
#                 # 匹配 *_classification 但排除 *_narrative_classification
#                 if (collection.endswith('_classification') and
#                         not collection.endswith('_narrative_classification')):
#                     classification_collections.append(collection)
#                 # 匹配 *_general_knowledge
#                 elif collection.endswith('_general_knowledge'):
#                     general_knowledge_collections.append(collection)

#         return {
#             'classification': classification_collections,
#             'general_knowledge': general_knowledge_collections
#         }

#     def _bm25_search(self, collection_name: str, query: str, top_k: int = 3) -> List[str]:
#         """
#         在指定集合中进行BM25检索

#         Args:
#             collection_name: 集合名称
#             query: 查询字符串
#             top_k: 返回结果数量

#         Returns:
#             List[str]: 检索到的text值列表
#         """
#         try:
#             # 检查集合是否存在
#             if not self.milvus_client.has_collection(collection_name):
#                 logger.warning(f"集合 {collection_name} 不存在")
#                 return []

#             # 加载集合
#             self.milvus_client.load_collection(collection_name=collection_name)

#             # 尝试导入BM25库
#             try:
#                 from rank_bm25 import BM25Okapi
#                 import jieba
#                 import json
#                 import re
#             except ImportError as e:
#                 logger.warning(f"缺少必要的库来执行BM25搜索: {e}")
#                 return []

#             # 从Milvus中获取所有文档用于构建BM25模型
#             try:
#                 all_results = self.milvus_client.query(
#                     collection_name=collection_name,
#                     filter="",
#                     output_fields=["id", "text", "items"],
#                     limit=10000  # 假设不超过10000条记录
#                 )
#             except Exception as e:
#                 logger.error(f"获取数据构建BM25索引时出错: {e}")
#                 return []

#             logger.info(
#                 f"从集合 {collection_name} 中获取到 {len(all_results)} 条记录用于BM25索引构建")

#             corpus = []
#             metadata = []

#             for i, result in enumerate(all_results):
#                 items_str = result.get("items", "")
#                 items_list = []

#                 # 处理特殊的items格式
#                 if items_str:
#                     # 移除特殊字符和结构符号
#                     # 移除JSON结构符号和特殊标记
#                     cleaned_str = re.sub(r'[{}"\[\]]', '', items_str)
#                     # 分割并清理词汇
#                     raw_tokens = [token.strip() for token in cleaned_str.split(
#                         ',') if token.strip()]
#                     # 过滤掉无意义的符号和标记
#                     items_list = [token for token in raw_tokens
#                                   if token and token not in ['#', '', ' ']
#                                   and not re.match(r'^\d+$', token)  # 过滤纯数字
#                                   and len(token) > 1]  # 过滤单字符

#                 # 记录前几条数据的情况用于调试
#                 if i < 3:
#                     logger.info(
#                         f"记录 {i}: items_str={items_str[:100]}..., parsed_items={items_list[:10] if items_list else None}")

#                 # 只有非空的文档才加入语料库
#                 if items_list and isinstance(items_list, list) and len(items_list) > 0:
#                     corpus.append(items_list)
#                     metadata.append({
#                         "id": result.get("id"),
#                         "text": result.get("text"),
#                         "items": items_str
#                     })
#                 elif i < 3:  # 记录被过滤的原因
#                     logger.info(
#                         f"记录 {i} 被过滤: items_list={items_list}, is_list={isinstance(items_list, list)}, len={len(items_list) if isinstance(items_list, list) else 'N/A'}")

#             logger.info(f"有效文档数量: {len(corpus)}")

#             if not corpus:
#                 logger.warning("没有有效的文档可用于构建BM25索引")
#                 return []

#             # 检查是否所有文档都是空的
#             total_tokens = sum(len(doc) for doc in corpus)
#             logger.info(f"总token数: {total_tokens}")

#             if total_tokens == 0:
#                 logger.warning("所有文档都为空，无法构建BM25索引")
#                 return []

#             # 构建BM25模型
#             try:
#                 bm25_model = BM25Okapi(corpus)
#                 logger.info("BM25模型构建成功")
#             except Exception as e:
#                 logger.error(f"构建BM25模型时出错: {e}")
#                 return []

#             # 对查询文本进行jieba分词
#             query_tokens = list(jieba.cut(query))
#             query_tokens = [token for token in query_tokens if len(
#                 token.strip()) > 0 and len(token) > 1]

#             logger.info(f"查询文本 '{query}' 分词结果: {query_tokens}")

#             if not query_tokens:
#                 logger.warning("查询文本分词后为空")
#                 return []

#             # 使用标准BM25计算得分
#             doc_scores = bm25_model.get_scores(query_tokens)

#             # 记录得分情况
#             logger.info(
#                 f"文档得分 (前10个): {doc_scores[:10] if len(doc_scores) > 10 else doc_scores}")

#             # 获取top_k结果
#             top_indices = doc_scores.argsort()[::-1][:top_k]

#             texts = []
#             for idx in top_indices:
#                 score = doc_scores[idx]
#                 logger.info(f"文档索引 {idx} 得分: {score}")
#                 if score > 0:  # 只返回得分大于0的结果
#                     metadata_entry = metadata[idx]
#                     texts.append(metadata_entry["text"])
#                     logger.info(f"添加结果: {metadata_entry['text'][:100]}...")
#                 else:
#                     logger.info(f"文档索引 {idx} 得分为0，跳过")

#             logger.info(f"最终返回 {len(texts)} 条BM25结果")
#             return texts
#         except Exception as e:
#             logger.error(
#                 f"BM25检索失败 (集合: {collection_name}): {e}", exc_info=True)
#             return []

#     # async def _vector_search(self, collection_name: str, query: str, top_k: int = 3) -> List[str]:
#     #     """
#     #     在指定集合中进行向量相似度检索

#     #     Args:
#     #         collection_name: 集合名称
#     #         query: 查询字符串
#     #         top_k: 返回结果数量

#     #     Returns:
#     #         List[str]: 检索到的text值列表
#     #     """
#     #     try:
#     #         # 检查集合是否存在
#     #         if not self.milvus_client.has_collection(collection_name):
#     #             logger.warning(f"集合 {collection_name} 不存在")
#     #             return []

#     #         # 加载集合
#     #         self.milvus_client.load_collection(collection_name=collection_name)

#     #         # 初始化向量客户端（使用特定的集合名称）
#     #         vector_client = VectorClient()
#     #         vector_client.collection_name = collection_name

#     #         # 获取查询文本的向量表示
#     #         query_vectors = await vector_client.get_embeddings([query])

#     #         # 在向量数据库中搜索
#     #         results = await vector_client.search(query_vectors, top_k)

#     #         # 提取text值
#     #         texts = []
#     #         for result in results[0]:  # 第一个查询的结果
#     #             if 'text' in result.get('entity', {}):
#     #                 texts.append(result['entity']['text'])

#     #         return texts
#     #     except Exception as e:
#     #         logger.error(f"向量相似度检索失败 (集合: {collection_name}): {e}")
#     #         return []
#     async def _vector_search(self, collection_name: str, query: str, top_k: int = 3) -> List[str]:
#         """
#         在指定集合中进行向量相似度检索

#         Args:
#             collection_name: 集合名称
#             query: 查询字符串
#             top_k: 返回结果数量

#         Returns:
#             List[str]: 检索到的text值列表
#         """
#         try:
#             # 检查集合是否存在
#             if not self.milvus_client.has_collection(collection_name):
#                 logger.warning(f"集合 {collection_name} 不存在")
#                 return []

#             # 加载集合
#             self.milvus_client.load_collection(collection_name=collection_name)

#             # 初始化向量客户端（使用特定的集合名称）
#             vector_client = VectorClient()
#             vector_client.collection_name = collection_name

#             # 获取查询文本的向量表示
#             query_vectors = await vector_client.get_embeddings([query])

#             # 在向量数据库中搜索
#             results = await vector_client.search(query_vectors, top_k)

#             # 提取text值
#             texts = []
#             for result in results[0]:  # 第一个查询的结果
#                 if 'text' in result.get('entity', {}):
#                     texts.append(result['entity']['text'])

#             return texts
#         except Exception as e:
#             logger.error(f"向量相似度检索失败 (集合: {collection_name}): {e}")
#             return []

#     def _build_query_sentence(self, request: DataRecognitionRequest) -> str:
#         """
#         构建用于向量检索的查询句子

#         Args:
#             request: 数据识别请求

#         Returns:
#             str: 构建好的查询句子
#         """
#         from app.core.utils import remove_annotations
        
#         parts = []

#         # 添加dbName（必须）
#         cleaned_db_name = remove_annotations(request.dbName)
#         parts.append(f"dbName:{cleaned_db_name}")

#         # 添加其他可选字段
#         if getattr(request, 'schemaName', None):
#             cleaned_schema_name = remove_annotations(request.schemaName)
#             parts.append(f"schemaName:{cleaned_schema_name}")
#         if getattr(request, 'tableName', None):
#             cleaned_table_name = remove_annotations(request.tableName)
#             parts.append(f"tableName:{cleaned_table_name}")
#         if getattr(request, 'tableComment', None):
#             cleaned_table_comment = remove_annotations(request.tableComment)
#             parts.append(f"tableComment:{cleaned_table_comment}")
#         if getattr(request, 'tableRows', None) is not None:
#             parts.append(f"tableRows:{str(request.tableRows)}")
#         if getattr(request, 'systemType', None):
#             cleaned_system_type = remove_annotations(request.systemType)
#             parts.append(f"systemType:{cleaned_system_type}")
#         if getattr(request, 'systemName', None):
#             cleaned_system_name = remove_annotations(request.systemName)
#             parts.append(f"systemName:{cleaned_system_name}")
#         parts.append("其他字段:")
#         if getattr(request, 'fields', None):
#             for field in request.fields:
#                 cleaned_field_name = remove_annotations(field.fieldName)
#                 parts.append(f"{cleaned_field_name}")
#                 if getattr(field, 'fieldComment', None):
#                     cleaned_field_comment = remove_annotations(field.fieldComment)
#                     parts.append(f"{cleaned_field_comment}")

#         return ",".join(parts)
    
#     def _build_query_sentence_similarity(self, request: DataRecognitionRequest) -> str:
#         """
#         构建用于向量检索的查询句子

#         Args:
#             request: 数据识别请求

#         Returns:
#             str: 构建好的查询句子
#         """
#         from app.core.utils import remove_annotations
#         parts = []

#         # 添加dbName（必须）
#         # parts.append(f"dbName:{request.dbName}")

#         # 添加其他可选字段
#         # if getattr(request, 'schemaName', None):
#         #     parts.append(f"schemaName:{request.schemaName}")
#         if getattr(request, 'tableName', None):
#             cleaned_table_name = remove_annotations(request.tableName)
#             parts.append(f"{cleaned_table_name}")
#         if getattr(request, 'tableComment', None):
#             cleaned_table_comment = remove_annotations(request.tableComment)
#             parts.append(f"{cleaned_table_comment}")
#         # if getattr(request, 'tableRows', None) is not None:
#         #     parts.append(f"tableRows:{str(request.tableRows)}")
#         # if getattr(request, 'systemType', None):
#         #     parts.append(f"systemType:{request.systemType}")
#         # if getattr(request, 'systemName', None):
#         #     parts.append(f"systemName:{request.systemName}")
#         if getattr(request, 'fields', None):
#             for field in request.fields:
#                 cleaned_field_name = remove_annotations(field.fieldName)
#                 parts.append(f"{cleaned_field_name}")
#                 if getattr(field, 'fieldComment', None):
#                     cleaned_field_comment = remove_annotations(field.fieldComment)
#                     parts.append(f"{cleaned_field_comment}")

#         # 用逗号连接所有部分
#         return ",".join(parts)

#     async def recognize_data(self, request: DataRecognitionRequest) -> List[TableAIScanResultDto]:
#         """
#         识别数据的AI分类和分级

#         Args:
#             request: 数据识别请求

#         Returns:
#             List[TableAIScanResultDto]: 表识别结果列表
#         """
#         # 查找相关集合，如果提供了任何一个或多个specificationUId则查找所有相关集合
#         # 收集所有提供的UId
#         specification_uids = []
#         if getattr(request, 'senSpecificationUId', None):
#             specification_uids.append(request.senSpecificationUId)
#         if getattr(request, 'impSpecificationUId', None):
#             specification_uids.append(request.impSpecificationUId)
#         if getattr(request, 'coreSpecificationUId', None):
#             specification_uids.append(request.coreSpecificationUId)
            
#         # 查找所有相关集合
#         all_classification_collections = []
#         all_general_knowledge_collections = []
        
#         for uid in specification_uids:
#             relevant_collections = self._find_relevant_collections(uid)
#             all_classification_collections.extend(relevant_collections['classification'])
#             all_general_knowledge_collections.extend(relevant_collections['general_knowledge'])
        
#         # 去重
#         classification_collections = list(set(all_classification_collections))
#         general_knowledge_collections = list(set(all_general_knowledge_collections))

#         logger.info(f"找到分类集合: {classification_collections}")
#         logger.info(f"找到通用知识集合: {general_knowledge_collections}")

#         if not classification_collections and not general_knowledge_collections:
#             logger.warning("未找到任何相关集合")
#             return []

#         # 获取dbName
#         db_name = request.dbName

#         # 获取coreSpecificationUId和impSpecificationUId
#         core_uid = getattr(request, 'coreSpecificationUId', None)
#         imp_uid = getattr(request, 'impSpecificationUId', None)

#         # 存储所有检索结果
#         bm25_results = []
#         vector_results = []
#         general_knowledge_results = []

#         # 在分类集合中进行BM25检索（使用top_k=15）
#         for collection_name in classification_collections:
#             # BM25检索
#             bm25_texts = self._bm25_search(collection_name, db_name, 15)
#             bm25_results.extend(bm25_texts)

#         # 构建查询句子
#         query_sentence = self._build_query_sentence(request)
#         query_sentence_similarity = self._build_query_sentence_similarity(request)
#         logger.info(f"构建的查询句子: {query_sentence_similarity}")

#         # 在分类集合中进行向量相似度检索（使用top_k=15）
#         vector_results = []
#         for collection_name in classification_collections:
#             # 向量相似度检索
#             collection_results = await self._vector_search(
#                 collection_name, query_sentence_similarity, 15)
#             vector_results.extend(collection_results)

#         # 在通用知识集合中进行向量相似度检索（使用top_k=15）
#         for collection_name in general_knowledge_collections:
#             # 向量相似度检索
#             collection_results = await self._vector_search(
#                 collection_name, query_sentence_similarity, 15)
#             general_knowledge_results.extend(collection_results)

#         # 筛选BM25和向量检索结果（直接使用未筛选的结果）
#         filtered_bm25_results = bm25_results
#         filtered_vector_results = vector_results

#         # 去重并保留前3个结果
#         unique_bm25_results = list(dict.fromkeys(filtered_bm25_results))[:5]
#         unique_vector_results = list(
#             dict.fromkeys(filtered_vector_results))[:5]
#         unique_general_knowledge_results = list(
#             dict.fromkeys(general_knowledge_results))[:5]

#         # 记录检索结果
#         logger.info(
#             f"原始BM25检索结果数: {len(bm25_results)}, 筛选后结果数: {len(filtered_bm25_results)}, 最终使用结果数: {len(unique_bm25_results)}")
#         logger.info(
#             f"原始向量检索结果数: {len(vector_results)}, 筛选后结果数: {len(filtered_vector_results)}, 最终使用结果数: {len(unique_vector_results)}")
#         logger.info(
#             f"通用知识检索结果数: {len(general_knowledge_results)}, 最终使用结果数: {len(unique_general_knowledge_results)}")
#         logger.info(f"BM25检索结果: {unique_bm25_results}")
#         logger.info(f"向量相似度检索结果: {unique_vector_results}")
#         logger.info(f"通用知识检索结果: {unique_general_knowledge_results}")

#         # 使用LLM进行分类判断
#         llm_result = await self._llm_classification(
#             query_sentence,
#             unique_bm25_results,
#             unique_vector_results,
#             unique_general_knowledge_results
#         )

#         # 初始化分类和分级信息
#         table_classification = ""
#         table_grade = ""
#         table_annotate = ""
#         table_element_list = []

#         # 处理LLM结果
#         if llm_result:
#             try:
#                 # 解析LLM响应
#                 parts = llm_result.split(',', 1)
#                 if len(parts) == 2:
#                     index_part = parts[0].strip()
#                     table_annotate = parts[1].strip()

#                     # 获取索引数字
#                     # selected_index = int(index_part)
#                     # 提取索引数字（处理任何格式，提取其中的数字）
#                     import re
#                     numbers = re.findall(r'\d+', index_part)
#                     if numbers:
#                         selected_index = int(numbers[0])
#                     else:
#                         raise ValueError(f"无法从 '{index_part}' 提取有效数字")
#                     # 获取对应的分类结果
#                     all_candidate_results = unique_bm25_results + unique_vector_results
#                     if 1 <= selected_index <= len(all_candidate_results):
#                         selected_result = all_candidate_results[selected_index - 1]

#                         # 解析分类结果
#                         try:
#                             import json
#                             result_data = json.loads(selected_result)

#                             # 提取分类信息
#                             header = result_data.get("header", {})
#                             data = result_data.get("data", {})

#                             # 查找最高级别的分类（支持任意级别，不局限于三级分类）
#                             classification_value = ""
#                             # 创建一个有序的分类级别列表，从高到低排序
#                             classification_levels = []
#                             level_mapping = {}

#                             # 收集所有分类相关字段
#                             for key, value in header.items():
#                                 if value.endswith("分类"):
#                                     # 提取级别数字（如"四级分类"提取出4）
#                                     if value.startswith("一级"):
#                                         level_num = 1
#                                     elif value.startswith("二级"):
#                                         level_num = 2
#                                     elif value.startswith("三级"):
#                                         level_num = 3
#                                     elif value.startswith("四级"):
#                                         level_num = 4
#                                     elif value.startswith("五级"):
#                                         level_num = 5
#                                     elif value.startswith("六级"):
#                                         level_num = 6
#                                     elif value.startswith("七级"):
#                                         level_num = 7
#                                     elif value.startswith("八级"):
#                                         level_num = 8
#                                     else:
#                                         # 尝试提取数字
#                                         import re
#                                         num_match = re.search(r'^(\d+)', value)
#                                         level_num = int(num_match.group(
#                                             1)) if num_match else 99  # 默认放到最后

#                                     classification_levels.append(
#                                         (level_num, key, value))
#                                     level_mapping[value] = key

#                             # 按级别数字排序（从高到低）
#                             classification_levels.sort(reverse=True)

#                             # 查找第一个非空的分类值
#                             for level_num, key, level_name in classification_levels:
#                                 candidate_value = data.get(key, "")
#                                 if candidate_value:
#                                     classification_value = candidate_value
#                                     break

#                             # 如果没有找到按级别命名的分类，尝试其他方法
#                             if not classification_value:
#                                 # 先查找三级分类（保持向后兼容）
#                                 for key, value in header.items():
#                                     if value == "三级分类":
#                                         classification_value = data.get(
#                                             key, "")
#                                         break

#                             # 如果三级分类为空，查找二级分类
#                             if not classification_value:
#                                 for key, value in header.items():
#                                     if value == "二级分类":
#                                         classification_value = data.get(
#                                             key, "")
#                                         break

#                             # 如果二级分类也为空，查找一级分类
#                             if not classification_value:
#                                 for key, value in header.items():
#                                     if value == "一级分类":
#                                         classification_value = data.get(
#                                             key, "")
#                                         break

#                             table_classification = classification_value

#                             # 确定数据等级
#                             # 直接从等级字段获取数据等级，不再根据分类值前缀判断
#                             # 查找等级字段
#                             for key, value in header.items():
#                                 if value == "等级":
#                                     raw_grade = data.get(key, "")
#                                     # 处理多级数据等级，根据配置选择最高或最低级别
#                                     from app.core.config import DataGradeConfig
#                                     if "/" in raw_grade:
#                                         grades = raw_grade.split("/")
#                                         if DataGradeConfig.grade_selection_strategy == "highest":
#                                             # 选择最高级别（数字最大）
#                                             numeric_grades = []
#                                             for grade in grades:
#                                                 import re
#                                                 match = re.search(
#                                                     r'第(\d+)级', grade)
#                                                 if match:
#                                                     numeric_grades.append(
#                                                         int(match.group(1)))
#                                             if numeric_grades:
#                                                 max_grade = max(
#                                                     numeric_grades)
#                                                 table_grade = f"第{max_grade}级"
#                                             else:
#                                                 table_grade = raw_grade  # 无法解析，返回原始值
#                                         else:
#                                             # 默认选择最低级别（数字最小）
#                                             numeric_grades = []
#                                             for grade in grades:
#                                                 import re
#                                                 match = re.search(
#                                                     r'第(\d+)级', grade)
#                                                 if match:
#                                                     numeric_grades.append(
#                                                         int(match.group(1)))
#                                             if numeric_grades:
#                                                 min_grade = min(
#                                                     numeric_grades)
#                                                 table_grade = f"第{min_grade}级"
#                                             else:
#                                                 table_grade = raw_grade  # 无法解析，返回原始值
#                                     else:
#                                         table_grade = raw_grade
#                                     break

#                             # 查找真实数据字段
#                             for key, value in header.items():
#                                 if value == "真实数据":
#                                     table_element = data.get(key, "")
#                                     # 如果是列表，直接使用；如果是字符串，转换为单元素列表
#                                     if isinstance(table_element, list):
#                                         table_element_list = table_element
#                                     elif isinstance(table_element, str):
#                                         table_element_list = [table_element]
#                                     break

#                         except json.JSONDecodeError:
#                             logger.warning(f"无法解析分类结果为JSON: {selected_result}")
#             except Exception as e:
#                 logger.error(f"处理LLM结果时出错: {e}")

#         # 处理字段级别的分类
#         field_results = []

#         # 将table_element_list转换为键值对形式，用于字段匹配
#         table_element_dict = {str(i): element for i,
#                               element in enumerate(table_element_list)}
#         logger.info(f"表元素字典: {table_element_dict}")

#         for field in request.fields:
#             # 构建字段查询句子
#             field_parts = []
#             if getattr(field, 'fieldName', None):
#                 field_parts.append(f"fieldName:{field.fieldName}")
#             if getattr(field, 'fieldComment', None):
#                 field_parts.append(f"fieldComment:{field.fieldComment}")
#             if getattr(field, 'sampleValue', None):
#                 field_parts.append(f"sampleValue:{field.sampleValue}")

#             field_query_sentence = ",".join(field_parts)

#             # 初始化字段结果
#             field_annotate = getattr(field, 'fieldComment', '') or ''
#             field_element = ""
#             field_classification = ""
#             field_grade = ""
#             field_reason = ""

#             # 调用新的字段映射方法
#             field_mapping_result = await self._map_field_to_table_elements(
#                 field.fieldName,
#                 field_query_sentence,
#                 table_element_dict
#             )

#             # 处理字段映射结果
#             if field_mapping_result and field_mapping_result != "-1":
#                 try:
#                     # 解析响应 (应该是三个部分: index, field_type, reason)
#                     parts = field_mapping_result.split(',', 2)  # 最多分割成3部分
#                     if len(parts) >= 3:
#                         index_part = parts[0].strip()
#                         field_annotate = parts[1].strip()
#                         field_reason = parts[2].strip()

#                         # 获取索引数字并映射到具体元素
#                         # selected_index = int(index_part)
#                         # if str(selected_index) in table_element_dict:
#                         # field_element = table_element_dict[str(selected_index)]
#                         # 提取索引数字（处理任何格式，提取其中的数字）
#                         import re
#                         numbers = re.findall(r'\d+', index_part)
#                         if numbers:
#                             selected_index = int(numbers[0])
#                         else:
#                             raise ValueError(f"无法从 '{index_part}' 提取有效数字")

#                         if str(selected_index) in table_element_dict:
#                             field_element = table_element_dict[str(
#                                 selected_index)]
#                 except Exception as e:
#                     logger.error(f"处理字段映射结果时出错: {e}")

#             field_result = FieldAIScanResultDto(
#                 fieldName=field.fieldName,
#                 fieldAnnotate=field_annotate,
#                 element=field_element,
#                 classification=field_classification,
#                 grade=field_grade,
#                 reason=field_reason
#             )
#             field_results.append(field_result)

#         table_result = TableAIScanResultDto(
#             dbName=request.dbName,
#             schemaName=getattr(request, 'schemaName', None),
#             tableName=getattr(request, 'tableName', "") or "",
#             tableAnnotate=table_annotate,
#             tableClassification=table_classification,
#             tableGrade=table_grade,
#             tableElement=table_element_list,
#             fields=field_results
#         )

#         result = [table_result]

#         return result

#     async def _llm_classification(self, query_sentence: str, bm25_results: List[str], vector_results: List[str], general_knowledge_results: List[str]) -> Optional[str]:
#         """
#         使用LLM对检索结果进行分类判断

#         Args:
#             query_sentence: 查询句子
#             bm25_results: BM25检索结果
#             vector_results: 向量检索结果
#             general_knowledge_results: 通用知识检索结果

#         Returns:
#             LLM分类结果
#         """
#         try:
#             from app.core.config import ChatLLMConfig
#             from app.core.utils import AsyncLLMClient
#             import aiohttp
#             import json

#             # 构建候选结果列表
#             llm_items = []
#             rank = 1

#             # 添加BM25检索结果
#             for result in bm25_results:
#                 llm_items.append({
#                     'rank': rank,
#                     'type': 'BM25',
#                     'data': result
#                 })
#                 rank += 1

#             # 添加向量检索结果
#             for result in vector_results:
#                 llm_items.append({
#                     'rank': rank,
#                     'type': '向量',
#                     'data': result
#                 })
#                 rank += 1

#             # 如果没有候选结果，直接返回
#             if not llm_items:
#                 logger.warning("没有候选结果用于LLM分类判断")
#                 return None

#             # 构建提示词
#             prompt = f"""## 角色定义
# 你是一个专业的数据表分类专家，具备跨行业数据理解能力。

# ## 任务背景
# - 我们的知识库是基于电信行业的数据建立的，有时可能用它来匹配其他行业的数据，这里需要你灵活判断。

# ## 输入数据

# ### 用户提供的某行业数据库中的表结构信息
# {query_sentence}

# ### 从分类分级知识库中检索到的候选结果
# """

#             for item in llm_items:
#                 prompt += f"""
# 候选结果 {item['rank']}（{item['type']}检索）：
# {item['data']}
# """

#             if general_knowledge_results:
#                 prompt += f"""
# ### 通识检索结果
# """
#                 for result in general_knowledge_results:
#                     prompt += f"""
# {result}
# """

#             prompt += f"""
# ## 评估准则
# 1. "通识检索结果"仅参考，通常不是很准确，重点是知识库的"候选结果"
# 2. 关注数据的本质属性和核心特征是否相符
# 3. 重点比较你从数据中理解的特征
# 4. 如果去除行业特有词汇后，核心内容基本一致，则也可判定为匹配

# ## 输出要求
# - 回复两个部分：
#     第一部分为最匹配的候选结果序号数字（1-{len(llm_items)}），无论如何必须强制选一个该区间的数字
#     第二部分为表类型判定，对用户提供的表结构信息进行主观判断，判断是属于哪个领域的表，回答格式：这是一张XXX表
# - 回复格式：
#     csv格式回复，逗号隔开两个部分，回答最相关的一条，格式：<数字>,<表类型判定>
# - 不要添加任何其他无关信息。
# """

#             logger.info(f"llm_items:\n\n{llm_items}")
#             logger.info(f"提示词:\n\n{prompt}")
#             # 构建请求数据
#             messages = [
#                 {"role": "user", "content": prompt}
#             ]

#             request_data = ChatLLMConfig.get_request_data(messages)

#             # 调用LLM
#             async with aiohttp.ClientSession() as session:
#                 llm_client = AsyncLLMClient()
#                 response = await llm_client.call_llm(
#                     session=session,
#                     url=ChatLLMConfig.url,
#                     headers=ChatLLMConfig.headers,
#                     request_data=request_data,
#                     timeout=2000
#                 )

#                 # 解析响应
#                 if "choices" in response and len(response["choices"]) > 0:
#                     msg = response["choices"][0]["message"]["content"]
#                     result_text = msg.split(
#                         "</think>\n\n")[1] if "</think>\n\n" in msg else msg

#                     logger.info(f"LLM分类结果: {result_text}")
#                     return result_text
#                 else:
#                     logger.warning("LLM响应格式不正确")
#                     return None

#         except Exception as e:
#             logger.error(f"LLM分类判断失败: {e}", exc_info=True)
#             return None

#     async def _map_field_to_table_elements(self, field_name: str, field_query_sentence: str,
#                                            table_element_dict: dict) -> Optional[str]:
#         """
#         使用LLM将字段映射到表元素

#         Args:
#             field_name: 字段名
#             field_query_sentence: 字段查询句子
#             table_element_dict: 表元素字典，格式如 {"0":"身份证号", "1":"出生日期", "2":"性别"}

#         Returns:
#             LLM字段映射结果，格式如 "0,这是身份证字段,与身份证号最匹配"
#         """
#         try:
#             from app.core.config import ChatLLMConfig
#             from app.core.utils import AsyncLLMClient
#             import aiohttp

#             # 如果没有表元素，直接返回
#             if not table_element_dict:
#                 logger.warning(f"字段 {field_name} 没有表元素用于映射判断")
#                 return "-1"

#             # 构建提示词
#             prompt = f"""## 角色定义
# 你是一个专业的数据字段映射专家，具备跨行业数据理解能力。

# ## 任务背景
# - 我们有一个已知的数据表分类结果，其中包含敏感数据元素列表
# - 现在需要将用户数据库中的字段与这些敏感数据元素进行匹配

# ## 输入数据

# ### 用户提供的某行业数据库中的字段相关信息
# {field_query_sentence}

# ### 已知的敏感数据元素列表
# """

#             # 添加表元素列表
#             for index, element in table_element_dict.items():
#                 prompt += f"{index}: {element}\n"

#             prompt += f"""
# ## 评估准则
# - 根据字段名称、注释和示例值，判断该字段最有可能对应哪个敏感数据元素
# - 如果找不到合适的匹配项，请回复-1
# - 匹配时考虑字段语义的相似性，而不仅仅是字面匹配

# ## 输出要求
# - 回复三个部分：
#     第一部分为最匹配的候选结果序号数字，从以上敏感数据元素列表中选取一个最相似的，选一个该区间的数字，都不相关回复-1
#     第二部分为字段类型判定，对用户提供的字段及信息进行主观判断，判断是属于哪个领域的字段，回答格式：这是XXX字段
#     第三部分为选择该数字的理由，限制20字说明一个肯定的理由，不要出现"无法判断"或"无关"之类的词
# - 回复格式：
#     csv格式回复，逗号隔开三个部分，回答最相关的一条，格式：<数字>,<字段判定>,<理由>
# - 不要添加任何其他无关信息。
# """

#             print(prompt)
#             # 构建请求数据
#             messages = [
#                 {"role": "user", "content": prompt}
#             ]

#             logger.info(f"字段 {field_name} 的映射提示词:\n\n{prompt}")

#             request_data = ChatLLMConfig.get_request_data(messages)

#             # 调用LLM
#             async with aiohttp.ClientSession() as session:
#                 llm_client = AsyncLLMClient()
#                 response = await llm_client.call_llm(
#                     session=session,
#                     url=ChatLLMConfig.url,
#                     headers=ChatLLMConfig.headers,
#                     request_data=request_data,
#                     timeout=2000
#                 )

#                 # 解析响应
#                 if "choices" in response and len(response["choices"]) > 0:
#                     msg = response["choices"][0]["message"]["content"]
#                     result_text = msg.split(
#                         "</think>\n\n")[1] if "</think>\n\n" in msg else msg

#                     logger.info(f"字段 {field_name} 映射结果: {result_text}")
#                     return result_text
#                 else:
#                     logger.warning(f"字段 {field_name} 映射响应格式不正确")
#                     return "-1"

#         except Exception as e:
#             logger.error(f"字段 {field_name} 映射判断失败: {e}", exc_info=True)
#             return "-1"

#     async def _llm_field_classification(self, field_name: str, field_comment: Optional[str], sample_value: Optional[str],
#                                         table_bm25_results: List[str], table_vector_results: List[str],
#                                         table_general_knowledge_results: List[str]) -> Optional[str]:
#         """
#         使用LLM对单个字段进行分类判断

#         Args:
#             field_name: 字段名
#             field_comment: 字段注释
#             sample_value: 示例值
#             table_bm25_results: 表级别的BM25检索结果
#             table_vector_results: 表级别的向量检索结果
#             table_general_knowledge_results: 表级别的通用知识检索结果

#         Returns:
#             LLM字段分类结果
#         """
#         try:
#             from app.core.config import ChatLLMConfig
#             from app.core.utils import AsyncLLMClient
#             import aiohttp
#             import json

#             # 构建字段查询句子
#             parts = []
#             if field_name:
#                 parts.append(f"fieldName:{field_name}")
#             if field_comment:
#                 parts.append(f"fieldComment:{field_comment}")
#             if sample_value:
#                 parts.append(f"sampleValue:{sample_value}")

#             query_sentence = ",".join(parts)

#             # 使用表级别的检索结果作为字段级别的候选结果
#             llm_items = []
#             rank = 1

#             # 添加BM25检索结果
#             for result in table_bm25_results:
#                 llm_items.append({
#                     'rank': rank,
#                     'type': 'BM25',
#                     'data': result
#                 })
#                 rank += 1

#             # 添加向量检索结果
#             for result in table_vector_results:
#                 llm_items.append({
#                     'rank': rank,
#                     'type': '向量',
#                     'data': result
#                 })
#                 rank += 1

#             # 如果没有候选结果，返回None
#             if not llm_items:
#                 logger.warning(f"字段 {field_name} 没有候选结果用于LLM分类判断")
#                 return None

#             # 构建提示词
#             prompt = f"""## 角色定义
# 你是一个专业的数据字段分类专家，具备跨行业数据理解能力。

# ## 任务背景
# - 我们的知识库是基于电信行业的数据建立的，有时可能用它来匹配其他行业的数据，这里需要你灵活判断。

# ## 输入数据

# ### 用户提供的某行业数据库中的字段相关信息
# {query_sentence}

# ### 从分类分级知识库中检索到的候选结果
# """

#             for item in llm_items:
#                 prompt += f"""
# 候选结果 {item['rank']}（{item['type']}检索）：
# {item['data']}
# """

#             if table_general_knowledge_results:
#                 prompt += f"""
# ### 通识检索结果
# """
#                 for result in table_general_knowledge_results:
#                     prompt += f"""
# {result}
# """

#             prompt += f"""
# ## 评估准则
# 1. "通识检索结果"仅参考，通常不是很准确，重点是知识库的"候选结果"
# 2. 关注数据的本质属性和核心特征是否相符
# 3. 重点比较你从数据中理解的特征
# 4. 如果去除行业特有词汇后，核心内容基本一致，则也可判定为匹配

# ## 输出要求
# - 回复三个部分：
#     第一部分为最匹配的候选结果序号数字（1-{len(llm_items)}），无论如何必须强制选一个该区间的数字
#     第二部分为字段类型判定，对用户提供的字段及信息进行主观判断，判断是属于哪个领域的字段，回答格式：这是XXX字段
#     第三部分为判定理由，限制20字说明一个理由，表达官方，不要出现"无法判断"之类的词
# - 回复格式：
#     csv格式回复，逗号隔开三个部分，回答最相关的一条，格式：<数字>,<字段类型判定>,<理由>
# - 不要添加任何其他无关信息。
# """

#             # 构建请求数据
#             messages = [
#                 {"role": "user", "content": prompt}
#             ]

#             logger.info(f"llm_items:\n\n{llm_items}")
#             logger.info(f"提示词:\n\n{prompt}")

#             request_data = ChatLLMConfig.get_request_data(messages)

#             # 调用LLM
#             async with aiohttp.ClientSession() as session:
#                 llm_client = AsyncLLMClient()
#                 response = await llm_client.call_llm(
#                     session=session,
#                     url=ChatLLMConfig.url,
#                     headers=ChatLLMConfig.headers,
#                     request_data=request_data,
#                     timeout=2000
#                 )

#                 # 解析响应
#                 if "choices" in response and len(response["choices"]) > 0:
#                     msg = response["choices"][0]["message"]["content"]
#                     result_text = msg.split(
#                         "</think>\n\n")[1] if "</think>\n\n" in msg else msg

#                     logger.info(f"LLM字段分类结果: {result_text}")
#                     return result_text
#                 else:
#                     logger.warning("LLM字段分类响应格式不正确")
#                     return None

#         except Exception as e:
#             logger.error(f"LLM字段分类判断失败: {e}", exc_info=True)
#             return None


# data_recognizing_service = DataRecognitionService()













# """
# 数据识别服务
# 处理AI数据识别逻辑
# """

# import logging
# import asyncio
# import re
# from typing import List, Optional
# from pymilvus import MilvusClient
# from app.schemas.knowledge_base import (
#     DataRecognitionRequest,
#     TableAIScanResultDto,
#     FieldAIScanResultDto,
#     FieldDataDto
# )
# from app.core.config import DatabaseConfig
# from app.core.vectoring import VectorClient

# logger = logging.getLogger(__name__)


# class DataRecognitionService:
#     """数据识别服务类"""

#     def __init__(self):
#         """初始化服务"""
#         self.db_path = DatabaseConfig.path
#         self.milvus_client = MilvusClient(self.db_path)

#     # def _find_relevant_collections(self) -> dict:
#     #     """
#     #     查找所有相关的集合，包括 *_classification（排除 *_narrative_classification）和 *_general_knowledge

#     #     Returns:
#     #         dict: 包含分类集合和通用知识集合的字典
#     #     """
#     #     try:
#     #         collections = self.milvus_client.list_collections()
#     #     except Exception as e:
#     #         logger.error(f"获取集合列表失败: {e}")
#     #         collections = []

#     #     classification_collections = []
#     #     general_knowledge_collections = []

#     #     for collection in collections:
#     #         # 匹配 *_classification 但排除 *_narrative_classification
#     #         if (collection.endswith('_classification') and
#     #             not collection.endswith('_narrative_classification')):
#     #             classification_collections.append(collection)
#     #         # 匹配 *_general_knowledge
#     #         elif collection.endswith('_general_knowledge'):
#     #             general_knowledge_collections.append(collection)

#     #     return {
#     #         'classification': classification_collections,
#     #         'general_knowledge': general_knowledge_collections
#     #     }
#     def _find_relevant_collections(self, specification_uid: str = None) -> dict:
#         """
#         查找相关的集合，包括 *_classification（排除 *_narrative_classification）和 *_general_knowledge
#         如果提供了specification_uid，则只返回与该规范相关的集合

#         Args:
#             specification_uid: 规范UID，如果提供则只查找相关集合

#         Returns:
#             dict: 包含分类集合和通用知识集合的字典
#         """
#         try:
#             collections = self.milvus_client.list_collections()
#         except Exception as e:
#             logger.error(f"获取集合列表失败: {e}")
#             collections = []

#         classification_collections = []
#         general_knowledge_collections = []

#         # 如果提供了specification_uid，则只查找相关集合
#         # if specification_uid:
#         #     specification_uid = specification_uid.replace("-", "_")
#         #     classification_collection = f"{specification_uid}_classification"
#         #     general_knowledge_collection = f"{specification_uid}_general_knowledge"
#         if specification_uid:
#             specification_uid = specification_uid.replace("-", "_")
#             # 确保集合名称以字母或下划线开头（Milvus要求）
#             if specification_uid and not specification_uid[0].isalpha() and specification_uid[0] != '_':
#                 specification_uid = '_' + specification_uid
                
#             classification_collection = f"{specification_uid}_classification"
#             general_knowledge_collection = f"{specification_uid}_general_knowledge"
            
            
            
            

#             if classification_collection in collections:
#                 classification_collections.append(classification_collection)
#             if general_knowledge_collection in collections:
#                 general_knowledge_collections.append(
#                     general_knowledge_collection)
#         else:
#             # 否则查找所有集合（原有逻辑）
#             for collection in collections:
#                 # 匹配 *_classification 但排除 *_narrative_classification
#                 if (collection.endswith('_classification') and
#                         not collection.endswith('_narrative_classification')):
#                     classification_collections.append(collection)
#                 # 匹配 *_general_knowledge
#                 elif collection.endswith('_general_knowledge'):
#                     general_knowledge_collections.append(collection)

#         return {
#             'classification': classification_collections,
#             'general_knowledge': general_knowledge_collections
#         }

#     def _bm25_search(self, collection_name: str, query: str, top_k: int = 3) -> List[str]:
#         """
#         在指定集合中进行BM25检索

#         Args:
#             collection_name: 集合名称
#             query: 查询字符串
#             top_k: 返回结果数量

#         Returns:
#             List[str]: 检索到的text值列表
#         """
#         try:
#             # 检查集合是否存在
#             if not self.milvus_client.has_collection(collection_name):
#                 logger.warning(f"集合 {collection_name} 不存在")
#                 return []

#             # 加载集合
#             self.milvus_client.load_collection(collection_name=collection_name)

#             # 尝试导入BM25库
#             try:
#                 from rank_bm25 import BM25Okapi
#                 import jieba
#                 import json
#                 import re
#             except ImportError as e:
#                 logger.warning(f"缺少必要的库来执行BM25搜索: {e}")
#                 return []

#             # 从Milvus中获取所有文档用于构建BM25模型
#             try:
#                 all_results = self.milvus_client.query(
#                     collection_name=collection_name,
#                     filter="",
#                     output_fields=["id", "text", "items"],
#                     limit=10000  # 假设不超过10000条记录
#                 )
#             except Exception as e:
#                 logger.error(f"获取数据构建BM25索引时出错: {e}")
#                 return []

#             logger.info(
#                 f"从集合 {collection_name} 中获取到 {len(all_results)} 条记录用于BM25索引构建")

#             corpus = []
#             metadata = []

#             for i, result in enumerate(all_results):
#                 items_str = result.get("items", "")
#                 items_list = []

#                 # 处理特殊的items格式
#                 if items_str:
#                     # 移除特殊字符和结构符号
#                     # 移除JSON结构符号和特殊标记
#                     cleaned_str = re.sub(r'[{}"\[\]]', '', items_str)
#                     # 分割并清理词汇
#                     raw_tokens = [token.strip() for token in cleaned_str.split(
#                         ',') if token.strip()]
#                     # 过滤掉无意义的符号和标记
#                     items_list = [token for token in raw_tokens
#                                   if token and token not in ['#', '', ' ']
#                                   and not re.match(r'^\d+$', token)  # 过滤纯数字
#                                   and len(token) > 1]  # 过滤单字符

#                 # 记录前几条数据的情况用于调试
#                 if i < 3:
#                     logger.info(
#                         f"记录 {i}: items_str={items_str[:100]}..., parsed_items={items_list[:10] if items_list else None}")

#                 # 只有非空的文档才加入语料库
#                 if items_list and isinstance(items_list, list) and len(items_list) > 0:
#                     corpus.append(items_list)
#                     metadata.append({
#                         "id": result.get("id"),
#                         "text": result.get("text"),
#                         "items": items_str
#                     })
#                 elif i < 3:  # 记录被过滤的原因
#                     logger.info(
#                         f"记录 {i} 被过滤: items_list={items_list}, is_list={isinstance(items_list, list)}, len={len(items_list) if isinstance(items_list, list) else 'N/A'}")

#             logger.info(f"有效文档数量: {len(corpus)}")

#             if not corpus:
#                 logger.warning("没有有效的文档可用于构建BM25索引")
#                 return []

#             # 检查是否所有文档都是空的
#             total_tokens = sum(len(doc) for doc in corpus)
#             logger.info(f"总token数: {total_tokens}")

#             if total_tokens == 0:
#                 logger.warning("所有文档都为空，无法构建BM25索引")
#                 return []

#             # 构建BM25模型
#             try:
#                 bm25_model = BM25Okapi(corpus)
#                 logger.info("BM25模型构建成功")
#             except Exception as e:
#                 logger.error(f"构建BM25模型时出错: {e}")
#                 return []

#             # 对查询文本进行jieba分词
#             query_tokens = list(jieba.cut(query))
#             query_tokens = [token for token in query_tokens if len(
#                 token.strip()) > 0 and len(token) > 1]

#             logger.info(f"查询文本 '{query}' 分词结果: {query_tokens}")

#             if not query_tokens:
#                 logger.warning("查询文本分词后为空")
#                 return []

#             # 使用标准BM25计算得分
#             doc_scores = bm25_model.get_scores(query_tokens)

#             # 记录得分情况
#             logger.info(
#                 f"文档得分 (前10个): {doc_scores[:10] if len(doc_scores) > 10 else doc_scores}")

#             # 获取top_k结果
#             top_indices = doc_scores.argsort()[::-1][:top_k]

#             texts = []
#             for idx in top_indices:
#                 score = doc_scores[idx]
#                 logger.info(f"文档索引 {idx} 得分: {score}")
#                 if score > 0:  # 只返回得分大于0的结果
#                     metadata_entry = metadata[idx]
#                     texts.append(metadata_entry["text"])
#                     logger.info(f"添加结果: {metadata_entry['text'][:100]}...")
#                 else:
#                     logger.info(f"文档索引 {idx} 得分为0，跳过")

#             logger.info(f"最终返回 {len(texts)} 条BM25结果")
#             return texts
#         except Exception as e:
#             logger.error(
#                 f"BM25检索失败 (集合: {collection_name}): {e}", exc_info=True)
#             return []

#     # async def _vector_search(self, collection_name: str, query: str, top_k: int = 3) -> List[str]:
#     #     """
#     #     在指定集合中进行向量相似度检索

#     #     Args:
#     #         collection_name: 集合名称
#     #         query: 查询字符串
#     #         top_k: 返回结果数量

#     #     Returns:
#     #         List[str]: 检索到的text值列表
#     #     """
#     #     try:
#     #         # 检查集合是否存在
#     #         if not self.milvus_client.has_collection(collection_name):
#     #             logger.warning(f"集合 {collection_name} 不存在")
#     #             return []

#     #         # 加载集合
#     #         self.milvus_client.load_collection(collection_name=collection_name)

#     #         # 初始化向量客户端（使用特定的集合名称）
#     #         vector_client = VectorClient()
#     #         vector_client.collection_name = collection_name

#     #         # 获取查询文本的向量表示
#     #         query_vectors = await vector_client.get_embeddings([query])

#     #         # 在向量数据库中搜索
#     #         results = await vector_client.search(query_vectors, top_k)

#     #         # 提取text值
#     #         texts = []
#     #         for result in results[0]:  # 第一个查询的结果
#     #             if 'text' in result.get('entity', {}):
#     #                 texts.append(result['entity']['text'])

#     #         return texts
#     #     except Exception as e:
#     #         logger.error(f"向量相似度检索失败 (集合: {collection_name}): {e}")
#     #         return []
#     async def _vector_search(self, collection_name: str, query: str, top_k: int = 3) -> List[str]:
#         """
#         在指定集合中进行向量相似度检索

#         Args:
#             collection_name: 集合名称
#             query: 查询字符串
#             top_k: 返回结果数量

#         Returns:
#             List[str]: 检索到的text值列表
#         """
#         try:
#             # 检查集合是否存在
#             if not self.milvus_client.has_collection(collection_name):
#                 logger.warning(f"集合 {collection_name} 不存在")
#                 return []

#             # 加载集合
#             self.milvus_client.load_collection(collection_name=collection_name)

#             # 初始化向量客户端（使用特定的集合名称）
#             vector_client = VectorClient()
#             vector_client.collection_name = collection_name

#             # 获取查询文本的向量表示
#             query_vectors = await vector_client.get_embeddings([query])

#             # 在向量数据库中搜索
#             results = await vector_client.search(query_vectors, top_k)

#             # 提取text值
#             texts = []
#             for result in results[0]:  # 第一个查询的结果
#                 if 'text' in result.get('entity', {}):
#                     texts.append(result['entity']['text'])

#             return texts
#         except Exception as e:
#             logger.error(f"向量相似度检索失败 (集合: {collection_name}): {e}")
#             return []

#     def _build_query_sentence(self, request: DataRecognitionRequest) -> str:
#         """
#         构建用于向量检索的查询句子

#         Args:
#             request: 数据识别请求

#         Returns:
#             str: 构建好的查询句子
#         """
#         from app.core.utils import remove_annotations
        
#         parts = []

#         # 添加dbName（必须）
#         cleaned_db_name = remove_annotations(request.dbName)
#         parts.append(f"dbName:{cleaned_db_name}")

#         # 添加其他可选字段
#         if getattr(request, 'schemaName', None):
#             cleaned_schema_name = remove_annotations(request.schemaName)
#             parts.append(f"schemaName:{cleaned_schema_name}")
#         if getattr(request, 'tableName', None):
#             cleaned_table_name = remove_annotations(request.tableName)
#             parts.append(f"tableName:{cleaned_table_name}")
#         if getattr(request, 'tableComment', None):
#             cleaned_table_comment = remove_annotations(request.tableComment)
#             parts.append(f"tableComment:{cleaned_table_comment}")
#         if getattr(request, 'tableRows', None) is not None:
#             parts.append(f"tableRows:{str(request.tableRows)}")
#         if getattr(request, 'systemType', None):
#             cleaned_system_type = remove_annotations(request.systemType)
#             parts.append(f"systemType:{cleaned_system_type}")
#         if getattr(request, 'systemName', None):
#             cleaned_system_name = remove_annotations(request.systemName)
#             parts.append(f"systemName:{cleaned_system_name}")
#         parts.append("其他字段:")
#         if getattr(request, 'fields', None):
#             for field in request.fields:
#                 cleaned_field_name = remove_annotations(field.fieldName)
#                 parts.append(f"{cleaned_field_name}")
#                 if getattr(field, 'fieldComment', None):
#                     cleaned_field_comment = remove_annotations(field.fieldComment)
#                     parts.append(f"{cleaned_field_comment}")

#         return ",".join(parts)
    
#     def _build_query_sentence_similarity(self, request: DataRecognitionRequest) -> str:
#         """
#         构建用于向量检索的查询句子

#         Args:
#             request: 数据识别请求

#         Returns:
#             str: 构建好的查询句子
#         """
#         from app.core.utils import remove_annotations
#         parts = []

#         # 添加dbName（必须）
#         # parts.append(f"dbName:{request.dbName}")

#         # 添加其他可选字段
#         # if getattr(request, 'schemaName', None):
#         #     parts.append(f"schemaName:{request.schemaName}")
#         if getattr(request, 'tableName', None):
#             cleaned_table_name = remove_annotations(request.tableName)
#             parts.append(f"{cleaned_table_name}")
#         if getattr(request, 'tableComment', None):
#             cleaned_table_comment = remove_annotations(request.tableComment)
#             parts.append(f"{cleaned_table_comment}")
#         # if getattr(request, 'tableRows', None) is not None:
#         #     parts.append(f"tableRows:{str(request.tableRows)}")
#         # if getattr(request, 'systemType', None):
#         #     parts.append(f"systemType:{request.systemType}")
#         # if getattr(request, 'systemName', None):
#         #     parts.append(f"systemName:{request.systemName}")
#         if getattr(request, 'fields', None):
#             for field in request.fields:
#                 cleaned_field_name = remove_annotations(field.fieldName)
#                 parts.append(f"{cleaned_field_name}")
#                 if getattr(field, 'fieldComment', None):
#                     cleaned_field_comment = remove_annotations(field.fieldComment)
#                     parts.append(f"{cleaned_field_comment}")

#         # 用逗号连接所有部分
#         return ",".join(parts)

#     async def recognize_data(self, request: DataRecognitionRequest) -> List[TableAIScanResultDto]:
#         """
#         识别数据的AI分类和分级

#         Args:
#             request: 数据识别请求

#         Returns:
#             List[TableAIScanResultDto]: 表识别结果列表
#         """
#         # 查找相关集合，如果提供了任何一个或多个specificationUId则查找所有相关集合
#         # 收集所有提供的UId
#         specification_uids = []
#         if getattr(request, 'senSpecificationUId', None):
#             specification_uids.append(request.senSpecificationUId)
#         if getattr(request, 'impSpecificationUId', None):
#             specification_uids.append(request.impSpecificationUId)
#         if getattr(request, 'coreSpecificationUId', None):
#             specification_uids.append(request.coreSpecificationUId)
            
#         # 查找所有相关集合
#         all_classification_collections = []
#         all_general_knowledge_collections = []
        
#         for uid in specification_uids:
#             relevant_collections = self._find_relevant_collections(uid)
#             all_classification_collections.extend(relevant_collections['classification'])
#             all_general_knowledge_collections.extend(relevant_collections['general_knowledge'])
        
#         # 去重
#         classification_collections = list(set(all_classification_collections))
#         general_knowledge_collections = list(set(all_general_knowledge_collections))

#         logger.info(f"找到分类集合: {classification_collections}")
#         logger.info(f"找到通用知识集合: {general_knowledge_collections}")

#         if not classification_collections and not general_knowledge_collections:
#             logger.warning("未找到任何相关集合")
#             return []

#         # 获取dbName
#         db_name = request.dbName

#         # 获取coreSpecificationUId和impSpecificationUId
#         core_uid = getattr(request, 'coreSpecificationUId', None)
#         imp_uid = getattr(request, 'impSpecificationUId', None)

#         # 存储所有检索结果
#         bm25_results = []
#         vector_results = []
#         general_knowledge_results = []

#         # 在分类集合中进行BM25检索（使用top_k=15）
#         for collection_name in classification_collections:
#             # BM25检索
#             bm25_texts = self._bm25_search(collection_name, db_name, 15)
#             bm25_results.extend(bm25_texts)

#         # 构建查询句子
#         query_sentence = self._build_query_sentence(request)
#         query_sentence_similarity = self._build_query_sentence_similarity(request)
#         logger.info(f"构建的查询句子: {query_sentence_similarity}")

#         # 在分类集合中进行向量相似度检索（使用top_k=15）
#         vector_results = []
#         for collection_name in classification_collections:
#             # 向量相似度检索
#             collection_results = await self._vector_search(
#                 collection_name, query_sentence_similarity, 15)
#             vector_results.extend(collection_results)

#         # 在通用知识集合中进行向量相似度检索（使用top_k=15）
#         for collection_name in general_knowledge_collections:
#             # 向量相似度检索
#             collection_results = await self._vector_search(
#                 collection_name, query_sentence_similarity, 15)
#             general_knowledge_results.extend(collection_results)

#         # 筛选BM25和向量检索结果（直接使用未筛选的结果）
#         filtered_bm25_results = bm25_results
#         filtered_vector_results = vector_results

#         # 去重并保留前3个结果
#         unique_bm25_results = list(dict.fromkeys(filtered_bm25_results))[:5]
#         unique_vector_results = list(
#             dict.fromkeys(filtered_vector_results))[:5]
#         unique_general_knowledge_results = list(
#             dict.fromkeys(general_knowledge_results))[:5]

#         # 记录检索结果
#         logger.info(
#             f"原始BM25检索结果数: {len(bm25_results)}, 筛选后结果数: {len(filtered_bm25_results)}, 最终使用结果数: {len(unique_bm25_results)}")
#         logger.info(
#             f"原始向量检索结果数: {len(vector_results)}, 筛选后结果数: {len(filtered_vector_results)}, 最终使用结果数: {len(unique_vector_results)}")
#         logger.info(
#             f"通用知识检索结果数: {len(general_knowledge_results)}, 最终使用结果数: {len(unique_general_knowledge_results)}")
#         logger.info(f"BM25检索结果: {unique_bm25_results}")
#         logger.info(f"向量相似度检索结果: {unique_vector_results}")
#         logger.info(f"通用知识检索结果: {unique_general_knowledge_results}")

#         # 使用LLM进行分类判断
#         llm_result = await self._llm_classification(
#             query_sentence,
#             unique_bm25_results,
#             unique_vector_results,
#             unique_general_knowledge_results
#         )

#         # 初始化分类和分级信息
#         table_classification = ""
#         table_grade = ""
#         table_annotate = ""
#         table_element_list = []

#         # 处理LLM结果
#         if llm_result:
#             try:
#                 # 解析LLM响应
#                 parts = llm_result.split(',', 1)
#                 if len(parts) == 2:
#                     index_part = parts[0].strip()
#                     table_annotate = parts[1].strip()

#                     # 获取索引数字
#                     # selected_index = int(index_part)
#                     # 提取索引数字（处理任何格式，提取其中的数字）
#                     import re
#                     numbers = re.findall(r'\d+', index_part)
#                     if numbers:
#                         selected_index = int(numbers[0])
#                     else:
#                         raise ValueError(f"无法从 '{index_part}' 提取有效数字")
#                     # 获取对应的分类结果
#                     all_candidate_results = unique_bm25_results + unique_vector_results
#                     if 1 <= selected_index <= len(all_candidate_results):
#                         selected_result = all_candidate_results[selected_index - 1]

#                         # 解析分类结果
#                         try:
#                             import json
#                             result_data = json.loads(selected_result)

#                             # 提取分类信息
#                             header = result_data.get("header", {})
#                             data = result_data.get("data", {})

#                             # 查找最高级别的分类（支持任意级别，不局限于三级分类）
#                             classification_value = ""
#                             # 创建一个有序的分类级别列表，从高到低排序
#                             classification_levels = []
#                             level_mapping = {}

#                             # 收集所有分类相关字段
#                             for key, value in header.items():
#                                 if value.endswith("分类"):
#                                     # 提取级别数字（如"四级分类"提取出4）
#                                     if value.startswith("一级"):
#                                         level_num = 1
#                                     elif value.startswith("二级"):
#                                         level_num = 2
#                                     elif value.startswith("三级"):
#                                         level_num = 3
#                                     elif value.startswith("四级"):
#                                         level_num = 4
#                                     elif value.startswith("五级"):
#                                         level_num = 5
#                                     elif value.startswith("六级"):
#                                         level_num = 6
#                                     elif value.startswith("七级"):
#                                         level_num = 7
#                                     elif value.startswith("八级"):
#                                         level_num = 8
#                                     else:
#                                         # 尝试提取数字
#                                         import re
#                                         num_match = re.search(r'^(\d+)', value)
#                                         level_num = int(num_match.group(
#                                             1)) if num_match else 99  # 默认放到最后

#                                     classification_levels.append(
#                                         (level_num, key, value))
#                                     level_mapping[value] = key

#                             # 按级别数字排序（从高到低）
#                             classification_levels.sort(reverse=True)

#                             # 查找第一个非空的分类值
#                             for level_num, key, level_name in classification_levels:
#                                 candidate_value = data.get(key, "")
#                                 if candidate_value:
#                                     classification_value = candidate_value
#                                     break

#                             # 如果没有找到按级别命名的分类，尝试其他方法
#                             if not classification_value:
#                                 # 先查找三级分类（保持向后兼容）
#                                 for key, value in header.items():
#                                     if value == "三级分类":
#                                         classification_value = data.get(
#                                             key, "")
#                                         break

#                             # 如果三级分类为空，查找二级分类
#                             if not classification_value:
#                                 for key, value in header.items():
#                                     if value == "二级分类":
#                                         classification_value = data.get(
#                                             key, "")
#                                         break

#                             # 如果二级分类也为空，查找一级分类
#                             if not classification_value:
#                                 for key, value in header.items():
#                                     if value == "一级分类":
#                                         classification_value = data.get(
#                                             key, "")
#                                         break

#                             table_classification = classification_value

#                             # 确定数据等级
#                             # 直接从等级字段获取数据等级，不再根据分类值前缀判断
#                             # 查找等级字段
#                             for key, value in header.items():
#                                 if value == "等级":
#                                     raw_grade = data.get(key, "")
#                                     # 处理多级数据等级，根据配置选择最高或最低级别
#                                     from app.core.config import DataGradeConfig
#                                     if "/" in raw_grade:
#                                         grades = raw_grade.split("/")
#                                         if DataGradeConfig.grade_selection_strategy == "highest":
#                                             # 选择最高级别（数字最大）
#                                             numeric_grades = []
#                                             for grade in grades:
#                                                 import re
#                                                 match = re.search(
#                                                     r'第(\d+)级', grade)
#                                                 if match:
#                                                     numeric_grades.append(
#                                                         int(match.group(1)))
#                                             if numeric_grades:
#                                                 max_grade = max(
#                                                     numeric_grades)
#                                                 table_grade = f"第{max_grade}级"
#                                             else:
#                                                 table_grade = raw_grade  # 无法解析，返回原始值
#                                         else:
#                                             # 默认选择最低级别（数字最小）
#                                             numeric_grades = []
#                                             for grade in grades:
#                                                 import re
#                                                 match = re.search(
#                                                     r'第(\d+)级', grade)
#                                                 if match:
#                                                     numeric_grades.append(
#                                                         int(match.group(1)))
#                                             if numeric_grades:
#                                                 min_grade = min(
#                                                     numeric_grades)
#                                                 table_grade = f"第{min_grade}级"
#                                             else:
#                                                 table_grade = raw_grade  # 无法解析，返回原始值
#                                     else:
#                                         table_grade = raw_grade
#                                     break

#                             # 查找真实数据字段
#                             for key, value in header.items():
#                                 if value == "真实数据":
#                                     table_element = data.get(key, "")
#                                     # 如果是列表，直接使用；如果是字符串，转换为单元素列表
#                                     if isinstance(table_element, list):
#                                         table_element_list = table_element
#                                     elif isinstance(table_element, str):
#                                         table_element_list = [table_element]
#                                     break

#                         except json.JSONDecodeError:
#                             logger.warning(f"无法解析分类结果为JSON: {selected_result}")
#             except Exception as e:
#                 logger.error(f"处理LLM结果时出错: {e}")

#         # 处理字段级别的分类
#         field_results = []

#         # 将table_element_list转换为键值对形式，用于字段匹配
#         table_element_dict = {str(i): element for i,
#                               element in enumerate(table_element_list)}
#         logger.info(f"表元素字典: {table_element_dict}")

#         for field in request.fields:
#             # 构建字段查询句子
#             field_parts = []
#             if getattr(field, 'fieldName', None):
#                 field_parts.append(f"fieldName:{field.fieldName}")
#             if getattr(field, 'fieldComment', None):
#                 field_parts.append(f"fieldComment:{field.fieldComment}")
#             if getattr(field, 'sampleValue', None):
#                 field_parts.append(f"sampleValue:{field.sampleValue}")

#             field_query_sentence = ",".join(field_parts)

#             # 初始化字段结果
#             field_annotate = getattr(field, 'fieldComment', '') or ''
#             field_element = ""
#             field_classification = ""
#             field_grade = ""
#             field_reason = ""

#             # 调用新的字段映射方法
#             field_mapping_result = await self._map_field_to_table_elements(
#                 field.fieldName,
#                 field_query_sentence,
#                 table_element_dict
#             )

#             # 处理字段映射结果
#             if field_mapping_result and field_mapping_result != "-1":
#                 try:
#                     # 解析响应 (应该是三个部分: index, field_type, reason)
#                     parts = field_mapping_result.split(',', 2)  # 最多分割成3部分
#                     if len(parts) >= 3:
#                         index_part = parts[0].strip()
#                         field_annotate = parts[1].strip()
#                         field_reason = parts[2].strip()

#                         # 获取索引数字并映射到具体元素
#                         # selected_index = int(index_part)
#                         # if str(selected_index) in table_element_dict:
#                         # field_element = table_element_dict[str(selected_index)]
#                         # 提取索引数字（处理任何格式，提取其中的数字）
#                         import re
#                         numbers = re.findall(r'\d+', index_part)
#                         if numbers:
#                             selected_index = int(numbers[0])
#                         else:
#                             raise ValueError(f"无法从 '{index_part}' 提取有效数字")

#                         if str(selected_index) in table_element_dict:
#                             field_element = table_element_dict[str(
#                                 selected_index)]
#                 except Exception as e:
#                     logger.error(f"处理字段映射结果时出错: {e}")

#             field_result = FieldAIScanResultDto(
#                 fieldName=field.fieldName,
#                 fieldAnnotate=field_annotate,
#                 element=field_element,
#                 classification=field_classification,
#                 grade=field_grade,
#                 reason=field_reason
#             )
#             field_results.append(field_result)

#         table_result = TableAIScanResultDto(
#             dbName=request.dbName,
#             schemaName=getattr(request, 'schemaName', None),
#             tableName=getattr(request, 'tableName', "") or "",
#             tableAnnotate=table_annotate,
#             tableClassification=table_classification,
#             tableGrade=table_grade,
#             tableElement=table_element_list,
#             fields=field_results
#         )

#         result = [table_result]

#         return result

#     async def _llm_classification(self, query_sentence: str, bm25_results: List[str], vector_results: List[str], general_knowledge_results: List[str]) -> Optional[str]:
#         """
#         使用LLM对检索结果进行分类判断

#         Args:
#             query_sentence: 查询句子
#             bm25_results: BM25检索结果
#             vector_results: 向量检索结果
#             general_knowledge_results: 通用知识检索结果

#         Returns:
#             LLM分类结果
#         """
#         try:
#             from app.core.config import ChatLLMConfig
#             from app.core.utils import AsyncLLMClient
#             import aiohttp
#             import json

#             # 构建候选结果列表
#             llm_items = []
#             rank = 1

#             # 添加BM25检索结果
#             for result in bm25_results:
#                 llm_items.append({
#                     'rank': rank,
#                     'type': 'BM25',
#                     'data': result
#                 })
#                 rank += 1

#             # 添加向量检索结果
#             for result in vector_results:
#                 llm_items.append({
#                     'rank': rank,
#                     'type': '向量',
#                     'data': result
#                 })
#                 rank += 1

#             # 如果没有候选结果，直接返回
#             if not llm_items:
#                 logger.warning("没有候选结果用于LLM分类判断")
#                 return None

#             # 构建提示词
#             prompt = f"""## 角色定义
# 你是一个专业的数据表分类专家，具备跨行业数据理解能力。

# ## 任务背景
# - 我们的知识库是基于电信行业的数据建立的，有时可能用它来匹配其他行业的数据，这里需要你灵活判断。

# ## 输入数据

# ### 用户提供的某行业数据库中的表结构信息
# {query_sentence}

# ### 从分类分级知识库中检索到的候选结果
# """

#             for item in llm_items:
#                 prompt += f"""
# 候选结果 {item['rank']}（{item['type']}检索）：
# {item['data']}
# """

#             if general_knowledge_results:
#                 prompt += f"""
# ### 通识检索结果
# """
#                 for result in general_knowledge_results:
#                     prompt += f"""
# {result}
# """

#             prompt += f"""
# ## 评估准则
# 1. "通识检索结果"仅参考，通常不是很准确，重点是知识库的"候选结果"
# 2. 关注数据的本质属性和核心特征是否相符
# 3. 重点比较你从数据中理解的特征
# 4. 如果去除行业特有词汇后，核心内容基本一致，则也可判定为匹配

# ## 输出要求
# - 回复两个部分：
#     第一部分为最匹配的候选结果序号数字（1-{len(llm_items)}），无论如何必须强制选一个该区间的数字
#     第二部分为表类型判定，对用户提供的表结构信息进行主观判断，判断是属于哪个领域的表，回答格式：这是一张XXX表
# - 回复格式：
#     csv格式回复，逗号隔开两个部分，回答最相关的一条，格式：<数字>,<表类型判定>
# - 不要添加任何其他无关信息。
# """

#             logger.info(f"llm_items:\n\n{llm_items}")
#             logger.info(f"提示词:\n\n{prompt}")
#             # 构建请求数据
#             messages = [
#                 {"role": "user", "content": prompt}
#             ]

#             request_data = ChatLLMConfig.get_request_data(messages)

#             # 调用LLM
#             async with aiohttp.ClientSession() as session:
#                 llm_client = AsyncLLMClient()
#                 response = await llm_client.call_llm(
#                     session=session,
#                     url=ChatLLMConfig.url,
#                     headers=ChatLLMConfig.headers,
#                     request_data=request_data,
#                     timeout=2000
#                 )

#                 # 解析响应
#                 if "choices" in response and len(response["choices"]) > 0:
#                     msg = response["choices"][0]["message"]["content"]
#                     result_text = msg.split(
#                         "</think>\n\n")[1] if "</think>\n\n" in msg else msg

#                     logger.info(f"LLM分类结果: {result_text}")
#                     return result_text
#                 else:
#                     logger.warning("LLM响应格式不正确")
#                     return None

#         except Exception as e:
#             logger.error(f"LLM分类判断失败: {e}", exc_info=True)
#             return None

#     async def _map_field_to_table_elements(self, field_name: str, field_query_sentence: str,
#                                            table_element_dict: dict) -> Optional[str]:
#         """
#         使用LLM将字段映射到表元素

#         Args:
#             field_name: 字段名
#             field_query_sentence: 字段查询句子
#             table_element_dict: 表元素字典，格式如 {"0":"身份证号", "1":"出生日期", "2":"性别"}

#         Returns:
#             LLM字段映射结果，格式如 "0,这是身份证字段,与身份证号最匹配"
#         """
#         try:
#             from app.core.config import ChatLLMConfig
#             from app.core.utils import AsyncLLMClient
#             import aiohttp

#             # 如果没有表元素，直接返回
#             if not table_element_dict:
#                 logger.warning(f"字段 {field_name} 没有表元素用于映射判断")
#                 return "-1"

#             # 构建提示词
#             prompt = f"""## 角色定义
# 你是一个专业的数据字段映射专家，具备跨行业数据理解能力。

# ## 任务背景
# - 我们有一个已知的数据表分类结果，其中包含敏感数据元素列表
# - 现在需要将用户数据库中的字段与这些敏感数据元素进行匹配

# ## 输入数据

# ### 用户提供的某行业数据库中的字段相关信息
# {field_query_sentence}

# ### 已知的敏感数据元素列表
# """

#             # 添加表元素列表
#             for index, element in table_element_dict.items():
#                 prompt += f"{index}: {element}\n"

#             prompt += f"""
# ## 评估准则
# - 根据字段名称、注释和示例值，判断该字段最有可能对应哪个敏感数据元素
# - 如果找不到合适的匹配项，请回复-1
# - 匹配时考虑字段语义的相似性，而不仅仅是字面匹配

# ## 输出要求
# - 回复三个部分：
#     第一部分为最匹配的候选结果序号数字，从以上敏感数据元素列表中选取一个最相似的，选一个该区间的数字，都不相关回复-1
#     第二部分为字段类型判定，对用户提供的字段及信息进行主观判断，判断是属于哪个领域的字段，回答格式：这是XXX字段
#     第三部分为选择该数字的理由，限制20字说明一个肯定的理由，不要出现"无法判断"或"无关"之类的词
# - 回复格式：
#     csv格式回复，逗号隔开三个部分，回答最相关的一条，格式：<数字>,<字段判定>,<理由>
# - 不要添加任何其他无关信息。
# """

#             print(prompt)
#             # 构建请求数据
#             messages = [
#                 {"role": "user", "content": prompt}
#             ]

#             logger.info(f"字段 {field_name} 的映射提示词:\n\n{prompt}")

#             request_data = ChatLLMConfig.get_request_data(messages)

#             # 调用LLM
#             async with aiohttp.ClientSession() as session:
#                 llm_client = AsyncLLMClient()
#                 response = await llm_client.call_llm(
#                     session=session,
#                     url=ChatLLMConfig.url,
#                     headers=ChatLLMConfig.headers,
#                     request_data=request_data,
#                     timeout=2000
#                 )

#                 # 解析响应
#                 if "choices" in response and len(response["choices"]) > 0:
#                     msg = response["choices"][0]["message"]["content"]
#                     result_text = msg.split(
#                         "</think>\n\n")[1] if "</think>\n\n" in msg else msg

#                     logger.info(f"字段 {field_name} 映射结果: {result_text}")
#                     return result_text
#                 else:
#                     logger.warning(f"字段 {field_name} 映射响应格式不正确")
#                     return "-1"

#         except Exception as e:
#             logger.error(f"字段 {field_name} 映射判断失败: {e}", exc_info=True)
#             return "-1"

#     async def _llm_field_classification(self, field_name: str, field_comment: Optional[str], sample_value: Optional[str],
#                                         table_bm25_results: List[str], table_vector_results: List[str],
#                                         table_general_knowledge_results: List[str]) -> Optional[str]:
#         """
#         使用LLM对单个字段进行分类判断

#         Args:
#             field_name: 字段名
#             field_comment: 字段注释
#             sample_value: 示例值
#             table_bm25_results: 表级别的BM25检索结果
#             table_vector_results: 表级别的向量检索结果
#             table_general_knowledge_results: 表级别的通用知识检索结果

#         Returns:
#             LLM字段分类结果
#         """
#         try:
#             from app.core.config import ChatLLMConfig
#             from app.core.utils import AsyncLLMClient
#             import aiohttp
#             import json

#             # 构建字段查询句子
#             parts = []
#             if field_name:
#                 parts.append(f"fieldName:{field_name}")
#             if field_comment:
#                 parts.append(f"fieldComment:{field_comment}")
#             if sample_value:
#                 parts.append(f"sampleValue:{sample_value}")

#             query_sentence = ",".join(parts)

#             # 使用表级别的检索结果作为字段级别的候选结果
#             llm_items = []
#             rank = 1

#             # 添加BM25检索结果
#             for result in table_bm25_results:
#                 llm_items.append({
#                     'rank': rank,
#                     'type': 'BM25',
#                     'data': result
#                 })
#                 rank += 1

#             # 添加向量检索结果
#             for result in table_vector_results:
#                 llm_items.append({
#                     'rank': rank,
#                     'type': '向量',
#                     'data': result
#                 })
#                 rank += 1

#             # 如果没有候选结果，返回None
#             if not llm_items:
#                 logger.warning(f"字段 {field_name} 没有候选结果用于LLM分类判断")
#                 return None

#             # 构建提示词
#             prompt = f"""## 角色定义
# 你是一个专业的数据字段分类专家，具备跨行业数据理解能力。

# ## 任务背景
# - 我们的知识库是基于电信行业的数据建立的，有时可能用它来匹配其他行业的数据，这里需要你灵活判断。

# ## 输入数据

# ### 用户提供的某行业数据库中的字段相关信息
# {query_sentence}

# ### 从分类分级知识库中检索到的候选结果
# """

#             for item in llm_items:
#                 prompt += f"""
# 候选结果 {item['rank']}（{item['type']}检索）：
# {item['data']}
# """

#             if table_general_knowledge_results:
#                 prompt += f"""
# ### 通识检索结果
# """
#                 for result in table_general_knowledge_results:
#                     prompt += f"""
# {result}
# """

#             prompt += f"""
# ## 评估准则
# 1. "通识检索结果"仅参考，通常不是很准确，重点是知识库的"候选结果"
# 2. 关注数据的本质属性和核心特征是否相符
# 3. 重点比较你从数据中理解的特征
# 4. 如果去除行业特有词汇后，核心内容基本一致，则也可判定为匹配

# ## 输出要求
# - 回复三个部分：
#     第一部分为最匹配的候选结果序号数字（1-{len(llm_items)}），无论如何必须强制选一个该区间的数字
#     第二部分为字段类型判定，对用户提供的字段及信息进行主观判断，判断是属于哪个领域的字段，回答格式：这是XXX字段
#     第三部分为判定理由，限制20字说明一个理由，表达官方，不要出现"无法判断"之类的词
# - 回复格式：
#     csv格式回复，逗号隔开三个部分，回答最相关的一条，格式：<数字>,<字段类型判定>,<理由>
# - 不要添加任何其他无关信息。
# """

#             # 构建请求数据
#             messages = [
#                 {"role": "user", "content": prompt}
#             ]

#             logger.info(f"llm_items:\n\n{llm_items}")
#             logger.info(f"提示词:\n\n{prompt}")

#             request_data = ChatLLMConfig.get_request_data(messages)

#             # 调用LLM
#             async with aiohttp.ClientSession() as session:
#                 llm_client = AsyncLLMClient()
#                 response = await llm_client.call_llm(
#                     session=session,
#                     url=ChatLLMConfig.url,
#                     headers=ChatLLMConfig.headers,
#                     request_data=request_data,
#                     timeout=2000
#                 )

#                 # 解析响应
#                 if "choices" in response and len(response["choices"]) > 0:
#                     msg = response["choices"][0]["message"]["content"]
#                     result_text = msg.split(
#                         "</think>\n\n")[1] if "</think>\n\n" in msg else msg

#                     logger.info(f"LLM字段分类结果: {result_text}")
#                     return result_text
#                 else:
#                     logger.warning("LLM字段分类响应格式不正确")
#                     return None

#         except Exception as e:
#             logger.error(f"LLM字段分类判断失败: {e}", exc_info=True)
#             return None


# data_recognizing_service = DataRecognitionService()













"""
数据识别服务
处理AI数据识别逻辑
"""

import logging
import asyncio
import re
from typing import List, Optional
from pymilvus import MilvusClient
from app.schemas.knowledge_base import (
    DataRecognitionRequest,
    TableAIScanResultDto,
    FieldAIScanResultDto,
    FieldDataDto
)
from app.core.config import DatabaseConfig
from app.core.vectoring import VectorClient

logger = logging.getLogger(__name__)


class DataRecognitionService:
    """数据识别服务类"""

    def __init__(self):
        """初始化服务"""
        self.db_path = DatabaseConfig.uri
        self.milvus_client = MilvusClient(self.db_path)

    # def _find_relevant_collections(self) -> dict:
    #     """
    #     查找所有相关的集合，包括 *_classification（排除 *_narrative_classification）和 *_general_knowledge

    #     Returns:
    #         dict: 包含分类集合和通用知识集合的字典
    #     """
    #     try:
    #         collections = self.milvus_client.list_collections()
    #     except Exception as e:
    #         logger.error(f"获取集合列表失败: {e}")
    #         collections = []

    #     classification_collections = []
    #     general_knowledge_collections = []

    #     for collection in collections:
    #         # 匹配 *_classification 但排除 *_narrative_classification
    #         if (collection.endswith('_classification') and
    #             not collection.endswith('_narrative_classification')):
    #             classification_collections.append(collection)
    #         # 匹配 *_general_knowledge
    #         elif collection.endswith('_general_knowledge'):
    #             general_knowledge_collections.append(collection)

    #     return {
    #         'classification': classification_collections,
    #         'general_knowledge': general_knowledge_collections
    #     }
    def _find_relevant_collections(self, specification_uid: str = None) -> dict:
        """
        查找相关的集合，包括 *_classification（排除 *_narrative_classification）和 *_general_knowledge
        如果提供了specification_uid，则只返回与该规范相关的集合

        Args:
            specification_uid: 规范UID，如果提供则只查找相关集合

        Returns:
            dict: 包含分类集合和通用知识集合的字典
        """
        try:
            collections = self.milvus_client.list_collections()
        except Exception as e:
            logger.error(f"获取集合列表失败: {e}")
            collections = []

        classification_collections = []
        general_knowledge_collections = []

        # 如果提供了specification_uid，则只查找相关集合
        # if specification_uid:
        #     specification_uid = specification_uid.replace("-", "_")
        #     classification_collection = f"{specification_uid}_classification"
        #     general_knowledge_collection = f"{specification_uid}_general_knowledge"
        if specification_uid:
            specification_uid = specification_uid.replace("-", "_")
            # 确保集合名称以字母或下划线开头（Milvus要求）
            if specification_uid and not specification_uid[0].isalpha() and specification_uid[0] != '_':
                specification_uid = '_' + specification_uid
                
            classification_collection = f"{specification_uid}_classification"
            general_knowledge_collection = f"{specification_uid}_general_knowledge"

            if classification_collection in collections:
                classification_collections.append(classification_collection)
            if general_knowledge_collection in collections:
                general_knowledge_collections.append(
                    general_knowledge_collection)
        else:
            # 否则查找所有集合（原有逻辑）
            for collection in collections:
                # 匹配 *_classification 但排除 *_narrative_classification
                if (collection.endswith('_classification') and
                        not collection.endswith('_narrative_classification')):
                    classification_collections.append(collection)
                # 匹配 *_general_knowledge
                elif collection.endswith('_general_knowledge'):
                    general_knowledge_collections.append(collection)

        return {
            'classification': classification_collections,
            'general_knowledge': general_knowledge_collections
        }

    def _bm25_search(self, collection_name: str, query: str, top_k: int = 3) -> List[str]:
        """
        在指定集合中进行BM25检索

        Args:
            collection_name: 集合名称
            query: 查询字符串
            top_k: 返回结果数量

        Returns:
            List[str]: 检索到的text值列表
        """
        try:
            # 检查集合是否存在
            if not self.milvus_client.has_collection(collection_name):
                logger.warning(f"集合 {collection_name} 不存在")
                return []

            # 加载集合
            self.milvus_client.load_collection(collection_name=collection_name)

            # 尝试导入BM25库
            try:
                from rank_bm25 import BM25Okapi
                import jieba
                import json
                import re
            except ImportError as e:
                logger.warning(f"缺少必要的库来执行BM25搜索: {e}")
                return []

            # 从Milvus中获取所有文档用于构建BM25模型
            try:
                all_results = self.milvus_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["id", "text", "items"],
                    limit=10000  # 假设不超过10000条记录
                )
            except Exception as e:
                logger.error(f"获取数据构建BM25索引时出错: {e}")
                return []

            logger.info(
                f"从集合 {collection_name} 中获取到 {len(all_results)} 条记录用于BM25索引构建")

            corpus = []
            metadata = []

            for i, result in enumerate(all_results):
                items_str = result.get("items", "")
                items_list = []

                # 处理特殊的items格式
                if items_str:
                    # 移除特殊字符和结构符号
                    # 移除JSON结构符号和特殊标记
                    cleaned_str = re.sub(r'[{}"\[\]]', '', items_str)
                    # 分割并清理词汇
                    raw_tokens = [token.strip() for token in cleaned_str.split(
                        ',') if token.strip()]
                    # 过滤掉无意义的符号和标记
                    items_list = [token for token in raw_tokens
                                  if token and token not in ['#', '', ' ']
                                  and not re.match(r'^\d+$', token)  # 过滤纯数字
                                  and len(token) > 1]  # 过滤单字符

                # 记录前几条数据的情况用于调试
                if i < 3:
                    logger.info(
                        f"记录 {i}: items_str={items_str[:100]}..., parsed_items={items_list[:10] if items_list else None}")

                # 只有非空的文档才加入语料库
                if items_list and isinstance(items_list, list) and len(items_list) > 0:
                    corpus.append(items_list)
                    metadata.append({
                        "id": result.get("id"),
                        "text": result.get("text"),
                        "items": items_str
                    })
                elif i < 3:  # 记录被过滤的原因
                    logger.info(
                        f"记录 {i} 被过滤: items_list={items_list}, is_list={isinstance(items_list, list)}, len={len(items_list) if isinstance(items_list, list) else 'N/A'}")

            logger.info(f"有效文档数量: {len(corpus)}")

            if not corpus:
                logger.warning("没有有效的文档可用于构建BM25索引")
                return []

            # 检查是否所有文档都是空的
            total_tokens = sum(len(doc) for doc in corpus)
            logger.info(f"总token数: {total_tokens}")

            if total_tokens == 0:
                logger.warning("所有文档都为空，无法构建BM25索引")
                return []

            # 构建BM25模型
            try:
                bm25_model = BM25Okapi(corpus)
                logger.info("BM25模型构建成功")
            except Exception as e:
                logger.error(f"构建BM25模型时出错: {e}")
                return []

            # 对查询文本进行jieba分词
            query_tokens = list(jieba.cut(query))
            query_tokens = [token for token in query_tokens if len(
                token.strip()) > 0 and len(token) > 1]

            logger.info(f"查询文本 '{query}' 分词结果: {query_tokens}")

            if not query_tokens:
                logger.warning("查询文本分词后为空")
                return []

            # 使用标准BM25计算得分
            doc_scores = bm25_model.get_scores(query_tokens)

            # 记录得分情况
            logger.info(
                f"文档得分 (前10个): {doc_scores[:10] if len(doc_scores) > 10 else doc_scores}")

            # 获取top_k结果
            top_indices = doc_scores.argsort()[::-1][:top_k]

            texts = []
            for idx in top_indices:
                score = doc_scores[idx]
                logger.info(f"文档索引 {idx} 得分: {score}")
                if score > 0:  # 只返回得分大于0的结果
                    metadata_entry = metadata[idx]
                    texts.append(metadata_entry["text"])
                    logger.info(f"添加结果: {metadata_entry['text'][:100]}...")
                else:
                    logger.info(f"文档索引 {idx} 得分为0，跳过")

            logger.info(f"最终返回 {len(texts)} 条BM25结果")
            return texts
        except Exception as e:
            logger.error(
                f"BM25检索失败 (集合: {collection_name}): {e}", exc_info=True)
            return []

    # async def _vector_search(self, collection_name: str, query: str, top_k: int = 3) -> List[str]:
    #     """
    #     在指定集合中进行向量相似度检索

    #     Args:
    #         collection_name: 集合名称
    #         query: 查询字符串
    #         top_k: 返回结果数量

    #     Returns:
    #         List[str]: 检索到的text值列表
    #     """
    #     try:
    #         # 检查集合是否存在
    #         if not self.milvus_client.has_collection(collection_name):
    #             logger.warning(f"集合 {collection_name} 不存在")
    #             return []

    #         # 加载集合
    #         self.milvus_client.load_collection(collection_name=collection_name)

    #         # 初始化向量客户端（使用特定的集合名称）
    #         vector_client = VectorClient()
    #         vector_client.collection_name = collection_name

    #         # 获取查询文本的向量表示
    #         query_vectors = await vector_client.get_embeddings([query])

    #         # 在向量数据库中搜索
    #         results = await vector_client.search(query_vectors, top_k)

    #         # 提取text值
    #         # texts = []
    #         # for result in results[0]:  # 第一个查询的结果
    #         #     if 'text' in result.get('entity', {}):
    #         #         texts.append(result['entity']['text'])

    #         # return texts
    #         results_data = []
    #         for result in results[0]:  # 第一个查询的结果
    #             entity = result.get('entity', {})
    #             if 'text' in entity:
    #                 # 返回包含更多信息的字典，而不只是text字符串
    #                 result_item = {
    #                     'text': entity['text']
    #                 }
    #                 # 如果存在vectorizing_text字段，也一并返回
    #                 if 'vectorizing_text' in entity:
    #                     result_item['vectorizing_text'] = entity['vectorizing_text']
    #                 results_data.append(result_item)

    #         return results_data
    #     except Exception as e:
    #         logger.error(f"向量相似度检索失败 (集合: {collection_name}): {e}")
    #         return []
    async def _vector_search(self, collection_name: str, query: str, top_k: int = 3) -> List[str]:
        """
        在指定集合中进行向量相似度检索

        Args:
            collection_name: 集合名称
            query: 查询字符串
            top_k: 返回结果数量

        Returns:
            List[str]: 检索到的text值列表
        """
        try:
            # 检查集合是否存在
            if not self.milvus_client.has_collection(collection_name):
                logger.warning(f"集合 {collection_name} 不存在")
                return []

            # 加载集合
            self.milvus_client.load_collection(collection_name=collection_name)

            # 初始化向量客户端（使用特定的集合名称）
            vector_client = VectorClient()
            vector_client.collection_name = collection_name

            # 获取查询文本的向量表示
            query_vectors = await vector_client.get_embeddings([query])

            # 在向量数据库中搜索
            results = await vector_client.search(query_vectors, top_k)

            # 提取text值
            texts = []
            for result in results[0]:  # 第一个查询的结果
                entity = result.get('entity', {})
                if 'text' in entity:
                    text_value = entity['text']
                    # 如果存在vectorizing_text且与text不同，可以组合显示
                    if 'vectorizing_text' in entity and entity['vectorizing_text'] != text_value:
                        combined_text = f"分类信息: {text_value}\n向量化文本: {entity['vectorizing_text']}"
                        texts.append(combined_text)
                    else:
                        texts.append(text_value)

            return texts
        except Exception as e:
            logger.error(f"向量相似度检索失败 (集合: {collection_name}): {e}")
            return []

    def _build_query_sentence(self, request: DataRecognitionRequest) -> str:
        """
        构建用于向量检索的查询句子

        Args:
            request: 数据识别请求

        Returns:
            str: 构建好的查询句子
        """
        from app.core.utils import remove_annotations
        
        parts = []

        # 添加dbName（必须）
        cleaned_db_name = remove_annotations(request.dbName)
        parts.append(f"dbName:{cleaned_db_name}")

        # 添加其他可选字段
        if getattr(request, 'schemaName', None):
            cleaned_schema_name = remove_annotations(request.schemaName)
            parts.append(f"schemaName:{cleaned_schema_name}")
        if getattr(request, 'tableName', None):
            cleaned_table_name = remove_annotations(request.tableName)
            parts.append(f"tableName:{cleaned_table_name}")
        if getattr(request, 'tableComment', None):
            cleaned_table_comment = remove_annotations(request.tableComment)
            parts.append(f"tableComment:{cleaned_table_comment}")
        if getattr(request, 'tableRows', None) is not None:
            parts.append(f"tableRows:{str(request.tableRows)}")
        if getattr(request, 'systemType', None):
            cleaned_system_type = remove_annotations(request.systemType)
            parts.append(f"systemType:{cleaned_system_type}")
        if getattr(request, 'systemName', None):
            cleaned_system_name = remove_annotations(request.systemName)
            parts.append(f"systemName:{cleaned_system_name}")
        parts.append("其他字段:")
        if getattr(request, 'fields', None):
            for field in request.fields:
                cleaned_field_name = remove_annotations(field.fieldName)
                parts.append(f"{cleaned_field_name}")
                if getattr(field, 'fieldComment', None):
                    cleaned_field_comment = remove_annotations(field.fieldComment)
                    parts.append(f"{cleaned_field_comment}")

        return ",".join(parts)
    
    def _build_query_sentence_similarity(self, request: DataRecognitionRequest) -> str:
        """
        构建用于向量检索的查询句子

        Args:
            request: 数据识别请求

        Returns:
            str: 构建好的查询句子
        """
        from app.core.utils import remove_annotations
        parts = []

        # 添加dbName（必须）
        # parts.append(f"dbName:{request.dbName}")

        # 添加其他可选字段
        # if getattr(request, 'schemaName', None):
        #     parts.append(f"schemaName:{request.schemaName}")
        if getattr(request, 'tableComment', None):
            cleaned_table_comment = remove_annotations(request.tableComment)
            parts.append(f"{cleaned_table_comment}")
        elif getattr(request, 'tableName', None):
            cleaned_table_name = remove_annotations(request.tableName)
            parts.append(f"{cleaned_table_name}")
        # if getattr(request, 'tableRows', None) is not None:
        #     parts.append(f"tableRows:{str(request.tableRows)}")
        # if getattr(request, 'systemType', None):
        #     parts.append(f"systemType:{request.systemType}")
        # if getattr(request, 'systemName', None):
        #     parts.append(f"systemName:{request.systemName}")
        if getattr(request, 'fields', None):
            for field in request.fields:
                cleaned_field_name = remove_annotations(field.fieldName)
                parts.append(f"{cleaned_field_name}")
                if getattr(field, 'fieldComment', None):
                    cleaned_field_comment = remove_annotations(field.fieldComment)
                    parts.append(f"{cleaned_field_comment}")

        # 用逗号连接所有部分
        return ",".join(parts)

    async def recognize_data(self, request: DataRecognitionRequest) -> List[TableAIScanResultDto]:
        """
        识别数据的AI分类和分级

        Args:
            request: 数据识别请求

        Returns:
            List[TableAIScanResultDto]: 表识别结果列表
        """
        from app.core.utils import remove_annotations
        
        senSpecificationUId = request.senSpecificationUId.replace("-", "_") if request.senSpecificationUId else None
        impSpecificationUId = request.impSpecificationUId.replace("-", "_") if request.impSpecificationUId else None
        coreSpecificationUId = request.coreSpecificationUId.replace("-", "_") if request.coreSpecificationUId else None
        
        senSpecificationUId = f"_{senSpecificationUId}" if senSpecificationUId else None
        impSpecificationUId = f"_{impSpecificationUId}" if impSpecificationUId else None
        coreSpecificationUId = f"_{coreSpecificationUId}" if coreSpecificationUId else None  
        
        # 查找相关集合，如果提供了任何一个或多个specificationUId则查找所有相关集合
        # 收集所有提供的UId
        specification_uids = []
        if getattr(request, 'senSpecificationUId', None):
            specification_uids.append(senSpecificationUId)
        if getattr(request, 'impSpecificationUId', None):
            specification_uids.append(impSpecificationUId)
        if getattr(request, 'coreSpecificationUId', None):
            specification_uids.append(coreSpecificationUId)
            
        # 查找所有相关集合
        all_classification_collections = []
        all_general_knowledge_collections = []
        
        for uid in specification_uids:
            relevant_collections = self._find_relevant_collections(uid)
            all_classification_collections.extend(relevant_collections['classification'])
            all_general_knowledge_collections.extend(relevant_collections['general_knowledge'])
        
        # 去重
        classification_collections = list(set(all_classification_collections))
        general_knowledge_collections = list(set(all_general_knowledge_collections))

        logger.info(f"找到分类集合: {classification_collections}")
        logger.info(f"找到通用知识集合: {general_knowledge_collections}")

        if not classification_collections and not general_knowledge_collections:
            logger.warning("未找到任何相关集合")
            return []

        # 获取dbName
        db_name = request.dbName

        # 获取coreSpecificationUId和impSpecificationUId
        core_uid = getattr(request, 'coreSpecificationUId', None)
        imp_uid = getattr(request, 'impSpecificationUId', None)

        # 存储所有检索结果
        bm25_results = []
        vector_results = []
        general_knowledge_results = []

        # 在分类集合中进行BM25检索（使用top_k=15）
        for collection_name in classification_collections:
            # BM25检索
            bm25_texts = self._bm25_search(collection_name, db_name, 15)
            bm25_results.extend(bm25_texts)

        # 构建查询句子
        query_sentence = self._build_query_sentence(request)
        query_sentence_similarity = self._build_query_sentence_similarity(request)
        logger.info(f"构建的查询句子: {query_sentence_similarity}")

        # 在分类集合中进行向量相似度检索（使用top_k=15）
        vector_results = []
        for collection_name in classification_collections:
            # 向量相似度检索
            collection_results = await self._vector_search(
                collection_name, query_sentence_similarity, 15)
            vector_results.extend(collection_results)

        # 在通用知识集合中进行向量相似度检索（使用top_k=15）
        for collection_name in general_knowledge_collections:
            # 向量相似度检索
            collection_results = await self._vector_search(
                collection_name, query_sentence_similarity, 15)
            general_knowledge_results.extend(collection_results)

        # 筛选BM25和向量检索结果（直接使用未筛选的结果）
        filtered_bm25_results = bm25_results
        filtered_vector_results = vector_results

        # 去重并保留前3个结果
        # unique_bm25_results = list(dict.fromkeys(filtered_bm25_results))[:5]
        # unique_vector_results = list(
        #     dict.fromkeys(filtered_vector_results))[:5]
        # unique_general_knowledge_results = list(
        #     dict.fromkeys(general_knowledge_results))[:5]
        unique_bm25_results = list(dict.fromkeys(filtered_bm25_results))[:5]
        # 修复：处理字典类型的去重
        unique_vector_results = []
        seen_texts = set()
        for result in filtered_vector_results:
            # 如果result是字典，使用text字段进行去重；如果是字符串，直接使用
            text_key = result['text'] if isinstance(result, dict) else result
            if text_key not in seen_texts:
                seen_texts.add(text_key)
                unique_vector_results.append(result)
        unique_vector_results = unique_vector_results[:5]
        
        unique_general_knowledge_results = list(
            dict.fromkeys(general_knowledge_results))[:5]

        # 记录检索结果
        logger.info(
            f"原始BM25检索结果数: {len(bm25_results)}, 筛选后结果数: {len(filtered_bm25_results)}, 最终使用结果数: {len(unique_bm25_results)}")
        logger.info(
            f"原始向量检索结果数: {len(vector_results)}, 筛选后结果数: {len(filtered_vector_results)}, 最终使用结果数: {len(unique_vector_results)}")
        logger.info(
            f"通用知识检索结果数: {len(general_knowledge_results)}, 最终使用结果数: {len(unique_general_knowledge_results)}")
        logger.info(f"BM25检索结果: {unique_bm25_results}")
        logger.info(f"向量相似度检索结果: {unique_vector_results}")
        logger.info(f"通用知识检索结果: {unique_general_knowledge_results}")

        # 使用LLM进行分类判断
        llm_result = await self._llm_classification(
            query_sentence,
            unique_bm25_results,
            unique_vector_results,
            unique_general_knowledge_results
        )

        # 初始化分类和分级信息
        table_classification = ""
        table_grade = ""
        table_annotate = ""
        table_element_list = []

        # 处理LLM结果
        if llm_result:
            try:
                # 解析LLM响应
                parts = llm_result.split(',', 1)
                if len(parts) == 2:
                    index_part = parts[0].strip()
                    table_annotate = parts[1].strip()

                    if index_part == "-1":
                        # 创建一个只有表注解信息的空结果对象
                        table_result = TableAIScanResultDto(
                            dbName=request.dbName,
                            schemaName=getattr(request, 'schemaName', None),
                            tableName=getattr(request, 'tableName', "") or "",
                            tableAnnotate=table_annotate,  # 保留表注解
                            tableClassification="",
                            tableGrade="",
                            tableElement=[],
                            fields=[]
                        )
                        
                        result = [table_result]
                        return result

                    # 获取索引数字
                    # selected_index = int(index_part)
                    # 提取索引数字（处理任何格式，提取其中的数字）
                    import re
                    numbers = re.findall(r'\d+', index_part)
                    if numbers:
                        selected_index = int(numbers[0])
                    else:
                        raise ValueError(f"无法从 '{index_part}' 提取有效数字")
                    # 获取对应的分类结果
                    all_candidate_results = unique_bm25_results + unique_vector_results
                    if 1 <= selected_index <= len(all_candidate_results):

                        selected_result = all_candidate_results[selected_index - 1]

                        if "\n向量化文本:" in selected_result:
                            # 分割字符串，取"向量化文本:"之前的部分，即分类信息
                            classification_part = selected_result.split("\n向量化文本:")[0]
                            selected_result = classification_part.strip()
                                
                        # 解析分类结果
                        # try:
                        #     import json
                        #     result_data = json.loads(selected_result)

                        #     # 提取分类信息
                        #     header = result_data.get("header", {})
                        #     data = result_data.get("data", {})
                        try:
                            import json
                            import re
                            
                            # 尝试直接解析，如果失败则尝试从包含额外文本的内容中提取JSON
                            try:
                                result_data = json.loads(selected_result)
                            except json.JSONDecodeError:
                                # 如果直接解析失败，尝试使用正则表达式提取JSON对象
                                # 匹配最外层的大括号内容
                                json_match = re.search(r'\{.*\}', selected_result, re.DOTALL)
                                if json_match:
                                    json_str = json_match.group(0)
                                    result_data = json.loads(json_str)
                                else:
                                    raise ValueError(f"无法从 '{selected_result}' 中提取有效的JSON")

                            # 提取分类信息
                            header = result_data.get("header", {})
                            data = result_data.get("data", {})

                            # 查找最高级别的分类（支持任意级别，不局限于三级分类）
                            classification_value = ""
                            # 创建一个有序的分类级别列表，从高到低排序
                            classification_levels = []
                            level_mapping = {}

                            # 收集所有分类相关字段
                            for key, value in header.items():
                                if value.endswith("分类"):
                                    # 提取级别数字（如"四级分类"提取出4）
                                    if value.startswith("一级"):
                                        level_num = 1
                                    elif value.startswith("二级"):
                                        level_num = 2
                                    elif value.startswith("三级"):
                                        level_num = 3
                                    elif value.startswith("四级"):
                                        level_num = 4
                                    elif value.startswith("五级"):
                                        level_num = 5
                                    elif value.startswith("六级"):
                                        level_num = 6
                                    elif value.startswith("七级"):
                                        level_num = 7
                                    elif value.startswith("八级"):
                                        level_num = 8
                                    else:
                                        # 尝试提取数字
                                        import re
                                        num_match = re.search(r'^(\d+)', value)
                                        level_num = int(num_match.group(
                                            1)) if num_match else 99  # 默认放到最后

                                    classification_levels.append(
                                        (level_num, key, value))
                                    level_mapping[value] = key

                            # 按级别数字排序（从高到低）
                            classification_levels.sort(reverse=True)

                            # 查找第一个非空的分类值
                            for level_num, key, level_name in classification_levels:
                                candidate_value = data.get(key, "")
                                if candidate_value:
                                    classification_value = candidate_value
                                    break

                            # 如果没有找到按级别命名的分类，尝试其他方法
                            if not classification_value:
                                # 先查找三级分类（保持向后兼容）
                                for key, value in header.items():
                                    if value == "三级分类":
                                        classification_value = data.get(
                                            key, "")
                                        break

                            # 如果三级分类为空，查找二级分类
                            if not classification_value:
                                for key, value in header.items():
                                    if value == "二级分类":
                                        classification_value = data.get(
                                            key, "")
                                        break

                            # 如果二级分类也为空，查找一级分类
                            if not classification_value:
                                for key, value in header.items():
                                    if value == "一级分类":
                                        classification_value = data.get(
                                            key, "")
                                        break

                            table_classification = classification_value

                            # 确定数据等级
                            # 直接从等级字段获取数据等级，不再根据分类值前缀判断
                            # 查找等级字段
                            for key, value in header.items():
                                if value == "等级":
                                    raw_grade = data.get(key, "")
                                    # 处理多级数据等级，根据配置选择最高或最低级别
                                    from app.core.config import DataGradeConfig
                                    if "/" in raw_grade:
                                        grades = raw_grade.split("/")
                                        if DataGradeConfig.grade_selection_strategy == "highest":
                                            # 选择最高级别（数字最大）
                                            numeric_grades = []
                                            for grade in grades:
                                                import re
                                                match = re.search(
                                                    r'第(\d+)级', grade)
                                                if match:
                                                    numeric_grades.append(
                                                        int(match.group(1)))
                                            if numeric_grades:
                                                max_grade = max(
                                                    numeric_grades)
                                                table_grade = f"第{max_grade}级"
                                            else:
                                                table_grade = raw_grade  # 无法解析，返回原始值
                                        else:
                                            # 默认选择最低级别（数字最小）
                                            numeric_grades = []
                                            for grade in grades:
                                                import re
                                                match = re.search(
                                                    r'第(\d+)级', grade)
                                                if match:
                                                    numeric_grades.append(
                                                        int(match.group(1)))
                                            if numeric_grades:
                                                min_grade = min(
                                                    numeric_grades)
                                                table_grade = f"第{min_grade}级"
                                            else:
                                                table_grade = raw_grade  # 无法解析，返回原始值
                                    else:
                                        table_grade = raw_grade
                                    break

                            # 查找真实数据字段
                            for key, value in header.items():
                                if value == "真实数据":
                                    table_element = data.get(key, "")
                                    # 如果是列表，直接使用；如果是字符串，转换为单元素列表
                                    if isinstance(table_element, list):
                                        table_element_list = table_element
                                    elif isinstance(table_element, str):
                                        table_element_list = [table_element]
                                    break

                        except json.JSONDecodeError:
                            print("=================================", selected_result)
                            logger.warning(f"无法解析分类结果为JSON: {selected_result}")
            except Exception as e:
                logger.error(f"处理LLM结果时出错: {e}")

        # 处理字段级别的分类
        field_results = []

        # 将table_element_list转换为键值对形式，用于字段匹配
        table_element_dict = {str(i): element for i,
                              element in enumerate(table_element_list)}
        logger.info(f"表元素字典: {table_element_dict}")

        table_info_parts = []
        if getattr(request, 'tableName', None):
            table_name = remove_annotations(request.tableName)
            table_info_parts.append(f"所属表为:{table_name}")
        if getattr(request, 'tableComment', None):
            table_comment = remove_annotations(request.tableComment)
            table_info_parts.append(f"{table_comment}")
        table_info_content = ",".join(table_info_parts)
            
        for field in request.fields:
            # 构建字段查询句子
            field_parts = []
            if getattr(field, 'fieldName', None):
                field_parts.append(f"fieldName:{field.fieldName}")
            if getattr(field, 'fieldComment', None):
                field_parts.append(f"fieldComment:{field.fieldComment}")
            if getattr(field, 'sampleValue', None):
                field_parts.append(f"sampleValue:{field.sampleValue}")

            field_query_sentence = ",".join(field_parts)

            # 初始化字段结果
            field_annotate = getattr(field, 'fieldComment', '') or ''
            field_element = ""
            field_classification = ""
            field_grade = ""
            field_reason = ""

            # 调用新的字段映射方法
            field_mapping_result = await self._map_field_to_table_elements(
                field.fieldName,
                table_info_content,
                field_query_sentence,
                table_element_dict
            )

            # 处理字段映射结果
            if field_mapping_result and field_mapping_result != "-1":
                try:
                    # 解析响应 (应该是三个部分: index, field_type, reason)
                    parts = field_mapping_result.split(',', 2)  # 最多分割成3部分
                    if len(parts) >= 3:
                        index_part = parts[0].strip()
                        field_annotate = parts[1].strip()
                        field_reason = parts[2].strip()

                        # 获取索引数字并映射到具体元素
                        # selected_index = int(index_part)
                        # if str(selected_index) in table_element_dict:
                        # field_element = table_element_dict[str(selected_index)]
                        # 提取索引数字（处理任何格式，提取其中的数字）

                        if numbers:
                            selected_index = int(index_part)
                        else:
                            raise ValueError(f"无法从 '{index_part}' 提取有效数字")

                        if str(selected_index) in table_element_dict:
                            field_element = table_element_dict[str(
                                selected_index)]
                except Exception as e:
                    logger.error(f"处理字段映射结果时出错: {e}")

            field_result = FieldAIScanResultDto(
                fieldName=field.fieldName,
                fieldAnnotate=field_annotate,
                element=field_element,
                classification=field_classification,
                grade=field_grade,
                reason=field_reason
            )
            field_results.append(field_result)

        table_result = TableAIScanResultDto(
            dbName=request.dbName,
            schemaName=getattr(request, 'schemaName', None),
            tableName=getattr(request, 'tableName', "") or "",
            tableAnnotate=table_annotate,
            tableClassification=table_classification,
            tableGrade=table_grade,
            tableElement=table_element_list,
            fields=field_results
        )

        result = [table_result]

        return result

    async def _llm_classification(self, query_sentence: str, bm25_results: List[str], vector_results: List[str], general_knowledge_results: List[str]) -> Optional[str]:
        """
        使用LLM对检索结果进行分类判断

        Args:
            query_sentence: 查询句子
            bm25_results: BM25检索结果
            vector_results: 向量检索结果
            general_knowledge_results: 通用知识检索结果

        Returns:
            LLM分类结果
        """
        try:
            from app.core.config import ChatLLMConfig
            from app.core.utils import AsyncLLMClient
            import aiohttp
            import json

            # 构建候选结果列表
            llm_items = []
            rank = 1

            # 添加BM25检索结果
            for result in bm25_results:
                llm_items.append({
                    'rank': rank,
                    'type': 'BM25',
                    'data': result
                })
                rank += 1

            # 添加向量检索结果
            # for result in vector_results:
            #     llm_items.append({
            #         'rank': rank,
            #         'type': '向量',
            #         'data': result
            #     })
            #     rank += 1
            for result in vector_results:
                # result可能是字符串（原始情况）或字典（包含更多信息的情况）
                if isinstance(result, dict):
                    # 如果result是字典，包含vectorizing_text信息
                    if 'vectorizing_text' in result and result['vectorizing_text'] != result.get('text'):
                        # 如果vectorizing_text与text不同，显示两者
                        full_data = f"分类信息: {result['text']}\n向量化文本: {result['vectorizing_text']}"
                    else:
                        # 如果相同或不存在，只显示text
                        full_data = result['text']
                else:
                    # 如果result是字符串（原始情况），直接使用
                    full_data = result
                
                llm_items.append({
                    'rank': rank,
                    'type': '向量',
                    'data': full_data
                })
                rank += 1

            # 如果没有候选结果，直接返回
            if not llm_items:
                logger.warning("没有候选结果用于LLM分类判断")
                return None

            # 构建提示词
            prompt = f"""## 角色定义
你是一个专业的数据表分类专家，具备跨行业数据理解能力。

## 任务背景
- 我们的知识库是基于电信行业的数据建立的，有时可能用它来匹配其他行业的数据，这里需要你灵活判断。

## 输入数据

### 用户提供的某行业数据库中的表结构信息
{query_sentence}

### 从分类分级知识库中检索到的候选结果
"""

#             for item in llm_items:
#                 prompt += f"""
# 候选结果 {item['rank']}（{item['type']}检索）：
# {item['data']}
# """
            for item in llm_items:
                if item['type'] == '向量' and isinstance(item['data'], dict):
                    # 如果是向量检索结果且包含多个字段
                    vector_data = item['data']
                    data_output = f"基础信息: {vector_data.get('text', '')}"
                    if 'vectorizing_text' in vector_data and vector_data['vectorizing_text'] != vector_data.get('text'):
                        data_output += f"\n额外重要数据有: {vector_data['vectorizing_text']}"
                    
                    prompt += f"""
候选结果 {item['rank']}（{item['type']}检索）：
{data_output}
"""
                else:
                    # 对于BM25检索或其他类型的结果，保持原样
                    prompt += f"""
候选结果 {item['rank']}（{item['type']}检索）：
{item['data']}
"""

            if general_knowledge_results:
                prompt += f"""
### 通识检索结果
"""
                for result in general_knowledge_results:
                    prompt += f"""
{result}
"""

            prompt += f"""
## 评估准则
1. "通识检索结果"仅参考，通常不是很准确，重点是知识库的"候选结果"
2. 关注数据的本质属性和核心特征是否相符
3. 重点比较你从数据中理解的特征
4. 如果去除行业特有词汇后，核心内容基本一致，则也可判定为匹配

## 输出要求
- 回复两个部分：
    第一部分为最匹配的候选结果序号数字（1-{len(llm_items)}），无论如何必须强制选一个数字，如果候选结果几乎不匹配，则回复-1
    第二部分为表类型判定，对用户提供的表结构信息进行主观判断，判断是属于哪个领域的表，回答格式：这是一张XXX表
- 回复格式：
    csv格式回复，逗号隔开两个部分，回答最相关的一条，格式：<数字>,<表类型判定>
- 不要添加任何其他无关信息。
"""

            logger.info(f"llm_items:\n\n{llm_items}")
            logger.info(f"提示词:\n\n{prompt}")
            # 构建请求数据
            messages = [
                {"role": "user", "content": prompt}
            ]

            request_data = ChatLLMConfig.get_request_data(messages)

            # 调用LLM
            async with aiohttp.ClientSession() as session:
                llm_client = AsyncLLMClient()
                response = await llm_client.call_llm(
                    session=session,
                    url=ChatLLMConfig.url,
                    headers=ChatLLMConfig.headers,
                    request_data=request_data,
                    timeout=2000
                )

                # 解析响应
                if "choices" in response and len(response["choices"]) > 0:
                    msg = response["choices"][0]["message"]["content"]
                    result_text = msg.split(
                        "</think>\n\n")[1] if "</think>\n\n" in msg else msg

                    logger.info(f"LLM分类结果: {result_text}")
                    return result_text
                else:
                    logger.warning("LLM响应格式不正确")
                    return None

        except Exception as e:
            logger.error(f"LLM分类判断失败: {e}", exc_info=True)
            return None

    async def _map_field_to_table_elements(self, field_name: str, 
                                           table_info_content: str, 
                                           field_query_sentence: str,
                                           table_element_dict: dict) -> Optional[str]:
        """
        使用LLM将字段映射到表元素

        Args:
            field_name: 字段名
            field_query_sentence: 字段查询句子
            table_element_dict: 表元素字典，格式如 {"0":"身份证号", "1":"出生日期", "2":"性别"}

        Returns:
            LLM字段映射结果，格式如 "0,这是身份证字段,与身份证号最匹配"
        """
        try:
            from app.core.config import ChatLLMConfig
            from app.core.utils import AsyncLLMClient
            import aiohttp

            # 如果没有表元素，直接返回
            if not table_element_dict:
                logger.warning(f"字段 {field_name} 没有表元素用于映射判断")
                return "-1"

            # 构建提示词
            prompt = f"""## 角色定义
你是一个专业的数据字段映射专家，具备跨行业数据理解能力。

## 任务背景
- 我们有一个已知的数据表分类结果，其中包含敏感数据元素列表
- 现在需要将用户数据库中的字段与这些敏感数据元素进行匹配

## 输入数据

### 用户提供的某行业数据库中({table_info_content})的字段相关信息
{field_query_sentence}

### 已知的敏感数据元素列表
"""

            # 添加表元素列表
            for index, element in table_element_dict.items():
                prompt += f"{index}: {element}\n"

            prompt += f"""
## 评估准则
- 根据字段名称、注释和示例值，判断该字段最有可能对应哪个敏感数据元素
- 如果找不到合适的匹配项，请回复-1
- 匹配时考虑字段语义的相似性，而不仅仅是字面匹配

## 输出要求
- 回复三个部分：
    第一部分为最匹配的候选结果序号数字，从以上敏感数据元素列表中选取一个最相似的，选一个该区间的数字，都不相关回复-1
    第二部分为字段类型判定，对用户提供的字段及信息进行判断，判断是属于哪个领域的字段，回答格式：这是XXX字段
    第三部分为选择该数字的理由，限制20字说明一个理由，如果判定的数字是-1，则回复理由：无法判断
- 回复格式：
    csv格式回复，逗号隔开三个部分，回答最相关的一条，格式：<数字>,<字段判定>,<理由>
    即使无法判断，第二部分也要有判断结果，当无法判断是回复：-1,这是XXX字段,无法判断
- 不要添加任何其他无关信息。
"""

            print(prompt)
            # 构建请求数据
            messages = [
                {"role": "user", "content": prompt}
            ]

            logger.info(f"字段 {field_name} 的映射提示词:\n\n{prompt}")

            request_data = ChatLLMConfig.get_request_data(messages)

            # 调用LLM
            async with aiohttp.ClientSession() as session:
                llm_client = AsyncLLMClient()
                response = await llm_client.call_llm(
                    session=session,
                    url=ChatLLMConfig.url,
                    headers=ChatLLMConfig.headers,
                    request_data=request_data,
                    timeout=2000
                )

                # 解析响应
                if "choices" in response and len(response["choices"]) > 0:
                    msg = response["choices"][0]["message"]["content"]
                    result_text = msg.split(
                        "</think>\n\n")[1] if "</think>\n\n" in msg else msg

                    logger.info(f"字段 {field_name} 映射结果: {result_text}")
                    return result_text
                else:
                    logger.warning(f"字段 {field_name} 映射响应格式不正确")
                    return "-1"
            logger.info(f"字段 {field_name} 映射结果: {result_text}")

        except Exception as e:
            logger.error(f"字段 {field_name} 映射判断失败: {e}", exc_info=True)
            return "-1"

    async def _llm_field_classification(self, field_name: str, field_comment: Optional[str], sample_value: Optional[str],
                                        table_bm25_results: List[str], table_vector_results: List[str],
                                        table_general_knowledge_results: List[str]) -> Optional[str]:
        """
        使用LLM对单个字段进行分类判断

        Args:
            field_name: 字段名
            field_comment: 字段注释
            sample_value: 示例值
            table_bm25_results: 表级别的BM25检索结果
            table_vector_results: 表级别的向量检索结果
            table_general_knowledge_results: 表级别的通用知识检索结果

        Returns:
            LLM字段分类结果
        """
        try:
            from app.core.config import ChatLLMConfig
            from app.core.utils import AsyncLLMClient
            import aiohttp
            import json

            # 构建字段查询句子
            parts = []
            if field_name:
                parts.append(f"fieldName:{field_name}")
            if field_comment:
                parts.append(f"fieldComment:{field_comment}")
            if sample_value:
                parts.append(f"sampleValue:{sample_value}")

            query_sentence = ",".join(parts)

            # 使用表级别的检索结果作为字段级别的候选结果
            llm_items = []
            rank = 1

            # 添加BM25检索结果
            for result in table_bm25_results:
                llm_items.append({
                    'rank': rank,
                    'type': 'BM25',
                    'data': result
                })
                rank += 1

            # 添加向量检索结果
            for result in table_vector_results:
                llm_items.append({
                    'rank': rank,
                    'type': '向量',
                    'data': result
                })
                rank += 1

            # 如果没有候选结果，返回None
            if not llm_items:
                logger.warning(f"字段 {field_name} 没有候选结果用于LLM分类判断")
                return None

            # 构建提示词
            prompt = f"""## 角色定义
你是一个专业的数据字段分类专家，具备跨行业数据理解能力。

## 任务背景
- 我们的知识库是基于电信行业的数据建立的，有时可能用它来匹配其他行业的数据，这里需要你灵活判断。

## 输入数据

### 用户提供的某行业数据库中的字段相关信息
{query_sentence}

### 从分类分级知识库中检索到的候选结果
"""

            for item in llm_items:
                prompt += f"""
候选结果 {item['rank']}（{item['type']}检索）：
{item['data']}
"""

            if table_general_knowledge_results:
                prompt += f"""
### 通识检索结果
"""
                for result in table_general_knowledge_results:
                    prompt += f"""
{result}
"""

            prompt += f"""
## 评估准则
1. "通识检索结果"仅参考，通常不是很准确，重点是知识库的"候选结果"
2. 关注数据的本质属性和核心特征是否相符
3. 重点比较你从数据中理解的特征
4. 如果去除行业特有词汇后，核心内容基本一致，则也可判定为匹配

## 输出要求
- 回复三个部分：
    第一部分为最匹配的候选结果序号数字（1-{len(llm_items)}），无论如何必须强制选一个该区间的数字
    第二部分为字段类型判定，对用户提供的字段及信息进行主观判断，判断是属于哪个领域的字段，回答格式：这是XXX字段
    第三部分为判定理由，限制20字说明一个理由，表达官方，不要出现"无法判断"之类的词
- 回复格式：
    csv格式回复，逗号隔开三个部分，回答最相关的一条，格式：<数字>,<字段类型判定>,<理由>
- 不要添加任何其他无关信息。
"""

            # 构建请求数据
            messages = [
                {"role": "user", "content": prompt}
            ]

            logger.info(f"llm_items:\n\n{llm_items}")
            logger.info(f"提示词:\n\n{prompt}")

            request_data = ChatLLMConfig.get_request_data(messages)

            # 调用LLM
            async with aiohttp.ClientSession() as session:
                llm_client = AsyncLLMClient()
                response = await llm_client.call_llm(
                    session=session,
                    url=ChatLLMConfig.url,
                    headers=ChatLLMConfig.headers,
                    request_data=request_data,
                    timeout=2000
                )

                # 解析响应
                if "choices" in response and len(response["choices"]) > 0:
                    msg = response["choices"][0]["message"]["content"]
                    result_text = msg.split(
                        "</think>\n\n")[1] if "</think>\n\n" in msg else msg

                    logger.info(f"LLM字段分类结果: {result_text}")
                    return result_text
                else:
                    logger.warning("LLM字段分类响应格式不正确")
                    return None

        except Exception as e:
            logger.error(f"LLM字段分类判断失败: {e}", exc_info=True)
            return None


data_recognizing_service = DataRecognitionService()
