"""
文件识别服务
处理AI文件识别逻辑
"""

import logging
import asyncio
import re
import json
import os
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from pymilvus import MilvusClient

from app.core.config import DatabaseConfig, ChatLLMConfig, MinerUConfig
from app.core.vectoring import VectorClient
from app.core.utils import AsyncLLMClient
from app.processors.pdf_processor import parse_pdf_with_mineru
from app.processors.word_processor import convert_word_to_pdf
from app.processors.excel_processor import process_excel_to_markdown
from app.processors.csv_processor import convert_csv_to_excel
from app.processors.txt_processor import convert_txt_to_md
from app.processors.file_processor import process_file

logger = logging.getLogger(__name__)


class FileRecognitionService:
    """文件识别服务类"""

    def __init__(self):
        """初始化服务"""
        # self.db_path = DatabaseConfig.path
        # self.milvus_client = MilvusClient(self.db_path)
        self.milvus_client = MilvusClient(uri=DatabaseConfig.uri)

    def _find_relevant_collections(self, specification_uid: str = None) -> dict:
        """
        查找相关的集合，包括 *_narrative_classification 和 *_general_knowledge
        如果提供了specification_uid，则只返回与该规范相关的集合
        如果 *_narrative_classification 和 *_general_knowledge 都不存在，则 fallback 到 *_classification

        Args:
            specification_uid: 规范UID，如果提供则只查找相关集合

        Returns:
            dict: 包含叙事分类集合和通用知识集合的字典
        """
        try:
            collections = self.milvus_client.list_collections()
        except Exception as e:
            logger.error(f"获取集合列表失败: {e}")
            collections = []

        narrative_classification_collections = []
        general_knowledge_collections = []
        classification_collections = []

        # 如果提供了specification_uid，则只查找相关集合
        if specification_uid:
            specification_uid = specification_uid.replace("-", "_")
            # 确保集合名称以字母或下划线开头（Milvus要求）
            if specification_uid and not specification_uid[0].isalpha() and specification_uid[0] != '_':
                specification_uid = '_' + specification_uid
                
            narrative_classification_collection = f"{specification_uid}_narrative_classification"
            general_knowledge_collection = f"{specification_uid}_general_knowledge"
            classification_collection = f"{specification_uid}_classification"  # fallback 集合

            if narrative_classification_collection in collections:
                narrative_classification_collections.append(
                    narrative_classification_collection)
            if general_knowledge_collection in collections:
                general_knowledge_collections.append(
                    general_knowledge_collection)
            if classification_collection in collections:
                classification_collections.append(classification_collection)
                
            # 如果前两个集合都不存在，则添加 fallback 集合
            if not narrative_classification_collections and not general_knowledge_collections:
                if classification_collection in collections:
                    # 将 classification 集合作为 narrative_classification 的 fallback
                    narrative_classification_collections.append(classification_collection)
        else:
            # 否则查找所有集合（原有逻辑）
            classification_collections = []  # 用于存储所有 classification 集合
            for collection in collections:
                # 匹配 *_narrative_classification
                if collection.endswith('_narrative_classification'):
                    narrative_classification_collections.append(collection)
                # 匹配 *_general_knowledge
                elif collection.endswith('_general_knowledge'):
                    general_knowledge_collections.append(collection)
                # 收集 *_classification 但排除 *_narrative_classification
                elif collection.endswith('_classification') and not collection.endswith('_narrative_classification'):
                    classification_collections.append(collection)

            # 如果没有找到 narrative_classification 和 general_knowledge 集合，则使用 classification 集合作为 fallback
            if not narrative_classification_collections and not general_knowledge_collections:
                narrative_classification_collections.extend(classification_collections)

        return {
            'narrative_classification': narrative_classification_collections,
            'general_knowledge': general_knowledge_collections,
            'classification': classification_collections
        }

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
                if 'text' in result.get('entity', {}):
                    texts.append(result['entity']['text'])

            return texts
        except Exception as e:
            logger.error(f"向量相似度检索失败 (集合: {collection_name}): {e}")
            return []

    def _extract_content_from_markdown(self, markdown_text: str, max_chars: int = 900) -> str:
        """
        从Markdown文本中提取内容：每个大小标题(即带"#"的行)和标题行下方的第一行文字

        Args:
            markdown_text: Markdown文本
            max_chars: 最大字符数

        Returns:
            str: 提取的内容
        """
        lines = markdown_text.split('\n')
        extracted_content = []
        char_count = 0

        i = 0
        while i < len(lines) and char_count < max_chars:
            line = lines[i]
            # 如果是标题行（以#开头）
            if line.strip().startswith('#'):
                # 添加标题行
                if char_count + len(line) > max_chars:
                    remaining_chars = max_chars - char_count
                    extracted_content.append(line[:remaining_chars])
                    char_count = max_chars
                    break
                else:
                    extracted_content.append(line)
                    char_count += len(line)

                # 查找标题行下方的第一行非空文字
                j = i + 1
                while j < len(lines):
                    next_line = lines[j].strip()
                    if next_line and not next_line.startswith('#'):  # 非空且不是标题
                        # 添加内容行
                        if char_count + len(next_line) > max_chars:
                            remaining_chars = max_chars - char_count
                            extracted_content.append(
                                next_line[:remaining_chars])
                            char_count = max_chars
                        else:
                            extracted_content.append(next_line)
                            char_count += len(next_line)
                        break
                    j += 1
            i += 1

        return '\n'.join(extracted_content)

    def _process_word_file(self, file_path: str) -> str:
        """
        处理Word文件(.doc/.docx)

        Args:
            file_path: Word文件路径

        Returns:
            str: 提取的文本内容
        """
        try:
            # 将Word转换为PDF
            pdf_path = convert_word_to_pdf(file_path)
            logger.info(f"已将Word文件转换为PDF: {pdf_path}")

            # 使用MinerU解析PDF
            result = parse_pdf_with_mineru(pdf_path)

            # 获取Markdown内容
            if 'results' in result:
                first_key = next(iter(result['results']))
                markdown_content = result['results'][first_key].get('md_content')
            else:
                # 原有逻辑
                markdown_content = result.get('markdown')

            # 提取结构化信息
            extracted_content = self._extract_content_from_markdown(
                markdown_content)
            
            if len(extracted_content) < 10:
                extracted_content = extracted_content + markdown_content[:900]
                
            # 清理临时PDF文件
            try:
                os.remove(pdf_path)
                logger.info(f"已清理临时PDF文件: {pdf_path}")
            except Exception as e:
                logger.warning(f"清理临时PDF文件失败: {e}")

            return extracted_content
        except Exception as e:
            logger.error(f"处理Word文件失败: {e}")
            raise

    def _process_pdf_file(self, file_path: str) -> str:
        """
        处理PDF文件

        Args:
            file_path: PDF文件路径

        Returns:
            str: 提取的文本内容
        """
        try:
            # 使用MinerU解析PDF
            result = parse_pdf_with_mineru(file_path)

            # 获取Markdown内容
            if 'results' in result:
                first_key = next(iter(result['results']))
                markdown_content = result['results'][first_key].get('md_content')
            else:
                # 原有逻辑
                markdown_content = result.get('markdown')

            # 提取结构化信息
            extracted_content = self._extract_content_from_markdown(
                markdown_content)
            
            if len(extracted_content) < 10:
                extracted_content = extracted_content + markdown_content[:900]

            return extracted_content
        except Exception as e:
            logger.error(f"处理PDF文件失败: {e}")
            raise

    def _process_markdown_file(self, file_path: str) -> str:
        """
        处理Markdown文件

        Args:
            file_path: Markdown文件路径

        Returns:
            str: 提取的文本内容
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                markdown_content = f.read()

            # 提取结构化信息
            extracted_content = self._extract_content_from_markdown(
                markdown_content)
            if len(extracted_content) < 10:
                extracted_content = extracted_content + markdown_content[:200]

            return extracted_content
        except Exception as e:
            logger.error(f"处理Markdown文件失败: {e}")
            raise

    def _process_excel_file(self, file_path: str) -> str:
        """
        处理Excel文件(.xls/.xlsx)

        Args:
            file_path: Excel文件路径

        Returns:
            str: 提取的文本内容
        """
        try:
            # 创建临时输出路径
            temp_output_path = Path(file_path).parent / \
                f"{Path(file_path).stem}_temp.md"

            # 处理Excel文件并转换为Markdown
            markdown_file_path = process_excel_to_markdown(
                file_path, temp_output_path)

            # 读取Markdown文件内容
            with open(markdown_file_path, 'r', encoding='utf-8') as f:
                markdown_content = f.read()

            # 获取前900个字符
            extracted_content = markdown_content[:900] if len(
                markdown_content) > 900 else markdown_content

            # 清理临时文件
            try:
                os.remove(markdown_file_path)
            except Exception as e:
                logger.warning(f"清理临时Markdown文件失败: {e}")

            return extracted_content
        except Exception as e:
            logger.error(f"处理Excel文件失败: {e}")
            raise

    def _process_csv_file(self, file_path: str) -> str:
        """
        处理CSV文件

        Args:
            file_path: CSV文件路径

        Returns:
            str: 提取的文本内容
        """
        try:
            # 将CSV转换为Excel
            excel_path = convert_csv_to_excel(file_path)
            logger.info(f"已将CSV文件转换为Excel: {excel_path}")

            # 创建临时输出路径
            temp_output_path = Path(file_path).parent / \
                f"{Path(file_path).stem}_temp.md"

            # 处理Excel文件并转换为Markdown
            markdown_file_path = process_excel_to_markdown(
                excel_path, temp_output_path)

            # 读取Markdown文件内容
            with open(markdown_file_path, 'r', encoding='utf-8') as f:
                markdown_content = f.read()

            # 获取前900个字符
            extracted_content = markdown_content[:900] if len(
                markdown_content) > 900 else markdown_content

            # 清理临时文件
            try:
                os.remove(excel_path)
                os.remove(markdown_file_path)
                logger.info(f"已清理临时文件: {excel_path}, {markdown_file_path}")
            except Exception as e:
                logger.warning(f"清理临时文件失败: {e}")

            return extracted_content
        except Exception as e:
            logger.error(f"处理CSV文件失败: {e}")
            raise

    def _process_txt_file(self, file_path: str) -> str:
        """
        处理TXT文件

        Args:
            file_path: TXT文件路径

        Returns:
            str: 提取的文本内容
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 获取前900个字符
            extracted_content = content[:900] if len(
                content) > 900 else content

            return extracted_content
        except Exception as e:
            logger.error(f"处理TXT文件失败: {e}")
            raise

    def process_file(self, file_path: str) -> str:
        """
        根据文件类型处理文件并提取内容

        Args:
            file_path: 文件路径

        Returns:
            str: 提取的文本内容
        """
        file_ext = Path(file_path).suffix.lower()

        if file_ext in ['.doc', '.docx']:
            return self._process_word_file(file_path)
        elif file_ext == '.pdf':
            return self._process_pdf_file(file_path)
        elif file_ext == '.md':
            return self._process_markdown_file(file_path)
        elif file_ext in ['.xls', '.xlsx']:
            return self._process_excel_file(file_path)
        elif file_ext == '.csv':
            return self._process_csv_file(file_path)
        elif file_ext == '.txt':
            return self._process_txt_file(file_path)
        else:
            raise ValueError(f"不支持的文件类型: {file_ext}")

    def _build_query_sentence(self, file_name: str, system_type: Optional[str], system_name: Optional[str], extracted_content: str) -> str:
        """
        构建用于向量检索的查询句子

        Args:
            file_name: 文件名
            system_type: 业务系统类型
            system_name: 业务系统名称
            extracted_content: 提取的文件内容

        Returns:
            str: 构建好的查询句子
        """
        parts = []

        # 添加文件名
        # parts.append(f"fileName:{file_name}")
        parts.append(f"{file_name}")

        # 添加其他可选字段
        if system_type:
            # parts.append(f"systemType:{system_type}")
            parts.append(f"{system_type}")
        if system_name:
            # parts.append(f"systemName:{system_name}")
            parts.append(f"{system_name}")

        # 用逗号连接所有部分，并添加提取的内容
        query_sentence = ",".join(parts)
        query_sentence += "+" + extracted_content[:900]  # 限制内容长度

        return query_sentence

    async def _llm_classification(self, query_sentence: str, vector_results: List[str], general_knowledge_results: List[str]) -> Optional[str]:
        """
        使用LLM对检索结果进行分类判断

        Args:
            query_sentence: 查询句子
            vector_results: 向量检索结果
            general_knowledge_results: 通用知识检索结果

        Returns:
            LLM分类结果
        """
        try:
            import aiohttp

            # 构建候选结果列表
            llm_items = []
            rank = 1

            # 添加向量检索结果
            for result in vector_results:
                llm_items.append({
                    'rank': rank,
                    'type': '向量',
                    'data': result
                })
                rank += 1

            # 如果没有候选结果，直接返回
            if not llm_items:
                logger.warning("没有候选结果用于LLM分类判断")
                return None

            # 构建提示词
            prompt = f"""## 角色定义
你是一个专业的文件分类专家，具备跨行业数据理解能力。

## 任务背景
- 我们的知识库是基于电信行业的数据建立的，有时可能用它来匹配其他行业的数据，这里需要你灵活判断。

## 输入数据

### 用户提供的文件信息
{query_sentence}

### 从分类分级知识库中检索到的候选结果
"""

            for item in llm_items:
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
    第一部分为最匹配的候选结果序号数字（1-{len(llm_items)}），无论如何必须强制选一个该区间的数字
    第二部分为判定理由，限制20字说明一个理由，表达官方，不要出现"无法判断"或"无关"之类的词
- 回复格式：
    csv格式回复，逗号隔开两个部分，回答最相关的一条，格式：<数字>,<理由>
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

    # async def recognize_file(self, file_name: str, file_path: str, system_type: Optional[str] = None,
    #                        system_name: Optional[str] = None, sen_spec_uid: Optional[str] = None,
    #                        imp_spec_uid: Optional[str] = None, core_spec_uid: Optional[str] = None) -> Dict[str, Any]:
    async def recognize_file(self, file_name: str, file_path: str, system_type: Optional[str] = None,
                             system_name: Optional[str] = None, sen_spec_uid: Optional[str] = None,
                             imp_spec_uid: Optional[str] = None, core_spec_uid: Optional[str] = None) -> Dict[str, Any]:
        """
        识别文件的AI分类和分级

        Args:
            file_name: 文件名
            file_path: 文件路径
            system_type: 业务系统类型
            system_name: 业务系统名称
            sen_spec_uid: 一般数据识别规范标识
            imp_spec_uid: 重要数据识别规范标识
            core_spec_uid: 核心数据识别规范标识

        Returns:
            Dict[str, Any]: 文件识别结果
        """
        try:
            # 处理文件并提取内容
            extracted_content = self.process_file(file_path)
            logger.info(f"已提取文件内容，长度: {len(extracted_content)} 字符")

            # 构建查询句子
            query_sentence = self._build_query_sentence(
                file_name, system_type, system_name, extracted_content)
            logger.info(f"构建的查询句子: {query_sentence[:200]}...")

            # 查找相关集合，如果提供了任何一个或多个specificationUId则查找所有相关集合
            # 收集所有提供的UId
            specification_uids = []
            if sen_spec_uid:
                specification_uids.append(sen_spec_uid)
            if imp_spec_uid:
                specification_uids.append(imp_spec_uid)
            if core_spec_uid:
                specification_uids.append(core_spec_uid)
                
            # 查找所有相关集合
            all_narrative_classification_collections = []
            all_general_knowledge_collections = []
            all_classification_collections = []
            
            for uid in specification_uids:
                relevant_collections = self._find_relevant_collections(uid)
                all_narrative_classification_collections.extend(relevant_collections['narrative_classification'])
                all_general_knowledge_collections.extend(relevant_collections['general_knowledge'])
                all_classification_collections.extend(relevant_collections['classification'])
            
            # 去重
            narrative_classification_collections = list(set(all_narrative_classification_collections))
            general_knowledge_collections = list(set(all_general_knowledge_collections))
            classification_collections = list(set(all_classification_collections))

            logger.info(f"找到叙事分类集合: {narrative_classification_collections}")
            logger.info(f"找到通用知识集合: {general_knowledge_collections}")
            logger.info(f"找到分类集合: {classification_collections}")

            if not narrative_classification_collections and not general_knowledge_collections and not classification_collections:
                logger.warning("未找到任何相关集合")
                return {
                    "fileName": file_name,
                    "fileClassification": "",
                    "fileGrade": "",
                    "reason": "未找到相关分类集合"
                }

            # 存储所有检索结果
            classification_results = []
            for collection_name in classification_collections:
                # 分类集合应用UID过滤
                collection_results = await self._vector_search(collection_name, query_sentence, 8)
                classification_results.extend(collection_results)

            # 在叙事分类集合中进行向量相似度检索（使用top_k=8）
            narrative_vector_results = []
            for collection_name in narrative_classification_collections:
                # 分类集合应用UID过滤
                collection_results = await self._vector_search(collection_name, query_sentence, 8)
                narrative_vector_results.extend(collection_results)

            # 在通用知识集合中进行向量相似度检索（使用top_k=8）
            general_knowledge_results = []
            for collection_name in general_knowledge_collections:
                # 不对通用知识集合应用UID过滤
                collection_results = await self._vector_search(collection_name, query_sentence, 8)
                general_knowledge_results.extend(collection_results)

            # 去重并保留前3个结果
            narrative_unique = list(dict.fromkeys(narrative_vector_results))[:10]
            classification_unique = list(dict.fromkeys(classification_results))[:10]
            unique_vector_results = narrative_unique + classification_unique
            
            unique_general_knowledge_results = list(dict.fromkeys(general_knowledge_results))[:10]

            # 记录检索结果
            logger.info(
                f"分类检索结果数: {len(classification_results)}, 去重后结果数: {len(classification_unique)}")
            logger.info(
                f"向量检索结果数: {len(narrative_vector_results)}, 去重后结果数: {len(unique_vector_results)}")
            logger.info(
                f"通用知识检索结果数: {len(general_knowledge_results)}, 去重后结果数: {len(unique_general_knowledge_results)}")
            logger.info(f"向量检索结果: {unique_vector_results}")
            logger.info(f"通用知识检索结果: {unique_general_knowledge_results}")

            # 使用LLM进行分类判断
            llm_result = await self._llm_classification(
                query_sentence,
                unique_vector_results,
                unique_general_knowledge_results
            )

            # 初始化分类和分级信息
            file_classification = ""
            file_grade = ""
            reason = ""

            # 处理LLM结果
            if llm_result:
                try:
                    # 解析LLM响应
                    parts = llm_result.split(',', 1)
                    if len(parts) == 2:
                        index_part = parts[0].strip()
                        reason = parts[1].strip()

                        # 获取索引数字
                        selected_index = int(index_part)

                        # 获取对应的分类结果
                        all_candidate_results = unique_vector_results
                        if 1 <= selected_index <= len(all_candidate_results):
                            selected_result = all_candidate_results[selected_index - 1]

                            # 解析分类结果
                            try:
                                result_data = json.loads(selected_result)

                                # 提取分类信息
                                header = result_data.get("header", {})
                                data = result_data.get("data", {})

                                # 查找最具体的分类（最深层的非空分类）
                                classification_value = ""

                                # 收集所有分类相关字段并按深度排序（从具体到抽象）
                                classification_levels = []

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
                                            num_match = re.search(
                                                r'^(\d+)', value)
                                            level_num = int(num_match.group(
                                                1)) if num_match else 99  # 默认放到最后

                                        classification_levels.append(
                                            (level_num, key, value))

                                # 按级别数字排序（从高到低，即从具体到抽象）
                                classification_levels.sort(reverse=True)

                                # 查找最深层（最具体）的非空分类值
                                for level_num, key, level_name in classification_levels:
                                    candidate_value = data.get(key, "")
                                    if candidate_value:
                                        classification_value = candidate_value
                                        break

                                # 如果没找到按级别命名的分类，尝试固定名称查找（作为后备方案）
                                if not classification_value:
                                    # 按照具体程度顺序查找：三级 > 二级 > 一级
                                    for level_name in ["三级分类", "二级分类", "一级分类"]:
                                        for key, value in header.items():
                                            if value == level_name:
                                                candidate_value = data.get(
                                                    key, "")
                                                if candidate_value:
                                                    classification_value = candidate_value
                                                    break
                                        if classification_value:
                                            break

                                file_classification = classification_value

                                # 处理等级字段
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
                                                    file_grade = f"第{max_grade}级"
                                                else:
                                                    file_grade = raw_grade  # 无法解析，返回原始值
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
                                                    file_grade = f"第{min_grade}级"
                                                else:
                                                    file_grade = raw_grade  # 无法解析，返回原始值
                                        else:
                                            file_grade = raw_grade
                                        break

                            except json.JSONDecodeError:
                                logger.warning(
                                    f"无法解析分类结果为JSON: {selected_result}")
                except Exception as e:
                    logger.error(f"处理LLM结果时出错: {e}")

            return {
                "fileName": file_name,
                "fileClassification": file_classification,
                "fileGrade": file_grade,
                "reason": reason
            }

        except Exception as e:
            logger.error(f"文件识别失败: {e}", exc_info=True)
            return {
                "fileName": file_name,
                "fileClassification": "",
                "fileGrade": "",
                "reason": f"处理文件时出错: {str(e)}"
            }


file_recognition_service = FileRecognitionService()
