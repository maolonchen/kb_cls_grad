#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
正则表达式匹配工具模块
提供基于正则表达式的文本匹配功能
"""

import re
import json
from pathlib import Path
from typing import List, Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)


def extract_clean_name(text: str) -> str:
    """
    通用去除前缀，提取非前缀部分

    规则优先级：
    1. 有空格分隔：按第一个空格分割
    2. 开头有括号对：按括号对结束位置分割
    3. 按异数据类型分割（编号字符→内容字符的转换点）

    Args:
        text: 包含前缀的分类文本

    Returns:
        str: 去除前缀后的文本
    """
    if not text or not isinstance(text, str):
        return ""

    text = text.strip()
    if not text:
        return ""

    # 规则1：有空格分隔，按第一个空格分割
    if ' ' in text:
        return text.split(' ', 1)[1]

    # 规则2：开头是括号，找配对的右括号
    bracket_pairs = {
        '(': ')', '（': '）',
        '【': '】', '［': '］',
        '「': '」', '《': '》',
        '<': '>', '[': ']',
    }
    if text[0] in bracket_pairs:
        close_bracket = bracket_pairs[text[0]]
        pos = text.find(close_bracket)
        if pos >= 0 and pos < len(text) - 1:
            return text[pos + 1:]

    # 规则2b：以"、"等符号分隔
    pos = text.find('、')
    if pos >= 0 and pos < len(text) - 1:
        return text[pos + 1:]

    # 规则3：按异数据类型分割（编号字符→内容字符的转换点）
    # 前缀由编号字符组成（字母、数字、.、-），遇到中文字符时前缀结束
    for i, ch in enumerate(text):
        if '\u4e00' <= ch <= '\u9fff':
            if i > 0:
                return text[i:]
            break

    # 没有找到分割点，返回原文
    return text


class RegexMatcher:
    """正则表达式匹配器"""

    def __init__(self, standard_file: str = None, specification_uid: str = None):
        """
        初始化正则匹配器

        Args:
            standard_file: 标准分类文件路径
            specification_uid: 规范UId，用于构建标准文件路径
        """
        # 如果没有指定标准文件路径，则根据specification_uid构建路径
        if standard_file is None:
            if specification_uid:
                specification_uid = '_' + specification_uid if not specification_uid.startswith('_') else specification_uid
                specification_uid = specification_uid.replace("-", "_")
                standard_file = f"./data/standards/{specification_uid}_standard.jsonl"
                print("*********************** standard_file *******************\n", standard_file)
            else:
                standard_file = "./data/standards/standard.jsonl"

        self.standard_file = Path(standard_file)
        self.standard_categories = self._load_standard_categories()
        print("-------------------------\n", standard_file)

    def _load_standard_categories(self) -> List[Dict[str, Any]]:
        """
        从标准文件中加载所有分类信息

        Returns:
            List[Dict[str, Any]]: 分类信息列表
        """
        categories = []
        try:
            if not self.standard_file.exists():
                logger.warning(f"标准分类文件不存在: {self.standard_file}")
                return categories

            with open(self.standard_file, 'r', encoding='utf-8') as f:
                for line in f:
                    data = json.loads(line)
                    categories.append(data)
        except Exception as e:
            logger.error(f"加载标准分类文件时出错: {str(e)}")

        return categories

    def extract_category_names(self) -> List[str]:
        """
        从标准分类中提取所有分类名称（包括带前缀和不带前缀的版本）

        Returns:
            List[str]: 所有分类名称列表
        """
        category_names = []

        for category_info in self.standard_categories:
            data_dict = category_info.get("data", {})

            # 遍历所有数据项
            for key, value in data_dict.items():
                if isinstance(value, str):
                    # 添加完整分类名称（包含前缀）
                    if value and value not in category_names:
                        category_names.append(value)

                    # 提取分类名称（去除前缀）
                    clean_name = self._extract_clean_name(value)
                    if clean_name and clean_name not in category_names:
                        category_names.append(clean_name)

        logger.info(f"提取到 {len(category_names)} 个分类名称")
        return category_names

    def _extract_clean_name(self, category_text: str) -> str:
        """
        从分类文本中提取干净的分类名称（去除前缀）

        Args:
            category_text: 包含前缀的分类文本

        Returns:
            str: 干净的分类名称
        """
        return extract_clean_name(category_text)

    def find_best_match(self, target_text: str) -> Tuple[str, float]:
        """
        使用更精确的匹配算法找到最佳匹配的分类名称

        Args:
            target_text: 目标文本

        Returns:
            Tuple[str, float]: (最佳匹配的分类名称, 匹配得分)
        """
        if not target_text:
            return "", 0.0

        best_match = ""
        max_score = 0.0

        # 获取所有分类名称
        category_names = self.extract_category_names()

        # 为每个分类名称计算与目标文本的匹配得分
        for category_name in category_names:
            # 使用更精确的匹配算法
            score = self._calculate_similarity(target_text, category_name)
            if score > max_score:
                max_score = score
                best_match = category_name

        return best_match, max_score

    def find_all_matches(self, target_text: str, min_score: float = 0.5) -> List[Tuple[Dict[str, Any], float]]:
        """
        查找所有匹配的分类信息

        Args:
            target_text: 目标文本
            min_score: 最小匹配分数阈值

        Returns:
            List[Tuple[Dict[str, Any], float]]: 包含匹配分类信息和匹配得分的列表
        """
        if not target_text:
            return []

        matches = []

        # 为每个分类计算与目标文本的匹配得分
        for category_info in self.standard_categories:
            data_dict = category_info.get("data", {})
            
            # 检查是否有任何一个值与目标文本匹配
            for key, value in data_dict.items():
                if isinstance(value, str):
                    score = self._calculate_similarity(target_text, value)
                    if score >= min_score:
                        matches.append((category_info, score))
                        break  # 找到一个匹配就足够了

        # 按匹配得分降序排列
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches

    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """
        计算两个文本之间的相似度得分

        策略：完全匹配 → 最长公共子串

        Args:
            text1: 第一个文本（目录名）
            text2: 第二个文本（标准分类值）

        Returns:
            float: 相似度得分 (0-1)
        """
        if not text1 or not text2:
            return 0.0

        # 1. 完全匹配
        if text1 == text2:
            return 1.0

        # 2. 提取去前缀后的名称，再做最长公共子串匹配
        clean1 = extract_clean_name(text1)
        clean2 = extract_clean_name(text2)

        # 去前缀后完全匹配
        if clean1 and clean2 and clean1 == clean2:
            return 0.95

        # 3. 最长公共子串得分
        lcs_len = self._longest_common_substring_length(text1, text2)
        max_len = max(len(text1), len(text2))
        return lcs_len / max_len if max_len > 0 else 0.0

    def _longest_common_substring_length(self, s1: str, s2: str) -> int:
        """
        计算两个字符串的最长公共子串长度

        Args:
            s1: 第一个字符串
            s2: 第二个字符串

        Returns:
            int: 最长公共子串长度
        """
        if not s1 or not s2:
            return 0

        # 使用滚动数组优化空间
        prev = [0] * (len(s2) + 1)
        max_len = 0

        for i in range(1, len(s1) + 1):
            curr = [0] * (len(s2) + 1)
            for j in range(1, len(s2) + 1):
                if s1[i - 1] == s2[j - 1]:
                    curr[j] = prev[j - 1] + 1
                    if curr[j] > max_len:
                        max_len = curr[j]
            prev = curr

        return max_len

    def get_category_info(self, target_category: str) -> Dict[str, Any]:
        """
        获取特定分类的完整信息

        Args:
            target_category: 目标分类名称

        Returns:
            Dict[str, Any]: 分类的完整信息
        """
        try:
            for category_info in self.standard_categories:
                data_dict = category_info.get("data", {})

                # 查找精确匹配的目标分类（完整名称匹配）
                for key, value in data_dict.items():
                    if isinstance(value, str) and value == target_category:
                        return category_info

                # 如果没有精确匹配，尝试使用原始值的后缀匹配
                for key, value in data_dict.items():
                    if isinstance(value, str) and value.endswith(target_category):
                        return category_info

        except Exception as e:
            logger.error(f"获取分类信息时出错: {str(e)}")

        return {}