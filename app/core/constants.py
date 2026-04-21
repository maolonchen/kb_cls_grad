#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
项目常量定义模块
集中管理项目中使用的各种常量值
"""

class HttpStatus:
    """HTTP状态码常量"""
    SUCCESS = 200
    CREATED = 201
    ACCEPTED = 202
    NO_CONTENT = 204
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    REQUEST_TIMEOUT = 408
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503


class FilePaths:
    """文件路径常量"""
    DATA_DIR = "data"
    RAW_DIR = "data/raw"
    PROCESSED_DIR = "data/processed"
    STANDARDS_DIR = "data/standards"
    CLASSIFICATIONS_DIR = "data/processed/classifications"


class SearchTopks:
    """搜索TopK常量"""
    BM25_TOPK_DATA_RECOGNITION = 15
    

class SearchRemoveDuplicates:
    """相似度结果去重常量，保留数"""
    REMOVE_DUPLICATES = 5


class FileNames:
    """文件名常量"""
    STANDARD_FILE_SUFFIX = "_standard.jsonl"
    ORIGIN_DATA_FILE = "origin_data.md"
    CHUNKS_SUFFIX = "_chunks"
    TEMP_MD_FILE = "_temp.md"
    COLLECTION_CLASSIFICATION_SUFFIX = "_classification"
    COLLECTION_GENERAL_KNOWLEDGE_SUFFIX = "_general_knowledge"
    COLLECTION_NARRATIVE_CLASSIFICATION_SUFFIX = "_narrative_classification"


class FileExtensions:
    """文件扩展名常量"""
    JSONL = ".jsonl"
    MARKDOWN = ".md"
    TEXT = ".txt"
    CSV = ".csv"
    PDF = ".pdf"
    WORD = ".doc"
    WORDX = ".docx"
    EXCEL = ".xls"
    EXCELX = ".xlsx"


class Operations:
    """操作类型常量"""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    ADD = "add"


class ChineseNumerals:
    """中文数词"""
    LEVEL_CLASSIFICATIONS = ['一', '二', '三', '四', '五', '六', '七', '八', '九', '十']
