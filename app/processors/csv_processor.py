import os
import logging
from pathlib import Path
import pandas as pd
from typing import List, Tuple

# 使用项目统一的日志配置
from app.core.logging import get_logger
logger = get_logger(__name__)


def convert_csv_to_excel(csv_file_path: str | Path, remove_original: bool = False) -> str:
    """
    将单个CSV文件转换为Excel格式（.xlsx）。

    参数:
        csv_file_path (str | Path): CSV文件路径。
        remove_original (bool): 是否在转换成功后删除原始CSV文件。默认为 False。

    返回:
        str: 转换后的Excel文件路径

    异常:
        FileNotFoundError: 如果文件不存在。
        RuntimeError: 如果无法读取文件。
    """
    csv_file = Path(csv_file_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"文件不存在: {csv_file_path}")

    try:
        # 尝试多种编码
        encodings = ['utf-8', 'gbk', 'latin-1']
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=encoding)
                logger.debug(f"使用编码 '{encoding}' 成功读取 {csv_file}")
                break
            except UnicodeDecodeError:
                continue
        if df is None:
            raise RuntimeError(f"无法使用常见编码读取文件: {csv_file}")

        excel_file = csv_file.with_suffix('.xlsx')
        df.to_excel(excel_file, index=False)
        logger.info(f"已转换: {csv_file} -> {excel_file}")

        if remove_original:
            csv_file.unlink()
            logger.info(f"已删除原始文件: {csv_file}")
            
        return str(excel_file)

    except Exception as e:
        logger.error(f"转换文件 {csv_file} 时出错: {e}", exc_info=True)
        raise


def convert_csv_to_excel_in_directory(directory_path: str, remove_original: bool = False) -> List[Tuple[str, bool]]:
    """
    在指定目录中查找所有CSV文件并将它们转换为Excel格式（.xlsx）。

    参数:
        directory_path (str): 要搜索和转换CSV文件的目录路径。
        remove_original (bool): 是否在转换成功后删除原始CSV文件。默认为 False。

    返回:
        List[Tuple[str, bool]]: 转换结果列表，每个元素包含文件路径和转换是否成功

    异常:
        ValueError: 如果目录路径无效。
    """
    dir_path = Path(directory_path)
    if not dir_path.is_dir():
        raise ValueError(f"提供的路径不是有效目录: {directory_path}")

    logger.info(f"正在扫描目录中的CSV文件: {directory_path}")
    
    csv_files_found = list(dir_path.rglob("*.csv"))
    
    if not csv_files_found:
        logger.info("未找到CSV文件")
        return []

    logger.info(f"找到 {len(csv_files_found)} 个CSV文件，正在转换为Excel格式...")

    results = []
    for csv_file in csv_files_found:
        try:
            excel_file_path = convert_csv_to_excel(csv_file, remove_original)
            results.append((str(csv_file), True))
        except Exception as e:
            logger.error(f"转换文件 {csv_file} 时出错: {e}", exc_info=True)
            results.append((str(csv_file), False))
            # 继续处理其他文件，不中断
            
    return results