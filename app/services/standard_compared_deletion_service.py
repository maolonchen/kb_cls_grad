import json
import logging
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import shutil


logger = logging.getLogger(__name__)


class StandardComparedDeletionService:
    """规范比对删除服务，用于删除在data/processed中存在但在data/standard中未定义的目录"""

    def __init__(self):
        self.standards_dir = Path("data/standards")
        self.processed_dir = Path("data/processed")

    def clear_standard_file(self, specification_uid: str) -> Dict[str, Any]:
        """
        清空指定规范ID的标准文件，如果文件不存在则创建一个空文件
        
        Args:
            specification_uid: 规范唯一标识符
            
        Returns:
            操作结果
        """
        try:
            standard_file_path = self.standards_dir / f"{specification_uid}_standard.jsonl"
            
            # 确保目录存在
            standard_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 无论文件是否存在，都创建或清空文件
            with open(standard_file_path, 'w', encoding='utf-8') as f:
                f.truncate(0)
            
            logger.info(f"已清空或创建标准文件: {standard_file_path}")
            return {
                "success": True,
                "msg": f"已清空或创建标准文件: {standard_file_path}"
            }
        except Exception as e:
            logger.error(f"清空或创建标准文件失败: {e}", exc_info=True)
            return {
                "success": False,
                "msg": f"清空或创建标准文件失败: {str(e)}"
            }

    async def compare_and_delete(self, specification_uid: str) -> Dict[str, Any]:
        """
        根据给定的规范UID进行比对删除操作
        
        Args:
            specification_uid: 规范唯一标识符
            
        Returns:
            包含操作结果的字典
        """
        try:
            specification_uid = specification_uid.replace("-", "_")
            # 处理以数字开头的规范ID，添加前缀
            prefixed_specification_uid = (
                f"_{specification_uid}"
                if specification_uid and not specification_uid.startswith('_') and specification_uid[0].isdigit()
                else specification_uid
            )

            # 检查标准文件是否存在
            standard_file_path = self.standards_dir / f"{specification_uid}_standard.jsonl"
            if not standard_file_path.exists():
                logger.warning(f"标准文件不存在: {standard_file_path}")
                return {
                    "success": False,
                    "msg": f"标准文件不存在: {standard_file_path}"
                }

            # 检查处理后的目录是否存在
            processed_chunks_dir = self.processed_dir / f"{prefixed_specification_uid}_chunks"
            if not processed_chunks_dir.exists():
                logger.warning(f"处理目录不存在: {processed_chunks_dir}")
                return {
                    "success": False,
                    "msg": f"处理目录不存在: {processed_chunks_dir}"
                }

            # 解析标准文件中的分类路径
            valid_paths = self._parse_standard_file(standard_file_path)
            
            # 获取当前处理目录中的所有目录路径（不包括非分类文件）
            all_dirs = self._get_all_directories(processed_chunks_dir)
            
            # 找出需要删除的路径
            paths_to_delete = []
            for dir_path in all_dirs:
                # 将目录路径转换为相对路径
                relative_path_parts = dir_path.relative_to(processed_chunks_dir).parts
                
                # 检查路径是否在有效路径中
                found_in_standards = False
                for valid_path in valid_paths:
                    if self._is_path_prefix(relative_path_parts, valid_path):
                        found_in_standards = True
                        break
                
                if not found_in_standards:
                    paths_to_delete.append(dir_path)
            
            # 按深度排序，确保先删除深层路径
            paths_to_delete.sort(key=lambda x: len(x.parts), reverse=True)
            
            # 删除不在标准中的路径
            deleted_count = 0
            for path_to_delete in paths_to_delete:
                try:
                    if path_to_delete.is_dir():
                        # 检查目录内容，如果目录中没有子目录或分类相关文件则删除
                        if self._should_delete_directory(path_to_delete, valid_paths, processed_chunks_dir):
                            # 删除目录及其所有内容
                            shutil.rmtree(path_to_delete)
                            logger.info(f"已删除目录: {path_to_delete}")
                            deleted_count += 1
                        else:
                            # 目录中有仍然需要保留的子项，跳过删除
                            continue
                except Exception as e:
                    logger.error(f"删除目录失败 {path_to_delete}: {e}")

            # 清理空的父目录
            self._cleanup_empty_directories(processed_chunks_dir)
            
            logger.info(f"完成比对删除操作，共删除 {deleted_count} 个路径")
            
            return {
                "success": True,
                "msg": f"完成比对删除操作，共删除 {deleted_count} 个项目",
                "deleted_count": deleted_count,
                "paths_deleted": [str(p) for p in paths_to_delete]
            }
            
        except Exception as e:
            logger.error(f"执行比对删除操作失败: {e}", exc_info=True)
            return {
                "success": False,
                "msg": f"执行比对删除操作失败: {str(e)}"
            }

    def _should_delete_directory(self, dir_path: Path, valid_paths: List[List[str]], base_path: Path) -> bool:
        """
        判断目录是否应该被删除
        
        Args:
            dir_path: 要判断的目录路径
            valid_paths: 有效的路径列表
            base_path: 基础路径
            
        Returns:
            是否应该删除此目录
        """
        # 获取目录相对于基础路径的部分
        relative_path_parts = dir_path.relative_to(base_path).parts
        
        # 检查是否有任何有效路径以此目录路径为前缀
        for valid_path in valid_paths:
            if self._is_path_prefix(relative_path_parts, valid_path):
                return False  # 如果有效路径以此目录为前缀，则不应删除
        
        # 检查目录内容，看是否有子目录或文件应该保留
        if dir_path.is_dir():
            for item in dir_path.iterdir():
                item_relative_parts = item.relative_to(base_path).parts
                for valid_path in valid_paths:
                    if self._is_path_prefix(item_relative_parts, valid_path):
                        # 发现目录中有应保留的子项，因此不应删除此目录
                        return False
        
        return True

    def _cleanup_empty_directories(self, base_path: Path):
        """
        清理空目录，从最深层开始
        
        Args:
            base_path: 基础路径
        """
        # 从最深层开始遍历，删除空目录
        for root, dirs, files in os.walk(base_path, topdown=False):
            for dir_name in dirs:
                dir_path = Path(root) / dir_name
                # 检查目录是否为空，或者只包含非分类文件（如origin_data.md）
                if self._is_empty_or_only_metadata(dir_path):
                    try:
                        dir_path.rmdir()
                        logger.info(f"已删除空目录: {dir_path}")
                    except Exception as e:
                        logger.error(f"删除空目录失败 {dir_path}: {e}")

    def _is_empty_or_only_metadata(self, dir_path: Path) -> bool:
        """
        检查目录是否为空或仅包含元数据文件
        
        Args:
            dir_path: 要检查的目录路径
            
        Returns:
            目录是否为空或仅包含元数据文件
        """
        if not dir_path.is_dir():
            return False
            
        contents = list(dir_path.iterdir())
        if not contents:
            return True
            
        # 检查是否只包含元数据文件（如origin_data.md, chunk_data.json等）
        metadata_files = {'origin_data.md', 'chunk_data.json'}
        for item in contents:
            if item.is_dir() or item.name not in metadata_files:
                # 如果目录包含子目录或其他类型的文件，则不是空目录
                return False
        
        # 目录只包含元数据文件，可以视为"空"
        return True

    def _parse_standard_file(self, standard_file_path: Path) -> List[List[str]]:
        """
        解析标准文件，提取有效的分类路径
        
        Args:
            standard_file_path: 标准文件路径
            
        Returns:
            有效的分类路径列表，每个路径是一个字符串列表
        """
        valid_paths = []
        
        with open(standard_file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    entry = json.loads(line)
                    data = entry.get('data', {})
                    
                    # 提取分类路径，从第一个分类字段开始直到遇到空值
                    path_parts = []
                    for i in range(10):  # 假设最多10层分类
                        key = str(i)
                        if key in data and data[key]:
                            path_parts.append(data[key])
                        else:
                            break
                    
                    if path_parts:
                        valid_paths.append(path_parts)
                        
                except json.JSONDecodeError as e:
                    logger.error(f"解析JSON行失败: {line}, 错误: {e}")
                    continue
        
        return valid_paths

    def _get_all_directories(self, base_path: Path) -> List[Path]:
        """
        获取基础路径下的所有子目录
        
        Args:
            base_path: 基础路径
            
        Returns:
            所有子目录的列表
        """
        directories = []
        
        for root, dirs, files in os.walk(base_path):
            for dir_name in dirs:
                dir_path = Path(root) / dir_name
                directories.append(dir_path)
        
        return directories

    def _is_path_prefix(self, path_parts: tuple, valid_path: List[str]) -> bool:
        """
        检查路径是否是有效路径的前缀或完全匹配
        
        Args:
            path_parts: 当前路径的各部分
            valid_path: 有效的路径
            
        Returns:
            是否匹配
        """
        # 如果当前路径长度大于有效路径长度，不可能是其前缀
        if len(path_parts) > len(valid_path):
            return False
        
        # 检查当前路径是否是有效路径的前缀
        for i, part in enumerate(path_parts):
            if i >= len(valid_path) or part != valid_path[i]:
                return False
        
        return True


# 创建服务实例
standard_compared_deletion_service = StandardComparedDeletionService()