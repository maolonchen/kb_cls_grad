import asyncio
import logging
from typing import Dict, Optional, Callable, Any
from concurrent.futures import CancelledError

logger = logging.getLogger(__name__)

class TaskManager:
    """任务管理器，用于管理向量化任务的调度和并发控制"""
    
    def __init__(self):
        # 存储每个规范ID的锁
        self._locks: Dict[str, asyncio.Lock] = {}
        # 存储每个规范ID的待执行或正在执行的向量化任务
        self._pending_tasks: Dict[str, asyncio.Task] = {}
    
    def get_lock(self, specification_uid: str) -> asyncio.Lock:
        """获取特定规范ID的锁，如果不存在则创建"""
        if specification_uid not in self._locks:
            self._locks[specification_uid] = asyncio.Lock()
        return self._locks[specification_uid]
    
    def cancel_pending_task(self, specification_uid: str):
        """取消指定规范ID的待执行任务"""
        if specification_uid in self._pending_tasks:
            task = self._pending_tasks[specification_uid]
            if not task.done():
                logger.info(f"取消待执行的向量化任务: {specification_uid}")
                task.cancel()
            del self._pending_tasks[specification_uid]
    
    async def schedule_vector_task(
        self, 
        specification_uid: str, 
        task_func: Callable, 
        *args, 
        **kwargs
    ) -> Any:
        """
        调度向量化任务，如果存在待执行任务则取消之
        
        Args:
            specification_uid: 规范ID
            task_func: 任务函数
            *args, **kwargs: 传递给任务函数的参数
            
        Returns:
            任务执行结果
        """
        # 取消之前为该规范ID计划的任何向量化任务
        self.cancel_pending_task(specification_uid)
        
        # 创建新任务
        task = asyncio.create_task(
            self._execute_serialized_task(specification_uid, task_func, *args, **kwargs)
        )
        
        # 记录新任务
        self._pending_tasks[specification_uid] = task
        
        try:
            # 等待任务完成并返回结果
            result = await task
            return result
        except CancelledError:
            logger.info(f"向量化任务被取消: {specification_uid}")
            raise
        except Exception as e:
            logger.error(f"向量化任务执行失败: {specification_uid}, 错误: {str(e)}")
            raise
        finally:
            # 清理已完成的任务
            if specification_uid in self._pending_tasks:
                del self._pending_tasks[specification_uid]
    
    async def _execute_serialized_task(
        self, 
        specification_uid: str, 
        task_func: Callable, 
        *args, 
        **kwargs
    ) -> Any:
        """
        在锁保护下串行执行任务
        
        Args:
            specification_uid: 规范ID
            task_func: 任务函数
            *args, **kwargs: 传递给任务函数的参数
            
        Returns:
            任务执行结果
        """
        lock = self.get_lock(specification_uid)
        
        async with lock:
            logger.info(f"开始执行向量化任务: {specification_uid}")
            try:
                result = await task_func(*args, **kwargs)
                logger.info(f"向量化任务完成: {specification_uid}")
                return result
            except Exception as e:
                logger.error(f"向量化任务执行失败: {specification_uid}, 错误: {str(e)}")
                raise

# 创建全局任务管理器实例
task_manager = TaskManager()