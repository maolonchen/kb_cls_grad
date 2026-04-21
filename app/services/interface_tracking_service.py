import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Set
from pathlib import Path
from .standard_compared_deletion_service import standard_compared_deletion_service

logger = logging.getLogger(__name__)


class InterfaceTrackingService:
    """接口调用跟踪服务，用于跟踪restClassification和restDataElement接口的调用，
    并在规定时间内执行标准比较删除操作"""

    def __init__(self):
        # 存储每个specificationUId的接口调用状态
        # key: specificationUId, value: set of called interfaces ('classification', 'data_element')
        self.interface_calls: Dict[str, Set[str]] = {}
        # 存储上次调用时间
        self.last_call_times: Dict[str, datetime] = {}
        # 2分钟超时时间
        self.timeout_seconds = 120  # 2 minutes
        # 标记是否正在运行监控任务
        self.is_monitoring = False
        # 监控任务
        self.monitor_task = None

    def record_interface_call(self, specification_u_id: str, interface_type: str):
        """记录接口调用"""
        if specification_u_id not in self.interface_calls:
            self.interface_calls[specification_u_id] = set()
        
        self.interface_calls[specification_u_id].add(interface_type)
        self.last_call_times[specification_u_id] = datetime.now()
        
        logger.info(f"记录接口调用: spec_id={specification_u_id}, interface={interface_type}, "
                   f"当前已调用接口: {self.interface_calls[specification_u_id]}")

    def should_process_deletion(self, spec_id: str) -> bool:
        """判断是否应该执行删除操作"""
        if spec_id not in self.interface_calls:
            return False
            
        # 情况1: 两个接口都已调用
        if {'classification', 'data_element'}.issubset(self.interface_calls[spec_id]):
            return True
        
        # 情况2: 只有分类接口被调用，且已超过2分钟
        if (self.interface_calls[spec_id] == {'classification'} and 
            datetime.now() - self.last_call_times[spec_id] > timedelta(seconds=self.timeout_seconds)):
            return True
            
        return False

    async def process_deletion(self, specification_u_id: str):
        """执行删除操作"""
        logger.info(f"开始执行标准比较删除操作: {specification_u_id}")
        
        try:
            # 调用标准比较删除服务
            result = await standard_compared_deletion_service.compare_and_delete(specification_u_id)
            
            if result["success"]:
                logger.info(f"标准比较删除操作成功: {specification_u_id}, 删除数量: {result.get('deleted_count', 0)}")
            else:
                logger.warning(f"标准比较删除操作失败: {specification_u_id}, 错误: {result.get('msg', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"执行标准比较删除操作时出现异常: {specification_u_id}, 错误: {str(e)}", exc_info=True)

    def cleanup_completed_calls(self, spec_id: str):
        """清理已完成的调用记录"""
        if spec_id in self.interface_calls:
            del self.interface_calls[spec_id]
        if spec_id in self.last_call_times:
            del self.last_call_times[spec_id]

    async def monitor_and_process(self):
        """监控并处理需要删除的规范"""
        while self.is_monitoring:
            try:
                # 复制当前的键集，防止在迭代过程中修改字典
                current_specs = list(self.interface_calls.keys())
                
                completed_specs = []
                for spec_id in current_specs:
                    if self.should_process_deletion(spec_id):
                        await self.process_deletion(spec_id)
                        completed_specs.append(spec_id)
                
                # 清理已完成的调用记录
                for spec_id in completed_specs:
                    self.cleanup_completed_calls(spec_id)
                
                # 每10秒检查一次
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"监控任务出现异常: {str(e)}", exc_info=True)
                await asyncio.sleep(10)  # 出错后稍等一下再继续

    def start_monitoring(self):
        """启动监控任务"""
        if not self.is_monitoring:
            self.is_monitoring = True
            self.monitor_task = asyncio.create_task(self.monitor_and_process())
            logger.info("接口调用监控服务已启动")

    def stop_monitoring(self):
        """停止监控任务"""
        self.is_monitoring = False
        if self.monitor_task:
            self.monitor_task.cancel()
        logger.info("接口调用监控服务已停止")


# 创建全局实例
interface_tracking_service = InterfaceTrackingService()