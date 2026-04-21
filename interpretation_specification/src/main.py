# -*- coding: utf-8 -*-
"""
主流程控制脚本

控制整个数据处理流程的执行顺序和依赖关系
"""

import os
import sys
import importlib.util
import asyncio
from pathlib import Path

# 添加项目路径到系统路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root.parent))

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def load_module_from_file(file_path):
    """动态加载Python模块"""
    # 构建模块名，基于项目根目录的相对路径
    try:
        # 获取相对于项目根目录的路径
        relative_path = file_path.relative_to(project_root)
        # 转换为模块名 (将路径分隔符替换为点号)
        module_name = str(relative_path.with_suffix('')).replace(os.sep, '.')
        
        # 检查模块是否已加载
        if module_name in sys.modules:
            return sys.modules[module_name]
        
        # 使用标准导入机制
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        # 将模块添加到sys.modules中，以便支持相对导入
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        logger.error(f"加载模块失败 {file_path}: {e}")
        raise

def check_file_exists(file_path, description=""):
    """检查文件是否存在"""
    if not os.path.exists(file_path):
        logger.error(f"所需文件不存在 {description}: {file_path}")
        return False
    return True

def run_step(step_number, module_path, required_files=None, specification_u_id=None):
    """运行指定步骤"""
    logger.info(f"开始执行步骤 {step_number}: {module_path}")
    
    if not module_path.exists():
        logger.error(f"模块文件不存在: {module_path}")
        return False
    
    # 检查依赖文件
    if required_files:
        for file_path in required_files:
            if not check_file_exists(file_path):
                logger.warning(f"步骤 {step_number} 依赖文件缺失，跳过执行")
                return False
    
    try:
        # 动态加载并执行模块
        module = load_module_from_file(module_path)
        if hasattr(module, 'main') and callable(module.main):
            logger.info(f"执行 {module_path.name}.main()...")
            # 检查main函数是否接受specification_u_id参数
            import inspect
            sig = inspect.signature(module.main)
            if specification_u_id is not None and 'specification_u_id' in sig.parameters:
                # 检查是否是异步函数
                if inspect.iscoroutinefunction(module.main):
                    # 异步函数需要使用asyncio.run运行
                    result = asyncio.run(module.main(specification_u_id=specification_u_id))
                else:
                    result = module.main(specification_u_id=specification_u_id)
            else:
                # 检查是否是异步函数
                if inspect.iscoroutinefunction(module.main):
                    # 异步函数需要使用asyncio.run运行
                    result = asyncio.run(module.main())
                else:
                    result = module.main()
            logger.info(f"步骤 {step_number} 执行完成")
            return True
        else:
            logger.warning(f"模块 {module_path.name} 没有可执行的 main 函数")
            return False
    except Exception as e:
        logger.error(f"步骤 {step_number} 执行出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主函数"""
    logger.info("数据处理流程控制器")
    logger.info(f"项目路径: {project_root}")
    
    # 定义处理流程和依赖关系
    steps = [
        {
            "number": 1,
            "module_path": project_root / "processors" / "excel_processor.py",
            "description": "Excel转JSON",
            "dependencies": []  # 依赖原始Excel文件
        },
        {
            "number": 2,
            "module_path": project_root / "processors" / "tree_processor.py",
            "description": "多树结构处理",
            "dependencies": [Path(os.path.dirname(os.path.dirname(__file__))) / "data" / "processed" / "ori_json.json"]  # 依赖步骤1的输出
        },
        {
            "number": 3,
            "module_path": project_root / "processors" / "entity_processor.py",
            "description": "梯度到实体转换",
            "dependencies": [Path(os.path.dirname(os.path.dirname(__file__))) / "data" / "processed" / "ori_json.json"]
        },
        {
            "number": 4,
            "module_path": project_root / "database" / "vector_db.py",
            "description": "数据库构建",
            "dependencies": [Path(os.path.dirname(os.path.dirname(__file__))) / "data" / "processed" / "output_sheet3.json"]
        },
        {
            "number": 5,
            "module_path": project_root / "processors" / "similarity_processor.py",
            "description": "相似度比较",
            "dependencies": [
                Path(os.path.dirname(os.path.dirname(__file__))) / "data" / "processed" / "output_sheet3.json"
            ]
        },
        {
            "number": 6,
            "module_path": project_root / "processors" / "llm_processor.py",
            "description": "LLM后处理相似度分析",
            "dependencies": []
        },
        {
            "number": 7,
            "module_path": project_root / "processors" / "feature_processor.py",
            "description": "特征到实体转换",
            "dependencies": []
        },
        {
            "number": 8,
            "module_path": project_root / "processors" / "content_processor.py",
            "description": "内容到等级转换",
            "dependencies": [Path(os.path.dirname(os.path.dirname(__file__))) / "data" / "processed" / "processed_results.jsonl"]
        },
        {
            "number": 9,
            "module_path": project_root / "processors" / "kafka_processor.py",
            "description": "生成Kafka输出数据",
            "dependencies": [Path(os.path.dirname(os.path.dirname(__file__))) / "data" / "processed" / "final.jsonl"]
        }
    ]
    
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='数据处理流程控制器')
    parser.add_argument('--step', type=int, help='执行特定步骤 (1-9)')
    parser.add_argument('--all', action='store_true', help='执行所有步骤')
    parser.add_argument('--from-step', type=int, help='从指定步骤开始执行')
    parser.add_argument('--specification-u-id', type=str, help='行业标识符')
    
    args = parser.parse_args()
    specification_u_id = args.specification_u_id
    
    if args.all:
        # 执行所有步骤
        for step in steps:
            success = run_step(
                step["number"], 
                step["module_path"], 
                step["dependencies"],
                specification_u_id=specification_u_id
            )
            if not success:
                logger.error(f"步骤 {step['number']} 执行失败，停止执行后续步骤")
                return 1
    elif args.step:
        # 执行特定步骤
        if 1 <= args.step <= 9:
            step = steps[args.step - 1]
            run_step(step["number"], step["module_path"], step["dependencies"], specification_u_id=specification_u_id)
        else:
            logger.error(f"无效步骤: {args.step}，请输入1-9之间的数字")
            return 1
    elif args.from_step:
        # 从指定步骤开始执行
        if 1 <= args.from_step <= 9:
            for i in range(args.from_step - 1, len(steps)):
                step = steps[i]
                success = run_step(
                    step["number"], 
                    step["module_path"], 
                    step["dependencies"],
                    specification_u_id=specification_u_id
                )
                if not success:
                    logger.error(f"步骤 {step['number']} 执行失败，停止执行后续步骤")
                    return 1
        else:
            logger.error(f"无效步骤: {args.from_step}，请输入1-9之间的数字")
            return 1
    else:
        # 显示帮助信息
        parser.print_help()
        print("\n步骤说明:")
        for step in steps:
            print(f"  {step['number']}. {step['description']}")
        return 0
    
    logger.info("数据处理流程执行完成")
    return 0

if __name__ == "__main__":
    sys.exit(main())