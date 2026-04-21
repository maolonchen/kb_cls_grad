from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn
import sys
import os

# 添加项目路径到 sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


# 定义 lifespan 上下文管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行
    try:
        from app.services.health_monitor_service import health_monitor_service
        health_monitor_service.start_monitoring()
        print("健康监控服务已启动")
    except Exception as e:
        print(f"启动健康监控服务失败: {e}")

    try:
        from app.services.interface_tracking_service import interface_tracking_service
        interface_tracking_service.start_monitoring()
        print("接口调用跟踪服务已启动")
    except Exception as e:
        print(f"启动接口调用跟踪服务失败: {e}")

    yield  # 这里是应用运行期间

    # 关闭时执行
    try:
        from app.services.health_monitor_service import health_monitor_service
        health_monitor_service.stop_monitoring()
        print("健康监控服务已停止")
    except Exception as e:
        print(f"停止健康监控服务失败: {e}")

    try:
        from app.services.interface_tracking_service import interface_tracking_service
        interface_tracking_service.stop_monitoring()
        print("接口调用跟踪服务已停止")
    except Exception as e:
        print(f"停止接口调用跟踪服务失败: {e}")


# 创建主应用，传入 lifespan
app = FastAPI(
    title="统一数据管理平台",
    description="整合知识库管理和规范解读功能",
    lifespan=lifespan
)


# 导入路由（保持不变）
try:
    from app.api.v1.endpoints.knowledge_base import router as kb_router
    app.include_router(kb_router)
    print("知识库管理应用已加载")
except Exception as e:
    print(f"DatabaseConfig: {e}")

try:
    from interpretation_specification.scripts.app import router as spec_router
    app.include_router(spec_router)
    print("规范解读应用路由已加载")
except Exception as e:
    print(f"加载规范解读应用失败: {e}")
    print(f"错误详情: {e}")


@app.get("/")
async def root():
    return {
        "message": "统一数据管理平台API服务",
        "available_endpoints": {
            "知识库管理": [
                "POST /api/v1/specification/knowledgeBase",
                "GET /api/v1/health"
            ],
            "规范解读": [
                "POST /api/v1/specification/tasks",
                "POST /api/v1/specification/tasks/final-jsonl",
                "GET /api/v1/specification/tasks/{specificationUId}/final-jsonl"
            ]
        }
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=64001,
        reload=False,
        log_level="info"
    )