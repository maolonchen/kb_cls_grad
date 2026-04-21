# 某数据处理平台

基于大语言模型和向量数据库的统一数据管理平台，提供**知识库管理**和**规范解读**两大核心功能，面向 ??? 数据安全分类分级需求。

## 功能特性

### 知识库管理
- **多格式文档解析** — 支持 PDF、Word、Excel、TXT、CSV 等格式
- **AI 智能分类** — 使用 LLM将文档分为叙述性文本或结构化数据
- **智能分块** — 叙述性文本采用标题固定分块，结构化数据采用 LLM 引导分块
- **向量检索** — 通过嵌入并存储至向量数据库，支持相似度检索
- **数据识别** — AI 驱动的表/字段级分类与定级
- **数据元管理** — 数据元的增删改查及批量匹配

### 规范解读
- **Excel 规则解析** — 从 Excel 文件中读取数据分类规则和分级规范
- **多步骤流水线** — 树结构提取、实体/特征提取、相似度比对、定级处理
- **Kafka 集成** — 结果输出至 Kafka，供下游系统消费
- **分级定级** — 将数据映射至核心数据/重要数据/一般数据等级及适用场景

## 项目结构

```
kb_cls_grad/
├── main.py                                  # FastAPI 入口（端口 64001）
├── app/                                     # 知识库管理模块
│   ├── core/                                # 配置、日志、工具类、向量客户端
│   ├── algorithms/                          # 分类、相似度、分块算法
│   ├── processors/                          # 文件解析（PDF、Word、Excel、TXT、CSV）
│   ├── schemas/                             # Pydantic 请求/响应模型
│   ├── services/                            # 业务逻辑层
│   ├── api/v1/endpoints/                    # REST API 端点
│   └── core/prompts/                        # LLM 提示词模板
├── interpretation_specification/            # 规范解读模块
│   ├── scripts/                             # 独立 FastAPI 路由
│   ├── config/                              # 模块配置
│   ├── src/                                 # 流水线处理器
│   └── services/                            # 辅助服务
├── scripts/                                 # 工具脚本
├── data/                                    # 数据存储（原始文件、处理结果、标准数据）
└── test/                                    # 测试文件
```

## 环境要求

- **Python 3.10+**
- **uv** 包管理器
- **Milvus 2.6.8**（向量数据库）
- **外部 AI 服务：**
  - MinerU — 文档解析（PDF 转 Markdown）
  - LLM — 对话大模型，用于分类和分块
  - EmbedM — 文本嵌入模型

## 快速开始

### 1. 安装依赖

```bash
uv sync
```

### 2. 启动 Milvus

```bash
docker compose up -d
```

### 3. 配置服务地址

编辑 `app/core/config.py` 设置各服务地址：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| Chat LLM | `0.0.0.0:8000` | LLM 服务地址 |
| Embedding | `0.0.0.0:9998` | EmbedM 服务地址 |
| MinerU | `0.0.0.0:8003` | 文档解析服务地址 |
| Milvus | `0.0.0.?:19530` | 向量数据库地址 |

### 4. 启动服务

```bash
# 生产模式
bash manage_interp_kb.sh start

# 开发模式（带调试日志）
bash manage_interp_kb.sh dev

# 或直接运行
uv run python main.py
```

服务启动后访问 `http://localhost:<自定义>`，API 文档在 `/docs`。

## API 接口

### 知识库管理

| 方法 | 接口 | 说明 |
|---|---|---|
| POST | `/api/v1/specification/knowledgeBase` | 上传文件至知识库 |
| POST | `/api/v1/specification/knowledgeBase/delete` | 删除知识库文件 |
| POST | `/api/v1/specification/knowledgeBase/rebuild` | 重建向量数据库 |
| POST | `/api/v1/dataRecognition` | AI 数据识别（表/字段级） |
| POST | `/api/v1/fileRecognition` | AI 文件级识别 |
| POST | `/api/v1/specification/knowledgeBase/classification` | 获取分类信息 |
| POST | `/api/v1/specification/knowledgeBase/dataElement` | 获取数据元信息 |
| GET | `/api/v1/specification/knowledgeBase/sizeInformation` | 知识库容量信息 |
| GET | `/api/v1/health` | 健康检查 |

### 规范解读

| 方法 | 接口 | 说明 |
|---|---|---|
| POST | `/api/v1/specification/tasks` | 创建解读任务（完整流水线） |
| POST | `/api/v1/specification/tasks/final-jsonl` | 创建任务（至 final.jsonl） |
| GET | `/api/v1/specification/tasks/{uid}/final-jsonl` | 获取任务结果 |
| POST | `/api/v1/dataElements/match` | 批量数据元匹配 |

## 处理流程

```
文件上传 → 格式转换 → Markdown 清洗 → LLM 分类
  → 智能分块 → 向量嵌入 → Milvus 存储 → Kafka 通知
```

## 管理命令

```bash
bash manage_interp_kb.sh start     # 启动服务
bash manage_interp_kb.sh stop      # 停止服务
bash manage_interp_kb.sh stop -a   # 强制停止所有相关进程
bash manage_interp_kb.sh status    # 查看服务状态
bash manage_interp_kb.sh logs      # 查看日志
bash manage_interp_kb.sh dev       # 开发模式运行
bash manage_interp_kb.sh clean     # 清理临时文件
```
