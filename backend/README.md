# 纽约电影院应用后端

这是WhatsgoodinNYCcinema应用的后端服务，提供电影和放映信息的API。

## 特性

- 电影数据库（包含标题、简介、评分等信息）
- 放映信息（包含日期、时间、售票链接等）
- 支持英文和中文内容
- API服务提供JSON格式数据
- 支持UTF-8中文内容直接显示（无Unicode转义问题）

## 安装与设置

### 环境要求

- Python 3.6+
- SQLite3

### 安装依赖

```bash
pip install -r requirements.txt
```

### 配置

创建`.env`文件并设置以下环境变量：

```
TMDB_API_KEY=your_api_key
```

## API服务器

我们提供了两种API服务器模式：

1. **Flask API**（标准模式，但中文可能显示为Unicode转义序列）
2. **直接模式**（轻量级，中文正确显示为UTF-8字符）

### 启动API服务器

使用提供的统一入口点：

```bash
# 默认启动直接模式（推荐，中文正确显示）
python backend/api.py

# 指定端口
python backend/api.py --port 8888

# 使用Flask模式（如需完整功能但不关心中文显示问题）
python backend/api.py --mode flask
```

### API端点

- **健康检查**: `GET /api/v1/health`
- **获取所有电影**: `GET /api/v1/movies?limit=10&offset=0`
- **获取特定电影**: `GET /api/v1/movies/{id}`

## 数据库维护

### 修复数据库中文格式问题

我们提供了一个工具来修复数据库中的中文格式问题：

```bash
python backend/fix_unicode.py
```

该工具可以：
- 清理多余的空格和换行符
- 标准化中文和英文之间的间距
- 修复特殊Unicode字符（如全角空格）

## 开发指南

### 项目结构

```
backend/
├── app/              # Flask应用
│   ├── api/          # API端点
│   ├── models/       # 数据模型
│   └── services/     # 业务逻辑服务
├── simple_direct_api.py  # 轻量级中文API服务器
├── api.py            # API统一入口点
├── fix_unicode.py    # 数据库中文修复工具
└── movies.db         # SQLite数据库
```

### 添加新功能

如需添加新的API端点：

- 对于**Flask API**：在`app/api/`目录中添加新的路由
- 对于**直接模式**：在`simple_direct_api.py`的`ChineseHTTPHandler`类中添加新的路由处理

## 中文内容支持

本项目专门优化了对中文内容的支持，解决了两个常见问题：

1. **数据库中的中文格式问题**：使用`fix_unicode.py`脚本修复
2. **API返回的Unicode转义问题**：使用`simple_direct_api.py`提供UTF-8原生输出

详细说明请参阅 [中文API服务文档](./README-api-utf8.md)。

## 项目结构

```
backend/
├── app/                  # 应用主目录
│   ├── api/              # API路由
│   ├── config/           # 配置文件
│   ├── models/           # 数据模型
│   ├── services/         # 服务层
│   ├── utils/            # 工具函数
│   └── __main__.py       # 应用入口点
├── data/                 # 数据文件
│   └── json/             # JSON数据
├── migrations/           # 数据库迁移文件
├── scripts/              # 命令行脚本
├── movies.db             # SQLite数据库
├── requirements.txt      # 项目依赖
└── README.md             # 项目文档
```

## 功能特性

- RESTful API 接口，提供电影和放映信息
- 支持中英文电影信息，包括标题、导演和概述
- 提供TMDB和OMDb集成，自动更新电影信息
- 支持分页、搜索和过滤
- 特殊情况处理机制，支持难以在标准数据库中找到的电影

## 技术栈

- Python 3.8+
- Flask: Web框架
- SQLite: 数据库
- TMDB API: 电影信息
- OMDb API: 额外电影数据

## 开发环境设置

1. 克隆仓库
```bash
git clone <repository_url>
cd whatsgoodinNYCcinema/backend
```

2. 创建虚拟环境并安装依赖
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# 或
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

3. 配置环境变量
创建`.env`文件：
```
TMDB_API_KEY=your_tmdb_api_key
FLASK_ENV=development
```

4. 初始化数据库
```bash
python -m app --init-db
```

5. 运行开发服务器
```bash
python -m app --debug
```

## API 接口

### 电影

- `GET /api/v1/movies`: 获取所有电影（支持分页）
- `GET /api/v1/movies/{id}`: 获取特定电影
- `GET /api/v1/movies/recent`: 获取最近添加的电影
- `POST /api/v1/movies/{id}/refresh`: 刷新电影信息

### 放映信息

- `GET /api/v1/screenings`: 获取即将上映的电影
- `GET /api/v1/screenings/movie/{id}`: 获取特定电影的所有放映时间
- `GET /api/v1/screenings/cinema/{cinema}`: 获取特定影院的所有放映时间
- `GET /api/v1/screenings/date/{date}`: 获取特定日期的所有放映时间

## 脚本

本项目提供两个主要脚本来更新电影信息：

### 更新所有电影信息

```bash
python scripts/update_movie_info.py
```

可选参数：
- `--overview-only`: 仅更新中文概述
- `--special-only`: 仅更新特殊情况

### 更新新添加的电影

```bash
python scripts/update_new_movies.py
```

可选参数：
- `--days N`: 更新过去N天内添加的电影（默认7天）

## 移动应用开发注意事项

如果计划将本后端与iOS应用（通过Xcode开发）集成，请考虑以下几点：

1. API接口设计遵循RESTful规范，便于与iOS应用集成
2. JSON响应格式一致，便于Swift/Objective-C解析
3. 支持CORS跨域请求
4. 提供健康检查接口 (`/api/v1/health`)
5. 分页机制一致，支持客户端缓存

## 部署

### 使用Gunicorn

```bash
gunicorn "app:create_app()" --bind 0.0.0.0:5000
```

## 贡献指南

1. 遵循PEP 8代码风格
2. 为新功能添加测试
3. 使用有意义的提交消息
4. 提交前运行测试 