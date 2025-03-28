# UTF-8中文API服务

该API服务器提供了直接以UTF-8格式返回中文内容的JSON API，解决了原Flask应用中中文字符被转换为Unicode转义序列的问题。

## 问题解决方案

本项目解决了两个主要问题：

1. **数据库中的中文编码和格式问题**：使用`fix_unicode.py`脚本修复了存储在SQLite数据库中的中文文本格式，包括不必要的空格、换行符和Unicode特殊字符。

2. **API输出中的Unicode转义序列问题**：通过`simple_direct_api.py`创建了一个简单高效的API服务器，确保中文字符直接以UTF-8格式返回，而非Unicode转义序列（\uXXXX）。

## 使用方法

### 启动服务器

```bash
python backend/simple_direct_api.py 8000
```

这将在端口8000上启动API服务器。

### API端点

- **健康检查**: `GET /api/v1/health`
- **获取所有电影**: `GET /api/v1/movies?limit=10&offset=0`
- **获取特定电影**: `GET /api/v1/movies/{id}`

### 特性

- **UTF-8中文输出**: 所有中文字符将直接显示，不会转换为Unicode转义序列
- **CORS支持**: 已启用跨域资源共享，允许前端应用从任何域名访问API
- **简单轻量**: 仅使用Python标准库，无需额外依赖

### 示例请求和响应

**请求**:
```
curl http://localhost:8000/api/v1/movies/1
```

**响应片段**:
```json
{
  "data": {
    "title_cn": "柏林苍穹下",
    "overview_cn": "柏林由两位天使守护着，一个是对人世疾苦冷眼旁观的卡西尔，另一个是常常感怀于人类疾苦的丹密尔..."
  }
}
```

## 技术实现

该API服务器通过以下方式确保中文字符正确显示：

1. 在JSON序列化时使用`ensure_ascii=False`参数
2. 将响应的Content-Type设置为`application/json; charset=utf-8`
3. 使用UTF-8编码响应内容

## 维护和扩展

如需添加新的API端点，请在`ChineseHTTPHandler`类的`do_GET`方法中添加新的路径处理逻辑。服务器使用简单的路径匹配和正则表达式来识别和处理不同的API请求。 