# 数据库迁移

本目录包含数据库架构变更的迁移文件。

## 如何应用迁移

SQLite数据库迁移采用脚本方式进行。当需要更新数据库架构时，请创建一个新的迁移脚本，添加到此目录。

### 迁移脚本命名规则

迁移脚本应遵循以下命名规则：

```
YYYYMMDD_序号_迁移描述.sql
```

例如：
```
20230601_001_add_director_cn_column.sql
```

### 应用迁移

迁移可通过`app/models/database.py`中的`apply_migrations()`函数应用。当应用启动时，该函数会自动执行所有未应用的迁移。

## 创建迁移脚本

创建一个新的迁移脚本：

1. 确定要做的架构变更
2. 创建一个新的SQL文件，包含变更语句
3. 加入回滚语句（以`-- ROLLBACK`开头的注释）

示例：

```sql
-- 添加导演中文名字段
ALTER TABLE movies ADD COLUMN director_cn TEXT;

-- ROLLBACK
-- ALTER TABLE movies DROP COLUMN director_cn;
``` 