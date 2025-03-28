#!/bin/bash

# 获取脚本目录的绝对路径
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
DB_PATH="$PARENT_DIR/database/movies.db"
SQL_PATH="$SCRIPT_DIR/clean_db.sql"

echo "使用数据库: $DB_PATH"
echo "使用SQL脚本: $SQL_PATH"

# 备份数据库
BACKUP_PATH="$PARENT_DIR/database/movies_backup_$(date +"%Y%m%d_%H%M%S").db"
cp "$DB_PATH" "$BACKUP_PATH"
echo "已创建数据库备份: $BACKUP_PATH"

# 运行SQL脚本
echo "开始清理数据库..."
sqlite3 "$DB_PATH" < "$SQL_PATH"

echo "数据库清理完成！" 