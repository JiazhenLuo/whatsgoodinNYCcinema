#!/bin/bash
# 丰富现有数据库中的电影信息
# 1. 将JSON数据导入到SQLite数据库
# 2. 使用TMDB API获取中文标题、导演中文名称和IMDb ID
# 3. 修复中文文本格式问题
# 4. 生成豆瓣链接和Letterboxd链接

# 设置工作目录
cd "$(dirname "$0")"
ROOT_DIR="$(pwd)"
echo "工作目录: $ROOT_DIR"

# 颜色设置
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}开始丰富电影信息流程...${NC}"

# 0. 将JSON数据导入到SQLite数据库
echo -e "${YELLOW}[1/5] 将JSON数据导入到SQLite数据库...${NC}"
python json_to_db.py
if [ $? -ne 0 ]; then
    echo "数据导入失败，但继续执行后续步骤"
fi
echo -e "${GREEN}数据导入完成！${NC}"

# 1. 使用TMDB API丰富电影信息
echo -e "${YELLOW}[2/5] 使用TMDB API丰富电影信息...${NC}"
python scripts/update_movie_info.py --all
if [ $? -ne 0 ]; then
    echo "电影信息更新失败，但继续执行后续步骤"
fi
echo -e "${GREEN}电影信息更新完成！${NC}"

# 2. 修复中文标题
echo -e "${YELLOW}[3/5] 修复中文标题...${NC}"
python fix_chinese_titles.py
if [ $? -ne 0 ]; then
    echo "中文标题修复失败，但继续执行后续步骤"
fi
echo -e "${GREEN}中文标题修复完成！${NC}"

# 3. 修复中文文本格式问题
echo -e "${YELLOW}[4/5] 修复中文文本格式问题...${NC}"
python fix_unicode.py
if [ $? -ne 0 ]; then
    echo "中文文本格式修复失败，但继续执行后续步骤"
fi
echo -e "${GREEN}中文文本格式修复完成！${NC}"

# 4. 生成豆瓣链接和Letterboxd链接
echo -e "${YELLOW}[5/5] 生成豆瓣链接和Letterboxd链接...${NC}"
python douban_link_manager.py smartlinks --auto-click
python douban_link_manager.py letterboxd
if [ $? -ne 0 ]; then
    echo "链接生成失败"
fi
echo -e "${GREEN}链接生成完成！${NC}"

# 显示链接统计
python douban_link_manager.py stats

# 导出数据以便备份
echo -e "${YELLOW}导出数据以便备份...${NC}"
python scripts/manage_db.py export
if [ $? -ne 0 ]; then
    echo "数据导出失败"
fi

echo -e "${GREEN}整个丰富流程完成！${NC}" 