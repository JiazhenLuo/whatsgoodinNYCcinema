# 电影信息自动更新

本文档介绍如何使用和设置电影信息自动更新工具。

## 功能概述

我们提供了两个脚本来更新电影信息：

1. `update_movie_info.py` - 更新数据库中所有缺少信息的电影，包括特殊情况和中文概述
2. `update_new_movies.py` - 专门更新最近添加的电影信息

这些脚本会：

1. 查找缺少信息的电影（或最近添加的电影）
2. 使用TMDB和OMDb API搜索并获取电影详细信息
3. 更新数据库中的电影记录，包括：
   - 中英文标题
   - 导演信息
   - 中英文电影概述
   - IMDb和TMDB ID
   - 电影评分
   - 预告片链接
   - Letterboxd链接
   - 豆瓣搜索链接

## 使用方法

### 更新所有电影信息

```bash
# 更新所有缺少信息的电影
python backend/update_movie_info.py
```

### 更新最近添加的电影

```bash
# 更新最近7天内添加的电影（默认）
python backend/update_new_movies.py

# 更新最近30天内添加的电影
python backend/update_new_movies.py --days 30

# 更新最近90天内添加的电影
python backend/update_new_movies.py --days 90
```

### 设置定时任务

要设置每天自动运行，可以使用cron作业：

```bash
# 编辑crontab
crontab -e

# 添加以下行（每天凌晨3点运行）
0 3 * * * cd /path/to/whatsgoodinNYCcinema && python backend/update_new_movies.py
```

或者使用macOS的launchd：

```bash
# 创建plist文件
cat > ~/Library/LaunchAgents/com.whatsgoodcinema.updateMovies.plist << EOL
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.whatsgoodcinema.updateMovies</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/path/to/whatsgoodinNYCcinema/backend/update_new_movies.py</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>3</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>RunAtLoad</key>
    <false/>
    <key>StandardErrorPath</key>
    <string>/tmp/updateMovies.err</string>
    <key>StandardOutPath</key>
    <string>/tmp/updateMovies.out</string>
</dict>
</plist>
EOL

# 加载定时任务
launchctl load ~/Library/LaunchAgents/com.whatsgoodcinema.updateMovies.plist
```

## 环境设置

该脚本依赖于以下环境变量：

1. `TMDB_API_KEY` - TMDB API密钥（必需）

在项目根目录创建`.env`文件：

```
TMDB_API_KEY=your_tmdb_api_key_here
```

OMDb API密钥已硬编码在脚本中（`85a51227`）。

## 特殊情况处理

有些电影不易在标准电影数据库中找到，如短片合集、特别展映等。对于这些情况，我们提供了特殊情况处理功能：

1. 在`update_movie_info.py`脚本中，有`update_special_cases()`函数，可以为这些特殊情况添加手动信息
2. 您可以添加新的特殊情况到`special_cases`字典中，格式为：
   ```python
   special_cases = {
       movie_id: {
           "title_cn": "中文标题",
           "overview_en": "英文概述",
           "overview_cn": "中文概述"
       },
       # 更多特殊情况...
   }
   ```

## 中文概述获取

对于缺少中文概述的电影，脚本会使用TMDB API来尝试获取中文概述：

1. 尝试使用多种中文语言代码（zh-CN、zh-TW、zh-HK、zh）从TMDB获取中文概述
2. 如果上述方法未能找到中文概述，会尝试使用TMDB的translations端点查找中文翻译

如果无法通过API自动获取中文概述，您可以考虑通过添加特殊情况来手动设置中文概述。

## 故障排除

如果脚本无法找到某些电影信息，可能的原因：

1. 电影标题特别不常见或格式特殊
2. API限制或服务不可用
3. 特殊字符导致搜索问题

解决方法：

1. 尝试手动在TMDB或OMDb上搜索电影
2. 将特殊情况添加到脚本的`special_cases`字典中
3. 直接在数据库中更新电影信息

## 脚本说明

主要函数：

- `clean_title()` - 清理电影标题以提高搜索准确性
- `search_movie()` - 在TMDB搜索电影
- `get_movie_details()` - 获取电影详细信息
- `get_omdb_info()` - 从OMDb获取电影信息
- `update_movie_info()` - 用TMDB数据更新电影信息
- `update_with_omdb()` - 用OMDb数据更新电影信息
- `update_chinese_overview()` - 更新中文概述
- `update_special_cases()` - 处理特殊情况 