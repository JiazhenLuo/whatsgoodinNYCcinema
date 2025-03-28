import sqlite3
import json
import os
import re
import logging
from datetime import datetime
from typing import List, Dict, Any

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 获取脚本目录
script_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(script_dir, "cinema.db")

def init_db():
    """初始化数据库，创建必要的表"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 创建电影表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title_en TEXT NOT NULL,
                title_cn TEXT,
                original_title TEXT,
                director TEXT,
                language TEXT,
                year INTEGER,
                duration TEXT,
                detail_url TEXT,
                image_url TEXT,
                overview_en TEXT,
                overview_cn TEXT,
                cinema TEXT NOT NULL,
                trailer_url TEXT,
                has_qa BOOLEAN DEFAULT FALSE,
                qa_details TEXT,
                has_introduction BOOLEAN DEFAULT FALSE,
                introduction_details TEXT
            )
        ''')
        
        # 创建放映信息表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS screenings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER NOT NULL,
                cinema TEXT NOT NULL,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                sold_out BOOLEAN DEFAULT FALSE,
                ticket_url TEXT,
                title_en TEXT,
                FOREIGN KEY (movie_id) REFERENCES movies(id)
            )
        ''')
        
        # 创建更新时间跟踪表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS last_update (
                cinema TEXT PRIMARY KEY,
                update_time TEXT NOT NULL
            )
        ''')
        
        conn.commit()
        print(f"✅ 数据库 {os.path.basename(db_path)} 创建成功！")
    
    except sqlite3.Error as e:
        print(f"❌ 创建数据库时出错: {str(e)}")
    
    finally:
        conn.close()

def import_metrograph_data():
    """将Metrograph爬虫数据导入数据库"""
    # 检查 JSON 文件是否存在
    json_path = os.path.join(script_dir, "database", "metrograph_movies.json")
    if not os.path.exists(json_path):
        print("❌ 找不到 metrograph_movies.json 文件！")
        return
    
    # 连接数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 首先检查movies表中的trailer_url列是否存在，如果不存在则添加
        cursor.execute("PRAGMA table_info(movies)")
        movie_columns = [col[1] for col in cursor.fetchall()]
        
        # 如果trailer_url列不存在，添加它
        if "trailer_url" not in movie_columns:
            print("添加 trailer_url 列到 movies 表...")
            cursor.execute("ALTER TABLE movies ADD COLUMN trailer_url TEXT")
            conn.commit()
        
        # 如果has_qa和has_introduction列不存在，添加它们
        if "has_qa" not in movie_columns:
            print("添加 Q&A 相关列到 movies 表...")
            cursor.execute("ALTER TABLE movies ADD COLUMN has_qa BOOLEAN DEFAULT FALSE")
            cursor.execute("ALTER TABLE movies ADD COLUMN qa_details TEXT")
            conn.commit()
        
        if "has_introduction" not in movie_columns:
            print("添加 introduction 相关列到 movies 表...")
            cursor.execute("ALTER TABLE movies ADD COLUMN has_introduction BOOLEAN DEFAULT FALSE")
            cursor.execute("ALTER TABLE movies ADD COLUMN introduction_details TEXT")
            conn.commit()
        
        # 检查screenings表中的title_en列是否存在
        cursor.execute("PRAGMA table_info(screenings)")
        screening_columns = [col[1] for col in cursor.fetchall()]
        
        has_title_en = "title_en" in screening_columns
        if not has_title_en:
            print("screenings表中没有title_en列，将跳过该字段")
        
        # 读取 JSON 文件
        with open(json_path, "r", encoding="utf-8") as f:
            movies = json.load(f)
        
        # 导入数据
        movies_count = 0
        screenings_count = 0
        
        for movie in movies:
            try:
                # 检查电影是否已存在
                cursor.execute("SELECT id FROM movies WHERE title_en = ? AND cinema = 'Metrograph'", 
                            (movie["title_en"],))
                result = cursor.fetchone()
                
                if result:
                    # 电影已存在，使用现有ID
                    movie_id = result[0]
                    print(f"- 电影 '{movie['title_en']}' 已存在，更新信息")
                    
                    # 更新电影信息
                    cursor.execute("""
                        UPDATE movies SET
                            director = COALESCE(?, director),
                            detail_url = COALESCE(?, detail_url),
                            image_url = COALESCE(?, image_url),
                            year = COALESCE(?, year),
                            overview_en = COALESCE(?, overview_en),
                            trailer_url = COALESCE(?, trailer_url),
                            has_qa = COALESCE(?, has_qa),
                            qa_details = COALESCE(?, qa_details),
                            has_introduction = COALESCE(?, has_introduction),
                            introduction_details = COALESCE(?, introduction_details)
                        WHERE id = ?
                    """, (
                        movie.get("director"),
                        movie.get("detail_url"),
                        movie.get("image_url"),
                        movie.get("year"),
                        movie.get("overview_en"),
                        movie.get("trailer_url"),
                        movie.get("has_qa", False),
                        movie.get("qa_details"),
                        movie.get("has_introduction", False),
                        movie.get("introduction_details"),
                        movie_id
                    ))
                else:
                    # 插入新电影
                    cursor.execute("""
                        INSERT INTO movies (
                            title_en, director, detail_url, image_url, cinema, 
                            year, overview_en, trailer_url, duration,
                            has_qa, qa_details, has_introduction, introduction_details
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        movie["title_en"],
                        movie.get("director"),
                        movie.get("detail_url"),
                        movie.get("image_url"),
                        "Metrograph",
                        movie.get("year"),
                        movie.get("overview_en"),
                        movie.get("trailer_url"),
                        movie.get("duration"),
                        movie.get("has_qa", False),
                        movie.get("qa_details"),
                        movie.get("has_introduction", False),
                        movie.get("introduction_details")
                    ))
                    movie_id = cursor.lastrowid
                    movies_count += 1
                
                # 首先删除该电影在Metrograph的所有旧放映记录
                cursor.execute("""
                    DELETE FROM screenings 
                    WHERE movie_id = ? AND cinema = 'Metrograph'
                """, (movie_id,))
                
                # 插入放映信息到 screenings 表
                if movie.get("show_dates"):
                    for date_info in movie["show_dates"]:
                        date = date_info.get("date")
                        for time_info in date_info.get("times", []):
                            # 根据screenings表的结构选择正确的INSERT语句
                            if has_title_en:
                                cursor.execute("""
                                    INSERT INTO screenings (
                                        movie_id, title_en, cinema, date, time, sold_out, ticket_url
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    movie_id,
                                    movie["title_en"],
                                    "Metrograph",
                                    date,
                                    time_info["time"],
                                    time_info.get("sold_out", False),
                                    time_info.get("ticket_url")
                                ))
                            else:
                                cursor.execute("""
                                    INSERT INTO screenings (
                                        movie_id, cinema, date, time, sold_out, ticket_url
                                    ) VALUES (?, ?, ?, ?, ?, ?)
                                """, (
                                    movie_id,
                                    "Metrograph",
                                    date,
                                    time_info["time"],
                                    time_info.get("sold_out", False),
                                    time_info.get("ticket_url")
                                ))
                            screenings_count += 1
            
            except Exception as e:
                print(f"❌ 处理电影 '{movie.get('title_en', '未知')}' 时出错: {str(e)}")
                continue
        
        # 提交
        conn.commit()
        print(f"✅ 成功导入 {movies_count} 部新电影，{screenings_count} 场放映信息到Metrograph数据库！")
    
    except Exception as e:
        print(f"❌ 导入Metrograph数据时出错: {str(e)}")
        conn.rollback()
    finally:
        # 关闭连接
        conn.close()

def import_filmforum_data():
    # 检查 JSON 文件是否存在
    json_path = os.path.join(script_dir, "database", "filmforum_movies.json")
    if not os.path.exists(json_path):
        print("❌ 找不到 filmforum_movies.json 文件！")
        return
    
    # 连接数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 首先检查movies表中的trailer_url列是否存在，如果不存在则添加
        cursor.execute("PRAGMA table_info(movies)")
        movie_columns = [col[1] for col in cursor.fetchall()]
        
        # 如果trailer_url列不存在，添加它
        if "trailer_url" not in movie_columns:
            print("添加 trailer_url 列到 movies 表...")
            cursor.execute("ALTER TABLE movies ADD COLUMN trailer_url TEXT")
            conn.commit()
        
        # 如果has_qa和has_introduction列不存在，添加它们
        if "has_qa" not in movie_columns:
            print("添加 Q&A 相关列到 movies 表...")
            cursor.execute("ALTER TABLE movies ADD COLUMN has_qa BOOLEAN DEFAULT FALSE")
            cursor.execute("ALTER TABLE movies ADD COLUMN qa_details TEXT")
            conn.commit()
        
        if "has_introduction" not in movie_columns:
            print("添加 introduction 相关列到 movies 表...")
            cursor.execute("ALTER TABLE movies ADD COLUMN has_introduction BOOLEAN DEFAULT FALSE")
            cursor.execute("ALTER TABLE movies ADD COLUMN introduction_details TEXT")
            conn.commit()
        
        # 检查screenings表中的title_en列是否存在
        cursor.execute("PRAGMA table_info(screenings)")
        screening_columns = [col[1] for col in cursor.fetchall()]
        
        has_title_en = "title_en" in screening_columns
        if not has_title_en:
            print("screenings表中没有title_en列，将跳过该字段")
        
        # 读取 JSON 文件
        with open(json_path, "r", encoding="utf-8") as f:
            movies = json.load(f)
        
        # 导入数据前先检查重复，统计当前数据
        print("分析数据库中的Film Forum电影放映数据...")
        cursor.execute("SELECT COUNT(DISTINCT movie_id) FROM screenings WHERE cinema='Film Forum'")
        existing_movie_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM screenings WHERE cinema='Film Forum'")
        existing_screening_count = cursor.fetchone()[0]
        
        print(f"当前数据库中有 {existing_movie_count} 部Film Forum电影，{existing_screening_count} 场放映")
        
        # 清除所有现有的Film Forum放映数据（避免重复）
        print("清除数据库中Film Forum现有放映数据...")
        cursor.execute("DELETE FROM screenings WHERE cinema='Film Forum'")
        conn.commit()
        
        # 导入数据
        movies_count = 0
        screenings_count = 0
        
        for movie in movies:
            try:
                # 检查电影是否已存在
                cursor.execute("SELECT id FROM movies WHERE title_en = ? AND cinema = ?", 
                            (movie["title_en"], movie.get("cinema", "Film Forum")))
                result = cursor.fetchone()
                
                if result:
                    # 电影已存在，使用现有ID，并更新信息
                    movie_id = result[0]
                    cursor.execute("""
                        UPDATE movies SET
                            director = COALESCE(?, director),
                            detail_url = COALESCE(?, detail_url),
                            image_url = COALESCE(?, image_url),
                            year = COALESCE(?, year),
                            overview_en = COALESCE(?, overview_en),
                            trailer_url = COALESCE(?, trailer_url),
                            has_qa = COALESCE(?, has_qa),
                            qa_details = COALESCE(?, qa_details),
                            has_introduction = COALESCE(?, has_introduction),
                            introduction_details = COALESCE(?, introduction_details)
                        WHERE id = ?
                    """, (
                        movie.get("director"),
                        movie.get("detail_url"),
                        movie.get("image_url"),
                        movie.get("year"),
                        movie.get("overview_en"),
                        movie.get("trailer_url"),
                        movie.get("has_qa", False),
                        movie.get("qa_details"),
                        movie.get("has_introduction", False),
                        movie.get("introduction_details"),
                        movie_id
                    ))
                    print(f"- 电影 '{movie['title_en']}' 已存在，更新信息")
                else:
                    # 尝试获取电影时长
                    duration = None
                    if movie.get("duration"):
                        # 电影时长格式可能是 "123 min"，需要提取数字部分
                        duration_match = re.search(r'(\d+)', movie.get("duration", ""))
                        if duration_match:
                            duration = int(duration_match.group(1))
                    
                    # 插入新电影，包含更多详细信息
                    cursor.execute("""
                        INSERT INTO movies (
                            title_en, director, detail_url, image_url, cinema, 
                            year, overview_en, trailer_url, duration,
                            has_qa, qa_details, has_introduction, introduction_details
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        movie["title_en"],
                        movie.get("director"),
                        movie.get("detail_url"),
                        movie.get("image_url"),
                        movie.get("cinema", "Film Forum"),
                        movie.get("year"),
                        movie.get("overview_en"),
                        movie.get("trailer_url"),
                        movie.get("duration"),
                        movie.get("has_qa", False),
                        movie.get("qa_details"),
                        movie.get("has_introduction", False),
                        movie.get("introduction_details")
                    ))
                    movie_id = cursor.lastrowid
                    movies_count += 1
                
                # 处理放映信息
                if movie.get("show_dates"):                    
                    # 插入放映信息到 screenings 表
                    for show_date in movie.get("show_dates", []):
                        date = show_date["date"]
                        for time_info in show_date.get("times", []):
                            # 根据screenings表的结构选择正确的INSERT语句
                            if has_title_en:
                                cursor.execute("""
                                    INSERT INTO screenings (
                                        movie_id, title_en, cinema, date, time, sold_out, ticket_url
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    movie_id,
                                    movie["title_en"],
                                    movie.get("cinema", "Film Forum"),
                                    date,
                                    time_info.get("time"),
                                    time_info.get("sold_out", False),
                                    time_info.get("ticket_url")
                                ))
                            else:
                                cursor.execute("""
                                    INSERT INTO screenings (
                                        movie_id, cinema, date, time, sold_out, ticket_url
                                    ) VALUES (?, ?, ?, ?, ?, ?)
                                """, (
                                    movie_id,
                                    movie.get("cinema", "Film Forum"),
                                    date,
                                    time_info.get("time"),
                                    time_info.get("sold_out", False),
                                    time_info.get("ticket_url")
                                ))
                            screenings_count += 1
            except Exception as e:
                print(f"❌ 处理电影 '{movie.get('title_en', '未知')}' 时出错: {str(e)}")
                continue
        
        # 提交
        conn.commit()
        print(f"✅ 成功导入 {movies_count} 部新电影，{screenings_count} 场放映信息到Film Forum数据库！")
    
    except Exception as e:
        print(f"❌ 导入Film Forum数据时出错: {str(e)}")
        conn.rollback()
    finally:
        # 关闭连接
        conn.close()

def import_ifc_data():
    """将IFC爬虫数据导入数据库"""
    # 检查 JSON 文件是否存在
    json_path = os.path.join(script_dir, "database", "ifc_movies.json")
    if not os.path.exists(json_path):
        print("❌ 找不到 ifc_movies.json 文件！")
        return
    
    # 连接数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 首先检查movies表中的trailer_url列是否存在，如果不存在则添加
        cursor.execute("PRAGMA table_info(movies)")
        movie_columns = [col[1] for col in cursor.fetchall()]
        
        # 如果trailer_url列不存在，添加它
        if "trailer_url" not in movie_columns:
            print("添加 trailer_url 列到 movies 表...")
            cursor.execute("ALTER TABLE movies ADD COLUMN trailer_url TEXT")
            conn.commit()
        
        # 检查screenings表中的title_en列是否存在
        cursor.execute("PRAGMA table_info(screenings)")
        screening_columns = [col[1] for col in cursor.fetchall()]
        
        has_title_en = "title_en" in screening_columns
        if not has_title_en:
            print("screenings表中没有title_en列，将跳过该字段")
        
        # 读取 JSON 文件
        with open(json_path, "r", encoding="utf-8") as f:
            movies = json.load(f)
        
        # 导入数据
        movies_count = 0
        screenings_count = 0
        
        for movie in movies:
            try:
                # 检查电影是否已存在
                cursor.execute("SELECT id FROM movies WHERE title_en = ? AND cinema = 'IFC'", 
                            (movie["title_en"],))
                result = cursor.fetchone()
                
                if result:
                    # 电影已存在，使用现有ID
                    movie_id = result[0]
                    print(f"- 电影 '{movie['title_en']}' 已存在，更新信息")
                    
                    # 更新电影信息，使用COALESCE确保不会用NULL覆盖现有数据
                    cursor.execute("""
                        UPDATE movies SET
                            director = COALESCE(?, director),
                            detail_url = COALESCE(?, detail_url),
                            image_url = COALESCE(?, image_url),
                            year = COALESCE(?, year),
                            overview_en = COALESCE(?, overview_en),
                            trailer_url = COALESCE(?, trailer_url)
                        WHERE id = ?
                    """, (
                        movie.get("director"),
                        movie.get("detail_url"),
                        movie.get("image_url"),
                        movie.get("year"),
                        movie.get("overview_en"),
                        movie.get("trailer_url"),
                        movie_id
                    ))
                else:
                    # 插入新电影
                    cursor.execute("""
                        INSERT INTO movies (
                            title_en, director, detail_url, image_url, cinema, 
                            year, overview_en, trailer_url
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        movie["title_en"],
                        movie.get("director"),
                        movie.get("detail_url"),
                        movie.get("image_url"),
                        "IFC",
                        movie.get("year"),
                        movie.get("overview_en"),
                        movie.get("trailer_url")
                    ))
                    movie_id = cursor.lastrowid
                    movies_count += 1
                
                # 首先删除该电影在IFC的所有旧放映记录
                cursor.execute("""
                    DELETE FROM screenings 
                    WHERE movie_id = ? AND cinema = 'IFC'
                """, (movie_id,))
                
                # 插入放映信息到 screenings 表
                if movie.get("show_dates"):
                    for date_info in movie["show_dates"]:
                        date = date_info.get("date")
                        for time_info in date_info.get("times", []):
                            # 根据screenings表的结构选择正确的INSERT语句
                            if has_title_en:
                                cursor.execute("""
                                    INSERT INTO screenings (
                                        movie_id, title_en, cinema, date, time, sold_out, ticket_url
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    movie_id,
                                    movie["title_en"],
                                    "IFC",
                                    date,
                                    time_info["time"],
                                    time_info.get("sold_out", False),
                                    time_info.get("ticket_url")
                                ))
                            else:
                                cursor.execute("""
                                    INSERT INTO screenings (
                                        movie_id, cinema, date, time, sold_out, ticket_url
                                    ) VALUES (?, ?, ?, ?, ?, ?)
                                """, (
                                    movie_id,
                                    "IFC",
                                    date,
                                    time_info["time"],
                                    time_info.get("sold_out", False),
                                    time_info.get("ticket_url")
                                ))
                            screenings_count += 1
            
            except Exception as e:
                print(f"❌ 处理电影 '{movie.get('title_en', '未知')}' 时出错: {str(e)}")
                continue
        
        # 提交
        conn.commit()
        print(f"✅ 成功导入 {movies_count} 部新电影，{screenings_count} 场放映信息到IFC数据库！")
    
    except Exception as e:
        print(f"❌ 导入IFC数据时出错: {str(e)}")
        conn.rollback()
    finally:
        # 关闭连接
        conn.close()

# 如果是直接运行这个文件，则初始化数据库并导入所有数据
if __name__ == "__main__":
    init_db()
    import_metrograph_data()
    import_filmforum_data()
    # import_ifc_data()  # 暂时注释掉，直到IFC爬虫完成 