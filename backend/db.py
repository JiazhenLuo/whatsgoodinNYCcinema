import sqlite3
import json
import os
import re
script_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(script_dir, "movies.db")

def create_database():
    conn = sqlite3.connect(db_path)  # 连接数据库
    cursor = conn.cursor()

    # 创建 movies 表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_en TEXT NOT NULL,
            title_cn TEXT,
            show_date TEXT,
            show_time TEXT,
            sold_out BOOLEAN DEFAULT FALSE,
            ticket_url TEXT,
            detail_url TEXT,
            image_url TEXT,
            director TEXT,
            year INTEGER,
            cinema TEXT,
            imdb_id TEXT,
            tmdb_id TEXT,
            overview_en TEXT,
            overview_cn TEXT,
            rating REAL,
            douban_url TEXT,
            trailer_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 创建 screenings 表 - 存储多个放映时间
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS screenings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_en TEXT NOT NULL,
            movie_id INTEGER,
            cinema TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            sold_out BOOLEAN DEFAULT FALSE,
            ticket_url TEXT,
            FOREIGN KEY (movie_id) REFERENCES movies (id)
        )
    """)

    conn.commit()
    conn.close()
    print("✅ 数据库 movies.db 创建成功！")

def import_metrograph_data():
    # 检查 JSON 文件是否存在
    json_path = os.path.join(script_dir, "database", "metrograph_movies.json")
    if not os.path.exists(json_path):
        print("❌ 找不到 metrograph_movies.json 文件！")
        return
    
    # 连接数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
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
                cursor.execute("SELECT id FROM movies WHERE title_en = ? AND cinema = ?", 
                            (movie["title_en"], movie.get("cinema", "Metrograph")))
                result = cursor.fetchone()
                
                if result:
                    # 电影已存在，使用现有ID
                    movie_id = result[0]
                    # 更新电影信息
                    cursor.execute("""
                        UPDATE movies SET
                            director = COALESCE(?, director),
                            detail_url = COALESCE(?, detail_url),
                            image_url = COALESCE(?, image_url),
                            year = COALESCE(?, year)
                        WHERE id = ?
                    """, (
                        movie.get("director"),
                        movie.get("detail_url"),
                        movie.get("image_url"),
                        movie.get("year"),
                        movie_id
                    ))
                    print(f"- 电影 '{movie['title_en']}' 已存在，更新信息")
                else:
                    # 插入新电影
                    cursor.execute("""
                        INSERT INTO movies (
                            title_en, director, detail_url, image_url, cinema, year
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        movie["title_en"],
                        movie.get("director"),
                        movie.get("detail_url"),
                        movie.get("image_url"),
                        movie.get("cinema", "Metrograph"),
                        movie.get("year")
                    ))
                    movie_id = cursor.lastrowid
                    movies_count += 1
                
                # 插入放映信息到 screenings 表
                for show_date in movie.get("show_dates", []):
                    date = show_date["date"]
                    for time_info in show_date.get("times", []):
                        # 检查放映信息是否已存在
                        cursor.execute("""
                            SELECT id FROM screenings 
                            WHERE movie_id = ? AND date = ? AND time = ?
                        """, (movie_id, date, time_info.get("time")))
                        
                        if not cursor.fetchone():
                            # 根据screenings表的结构选择正确的INSERT语句
                            if has_title_en:
                                cursor.execute("""
                                    INSERT INTO screenings (
                                        movie_id, title_en, cinema, date, time, sold_out, ticket_url
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    movie_id,
                                    movie["title_en"],
                                    movie.get("cinema", "Metrograph"),
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
                                    movie.get("cinema", "Metrograph"),
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
                            year, overview_en, trailer_url
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        movie["title_en"],
                        movie.get("director"),
                        movie.get("detail_url"),
                        movie.get("image_url"),
                        movie.get("cinema", "Film Forum"),
                        movie.get("year"),
                        movie.get("overview_en"),
                        movie.get("trailer_url")
                    ))
                    movie_id = cursor.lastrowid
                    movies_count += 1
                
                # 插入放映信息到 screenings 表
                for show_date in movie.get("show_dates", []):
                    date = show_date["date"]
                    for time_info in show_date.get("times", []):
                        # 检查放映信息是否已存在
                        cursor.execute("""
                            SELECT id FROM screenings 
                            WHERE movie_id = ? AND date = ? AND time = ?
                        """, (movie_id, date, time_info.get("time")))
                        
                        if not cursor.fetchone():
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

# 运行，创建数据库
create_database()

# 导入 Metrograph 电影数据
import_metrograph_data()

# 导入 Film Forum 电影数据
import_filmforum_data() 