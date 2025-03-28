#!/usr/bin/env python
"""
脚本用于清理数据库中的重复放映记录
"""
import os
import sys
import sqlite3
import time
from pathlib import Path

# 添加父目录到系统路径
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from app.config.settings import DB_PATH

def get_db_connection(max_attempts=5, wait_time=1):
    """
    获取数据库连接，如果数据库被锁定则重试
    
    Args:
        max_attempts: 最大重试次数
        wait_time: 重试等待时间（秒）
        
    Returns:
        sqlite3.Connection 对象或 None
    """
    attempt = 0
    while attempt < max_attempts:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20)
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                attempt += 1
                if attempt < max_attempts:
                    print(f"数据库被锁定，等待 {wait_time} 秒后重试 ({attempt}/{max_attempts})...")
                    time.sleep(wait_time)
                    wait_time *= 2  # 指数退避
                else:
                    print("达到最大重试次数，无法连接数据库")
                    return None
            else:
                print(f"连接数据库时出错: {str(e)}")
                return None
    return None

def clean_duplicate_screenings():
    """
    清理数据库中的重复放映记录
    """
    print(f"使用数据库: {DB_PATH}")
    
    if not os.path.exists(DB_PATH):
        print(f"❌ 数据库文件不存在: {DB_PATH}")
        return False
    
    try:
        # 连接数据库
        conn = get_db_connection()
        if not conn:
            print("❌ 无法连接数据库")
            return False
            
        cursor = conn.cursor()
        
        # 1. 查找重复的放映记录
        cursor.execute("""
            SELECT movie_id, date, time, COUNT(*) as count
            FROM screenings
            GROUP BY movie_id, date, time
            HAVING count > 1
            ORDER BY count DESC
        """)
        
        duplicates = cursor.fetchall()
        total_duplicates = len(duplicates)
        print(f"找到 {total_duplicates} 组重复的放映记录")
        
        if total_duplicates == 0:
            print("✅ 没有发现重复放映记录，数据库已是干净状态")
        else:
            # 2. 为每组重复记录删除除了ID最小的记录之外的所有记录
            total_removed = 0
            
            for dup in duplicates:
                movie_id = dup['movie_id']
                date = dup['date']
                time = dup['time']
                count = dup['count']
                
                # 查询这组重复记录的所有ID
                cursor.execute("""
                    SELECT id, cinema, title_en
                    FROM screenings
                    WHERE movie_id = ? AND date = ? AND time = ?
                    ORDER BY id
                """, (movie_id, date, time))
                
                records = cursor.fetchall()
                keep_id = records[0]['id']  # 保留ID最小的记录
                cinema = records[0]['cinema']
                title = records[0]['title_en']
                
                # 删除其他重复记录
                delete_ids = [r['id'] for r in records[1:]]
                cursor.execute(f"""
                    DELETE FROM screenings
                    WHERE id IN ({','.join(['?'] * len(delete_ids))})
                """, delete_ids)
                
                removed = len(delete_ids)
                total_removed += removed
                print(f"- 电影 '{title}' 在 {cinema} 的 {date} {time} 放映场次: 保留ID={keep_id}, 删除了 {removed} 条重复记录")
            
            # 提交更改
            conn.commit()
            
            # 3. 再次检查确保没有重复记录
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM (
                    SELECT movie_id, date, time, COUNT(*) as count
                    FROM screenings
                    GROUP BY movie_id, date, time
                    HAVING count > 1
                )
            """)
            
            remaining_duplicates = cursor.fetchone()['count']
            
            if remaining_duplicates > 0:
                print(f"⚠️ 清理后仍有 {remaining_duplicates} 组重复记录")
            else:
                print(f"✅ 成功清理所有重复记录")
            
            print(f"总结: 清理了 {total_removed} 条重复放映记录")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 清理数据库时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def clean_showtimes_coming_soon():
    """
    清理数据库中"Showtimes coming soon"的电影和放映记录
    """
    print(f"使用数据库: {DB_PATH}")
    
    if not os.path.exists(DB_PATH):
        print(f"❌ 数据库文件不存在: {DB_PATH}")
        return False
    
    try:
        # 连接数据库
        conn = get_db_connection()
        if not conn:
            print("❌ 无法连接数据库")
            return False
            
        cursor = conn.cursor()
        
        # 1. 查找"Showtimes coming soon"的电影
        cursor.execute("""
            SELECT id, title_en
            FROM movies
            WHERE title_en LIKE '%Showtimes coming soon%'
        """)
        
        movies = cursor.fetchall()
        
        if not movies:
            print("没有找到标题为'Showtimes coming soon'的电影")
            conn.close()
            return True
        
        # 2. 删除找到的电影及其放映记录
        for movie in movies:
            movie_id = movie['id']
            title = movie['title_en']
            
            # 首先删除该电影的所有放映记录
            cursor.execute("DELETE FROM screenings WHERE movie_id = ?", (movie_id,))
            deleted_screenings = cursor.rowcount
            
            # 然后删除电影本身
            cursor.execute("DELETE FROM movies WHERE id = ?", (movie_id,))
            
            print(f"- 已删除电影 '{title}' (ID: {movie_id}) 及其 {deleted_screenings} 条放映记录")
        
        # 提交更改
        conn.commit()
        print(f"✅ 成功清理 {len(movies)} 部'Showtimes coming soon'电影")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 清理'Showtimes coming soon'时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主函数"""
    print("开始清理数据库...")
    
    # 先删除"Showtimes coming soon"的记录
    clean_showtimes_coming_soon()
    
    # 再清理重复的放映记录
    clean_duplicate_screenings()
    
    print("数据库清理完成!")

if __name__ == "__main__":
    main() 