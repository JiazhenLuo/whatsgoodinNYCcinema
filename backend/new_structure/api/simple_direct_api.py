#!/usr/bin/env python
"""
超简单的API服务器，仅使用内置模块直接从数据库读取电影数据并以正确的UTF-8 JSON格式返回
"""
import sqlite3
import json
import os
import sys
import logging
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
import re
from urllib.parse import urlparse, parse_qs

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('chinese_api')

# 数据库路径
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "movies.db")

def get_movie_by_id(movie_id):
    """从数据库中获取电影数据"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM movies WHERE id = ?", (movie_id,))
        movie = cursor.fetchone()
        conn.close()
        
        if movie:
            return {key: movie[key] for key in movie.keys()}
        return None
    except Exception as e:
        logger.error(f"获取电影ID {movie_id} 时出错: {e}")
        return None

def get_all_movies(limit=10, offset=0):
    """获取所有电影的列表"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM movies LIMIT ? OFFSET ?", (limit, offset))
        movies = cursor.fetchall()
        conn.close()
        
        return [{key: movie[key] for key in movie.keys()} for movie in movies]
    except Exception as e:
        logger.error(f"获取电影列表时出错: {e}")
        return []

def get_movies_count():
    """获取电影总数"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM movies")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        logger.error(f"获取电影总数时出错: {e}")
        return 0

class ChineseHTTPHandler(BaseHTTPRequestHandler):
    """处理HTTP请求并返回正确编码的中文JSON"""
    
    # 禁用HTTP请求日志，避免日志过多
    def log_message(self, format, *args):
        if args and args[1] != 200:  # 只记录非200状态码
            logger.info(f"{self.address_string()} - {format % args}")
    
    def _set_headers(self, content_type="application/json; charset=utf-8"):
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
    
    def _send_json_response(self, data, status=200):
        """发送JSON响应，确保中文字符正确显示"""
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        
        # 关键点：使用ensure_ascii=False确保中文字符直接显示而不是Unicode转义
        response = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.wfile.write(response)
    
    def do_HEAD(self):
        """处理HEAD请求"""
        self._set_headers()
    
    def do_OPTIONS(self):
        """处理OPTIONS请求，支持CORS预检"""
        self._set_headers()
        self.wfile.write(b'')
    
    def do_GET(self):
        """处理GET请求"""
        start_time = time.time()
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        
        # 健康检查端点
        if path == "/api/v1/health":
            return self._send_json_response({"status": "ok"})
        
        # 获取所有电影
        elif path == "/api/v1/movies":
            query = parse_qs(parsed_url.query)
            try:
                limit = min(100, int(query.get("limit", [10])[0]))
                offset = max(0, int(query.get("offset", [0])[0]))
            except (ValueError, IndexError):
                limit, offset = 10, 0
                
            movies = get_all_movies(limit, offset)
            total = get_movies_count()
            
            response_data = {
                "data": movies,
                "meta": {
                    "limit": limit,
                    "offset": offset,
                    "total": total
                }
            }
            
            logger.info(f"获取电影列表：limit={limit}, offset={offset}, count={len(movies)}, 耗时={time.time()-start_time:.3f}秒")
            return self._send_json_response(response_data)
        
        # 获取特定ID的电影
        elif re.match(r"^/api/v1/movies/\d+$", path):
            try:
                movie_id = int(path.split("/")[-1])
                movie = get_movie_by_id(movie_id)
                
                if movie:
                    logger.info(f"获取电影ID {movie_id} 成功，耗时={time.time()-start_time:.3f}秒")
                    return self._send_json_response({"data": movie})
                else:
                    logger.warning(f"电影ID {movie_id} 未找到")
                    return self._send_json_response({"error": "Movie not found"}, 404)
            except ValueError:
                return self._send_json_response({"error": "Invalid movie ID"}, 400)
        
        # 未找到端点
        else:
            logger.warning(f"请求了未知路径: {path}")
            return self._send_json_response({"error": "Not found", "path": path}, 404)

def run(port=8000):
    """运行服务器"""
    server_address = ("", port)
    httpd = HTTPServer(server_address, ChineseHTTPHandler)
    logger.info(f"启动API服务器在端口 {port}...")
    logger.info("所有中文内容将直接以UTF-8返回，不使用Unicode转义")
    logger.info("已启用CORS，允许所有来源访问")
    logger.info(f"访问健康检查端点: http://localhost:{port}/api/v1/health")
    logger.info(f"访问电影详情示例: http://localhost:{port}/api/v1/movies/1")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("\n关闭服务器...")
        httpd.server_close()

if __name__ == "__main__":
    port = 8000
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print(f"无效端口号: {sys.argv[1]}，使用默认端口 8000")
    
    run(port) 