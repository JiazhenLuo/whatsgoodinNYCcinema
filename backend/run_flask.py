#!/usr/bin/env python
"""
Flask API启动脚本
"""
import os
import sys

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from backend.app.api.server import create_app, run_server

if __name__ == '__main__':
    # 默认端口
    port = 5556
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print(f"Invalid port: {sys.argv[1]}, using default port {port}")
    
    app = create_app()
    print(f"Starting Flask server on port {port}...")
    print(f"健康检查: http://localhost:{port}/api/v1/health")
    print(f"电影API: http://localhost:{port}/api/v1/movies")
    run_server(app, host='0.0.0.0', port=port, debug=True) 