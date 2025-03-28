#!/usr/bin/env python
"""
API服务入口点，提供了两种启动API服务器的方式:
1. 使用原始的Flask API（可能有中文Unicode转义问题）
2. 使用直接返回UTF-8中文的简易API（推荐）
"""
import os
import sys
import argparse
import subprocess

def start_flask_api(port):
    """启动原始的Flask API"""
    try:
        print(f"正在使用Flask启动API服务器在端口 {port}...")
        print("注意: 已修复中文显示问题，应该能正确显示中文")
        
        # 设置环境变量
        os.environ["FLASK_APP"] = "backend.app.api.server"
        os.environ["FLASK_DEBUG"] = "1"
        
        # 在Python路径中添加项目根目录
        current_path = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_path)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        
        # 导入Flask应用
        from backend.app.api.server import create_app
        from backend.app.__main__ import run_server
        
        # 创建并运行Flask应用
        app = create_app()
        run_server(app, host='0.0.0.0', port=port, debug=True)
        
    except ImportError as e:
        print(f"无法启动Flask API: {e}")
        print("请确保已安装所需的依赖项")
        sys.exit(1)
    except Exception as e:
        print(f"启动Flask API时出错: {e}")
        sys.exit(1)

def start_direct_api(port):
    """启动直接返回UTF-8中文的简易API"""
    api_script = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 
        "simple_direct_api.py"
    )
    
    if not os.path.exists(api_script):
        print(f"找不到API脚本: {api_script}")
        sys.exit(1)
    
    print(f"正在启动简易API服务器在端口 {port}...")
    try:
        subprocess.run([sys.executable, api_script, str(port)])
    except KeyboardInterrupt:
        print("API服务器已停止")
    except Exception as e:
        print(f"启动API服务器时出错: {e}")
        sys.exit(1)

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="WhatsgoodinNYCcinema API服务器")
    parser.add_argument("--port", type=int, default=8000, help="服务器端口号")
    parser.add_argument(
        "--mode", 
        choices=["flask", "direct"], 
        default="direct",
        help="API模式: flask (原始Flask API) 或 direct (UTF-8中文API, 默认)"
    )
    
    args = parser.parse_args()
    
    if args.mode == "flask":
        start_flask_api(args.port)
    else:
        start_direct_api(args.port)

if __name__ == "__main__":
    main() 