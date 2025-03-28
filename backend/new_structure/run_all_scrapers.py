import os
import sys
import time
import subprocess
import logging
from datetime import datetime

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/run_scrapers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger("scraper_runner")

def run_scraper(script_name):
    """运行指定的爬虫脚本"""
    logger.info(f"开始运行爬虫: {script_name}")
    start_time = time.time()
    
    try:
        # 使用subprocess运行Python脚本
        result = subprocess.run(
            ["python", f"backend/scrapers/{script_name}.py"],
            capture_output=True,
            text=True,
            check=True
        )
        
        # 记录输出
        logger.info(f"{script_name} 输出:")
        for line in result.stdout.splitlines():
            logger.info(f"  {line}")
        
        if result.stderr:
            logger.warning(f"{script_name} 错误输出:")
            for line in result.stderr.splitlines():
                logger.warning(f"  {line}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"✅ {script_name} 爬虫运行完成，耗时 {elapsed_time:.2f} 秒")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {script_name} 爬虫运行失败: {str(e)}")
        logger.error(f"错误输出: {e.stderr}")
        return False
    
    except Exception as e:
        logger.error(f"❌ 运行 {script_name} 时发生未知错误: {str(e)}")
        return False

def import_data_to_db():
    """导入所有数据到数据库"""
    logger.info("开始导入数据到数据库...")
    
    try:
        # 导入数据库模块
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        import db
        
        # 初始化数据库
        db.init_db()
        
        # 导入各个影院数据
        logger.info("导入 Metrograph 数据...")
        db.import_metrograph_data()
        
        logger.info("导入 Film Forum 数据...")
        db.import_filmforum_data()
        
        logger.info("导入 IFC 数据...")
        db.import_ifc_data()
        
        logger.info("✅ 所有数据导入完成")
        return True
    
    except Exception as e:
        logger.error(f"❌ 导入数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    """主函数"""
    logger.info("=" * 50)
    logger.info("开始运行所有爬虫和导入数据")
    logger.info("=" * 50)
    
    # 确保日志目录存在
    os.makedirs("logs", exist_ok=True)
    
    # 运行各个爬虫
    scrapers = ["metrograph", "filmforum", "ifc"]
    success_count = 0
    
    for scraper in scrapers:
        if run_scraper(scraper):
            success_count += 1
    
    logger.info(f"爬虫运行完成: {success_count}/{len(scrapers)} 个爬虫成功")
    
    # 如果至少有一个爬虫成功，则导入数据
    if success_count > 0:
        import_result = import_data_to_db()
        if import_result:
            logger.info("🎉 整个过程完成，爬虫数据已成功导入到数据库")
        else:
            logger.error("⚠️ 爬虫完成，但数据导入过程失败")
    else:
        logger.error("❌ 所有爬虫均失败，不进行数据导入")
    
    logger.info("=" * 50)

if __name__ == "__main__":
    main() 