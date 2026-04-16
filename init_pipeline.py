import sqlite3
import os

# 自动获取当前目录下的数据库路径，避免路径报错
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'stock_quant.db')

def build_pipeline_table():
    print("🔧 正在初始化策略状态机表 (stock_pipeline)...")
    
    # 连接数据库
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 创建 stock_pipeline 表
    # 如果表不存在则创建，如果存在则跳过，非常安全
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_pipeline (
            code TEXT PRIMARY KEY,
            name TEXT,
            status INTEGER DEFAULT 0,
            test_high REAL,
            entry_date TEXT,
            update_time TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    
    print("✅ 初始化完成！stock_pipeline 表已准备就绪。")
    print("💡 现在你可以再次运行 streamlit run app.py 来启动你的 Web 总控台了！")

if __name__ == "__main__":
    build_pipeline_table()