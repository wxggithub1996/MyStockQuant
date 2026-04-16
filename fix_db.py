import sqlite3
from config import DB_PATH  # 确保这里引入了你的数据库路径

def rebuild_log_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("🧹 正在清理旧的日志表残骸...")
    # 1. 无情删掉旧表
    cursor.execute("DROP TABLE IF EXISTS operation_log")
    
    print("🏗️ 正在按照全新架构重建日志表...")
    # 2. 重新建立拥有完整字段的新表
    cursor.execute("""
        CREATE TABLE operation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            log_date TEXT,
            log_time TEXT,
            source TEXT,
            code TEXT,
            name TEXT,
            detail TEXT
        )
    """)
    
    conn.commit()
    conn.close()
    print("✅ 完美！日志表重建成功，你可以重启 server.py 了！")

if __name__ == "__main__":
    rebuild_log_table()