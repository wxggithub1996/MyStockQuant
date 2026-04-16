import sqlite3
from config import DB_PATH

def upgrade_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    print("🚀 正在为 App 端扩容核心表...")
    try:
        cursor.execute("ALTER TABLE stock_pipeline ADD COLUMN latest_price REAL DEFAULT 0.0")
        cursor.execute("ALTER TABLE stock_pipeline ADD COLUMN latest_change REAL DEFAULT 0.0")
        cursor.execute("ALTER TABLE stock_pipeline ADD COLUMN turnover TEXT DEFAULT '--'")
        cursor.execute("ALTER TABLE stock_pipeline ADD COLUMN volume TEXT DEFAULT '--'")
        print("✅ 扩容成功！准备迎接毫秒级响应。")
    except Exception as e:
        print(f"提示: {e} (如果提示 duplicate column，说明已经扩容过了)")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    upgrade_database()