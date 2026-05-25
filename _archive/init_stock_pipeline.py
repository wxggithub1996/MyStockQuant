import sqlite3
import os

# 确保路径正确
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'stock_quant.db')

print("🔄 正在将全市场股票清单同步至策略状态机...")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 核心神仙 SQL：直接从 basic 表把代码和名字抄过来，状态默认 0
cursor.execute('''
    INSERT OR IGNORE INTO stock_pipeline (code, name, status, entry_date, update_time)
    SELECT code, name, 0, date('now'), date('now') FROM stock_basic
''')

rows_affected = cursor.rowcount
conn.commit()
conn.close()

print(f"✅ 同步大功告成！成功为状态机注入 {rows_affected} 只初始股票。")