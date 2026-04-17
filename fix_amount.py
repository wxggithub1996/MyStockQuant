import sqlite3
from config import DB_PATH

def clean_dirty_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 删除所有 amount 为空或 None 的残缺 K 线数据
    cursor.execute("DELETE FROM daily_k_line WHERE amount IS NULL OR amount = ''")
    
    deleted_rows = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"✅ 成功清理了 {deleted_rows} 条缺少成交额的残缺历史数据！")
    print("👉 现在去 App 里点击一下【🔄 增量同步】按钮，系统会自动重新拉取这些天的数据并填补空白。")

if __name__ == "__main__":
    clean_dirty_data()