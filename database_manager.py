import sqlite3
import pandas as pd
from config import DB_PATH

def get_connection():
    """获取数据库连接"""
    return sqlite3.connect(DB_PATH)

def init_db():
    """初始化表结构"""
    conn = get_connection()
    cursor = conn.cursor()
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
    print("✅ 数据库结构初始化/校验完成。")

def load_recent_kline(months=5):
    """提取最近几个月的 K 线数据用于策略计算"""
    conn = get_connection()
    query = f"""
        SELECT code, date, open, high, low, close, volume 
        FROM daily_k_line 
        WHERE date >= date('now', '-{months} months')
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def update_stock_status(target_codes, status_code, test_high_values=None):
    """批量更新股票状态"""
    if not target_codes:
        return
        
    conn = get_connection()
    cursor = conn.cursor()
    for i, code in enumerate(target_codes):
        high_val = test_high_values[i] if test_high_values else 'NULL'
        cursor.execute(f"""
            UPDATE stock_pipeline 
            SET status = {status_code}, 
                test_high = {high_val}, 
                update_time = date('now')
            WHERE code = '{code}'
        """)
    conn.commit()
    conn.close()