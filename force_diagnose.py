import sqlite3
import pandas as pd
from update_daily import update_daily_k_lines
from sync_app_data import sync_data_to_app_table
from config import DB_PATH

def diagnose():
    print("========================================")
    print("🛠️ 第 1 步：强制触发底层的 K 线拉取...")
    print("========================================")
    try:
        update_daily_k_lines()
    except Exception as e:
        print(f"❌ 拉取失败: {e}")

    print("\n========================================")
    print("🛠️ 第 2 步：强制触发 App 数据清洗装配...")
    print("========================================")
    try:
        sync_data_to_app_table()
    except Exception as e:
        print(f"❌ 装配失败: {e}")

    print("\n========================================")
    print("🩺 第 3 步：抽查数据库最终状态 (体检报告)")
    print("========================================")
    conn = sqlite3.connect(DB_PATH)
    
    # 抽查底表
    print("\n[底表 daily_k_line] 最新的一条记录：")
    df_k = pd.read_sql("SELECT date, code, close, amount, turn FROM daily_k_line ORDER BY date DESC LIMIT 1", conn)
    print(df_k)
    
    # 抽查 App 展现表
    print("\n[展现表 stock_pipeline] 试盘池的前 5 只股票：")
    df_p = pd.read_sql("SELECT code, name, latest_price, volume FROM stock_pipeline WHERE status = 1 LIMIT 5", conn)
    print(df_p)
    
    conn.close()

if __name__ == "__main__":
    diagnose()