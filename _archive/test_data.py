import sqlite3
import pandas as pd
import baostock as bs
from config import DB_PATH

def check_database():
    print("=== 🔍 测试 1：检查本地数据库里到底存了什么 ===")
    try:
        conn = sqlite3.connect(DB_PATH)
        # 查最近的 10 条 K 线数据，重点看 amount 和 turn 字段
        query = "SELECT date, code, close, volume, amount, turn FROM daily_k_line ORDER BY date DESC LIMIT 10"
        df = pd.read_sql(query, conn)
        print(df)
        conn.close()
    except Exception as e:
        print("数据库查询失败:", e)

def check_baostock():
    print("\n=== 📡 测试 2：直接去 BaoStock 接口抓取原始数据 ===")
    bs.login()
    # 随便查一只常见股票（平安银行）最近几天的数据
    rs = bs.query_history_k_data_plus(
        "sz.000001",
        "date,code,close,volume,amount,turn",
        frequency="d", adjustflag="3"
    )
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
        
    df = pd.DataFrame(data_list, columns=["date", "code", "close", "volume", "amount", "turn"])
    print(df.tail(5)) # 打印最后 5 天的数据
    bs.logout()

if __name__ == "__main__":
    check_database()
    check_baostock()