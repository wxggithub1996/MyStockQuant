import akshare as ak
import baostock as bs
import pandas as pd
import sqlite3
import time
from datetime import datetime

db_name = 'stock_quant.db'
start_date = "2023-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# ==========================================
# 【危险动作警告】
# 既然要加字段，我们直接把旧的 K 线表干掉，但保留 pipeline 和 basic 表
# ==========================================
print("🗑️ 正在清空旧版 K 线表...")
cursor.execute('DROP TABLE IF EXISTS daily_k_line')
conn.commit()

print("1. 正在获取全市场 A 股代码列表...")
df_all_stocks = ak.stock_info_a_code_name()
total_stocks = len(df_all_stocks)

print("2. 登陆 BaoStock 系统，开始携带『换手率』和『涨跌幅』全新拉取...")
lg = bs.login()

if lg.error_code == '0':
    success_count = 0
    empty_count = 0
    error_count = 0

    for index, row in df_all_stocks.iterrows():
        code = row['code']
        name = row['name']
        
        # 只拉主板和创业板，顺手把北交所 (8和9开头) 的垃圾股过滤掉，提升效率！
        if code.startswith(('8', '9')):
            continue
            
        bs_code = f"sh.{code}" if code.startswith('6') else f"sz.{code}"
        
        try:
            # 【核心修改点】：请求字段加入了 turn(换手率) 和 pctChg(涨跌幅)
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,volume,amount,turn,pctChg",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2"
            )
            
            if rs.error_code == '0':
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())
                
                if data_list:
                    df_hist = pd.DataFrame(data_list, columns=rs.fields)
                    
                    # 转换数据类型（极其重要，否则数据库里存的是字符串）
                    # 把 turn 和 pctChg 如果是空字符串，替换成 0
                    cols_to_convert = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctChg']
                    for col in cols_to_convert:
                        df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce').fillna(0)
                        
                    df_hist['code'] = code 
                    
                    df_hist.to_sql(name='daily_k_line', con=conn, if_exists='append', index=False)
                    
                    success_count += 1
                else:
                    empty_count += 1
            else:
                error_count += 1
                
            current_processed = success_count + empty_count + error_count
            if current_processed % 50 == 0:
                print(f"🚀 进度: {current_processed}只 - 最新入库: {name}")

        except Exception as e:
            error_count += 1

    bs.logout()
    
    # 建立索引：这会让你以后查询某只股票的时间大大缩短
    print("⚡ 正在为日期和代码建立底层加速索引...")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_kline_code_date ON daily_k_line(code, date)')
    conn.commit()
    
    print("\n🎉 全新底层架构数据拉取完毕！包含了换手率与涨跌幅！")
    print(f"📊 成功入库: {success_count} 只")
else:
    print(f"❌ BaoStock 登陆失败: {lg.error_msg}")

conn.close()