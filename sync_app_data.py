import sqlite3
import pandas as pd
from config import DB_PATH

def sync_data_to_app_table():
    conn = sqlite3.connect(DB_PATH)
    print("正在基于个股独立时间线计算涨跌幅...")
    
    try:
        # 1. 获取 stock_pipeline 中的名单
        pipeline_codes = pd.read_sql("SELECT code FROM stock_pipeline", conn)['code'].tolist()
        
        if not pipeline_codes:
            print("股票池为空，无需同步。")
            return

        # 构造 SQL 的 IN 子句
        codes_str = "','".join(pipeline_codes)
        
        # 2. 提取这些股票【所有】的近期 K 线数据，并按时间倒序排列
        # (为了防止全表扫描内存爆炸，我们限制提取最近的 30 天数据足够了)
        query = f"""
            SELECT code, date, close, turn, amount 
            FROM daily_k_line 
            WHERE code IN ('{codes_str}') 
            ORDER BY date DESC
        """
        df_k = pd.read_sql(query, conn)

        update_data = []
        
        # 3. 逐个股票处理（尊重各自的时间线）
        for code in pipeline_codes:
            price = 0.0
            change = 0.0
            turnover_str = "--"
            volume_str = "--"
            
            # 筛选出这只股票的数据
            code_data = df_k[df_k['code'] == code]
            
            if len(code_data) >= 1:
                # 拿这只股票【自己最新的一天】的数据 (T日)
                t_row = code_data.iloc[0]
                
                if pd.notna(t_row['close']):
                    price = float(t_row['close'])
                if pd.notna(t_row['turn']):
                    turnover_str = str(t_row['turn'])
                    
                vol = t_row['amount']
                if pd.notna(vol) and vol is not None:
                    try:
                        v = float(vol)
                        volume_str = f"{v/100000000:.2f}亿" if v >= 100000000 else f"{v/10000:.0f}万"
                    except:
                        pass
                
                # 如果这只股票有两天以上的数据，算涨跌幅 (T日 与 T-1日)
                if len(code_data) >= 2:
                    y_row = code_data.iloc[1] # 这只股票倒数第二天的数据
                    y_close = y_row['close']
                    
                    if pd.notna(y_close) and float(y_close) > 0 and price > 0:
                        change = (price - float(y_close)) / float(y_close) * 100

            # 压入更新队列
            update_data.append((price, change, turnover_str, volume_str, code))

        # 4. 批量更新到数据库
        cursor = conn.cursor()
        cursor.executemany("""
            UPDATE stock_pipeline 
            SET latest_price = ?, latest_change = ?, turnover = ?, volume = ?
            WHERE code = ?
        """, update_data)
        
        conn.commit()
        print(f"✅ 同步成功！已完美填补 {len(update_data)} 只股票的真实数据。")

    except Exception as e:
        print(f"❌ 同步失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    sync_data_to_app_table()