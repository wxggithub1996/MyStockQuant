import pandas as pd
import sqlite3
import datetime
import os
from config import DB_PATH

# 内部记录日志函数
def log_action(cursor, code, name, source, detail):
    now = datetime.datetime.now()
    cursor.execute("INSERT INTO operation_log (date, time, code, name, source, detail) VALUES (?, ?, ?, ?, ?, ?)",
                   (now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), code, name, source, detail))

def run_strategy_engine(source="自动处理"):
    print(f"⚙️ 状态机引擎启动 (触发来源: {source})...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    pipeline_df = pd.read_sql("SELECT code, name, status, test_high, entry_date FROM stock_pipeline WHERE status IN (1, 2, 3)", conn)
    
    for _, row in pipeline_df.iterrows():
        code = row['code']
        df_k = pd.read_sql(f"SELECT date, open, close, high, low, volume FROM daily_k_line WHERE code = '{code}' ORDER BY date ASC", conn)
        if df_k.empty: continue
        
        df_k['ma10'] = df_k['close'].rolling(window=10).mean()
        today = df_k.iloc[-1]
        
        test_day_df = df_k[df_k['date'] == row['entry_date']]
        if test_day_df.empty: continue
        test_day = test_day_df.iloc[0]
        
        benchmark = row['test_high']
        support = test_day['low']
        test_vol = test_day['volume']
        
        # --- 突破池 (3) 离场逻辑 ---
        if row['status'] == 3:
            ma10_price = today['ma10']
            if today['close'] < benchmark * 0.97:
                cursor.execute(f"UPDATE stock_pipeline SET status = 99, update_time = date('now') WHERE code = '{code}'")
                log_action(cursor, code, row['name'], source, "假突破跌穿防线 ➔ 移入废弃")
                continue
            if pd.notna(ma10_price) and today['close'] < ma10_price:
                cursor.execute(f"UPDATE stock_pipeline SET status = 99, update_time = date('now') WHERE code = '{code}'")
                log_action(cursor, code, row['name'], source, "跌破10日线波段结束 ➔ 移入废弃")
                continue
            continue

        # --- 试盘/回踩池 (1, 2) 流转逻辑 ---
        if today['close'] < support:
            cursor.execute(f"UPDATE stock_pipeline SET status = 99, update_time = date('now') WHERE code = '{code}'")
            log_action(cursor, code, row['name'], source, "跌破主力支撑位 ➔ 移入废弃")
            continue
            
        days_in_pool = len(df_k[df_k['date'] > row['entry_date']])
        if days_in_pool > 25:
            cursor.execute(f"UPDATE stock_pipeline SET status = 99, update_time = date('now') WHERE code = '{code}'")
            log_action(cursor, code, row['name'], source, "潜伏超时(>25天) ➔ 移入废弃")
            continue

        if today['close'] > benchmark:
            cursor.execute(f"UPDATE stock_pipeline SET status = 3, update_time = date('now') WHERE code = '{code}'")
            log_action(cursor, code, row['name'], source, "放量突破起爆 ➔ 晋级突破池")
            continue

        if row['status'] == 1 and today['volume'] <= test_vol * 0.5:
            cursor.execute(f"UPDATE stock_pipeline SET status = 2, update_time = date('now') WHERE code = '{code}'")
            log_action(cursor, code, row['name'], source, "极致缩量洗盘 ➔ 晋级回踩池")

    conn.commit()

    # --- 增量扫盘逻辑 ---
    all_basic = pd.read_sql("SELECT code, name FROM stock_basic", conn)
    existing_codes = set(pd.read_sql("SELECT code FROM stock_pipeline WHERE status != 0", conn)['code'])
    
    for _, row in all_basic.iterrows():
        code = row['code']
        if code in existing_codes: continue
        
        df = pd.read_sql(f"SELECT date, open, close, high, low, volume FROM daily_k_line WHERE code = '{code}' ORDER BY date ASC", conn)
        if len(df) < 40: continue
        
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()
        today_idx = df.index[-1]
        today = df.loc[today_idx]
        
        is_test = (today['volume'] >= df['vol_ma20'].shift(1).loc[today_idx] * 2.5) and \
                  (today['close'] > today['open']) and \
                  ((today['close'] - today['open']) / today['open'] > 0.02)
        
        if is_test:
            pre_df = df.loc[today_idx-20 : today_idx-1]
            if (pre_df['high'].max() / pre_df['low'].min()) < 1.25:
                upper_shadow = today['high'] - today['close']
                body = today['close'] - today['open']
                benchmark = today['high'] if upper_shadow > body else today['close']
                
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_pipeline (code, name, status, test_high, entry_date, update_time)
                    VALUES (?, ?, 1, ?, ?, date('now'))
                ''', (code, row['name'], benchmark, today['date']))
                log_action(cursor, code, row['name'], source, "发现全新倍量异动 ➔ 纳入试盘池")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    run_strategy_engine(source="直接运行")