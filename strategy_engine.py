import pandas as pd
import sqlite3
import datetime
import os
from config import DB_PATH

# 内部记录日志函数
def log_action(cursor, code, name, source, detail):
    now = datetime.datetime.now()
    cursor.execute("INSERT INTO operation_log (log_date, log_time, code, name, source, detail) VALUES (?, ?, ?, ?, ?, ?)",
                   (now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), code, name, source, detail))

def run_strategy_engine(source="自动处理"):
    print(f"⚙️ 状态机引擎启动 (触发来源: {source})...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        pipeline_df = pd.read_sql("SELECT code, name, status, test_high, entry_date FROM stock_pipeline WHERE status IN (1, 2, 3)", conn)
    except Exception as e:
        print(f"❌ 读取 pipeline 表失败: {e}")
        return

    # 定义需要强制转换为数字的列
    num_cols = ['open', 'close', 'high', 'low', 'volume']

    # --- 逻辑 A: 现有池子标的状态流转 ---
    for _, row in pipeline_df.iterrows():
        code = row['code']
        name = row['name']
        try:
            df_k = pd.read_sql(f"SELECT date, open, close, high, low, volume FROM daily_k_line WHERE code = '{code}' ORDER BY date ASC", conn)
            if df_k.empty: continue
            
            # 🚨 核心修复：强制转换类型
            df_k[num_cols] = df_k[num_cols].apply(pd.to_numeric, errors='coerce')
            df_k = df_k.dropna(subset=num_cols) # 剔除转换失败的行
            if len(df_k) < 10: continue

            df_k['ma10'] = df_k['close'].rolling(window=10).mean()
            today = df_k.iloc[-1]
            
            # ... 原有策略逻辑保持不变 ...
            
        except Exception as e:
            print(f"🚨 [池子流转异常] 股票: {name}({code}) | 详情: {e}")
            continue

    # --- 逻辑 B: 全市场新信号扫描 ---
    try:
        # 这里假设你之前已经读取了 stock_basic
        stock_basic = pd.read_sql("SELECT code, name FROM stock_basic", conn)
        existing_codes = set(pipeline_df['code'].tolist())

        for _, row in stock_basic.iterrows():
            code = row['code']
            name = row['name']
            if code in existing_codes: continue
            
            try:
                df = pd.read_sql(f"SELECT date, open, close, high, low, volume FROM daily_k_line WHERE code = '{code}' ORDER BY date ASC", conn)
                
                # 🚨 核心修复：强制转换类型
                df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')
                df = df.dropna(subset=num_cols)
                if len(df) < 40: continue
                
                df['vol_ma20'] = df['volume'].rolling(window=20).mean()
                today_idx = df.index[-1]
                today = df.loc[today_idx]
                
                # ... 原有信号判断逻辑保持不变 ...
                
            except Exception as e:
                print(f"🚨 [新信号扫描异常] 股票: {name}({code}) | 详情: {e}")
                continue
                
    except Exception as e:
        print(f"❌ 全市场扫描主循环崩溃: {e}")

    conn.commit()
    conn.close()
    print(f"✅ 状态机引擎执行完毕。")