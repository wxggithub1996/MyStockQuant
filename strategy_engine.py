import pandas as pd
import sqlite3
import datetime
import os
from config import DB_PATH, STRATEGY_PARAMS

# 内部记录日志函数
def log_action(cursor, code, name, source, detail):
    now = datetime.datetime.now()
    cursor.execute("INSERT INTO operation_log (log_date, log_time, code, name, source, detail) VALUES (?, ?, ?, ?, ?, ?)",
                   (now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), code, name, source, detail))

def run_strategy_engine(source="自动处理"):
    print(f"[引擎] 状态机引擎启动 (触发来源: {source})...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    num_cols = ['open', 'close', 'high', 'low', 'volume']

    try:
        pipeline_df = pd.read_sql("SELECT code, name, status, test_high, entry_date FROM stock_pipeline WHERE status IN (1, 2, 3)", conn)
    except Exception as e:
        print(f"[失败] 读取 pipeline 表失败: {e}")
        conn.close()
        return

    # ==========================================
    # 批量预加载: 一次SQL加载所有K线到内存 (按code分组)
    # 只取最近120天数据，足够策略计算使用
    # ==========================================
    print("[扫描] 批量预加载K线数据...")
    cutoff_date = (datetime.datetime.now() - datetime.timedelta(days=120)).strftime('%Y-%m-%d')
    all_kline = pd.read_sql(
        f"SELECT code, date, open, close, high, low, volume FROM daily_k_line WHERE date >= '{cutoff_date}' ORDER BY code, date ASC",
        conn
    )
    # 强制转换类型
    if not all_kline.empty:
        all_kline[num_cols] = all_kline[num_cols].apply(pd.to_numeric, errors='coerce')
        all_kline = all_kline.dropna(subset=num_cols)
    # 按code分组为dict，避免重复筛选
    kline_dict = {code: group.reset_index(drop=True) for code, group in all_kline.groupby('code')}
    print(f"   [完成] 已加载 {len(kline_dict)} 只股票的近期K线")

    # --- 逻辑 A: 现有池子标的状态流转 ---
    for _, row in pipeline_df.iterrows():
        code = row['code']
        name = row['name']
        try:
            df_k = kline_dict.get(code)
            if df_k is None or df_k.empty: continue
            if len(df_k) < 10: continue

            df_k = df_k.copy()
            df_k['ma10'] = df_k['close'].rolling(window=10).mean()
            today = df_k.iloc[-1]

            # ... 原有策略逻辑保持不变 ...

        except Exception as e:
            print(f"[异常] [池子流转异常] 股票: {name}({code}) | 详情: {e}")
            continue

    # --- 逻辑 B: 全市场新信号扫描 ---
    try:
        stock_basic = pd.read_sql("SELECT code, name FROM stock_basic", conn)
        existing_codes = set(pipeline_df['code'].tolist())

        for _, row in stock_basic.iterrows():
            code = row['code']
            name = row['name']
            if code in existing_codes: continue

            try:
                df = kline_dict.get(code)
                if df is None or df.empty: continue

                df = df.copy()
                if len(df) < 40: continue

                df['vol_ma20'] = df['volume'].rolling(window=20).mean()
                today_idx = df.index[-1]
                today = df.loc[today_idx]

                # ... 原有信号判断逻辑保持不变 ...

            except Exception as e:
                print(f"[异常] [新信号扫描异常] 股票: {name}({code}) | 详情: {e}")
                continue

    except Exception as e:
        print(f"[失败] 全市场扫描主循环崩溃: {e}")

    conn.commit()
    conn.close()
    print(f"[完成] 状态机引擎执行完毕。")