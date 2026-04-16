import pandas as pd
import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'stock_quant.db')

def run_bootstrap():
    print("🚀 启动全市场历史形态回溯雷达 (V3.0 极客严苛版)...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    stocks_df = pd.read_sql("SELECT DISTINCT code, name FROM stock_basic", conn)
    codes = stocks_df['code'].tolist()
    name_dict = dict(zip(stocks_df['code'], stocks_df['name']))

    cursor.execute("DELETE FROM stock_pipeline")
    conn.commit()

    results = []
    print(f"📡 正在以最高标准扫描 {len(codes)} 只股票，过滤伪形态...")

    for code in codes:
        df = pd.read_sql(f"SELECT date, open, close, high, low, volume FROM daily_k_line WHERE code = '{code}' ORDER BY date ASC", conn)
        
        # 重置索引，确保切片时是纯数字索引，避免日期索引带来的边界错误
        df = df.reset_index(drop=True)
        
        # 数据量太少不够判断前期横盘的，直接跳过
        if len(df) < 40:
            continue

        # 【铁律1】计算过去20天的平均成交量，用于界定真正的“突然爆量”
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()

        # 【铁律2】寻找真正的主力试盘：
        # 1. 成交量 > 过去20天均量的 2.5 倍 (排除趋势中的小波动)
        # 2. 必须是阳线
        # 3. 实体涨幅必须大于 2% (排除十字星骗线)
        df['is_test'] = (df['volume'] >= df['vol_ma20'].shift(1) * 2.5) & \
                        (df['close'] > df['open']) & \
                        ((df['close'] - df['open']) / df['open'] > 0.02)

        test_days = df[df['is_test']].index
        if len(test_days) == 0:
            continue

        valid_test = False
        
        # 倒序查找最近的一次有效试盘
        for test_idx in reversed(test_days):
            # 【铁律3】游资是有耐心的，但不能无限期。超过 25 天没突破，视为废卡。
            if df.index[-1] - test_idx > 25:
                continue 

            # 前期数据不足，无法判断是否横盘
            if test_idx < 20:
                continue

            # --- 切片1：审查试盘前的“潜伏期” ---
            pre_df = df.loc[test_idx-20 : test_idx-1]
            base_high = pre_df['high'].max()
            base_low = pre_df['low'].min()
            
            # 【铁律4】试盘前必须是“地量横盘”，振幅不能超过 25% (排除了徐工机械这种已经在爬坡的)
            if base_high / base_low > 1.25:
                continue

            # --- 确立试盘日标杆参数 ---
            test_row = df.loc[test_idx]
            upper_shadow = test_row['high'] - test_row['close']
            body = test_row['close'] - test_row['open']
            benchmark_price = test_row['high'] if upper_shadow > body else test_row['close']
            support_price = test_row['low'] # 也可以用开盘价，这里用最低价更宽容一点洗盘
            test_volume = test_row['volume']
            test_date = test_row['date']

            # --- 切片2：审查试盘后的“洗盘期” ---
            washout_df = df.loc[test_idx + 1 :]

            if len(washout_df) > 0:
                # 【铁律5】绝杀“过山车”：洗盘期间冲高超过标杆价的 8%，说明主力已经拉高出货，周期结束！(排除了深华发A)
                if washout_df['high'].max() > benchmark_price * 1.08:
                    continue
                
                # 【铁律6】底线不可破：收盘价跌破试盘低点，主力跑路
                if washout_df['close'].min() < support_price:
                    continue
                
                # 【铁律7】必须有极致缩量洗盘的过程
                if washout_df['volume'].min() > test_volume * 0.5:
                    continue

            # 恭喜，如果能走到这里，说明这只股票完美通过了所有变态测试！
            valid_test = True
            break # 找到了最近的一次完美形态，跳出循环

        # 如果找了一圈没找到完美的，直接看下一只股票
        if not valid_test:
            continue

        # --- 形态分流 (入库) ---
        today_row = df.iloc[-1]
        
        # 突破起爆判定：今天收盘越过标杆价
        if today_row['close'] > benchmark_price:
            results.append((code, name_dict.get(code, code), 3, benchmark_price, test_date))
        else:
            # 没突破，如果在洗盘就是状态2，今天刚爆量就是状态1
            if test_idx == df.index[-1]:
                 results.append((code, name_dict.get(code, code), 1, benchmark_price, test_date))
            else:
                 results.append((code, name_dict.get(code, code), 2, benchmark_price, test_date))

    # 写入数据库
    if results:
        cursor.executemany('''
            INSERT INTO stock_pipeline (code, name, status, test_high, entry_date, update_time)
            VALUES (?, ?, ?, ?, ?, date('now'))
        ''', results)
        conn.commit()

    conn.close()
    
    status_1 = sum(1 for r in results if r[2] == 1)
    status_2 = sum(1 for r in results if r[2] == 2)
    status_3 = sum(1 for r in results if r[2] == 3)
    
    print("\n" + "🔥"*25)
    print("🎯 V3.0 过滤网执行完毕！过滤掉了所有杂质！")
    print(f"   👀 纯血试盘 (状态 1) : {status_1} 只")
    print(f"   📉 黄金回踩 (状态 2) : {status_2} 只")
    print(f"   🚀 确立突破 (状态 3) : {status_3} 只")
    print("🔥"*25)

if __name__ == "__main__":
    run_bootstrap()