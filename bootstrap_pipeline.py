import pandas as pd
import sqlite3
import os
from config import STRATEGY_PARAMS

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'stock_quant.db')

SHORT_WINDOW_DAYS = STRATEGY_PARAMS.get('short_window_days', 20)
LONG_WINDOW_DAYS = STRATEGY_PARAMS.get('lookback_days', 60)
MIN_LONG_WINDOW_DAYS = STRATEGY_PARAMS.get('min_long_window_days', 40)
SHORT_MAX_AMPLITUDE = STRATEGY_PARAMS.get('short_max_amplitude', 0.25)
LONG_MAX_AMPLITUDE = STRATEGY_PARAMS.get('max_amplitude', 0.40)
VOLUME_MULTIPLE = STRATEGY_PARAMS.get('volume_multiple', 2.0)
TEST_BODY_MIN_RISE = STRATEGY_PARAMS.get('test_body_min_rise', 0.01)
TEST_CLOSE_POSITION_MIN = STRATEGY_PARAMS.get('test_close_position_min', 0.55)
BOTTOM_MAX_RISE = STRATEGY_PARAMS.get('bottom_max_rise', 0.40)
MAX_BREAKOUT_WAIT_DAYS = STRATEGY_PARAMS.get('max_breakout_wait_days', 25)
WASHOUT_VOLUME_RATIO = STRATEGY_PARAMS.get('washout_volume_ratio', 0.60)
WASHOUT_MAX_OVERRUN = STRATEGY_PARAMS.get('washout_max_overrun', 0.08)
RETEST_LOOKAHEAD_DAYS = STRATEGY_PARAMS.get('retest_lookahead_days', 5)
RETEST_VOLUME_RATIO = STRATEGY_PARAMS.get('retest_volume_ratio', 0.80)
RETEST_PRICE_TOLERANCE = STRATEGY_PARAMS.get('retest_price_tolerance', 0.01)

def calc_amplitude(high_price, low_price):
    if pd.isna(high_price) or pd.isna(low_price) or low_price <= 0:
        return float('inf')
    return high_price / low_price - 1

def is_sideways(df_slice, max_amplitude):
    if df_slice.empty:
        return False
    return calc_amplitude(df_slice['high'].max(), df_slice['low'].min()) <= max_amplitude

def is_low_position(test_close, long_low):
    if pd.isna(test_close) or pd.isna(long_low) or long_low <= 0:
        return False
    return (test_close / long_low - 1) <= BOTTOM_MAX_RISE

def detect_breakout_retest(df, start_idx, benchmark_price):
    breakout_candidates = df.loc[start_idx + 1:]
    breakout_df = breakout_candidates[breakout_candidates['close'] > benchmark_price]
    if breakout_df.empty:
        return 0

    breakout_idx = breakout_df.index[0]
    breakout_volume = df.loc[breakout_idx, 'volume']
    retest_window = df.loc[breakout_idx + 1 : breakout_idx + RETEST_LOOKAHEAD_DAYS]
    if retest_window.empty:
        return 0

    for _, row in retest_window.iterrows():
        touched_benchmark = row['low'] <= benchmark_price * (1 + RETEST_PRICE_TOLERANCE)
        held_above = row['close'] >= benchmark_price
        shrink_back = row['volume'] <= breakout_volume * RETEST_VOLUME_RATIO
        if touched_benchmark and held_above and shrink_back:
            return 1

    return 0

def run_bootstrap():
    print("[启动] 启动全市场历史形态回溯雷达 (V3.0 极客严苛版)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    try:
        cursor.execute("ALTER TABLE stock_pipeline ADD COLUMN breakout_retest_ready INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    stocks_df = pd.read_sql("SELECT DISTINCT code, name FROM stock_basic", conn)
    codes = stocks_df['code'].tolist()
    name_dict = dict(zip(stocks_df['code'], stocks_df['name']))

    cursor.execute("DELETE FROM stock_pipeline")
    conn.commit()

    # ==========================================
    # 批量预加载: 一次SQL加载全量K线到内存 (按code分组)
    # ==========================================
    print(f"[扫描] 正在批量加载 {len(codes)} 只股票的K线数据...")
    all_kline = pd.read_sql(
        "SELECT code, date, open, close, high, low, volume FROM daily_k_line ORDER BY code, date ASC",
        conn
    )
    # 强制转换类型
    numeric_cols = ['open', 'close', 'high', 'low', 'volume']
    if not all_kline.empty:
        all_kline[numeric_cols] = all_kline[numeric_cols].apply(pd.to_numeric, errors='coerce')
    kline_dict = {code: group.reset_index(drop=True) for code, group in all_kline.groupby('code')}
    print(f"   [完成] 已加载 {len(kline_dict)} 只股票的K线数据")

    results = []
    print(f"[扫描] 正在以最高标准扫描 {len(codes)} 只股票，过滤伪形态...")

    for code in codes:
        df = kline_dict.get(code)
        if df is None or df.empty:
            continue
        df = df.copy()

        # 数据量太少不够判断前期横盘的，直接跳过
        if len(df) < MIN_LONG_WINDOW_DAYS:
            continue

        # 【铁律1】计算过去20天的平均成交量，用于界定真正的“突然爆量”
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()
        daily_range = (df['high'] - df['low']).replace(0, pd.NA)
        df['close_position'] = ((df['close'] - df['low']) / daily_range).fillna(1.0)

        # 【铁律2】寻找真正的主力试盘：
        # 1. 成交量 > 过去20天均量的 2.0 倍
        # 2. 必须是阳线
        # 3. 实体涨幅必须大于 1%
        # 4. 收盘至少落在当日振幅的上半区，避免弱势上影冲高
        df['is_test'] = (df['volume'] >= df['vol_ma20'].shift(1) * VOLUME_MULTIPLE) & \
                        (df['close'] > df['open']) & \
                        ((df['close'] - df['open']) / df['open'] > TEST_BODY_MIN_RISE) & \
                        (df['close_position'] >= TEST_CLOSE_POSITION_MIN)

        test_days = df[df['is_test']].index
        if len(test_days) == 0:
            continue

        valid_test = False
        
        # 倒序查找最近的一次有效试盘
        for test_idx in reversed(test_days):
            # 【铁律3】游资是有耐心的，但不能无限期。超过 25 天没突破，视为废卡。
            if df.index[-1] - test_idx > MAX_BREAKOUT_WAIT_DAYS:
                continue 

            # 前期数据不足，无法判断是否横盘
            if test_idx < SHORT_WINDOW_DAYS:
                continue

            short_pre_df = df.loc[test_idx - SHORT_WINDOW_DAYS : test_idx - 1]
            long_start = max(0, test_idx - LONG_WINDOW_DAYS)
            long_pre_df = df.loc[long_start : test_idx - 1]
            if len(long_pre_df) < MIN_LONG_WINDOW_DAYS:
                continue
            
            # 【铁律4】试盘前既要有短期横盘，也要能体现中期蓄势，而不是已经走出明显趋势。
            if not is_sideways(short_pre_df, SHORT_MAX_AMPLITUDE):
                continue

            if not is_sideways(long_pre_df, LONG_MAX_AMPLITUDE):
                continue

            # --- 确立试盘日标杆参数 ---
            test_row = df.loc[test_idx]
            if not is_low_position(test_row['close'], long_pre_df['low'].min()):
                continue

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
                if washout_df['high'].max() > benchmark_price * (1 + WASHOUT_MAX_OVERRUN):
                    continue
                
                # 【铁律6】底线不可破：收盘价跌破试盘低点，主力跑路
                if washout_df['close'].min() < support_price:
                    continue
                
                # 【铁律7】必须有极致缩量洗盘的过程
                if washout_df['volume'].min() > test_volume * WASHOUT_VOLUME_RATIO:
                    continue

            # 恭喜，如果能走到这里，说明这只股票完美通过了所有变态测试！
            valid_test = True
            break # 找到了最近的一次完美形态，跳出循环

        # 如果找了一圈没找到完美的，直接看下一只股票
        if not valid_test:
            continue

        # --- 形态分流 (入库) ---
        today_row = df.iloc[-1]
        breakout_retest_ready = detect_breakout_retest(df, test_idx, benchmark_price)
        
        # 突破起爆判定：今天收盘越过标杆价
        if today_row['close'] > benchmark_price:
            results.append((code, name_dict.get(code, code), 3, benchmark_price, test_date, breakout_retest_ready))
        else:
            # 没突破，如果在洗盘就是状态2，今天刚爆量就是状态1
            if test_idx == df.index[-1]:
                 results.append((code, name_dict.get(code, code), 1, benchmark_price, test_date, 0))
            else:
                 results.append((code, name_dict.get(code, code), 2, benchmark_price, test_date, breakout_retest_ready))

    # 写入数据库
    if results:
        cursor.executemany('''
            INSERT INTO stock_pipeline (code, name, status, test_high, entry_date, breakout_retest_ready, update_time)
            VALUES (?, ?, ?, ?, ?, ?, date('now'))
        ''', results)
        conn.commit()

    conn.close()
    
    status_1 = sum(1 for r in results if r[2] == 1)
    status_2 = sum(1 for r in results if r[2] == 2)
    status_3 = sum(1 for r in results if r[2] == 3)
    retest_ready_count = sum(1 for r in results if r[5] == 1)
    
    print("\n" + "[热]"*25)
    print("[目标] V3.0 过滤网执行完毕！过滤掉了所有杂质！")
    print(f"   [观察] 纯血试盘 (状态 1) : {status_1} 只")
    print(f"   [回踩] 黄金回踩 (状态 2) : {status_2} 只")
    print(f"   [启动] 确立突破 (状态 3) : {status_3} 只")
    print(f"   [完成] 突破后缩量回踩确认 : {retest_ready_count} 只")
    print("[热]"*25)

if __name__ == "__main__":
    run_bootstrap()
