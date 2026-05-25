"""
全市场历史K线数据拉取 (并发版)
使用东方财富API + ThreadPoolExecutor 5线程并发
支持增量模式：只拉取数据库中缺失的股票
"""
import akshare as ak
import sqlite3
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from eastmoney_api import fetch_kline, _get_session
from config import DB_PATH

START_DATE = "20230101"
END_DATE = datetime.now().strftime("%Y%m%d")
MAX_WORKERS = 3

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
cursor = conn.cursor()

# 创建表（如果不存在）
cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_k_line (
        date TEXT, code TEXT, open REAL, high REAL, low REAL,
        close REAL, volume REAL, amount REAL, turn REAL, pctChg REAL
    )
''')
conn.commit()

# 增量模式：获取数据库中已有数据的股票代码
cursor.execute("SELECT DISTINCT code FROM daily_k_line")
existing_codes = set(row[0] for row in cursor.fetchall())
print(f"[1/5] 数据库中已有 {len(existing_codes)} 只股票的K线数据")

print("[2/5] 正在获取全市场 A 股代码列表...")
df_all_stocks = ak.stock_info_a_code_name()
df_all_stocks = df_all_stocks[~df_all_stocks['code'].str.startswith(('8', '9'))]
all_codes = df_all_stocks['code'].tolist()

# 增量过滤：只拉取缺失的股票
codes = [c for c in all_codes if c not in existing_codes]
total = len(codes)
print(f"   全市场共 {len(all_codes)} 只标的 (已排除北交所)")
print(f"   需要新拉取: {total} 只")

if total == 0:
    print("[完成] 所有股票数据已齐全，无需拉取。")
    conn.close()
    exit(0)

print(f"[3/5] 使用 {MAX_WORKERS} 线程并发拉取历史K线数据 (含自动重试)...")

# 预热连接池
_get_session()

success_count = 0
empty_count = 0
error_count = 0
batch_data = []
COMMIT_INTERVAL = 200

def flush_to_db(data_list):
    if not data_list:
        return
    cursor.executemany('''
        INSERT INTO daily_k_line (date, code, open, high, low, close, volume, amount, turn, pctChg)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', data_list)
    conn.commit()

t_start = time.time()

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_map = {}
    for code in codes:
        future = executor.submit(fetch_kline, code, START_DATE, END_DATE)
        future_map[future] = code

    for future in as_completed(future_map):
        code = future_map[future]
        try:
            data = future.result()
            if data:
                batch_data.extend(data)
                success_count += 1
            else:
                empty_count += 1
        except Exception as e:
            error_count += 1
            if error_count <= 10:
                print(f"   [异常] {code}: {e}")

        current = success_count + empty_count + error_count
        if current % COMMIT_INTERVAL == 0:
            flush_to_db(batch_data)
            batch_data = []
            elapsed = time.time() - t_start
            speed = current / elapsed if elapsed > 0 else 0
            eta = (total - current) / speed if speed > 0 else 0
            print(f"   [进度] {current}/{total} ({current*100//total}%) | "
                  f"速度: {speed:.1f}只/秒 | 预计剩余: {eta/60:.1f}分钟")

# 最后一批写入
flush_to_db(batch_data)

elapsed = time.time() - t_start
print(f"[4/5] 正在建立底层加速索引...")
cursor.execute('CREATE INDEX IF NOT EXISTS idx_kline_code_date ON daily_k_line(code, date)')
conn.commit()
conn.close()

print(f"\n[5/5] 增量K线数据拉取完毕！")
print(f"结果: 成功={success_count} | 空数据={empty_count} | 失败={error_count}")
print(f"数据库总计: {len(existing_codes) + success_count} 只股票")
print(f"耗时: {elapsed/60:.1f} 分钟 ({total/elapsed:.1f} 只/秒)")
