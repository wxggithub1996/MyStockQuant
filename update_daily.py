"""
智能分级增量K线更新 (并发版)
VIP池优先 + 5线程并发拉取 + 批量commit
"""
import pandas as pd
import sqlite3
import datetime
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from eastmoney_api import fetch_kline, _get_session
from config import DB_PATH

def update_daily_k_lines():
    print("\n[系统] 开始执行智能分级增量同步 (东方财富数据源, 并发模式)...")
    _get_session()  # 预热连接池
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    timezone = datetime.timezone(datetime.timedelta(hours=8))
    today = datetime.datetime.now(timezone).strftime('%Y-%m-%d')

    # 1. 阵营划分
    pipeline_df = pd.read_sql("SELECT code FROM stock_pipeline WHERE status IN (1, 2, 3)", conn)
    priority_codes = set(pipeline_df['code'].tolist())

    all_df = pd.read_sql("SELECT code FROM stock_basic", conn)
    all_codes = set(all_df['code'].tolist())
    normal_codes = all_codes - priority_codes

    # 2. 预计算每只股票的最新日期 (一次SQL)
    cursor.execute("SELECT code, MAX(date) FROM daily_k_line GROUP BY code")
    last_date_map = dict(cursor.fetchall())

    # 3. 并发拉取引擎
    def fetch_codes(codes_set, batch_name, max_workers=5):
        if not codes_set:
            return 0
        print(f"\n[{batch_name}] 共 {len(codes_set)} 只标的...")

        # 筛选出需要更新的股票
        tasks = []
        for code in codes_set:
            last_date = last_date_map.get(code, '2020-01-01')
            if last_date >= today:
                continue
            start_date_obj = datetime.datetime.strptime(last_date, '%Y-%m-%d') + datetime.timedelta(days=1)
            start_date = start_date_obj.strftime('%Y-%m-%d')
            tasks.append((code, start_date, today))

        if not tasks:
            print(f"   所有股票已是最新，无需更新")
            return 0

        print(f"   需更新: {len(tasks)} 只 (并发={max_workers})")

        batch_data = []
        done_count = 0
        t_start = _time.time()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {}
            for code, start_date, end_date in tasks:
                future = executor.submit(fetch_kline, code, start_date, end_date)
                future_map[future] = code

            for future in as_completed(future_map):
                code = future_map[future]
                try:
                    data = future.result()
                    if data:
                        batch_data.extend(data)
                except Exception:
                    pass

                done_count += 1
                # 每200只批量写入
                if done_count % 200 == 0 and batch_data:
                    cursor.executemany('''
                        INSERT OR REPLACE INTO daily_k_line (date, code, open, high, low, close, volume, amount, turn, pctChg)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', batch_data)
                    conn.commit()
                    batch_data = []
                    elapsed = _time.time() - t_start
                    speed = done_count / elapsed if elapsed > 0 else 0
                    print(f"   [进度] {done_count}/{len(tasks)} ({done_count*100//len(tasks)}%) | "
                          f"速度: {speed:.1f}只/秒")

        # 最终写入
        if batch_data:
            cursor.executemany('''
                INSERT OR REPLACE INTO daily_k_line (date, code, open, high, low, close, volume, amount, turn, pctChg)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', batch_data)
            conn.commit()

        elapsed = _time.time() - t_start
        print(f"   [完成] {done_count} 只, 耗时 {elapsed:.1f}秒 ({done_count/elapsed:.1f}只/秒)")
        return done_count

    # 第一梯队: VIP池 (2线程，稳定优先)
    fetch_codes(priority_codes, "VIP核心池", max_workers=2)

    # 第二梯队: 全市场 (3线程)
    fetch_codes(normal_codes, "全市场海选", max_workers=3)

    conn.close()
    print("\n[完成] 全市场智能分级增量同步完毕！")


if __name__ == "__main__":
    update_daily_k_lines()


def refresh_pipeline_latest_data():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    print("[同步] 正在预计算 App 端行情数据...")
    update_sql = """
        UPDATE stock_pipeline
        SET
            latest_price = (SELECT close FROM daily_k_line k WHERE k.code = stock_pipeline.code ORDER BY date DESC LIMIT 1),
            latest_change = (SELECT pctChg FROM daily_k_line k WHERE k.code = stock_pipeline.code ORDER BY date DESC LIMIT 1),
            turnover = (SELECT turn FROM daily_k_line k WHERE k.code = stock_pipeline.code ORDER BY date DESC LIMIT 1),
            volume = (SELECT amount FROM daily_k_line k WHERE k.code = stock_pipeline.code ORDER BY date DESC LIMIT 1)
        WHERE EXISTS (
            SELECT 1 FROM daily_k_line k WHERE k.code = stock_pipeline.code
        )
    """
    cursor.execute(update_sql)
    conn.commit()
    conn.close()
    print("[完成] App 端数据预处理完毕！")
