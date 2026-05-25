"""
性能优化验证脚本
测试各模块执行耗时，与优化前对比
"""
import time
import sqlite3
from config import DB_PATH

def test_wal_mode():
    """验证 WAL 模式是否生效"""
    conn = sqlite3.connect(DB_PATH)
    result = conn.execute("PRAGMA journal_mode").fetchone()
    conn.close()
    mode = result[0] if result else "unknown"
    status = "[完成] WAL" if mode == "wal" else f"[失败] {mode}"
    print(f"  数据库日志模式: {status}")
    return mode == "wal"

def test_sync_app_data():
    """测试 sync_app_data 耗时"""
    from sync_app_data import sync_data_to_app_table
    start = time.time()
    sync_data_to_app_table()
    elapsed = time.time() - start
    print(f"  sync_app_data 耗时: {elapsed:.2f}秒")
    return elapsed

def test_strategy_engine():
    """测试 strategy_engine 耗时"""
    from strategy_engine import run_strategy_engine
    start = time.time()
    run_strategy_engine(source="性能测试")
    elapsed = time.time() - start
    print(f"  strategy_engine 耗时: {elapsed:.2f}秒")
    return elapsed

def test_update_daily():
    """测试 update_daily 耗时 (会从BaoStock拉数据，需要网络)"""
    from update_daily import update_daily_k_lines
    start = time.time()
    update_daily_k_lines()
    elapsed = time.time() - start
    print(f"  update_daily 耗时: {elapsed:.2f}秒")
    return elapsed

def test_batch_commit():
    """验证批量commit效果：统计每100只提交一次"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    # 模拟批量写入测试
    cursor.execute("CREATE TABLE IF NOT EXISTS _perf_test (id INTEGER, val TEXT)")
    conn.commit()

    start = time.time()
    for i in range(1000):
        cursor.execute("INSERT INTO _perf_test VALUES (?, ?)", (i, f"test_{i}"))
        if (i + 1) % 100 == 0:
            conn.commit()
    conn.commit()
    batch_time = time.time() - start

    cursor.execute("DELETE FROM _perf_test")
    conn.commit()

    start = time.time()
    for i in range(1000):
        cursor.execute("INSERT INTO _perf_test VALUES (?, ?)", (i, f"test_{i}"))
        conn.commit()
    single_time = time.time() - start

    cursor.execute("DROP TABLE IF EXISTS _perf_test")
    conn.commit()
    conn.close()

    speedup = single_time / batch_time if batch_time > 0 else 0
    print(f"  批量commit (每100只): {batch_time:.3f}秒")
    print(f"  逐只commit (每只):   {single_time:.3f}秒")
    print(f"  提速倍数: {speedup:.1f}x")
    return speedup

def main():
    print("=" * 50)
    print(" MyStockQuant 性能优化验证")
    print("=" * 50)

    # 1. WAL 模式验证
    print("\n[1] SQLite WAL 模式验证")
    test_wal_mode()

    # 2. 批量 commit 对比
    print("\n[2] 批量 commit 性能对比")
    test_batch_commit()

    # 3. sync_app_data 耗时
    print("\n[3] sync_app_data 耗时测试")
    test_sync_app_data()

    # 4. strategy_engine 耗时
    print("\n[4] strategy_engine 耗时测试")
    test_strategy_engine()

    # 5. update_daily (可选，需要网络)
    print("\n[5] update_daily 耗时测试 (需要网络)")
    ans = input("  是否测试 update_daily? (y/N): ").strip().lower()
    if ans == 'y':
        test_update_daily()
    else:
        print("  跳过")

    print("\n" + "=" * 50)
    print("[完成] 验证完毕")
    print("=" * 50)

if __name__ == "__main__":
    main()
