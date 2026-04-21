import schedule
import time
import datetime
import os

# 导入我们之前写好的核心模块
from update_daily import update_daily_k_lines
from strategy_engine import run_strategy_engine
from sync_app_data import sync_data_to_app_table # 🚀 新增导入

def daily_quant_job():
    print("\n" + "="*50)
    print(f"⏰ [定时任务触发] 当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 第一步：自动拉取今日最新 K 线数据
        print("➡️ 步骤 1/2: 开始更新数据...")
        # update_daily_k_lines()
        
        # 给系统一点缓冲时间（防止数据库锁死），暂停 2 秒
        time.sleep(2)
        
        # 第二步：自动执行状态机，进行晋级和淘汰
        print("➡️ 步骤 2/2: 开始流转策略状态机...")
        run_strategy_engine()
        
        # 🚀 第三步：自动格式化 App 所需的展示数据
        print("➡️ 步骤 3/3: 正在同步 App 视图数据...")
        sync_data_to_app_table()

        print("✅ 今日量化任务圆满完成！各池子数据已更新。")
        
    except Exception as e:
        print(f"❌ 自动任务执行发生严重错误: {e}")
        
    print("="*50 + "\n")

# ==========================================
# ⏱️ 核心调度设置：设定每天 15:35 自动执行
# ==========================================
schedule.every().day.at("12:11").do(daily_quant_job)

if __name__ == "__main__":
    print("🤖 量化无人值守守护进程已启动...")
    print("⏳ 系统正在后台静默运行，每天 15:35 将自动执行【拉取数据 + 策略流转】。")
    print("💡 请保持此终端不被关闭 (你可以将其最小化)。")
    print("   如需测试，可修改代码中的时间为当前时间的一分钟后。\n")

    # 守护循环：每 10 秒检查一次时间，极其节省 CPU
    while True:
        schedule.run_pending()
        time.sleep(10)