from database_manager import init_db
from data_fetcher import sync_stock_list, fetch_history_data
from strategy_engine import run_double_volume_strategy

def run_daily_pipeline():
    print("=" * 50)
    print("🚀 欢迎使用个人量化流水线系统 v1.0")
    print("=" * 50)

    # 步骤 1：确保数据库正常
    init_db()

    # 步骤 2：更新数据（如果是建库后第二次运行，它会瞬间完成）
    # print("\n[阶段 1] 校验并更新行情数据...")
    # df_stocks = sync_stock_list()
    # fetch_history_data(df_stocks)

    # 步骤 3：跑策略
    print("\n[阶段 2] 执行量化策略筛选...")
    run_double_volume_strategy()
    
    print("\n" + "=" * 50)
    print("🏆 今日量化流水线任务全部执行完毕！")

if __name__ == "__main__":
    run_daily_pipeline()