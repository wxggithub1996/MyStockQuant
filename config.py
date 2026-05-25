import os
from datetime import datetime

# 1. 路径与数据库配置
# 自动获取当前文件所在目录，避免路径报错
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'stock_quant.db')

# 2. 策略核心参数 (随时可根据实战微调)
STRATEGY_PARAMS = {
    'lookback_days': 60,               # 长周期横盘观察窗口
    'min_long_window_days': 40,        # 至少需要多少天的历史，才认定为“长期横盘”
    'short_window_days': 20,           # 短周期横盘观察窗口
    'short_max_amplitude': 0.25,       # 短周期横盘最大振幅限制
    'max_amplitude': 0.40,             # 长周期横盘最大振幅限制
    'volume_multiple': 2.0,            # 试盘所需的成交量倍数
    'test_body_min_rise': 0.01,        # 试盘日最小实体涨幅，放宽到 1%
    'test_close_position_min': 0.55,   # 收盘至少落在当日振幅上半区，避免弱势冲高回落
    'bottom_max_rise': 0.40,           # 试盘日收盘价距离长周期最低价的极限涨幅
    'max_breakout_wait_days': 25,      # 试盘后最长等待突破天数
    'washout_volume_ratio': 0.60,      # 洗盘期最小成交量相对试盘量的比例阈值
    'washout_max_overrun': 0.08,       # 洗盘期超过标杆价的容忍上冲比例
    'retest_lookahead_days': 5,        # 突破后回踩确认的观察天数
    'retest_volume_ratio': 0.80,       # 回踩日成交量相对突破日的最大比例
    'retest_price_tolerance': 0.01     # 回踩触碰标杆价附近的容忍度
}

# 3. 数据更新参数
DATA_START_DATE = "2023-01-01"
DATA_END_DATE = datetime.now().strftime("%Y-%m-%d")
