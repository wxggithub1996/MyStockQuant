import os
from datetime import datetime

# 1. 路径与数据库配置
# 自动获取当前文件所在目录，避免路径报错
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'stock_quant.db')

# 2. 策略核心参数 (随时可根据实战微调)
STRATEGY_PARAMS = {
    'lookback_days': 60,       # 观察横盘的天数
    'max_amplitude': 0.30,     # 横盘期间最大振幅限制 (30%)
    'volume_multiple': 2.0,    # 试盘所需的成交量倍数 (比如 2 倍)
    'bottom_max_rise': 0.40    # 试盘日收盘价距离60日最低价的极限涨幅 (防止追高)
}

# 3. 数据更新参数
DATA_START_DATE = "2023-01-01"
DATA_END_DATE = datetime.now().strftime("%Y-%m-%d")