import baostock as bs
import pandas as pd
import sqlite3
import datetime
from config import DB_PATH

def update_daily_k_lines():
    print("\n🚀 [系统] 开始执行智能分级增量同步...")
    bs.login()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 强制获取北京时间，防止云端或本地系统时差导致跳过更新
    timezone = datetime.timezone(datetime.timedelta(hours=8))
    today = datetime.datetime.now(timezone).strftime('%Y-%m-%d')

    # ==========================================
    # 1. 阵营划分：提取核心池与全市场
    # ==========================================
    # 提取当前在我们的 1、2、3 池子里的股票（VIP 梯队）
    pipeline_df = pd.read_sql("SELECT code FROM stock_pipeline WHERE status IN (1, 2, 3)", conn)
    priority_codes = set(pipeline_df['code'].tolist())

    # 提取全市场所有基础股票
    all_df = pd.read_sql("SELECT code FROM stock_basic", conn)
    all_codes = set(all_df['code'].tolist())

    # 用集合相减，剩下的就是海选普通池（群众梯队）
    normal_codes = all_codes - priority_codes

    # ==========================================
    # 2. 核心拉取执行引擎
    # ==========================================
    # ==========================================
    # 2. 核心拉取执行引擎
    # ==========================================
    def fetch_codes(codes_set, batch_name):
        if not codes_set: return
        print(f"\n📦 开始同步 [{batch_name}] 共 {len(codes_set)} 只标的...")
        
        for code in codes_set:
            # 查这只股票在数据库里的最新一天
            cursor.execute(f"SELECT MAX(date) FROM daily_k_line WHERE code = '{code}'")
            last_date_row = cursor.fetchone()
            last_date = last_date_row[0] if last_date_row[0] else '2020-01-01'
            
            # 如果已经是今天的数据，跳过
            if last_date >= today:
                continue
                
            start_date_obj = datetime.datetime.strptime(last_date, '%Y-%m-%d') + datetime.timedelta(days=1)
            start_date = start_date_obj.strftime('%Y-%m-%d')
            
            # 🛠️ 修复 1：请求前，动态为 BaoStock 拼接 9 位代码
            bs_code = code
            if len(code) == 6:  # 只有纯 6 位数字才需要拼接
                if code.startswith('6'):
                    bs_code = f"sh.{code}"
                elif code.startswith('0') or code.startswith('3'):
                    bs_code = f"sz.{code}"
                elif code.startswith('8') or code.startswith('4') or code.startswith('9'):
                    bs_code = f"bj.{code}"
            
            # 请求 BaoStock，传入拼接好的 bs_code (如 sz.000001)
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,volume,amount,turn",
                start_date=start_date, end_date=today,
                frequency="d", adjustflag="3"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                # 🛠️ 修复 2：保存回数据库前，强制把代码改回纯净的 6 位格式 (code)，
                # 防止 sz.000001 混入数据库，导致后续 App 端查询不到数据！
                row[1] = code 
                data_list.append(row)
            
            if data_list:
                # 批量写入数据库
                cursor.executemany('''
                    INSERT OR REPLACE INTO daily_k_line (date, code, open, high, low, close, volume, amount, turn)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_list)
                conn.commit()
                print(f"   [OK] {code} 极速更新至 {today}")

    # ==========================================
    # 3. 排队执行：优先 VIP，再跑群众
    # ==========================================
    # 第一梯队：优先更新我们盯着的几十只票（仅需几秒钟即可完成）
    fetch_codes(priority_codes, "🔥 核心关注池 (试盘/回踩/突破)")
    
    # 第二梯队：海量更新剩下的 4000 多只票（在后台慢慢跑）
    fetch_codes(normal_codes, "🌐 全市场海选普通池")

    conn.close()
    bs.logout()
    print("\n✅ 全市场智能分级增量同步完毕！")

# 如果在这个文件直接运行，则执行
if __name__ == "__main__":
    update_daily_k_lines()

# 在你的 update_daily.py 的最后，加上这个刷新池子状态的函数
def refresh_pipeline_latest_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("🔄 正在预计算 App 端行情数据并固化到数据库...")
    # 用一句 SQL，把 daily_k_line 里最新的价格直接刷进 stock_pipeline 里
    # 这就是你说的“数据扫描出来之后再同步更新”
    update_sql = """
        UPDATE stock_pipeline 
        SET 
            latest_price = (SELECT close FROM daily_k_line k WHERE k.code = stock_pipeline.code ORDER BY date DESC LIMIT 1),
            latest_change = (SELECT pct_change FROM daily_k_line k WHERE k.code = stock_pipeline.code ORDER BY date DESC LIMIT 1),
            turnover = (SELECT turn FROM daily_k_line k WHERE k.code = stock_pipeline.code ORDER BY date DESC LIMIT 1),
            volume = (SELECT amount FROM daily_k_line k WHERE k.code = stock_pipeline.code ORDER BY date DESC LIMIT 1)
        WHERE EXISTS (
            SELECT 1 FROM daily_k_line k WHERE k.code = stock_pipeline.code
        )
    """
    cursor.execute(update_sql)
    conn.commit()
    conn.close()
    print("✅ App 端数据预处理完毕！")