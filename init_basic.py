import baostock as bs
import pandas as pd
import sqlite3

def build_stock_basic_baostock():
    print("🌟 开始使用 BaoStock 构建股票静态画像表...")
    conn = sqlite3.connect('stock_quant.db')
    cursor = conn.cursor()
    
    # 每次更新前，先删掉旧表
    cursor.execute('DROP TABLE IF EXISTS stock_basic')
    conn.commit()

    print("正在登陆 BaoStock...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ BaoStock 登陆失败: {lg.error_msg}")
        return

    try:
        print("正在拉取全市场行业基本面数据...")
        # 拉取行业分类数据
        rs = bs.query_stock_industry()
        
        industry_list = []
        while (rs.error_code == '0') & rs.next():
            industry_list.append(rs.get_row_data())
            
        if industry_list:
            df_basic = pd.DataFrame(industry_list, columns=rs.fields)
            
            # BaoStock 的代码带有 sh./sz. 前缀，我们需要清洗掉它，保持数据库纯洁
            df_basic['code'] = df_basic['code'].apply(lambda x: x.split('.')[1] if '.' in x else x)
            
            # 只保留我们需要的基础列，并重命名
            df_final = df_basic[['code', 'code_name', 'industry']].copy()
            df_final.rename(columns={
                'code_name': 'name',
                'industry': 'sector'
            }, inplace=True)
            
            # 补齐表结构（为了以后的兼容性，把缺失的市值字段补上 0）
            df_final['float_market_cap'] = 0.0
            df_final['total_market_cap'] = 0.0
            df_final['pe_ratio'] = 0.0
            df_final['pb_ratio'] = 0.0
            
            # 写入数据库
            df_final.to_sql('stock_basic', conn, if_exists='replace', index=False)
            
            # 建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_basic_code ON stock_basic(code)')
            conn.commit()
            
            print(f"✅ stock_basic 表构建成功！共写入 {len(df_final)} 只股票的基础信息。")
            print("💡 虽然暂时没有市值数据，但你现在可以按『sector(行业)』进行选股了！")
        else:
            print("❌ 获取数据为空。")
            
    except Exception as e:
        print(f"❌ 构建失败: {e}")
        
    finally:
        bs.logout()
        conn.close()

if __name__ == "__main__":
    build_stock_basic_baostock()