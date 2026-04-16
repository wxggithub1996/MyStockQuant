import akshare as ak
import pandas as pd
import sqlite3
import time

def build_concept_mapping():
    print("🌟 开始构建股票概念映射库...")
    conn = sqlite3.connect('stock_quant.db')
    cursor = conn.cursor()
    
    # 1. 每次更新前，直接清空旧表（推倒重来策略）
    cursor.execute('DROP TABLE IF EXISTS stock_concept_mapping')
    cursor.execute('''
        CREATE TABLE stock_concept_mapping (
            code TEXT,
            concept_name TEXT,
            PRIMARY KEY (code, concept_name)
        )
    ''')
    conn.commit()

    try:
        # 2. 获取东财的所有概念板块名单
        print("正在拉取板块目录...")
        df_concepts = ak.stock_board_concept_name_em()
        concept_list = df_concepts['板块名称'].tolist()
        print(f"共发现 {len(concept_list)} 个有效概念。")
        
        all_mapping_data = []
        
        # 3. 遍历每个概念，获取里面的成分股
        for i, concept in enumerate(concept_list):
            try:
                # 获取该概念下的所有股票
                df_cons = ak.stock_board_concept_cons_em(symbol=concept)
                if not df_cons.empty:
                    # 提取股票代码并加上概念标签
                    for _, row in df_cons.iterrows():
                        all_mapping_data.append({
                            'code': str(row['代码']).zfill(6),
                            'concept_name': concept
                        })
                
                # 打印进度，防止干等
                if (i + 1) % 50 == 0:
                    print(f"已拉取进度: {i + 1}/{len(concept_list)}...")
                    
                time.sleep(0.5) # 极其重要：防止被东方财富封锁 IP
                
            except Exception as e:
                print(f"⚠️ 拉取概念 [{concept}] 失败: {e}")
                
        # 4. 统一写入数据库
        if all_mapping_data:
            df_mapping = pd.DataFrame(all_mapping_data)
            df_mapping.to_sql('stock_concept_mapping', conn, if_exists='append', index=False)
            print(f"\n✅ 概念库构建完成！共写入 {len(df_mapping)} 条映射关系。")
            
    except Exception as e:
        print(f"❌ 主流程异常: {e}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    build_concept_mapping()