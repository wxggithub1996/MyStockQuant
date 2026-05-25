import streamlit as st
import sqlite3
import pandas as pd
from config import DB_PATH

# ==========================================
# 1. 页面全局配置 (极简风格)
# ==========================================
st.set_page_config(page_title="量化策略总控台", page_icon="📈", layout="wide")

hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)


# ==========================================
# 2. 数据库交互与业务逻辑
# ==========================================
def load_dashboard_data():
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT 
            p.status,
            p.code AS '代码',
            p.name AS '名称',
            COALESCE(b.sector, '--') AS '所属板块',
            p.test_high AS '试盘标杆价',
            p.entry_date AS '入池日期',
            CAST(julianday('now') - julianday(p.entry_date) AS INTEGER) AS '潜伏天数'
        FROM stock_pipeline p
        LEFT JOIN stock_basic b ON p.code = b.code
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def update_status(code, new_status):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"UPDATE stock_pipeline SET status = {new_status}, update_time = date('now') WHERE code = '{code}'")
    conn.commit()
    conn.close()
    st.rerun() 


# ==========================================
# 3. 前端 UI 渲染
# ==========================================
def main():
    st.title("📈 量化策略总控台 v2.0")
    st.markdown("<span style='color:gray; font-size:14px;'>核心策略：倍量试盘 ➔ 缩量回踩 ➔ 确认突破</span>", unsafe_allow_html=True)
    st.write("") 

    try:
        df_pipeline = load_dashboard_data()
    except Exception as e:
        st.error(f"数据加载失败，请检查数据库：{e}")
        return

    # --- 左侧边栏 (Sidebar) ---
    with st.sidebar:
        col_title, col_btn = st.columns([4, 1])
        with col_title:
            st.markdown("### ⚙️ 控制面板")
        with col_btn:
            # 【修复点 1】替换为 width="stretch"
            if st.button("🔄", help="强制刷新数据", width="stretch"):
                st.rerun()

        # 【修复点 2】替换为 width="stretch"
        if st.button("🚀 一键执行今日策略", width="stretch", type="primary"):
            with st.spinner("⚙️ 正在执行海量 K 线向量化计算，请稍候..."):
                try:
                    from strategy_engine import run_double_volume_strategy
                    run_double_volume_strategy()
                    st.success("✅ 策略执行完毕！")
                    import time
                    time.sleep(1)
                    st.rerun() 
                except Exception as e:
                    st.error(f"❌ 策略执行失败：{e}")
            
        st.divider()
        
        st.markdown("### 📊 策略池容量")
        st.metric(label="👀 试盘监控", value=len(df_pipeline[df_pipeline['status'] == 1]))
        st.metric(label="📉 缩量回踩", value=len(df_pipeline[df_pipeline['status'] == 2]))
        st.metric(label="🔥 突破确认", value=len(df_pipeline[df_pipeline['status'] == 3]))
        st.metric(label="🗑️ 废弃回收", value=len(df_pipeline[df_pipeline['status'] == 99]))


    # --- 核心数据展示区 (Tabs) ---
    tab1, tab2, tab3, tab4 = st.tabs([
        "👀 试盘监控池", "📉 缩量回踩池", "🔥 突破确认组", "🗑️ 废弃回收站"
    ])

    def render_tab(status_code, empty_msg):
        df_subset = df_pipeline[df_pipeline['status'] == status_code].drop(columns=['status'])
        
        if df_subset.empty:
            st.info(empty_msg)
        else:
            # 【修复点 3】替换为 width="stretch"
            st.dataframe(
                df_subset,
                width="stretch",
                hide_index=True,
                column_config={
                    "代码": st.column_config.TextColumn("股票代码"),
                    "试盘标杆价": st.column_config.NumberColumn("试盘标杆价", format="¥ %.2f"),
                    "潜伏天数": st.column_config.ProgressColumn("潜伏天数", format="%d 天", min_value=0, max_value=20)
                }
            )
            
            st.markdown("<br>", unsafe_allow_html=True)
            col1, col2, col3 = st.columns([3, 1, 4])
            with col1:
                target_stock = st.selectbox(
                    "🔧 选择操作标的", 
                    df_subset['代码'] + " - " + df_subset['名称'], 
                    key=f"select_{status_code}",
                    label_visibility="collapsed"
                )
            with col2:
                if status_code in [1, 2]:
                    # 【修复点 4】替换为 width="stretch"
                    if st.button("🗑️ 淘汰破位标的", key=f"btn_drop_{status_code}", width="stretch"):
                        real_code = target_stock.split(" - ")[0]
                        update_status(real_code, 99)
                elif status_code == 99:
                    # 【修复点 5】替换为 width="stretch"
                    if st.button("♻️ 恢复观察", key=f"btn_restore_{status_code}", width="stretch"):
                        real_code = target_stock.split(" - ")[0]
                        update_status(real_code, 0)

    with tab1: render_tab(1, "当前没有新触发试盘的股票。")
    with tab2: render_tab(2, "当前没有正在回踩的股票。")
    with tab3: render_tab(3, "当前没有突破起爆的股票。")
    with tab4: render_tab(99, "回收站是空的。")

if __name__ == "__main__":
    main()