from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles # ⚠️ 1. 引入 StaticFiles

from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import sqlite3
from datetime import datetime
import pandas as pd
import uvicorn
import datetime
import os
from config import DB_PATH

app = FastAPI()

# ⚠️ 2. 挂载静态资源目录（这行代码必须有！）
# 参数解释：
# "/static" -> 前端请求的 URL 前缀
# directory="static" -> 你 Python 项目根目录下真实的文件夹名字 (里面放着你的 index.html)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ==========================================
# 📊 [新增] 数据库初始化：创建审计日志表
# ==========================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS operation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            time TEXT,
            code TEXT,
            name TEXT,
            source TEXT,
            detail TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db() # 启动时自动检查建表

import sqlite3
from datetime import datetime

# 确保你的 server.py 启动时会执行一次这个建表函数
def init_log_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS operation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            log_date TEXT,
            log_time TEXT,
            source TEXT,
            code TEXT,
            name TEXT,
            detail TEXT
        )
    """)
    conn.commit()
    conn.close()

# 启动时执行
init_log_table()

# ==========================================
# 📝 [新增] 日志写入工具函数
# ==========================================
def write_log(code, name, source, detail):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.datetime.now()
    cursor.execute("INSERT INTO operation_log (date, time, code, name, source, detail) VALUES (?, ?, ?, ?, ?, ?)",
                   (now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), code, name, source, detail))
    conn.commit()
    conn.close()

# --- API 模型 ---
class StatusUpdate(BaseModel):
    code: str
    new_status: int
    source: str = "手动干预"

class StrategyReq(BaseModel):
    source: str = "按钮处理"

# --- 路由 ---
@app.get("/", response_class=HTMLResponse)
def read_root():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

from fastapi import Query

# 🚨 修改原来的 counts 接口，增加 show_special 参数
@app.get("/api/counts")
def get_counts(show_special: str = "false"): # 强制声明为字符串接收
    import sqlite3
    from config import DB_PATH
    
    # 🚨 极度严谨的布尔转换逻辑
    # 只有当参数明确为 'true' (不区分大小写) 时，才判定为显示特殊票
    is_show_actual = show_special.lower() == "true"
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    sql = "SELECT status, COUNT(*) FROM stock_pipeline WHERE 1=1"
    
    # 如果判定为 False，则执行过滤逻辑（不显示ST和科创）
    if not is_show_actual:
        sql += " AND name NOT LIKE '%ST%' AND code NOT LIKE '688%'"
        
    sql += " GROUP BY status"
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    
    counts = {1: 0, 2: 0, 3: 0, 99: 0}
    for row in rows:
        counts[row[0]] = row[1]
        
    return counts

import pandas as pd
import sqlite3
# ... 确保顶部有这些 import

# ----------------------------------------------------
# 🌐 原版 Web 端接口 (保持纯洁，绝对不动它！)
# ----------------------------------------------------
@app.get("/api/pool/{status}")
def get_web_pool(status: int):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT code, name FROM stock_pipeline WHERE status=?", (status,))
    data = [{"code": r[0], "name": r[1]} for r in cursor.fetchall()]
    conn.close()
    return data

# ----------------------------------------------------
# 📱 专为 App 端打造的新接口 (BFF 模式)
# ----------------------------------------------------
@app.get("/api/pool/app/{status}")
def get_app_pool(status: int):
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT 
            p.code, 
            p.name, 
            p.latest_price as price,
            p.latest_change as change,
            p.turnover,
            p.volume as amount, -- sync_app_data 中把成交额存进了 volume 字段
            (SELECT volume FROM daily_k_line k WHERE k.code = p.code ORDER BY date DESC LIMIT 1) as raw_volume -- 原始成交量(股)
        FROM stock_pipeline p
        WHERE p.status = ?
    """
    import pandas as pd
    df = pd.read_sql(query, conn, params=(status,))
    conn.close()
    
    if df.empty:
        return []
        
    df = df.fillna("")
    return df.to_dict(orient='records')


@app.get("/api/kline/{code}")
def get_kline(code: str):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT date, open, close, high, low, volume, turn FROM daily_k_line WHERE code = '{code}' ORDER BY date ASC", conn)
    conn.close()
    return df.to_dict(orient="records")

# [修改] 触发引擎，先同步数据，再流转状态
@app.post("/api/run_strategy")
def api_run_strategy(req: StrategyReq):
    try:
        from update_daily import update_daily_k_lines
        update_daily_k_lines() 
        
        import time
        time.sleep(2)
        
        from strategy_engine import run_strategy_engine
        run_strategy_engine(source=req.source)
        
        # 💥 核心修复：执行完毕后，强制调用 App 数据格式化脚本！
        from sync_app_data import sync_data_to_app_table
        sync_data_to_app_table()
        
        return {"status": "success", "msg": "✅ 数据同步、策略流转及App视图更新完毕！"}
    except Exception as e:
        return {"status": "error", "msg": str(e)}


class StatusUpdate(BaseModel):
    code: str
    new_status: int
    source: str = "手工干预" # 对应你前端的 log.source
@app.post("/api/update_status")
def update_stock_status(data: StatusUpdate):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. 查询该股票原来的名字（为了写进日志）
    cursor.execute("SELECT name FROM stock_pipeline WHERE code = ?", (data.code,))
    row = cursor.fetchone()
    name = row[0] if row else "未知股票"

    # 2. 更新股票状态 (你原有的逻辑)
    cursor.execute("UPDATE stock_pipeline SET status = ? WHERE code = ?", (data.new_status, data.code))
    
    # 3. 💥 核心补救：写入操作日志表！
    now = datetime.now()
    log_date = now.strftime("%Y-%m-%d")
    log_time = now.strftime("%H:%M:%S")
    detail_msg = f"将状态强制变更为: {data.new_status}"
    
    cursor.execute("""
        INSERT INTO operation_log (log_date, log_time, source, code, name, detail)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (log_date, log_time, data.source, data.code, name, detail_msg))
    
    conn.commit()
    conn.close()
    return {"status": "success", "msg": "更新并记录日志成功"}

# [新增 API] 拉取日志列表并按日期分组
import os

@app.get("/api/logs")
def get_logs():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        # 按日期和时间倒序查询所有日志
        cursor.execute("SELECT log_date, log_time, source, code, name, detail FROM operation_log ORDER BY log_date DESC, log_time DESC")
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return {} # 如果没有数据，返回空对象

        result = {}
        for row in rows:
            date = row[0]
            if date not in result:
                result[date] = []
            
            result[date].append({
                "time": row[1],
                "source": row[2],
                "code": row[3],
                "name": row[4],
                "detail": row[5]
            })
        return result
    except Exception as e:
        print(f"读取日志报错: {e}")
        return {}

@app.post("/api/reset_bootstrap")
def api_reset_bootstrap():
    try:
        from bootstrap_pipeline import run_bootstrap
        run_bootstrap()
        write_log("ALL", "全市场", "手动干预", "执行了系统核弹级重置与历史雷达回溯")
        return {"status": "success", "msg": "核弹重置完毕！"}
    except Exception as e:
        return {"status": "error", "msg": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)