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

@app.get("/api/counts")
def get_counts(st: bool = True, cy: bool = True, kc: bool = True):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT status, code, name FROM stock_pipeline", conn)
    conn.close()
    if not df.empty:
        if not st: df = df[~df['name'].str.contains('ST', na=False)]
        if not cy: df = df[~df['code'].str.startswith('300', na=False)]
        if not kc: df = df[~df['code'].str.startswith('688', na=False)]
    counts = df['status'].value_counts().to_dict()
    return {"1": counts.get(1, 0), "2": counts.get(2, 0), "3": counts.get(3, 0), "99": counts.get(99, 0)}

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
        SELECT code, name, latest_price as price, latest_change as change, turnover, volume 
        FROM stock_pipeline WHERE status=?
    """
    import pandas as pd
    
    # ⚠️ 修复：必须加上 params=(status,)，把前端传来的 status 填入 SQL 的问号中
    df = pd.read_sql(query, conn, params=(status,))
    
    conn.close()
    
    if df.empty:
        return []
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
        # 1. 强制先运行增量数据拉取
        from update_daily import update_daily_k_lines
        update_daily_k_lines() 
        
        # 为了防止数据库锁死，这里最好给 SQLite 一点缓冲时间
        import time
        time.sleep(2)
        
        # 2. 数据更新完毕后，再执行大脑引擎
        from strategy_engine import run_strategy_engine
        run_strategy_engine(source=req.source)
        
        return {"status": "success", "msg": "✅ 数据增量同步与策略流转已全部完成！"}
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