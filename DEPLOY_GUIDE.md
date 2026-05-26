# MyStockQuant 服务器部署指南

## 一、环境要求

| 项目 | 最低要求 | 推荐配置 |
|------|---------|---------|
| 操作系统 | Windows 10 / Linux / macOS | Windows Server 2019+ / Ubuntu 22.04+ |
| Python | 3.8+ | 3.11+ |
| 内存 | 2GB | 4GB+ |
| 磁盘 | 1GB（数据库约500MB） | SSD 2GB+ |
| 网络 | 能访问 push2his.eastmoney.com (HTTPS 443) | 稳定宽带 |

## 二、安装步骤

### 2.1 克隆项目

```bash
git clone <你的仓库地址> MyStockQuant
cd MyStockQuant
```

### 2.2 创建虚拟环境

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/macOS
source venv/bin/activate
```

### 2.3 安装依赖

```bash
pip install -r requirements.txt
```

核心依赖说明：

| 包名 | 用途 |
|------|------|
| fastapi + uvicorn | Web 服务框架 |
| pandas | 数据分析计算 |
| requests | HTTP 请求（东方财富API） |
| akshare | A股股票列表数据源 |
| schedule | 定时任务调度 |
| pydantic | API 参数校验 |
| lxml | akshare 底层解析依赖 |
| openpyxl | Excel 读写支持 |

## 三、首次初始化

### 方式一：上传本地数据库（推荐，5分钟搞定）

如果你本地已有完整的 `stock_quant.db` 文件，直接上传到服务器即可**跳过全量数据拉取**：

```bash
# 1. 创建数据目录并上传数据库文件
ssh user@server "mkdir -p /path/to/MyStockQuant/data"
scp data/stock_quant.db user@server:/path/to/MyStockQuant/data/

# 2. 登录服务器，只需同步一次 App 数据即可
python -c "from sync_app_data import sync_data_to_app_table; sync_data_to_app_table()"
```

上传后直接跳到「第四步：启动服务」。

### 方式二：从零拉取全量数据（约3-4小时）

如果无法获取本地数据库文件，需要依次执行以下步骤：

#### 3.1 构建股票基础信息表

```bash
python init_basic.py
```

功能：从 akshare 拉取全市场 A 股代码、名称、行业等基础信息，写入 `stock_basic` 表。

耗时：约 1-2 分钟。

### 3.2 构建概念板块映射（可选）

```bash
python init_concepts.py
```

功能：构建概念板块与股票的映射关系，写入 `stock_concept_mapping` 表。

耗时：约 2-5 分钟。

### 3.3 拉取全量历史K线数据

```bash
python data_fetcher.py
```

功能：使用东方财富 API 并发拉取全市场 A 股的历史 K 线数据（2023-01-01 至今），写入 `daily_k_line` 表。

技术特点：
- 3 线程并发拉取
- 每只股票自动重试 3 次（指数退避）
- 每 200 只批量写入数据库
- 支持增量模式（中断后重新运行会跳过已有数据）

耗时：首次约 2-3 小时（受 API 限流）。中断后重跑会自动增量补全。

### 3.4 全市场历史回溯扫描

```bash
python bootstrap_pipeline.py
```

功能：对全市场股票执行「倍量试盘 -> 缩量洗盘 -> 突破确认」三阶段形态识别，结果写入 `stock_pipeline` 表。

耗时：约 30-60 秒。

### 3.5 同步 App 端数据

```bash
python -c "from sync_app_data import sync_data_to_app_table; sync_data_to_app_table()"
```

功能：为 pipeline 中的股票计算最新价、涨跌幅、成交额等展示数据。

耗时：约 1 秒。

## 四、启动服务

### 4.1 标准启动

```bash
python server.py
```

启动后：
- Web 总控台：http://localhost:8000
- API 服务：http://localhost:8000/api/
- 定时任务：每天 18:30 自动执行数据更新 + 策略流转

### 4.2 后台运行（Linux）

```bash
# 使用 nohup
nohup python server.py > server.log 2>&1 &

# 或使用 systemd（推荐）
# 参见下方 4.4 节
```

### 4.3 后台运行（Windows）

```bash
# 使用 pythonw（无窗口）
pythonw server.py

# 或使用 NSSM 注册为 Windows 服务（推荐）
# 参见下方 4.5 节
```

### 4.4 Linux systemd 服务配置

创建服务文件 `/etc/systemd/system/stockquant.service`：

```ini
[Unit]
Description=MyStockQuant Server
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/path/to/MyStockQuant
ExecStart=/path/to/MyStockQuant/venv/bin/python server.py
Restart=always
RestartSec=10
StandardOutput=append:/var/log/stockquant.log
StandardError=append:/var/log/stockquant.log

[Install]
WantedBy=multi-user.target
```

启动命令：

```bash
sudo systemctl daemon-reload
sudo systemctl enable stockquant    # 开机自启
sudo systemctl start stockquant     # 启动服务
sudo systemctl status stockquant    # 查看状态
sudo journalctl -u stockquant -f    # 查看日志
```

### 4.5 Windows NSSM 服务配置

```powershell
# 下载 NSSM: https://nssm.cc/download
nssm install MyStockQuant "C:\path\to\MyStockQuant\venv\Scripts\python.exe" "C:\path\to\MyStockQuant\server.py"
nssm set MyStockQuant AppDirectory "C:\path\to\MyStockQuant"
nssm set MyStockQuant DisplayName "MyStockQuant 量化服务"
nssm set MyStockQuant Start SERVICE_AUTO_START
nssm start MyStockQuant
```

### 4.6 Docker 部署（推荐）

项目已包含 `Dockerfile` 和 `docker-compose.yml`，开箱即用。

**快速启动：**

```bash
# 1. 确保 stock_quant.db 在 data/ 目录下
ls data/stock_quant.db

# 2. 构建并启动
docker compose up -d --build

# 3. 查看日志
docker compose logs -f

# 4. 访问
# Web: http://localhost:8000
```

**常用命令：**

```bash
docker compose up -d          # 后台启动
docker compose down           # 停止服务
docker compose logs -f        # 查看实时日志
docker compose restart        # 重启服务
docker compose ps             # 查看运行状态
```

**数据库持久化：**

`docker-compose.yml` 已配置将宿主机的 `stock_quant.db` 挂载到容器内，容器重建不会丢失数据。

**在容器内执行维护命令：**

```bash
# 进入容器
docker exec -it stockquant bash

# 重建策略池
python bootstrap_pipeline.py

# 手动更新K线
python update_daily.py

# 同步App数据
python -c "from sync_app_data import sync_data_to_app_table; sync_data_to_app_table()"

# 退出容器
exit
```

## 五、数据库说明

### 5.1 数据库文件

| 文件 | 说明 | 大小 |
|------|------|------|
| `data/stock_quant.db` | SQLite 主数据库 | ~500MB |

### 5.2 核心表结构

| 表名 | 用途 | 行数（约） |
|------|------|-----------|
| `stock_basic` | 股票基础信息（代码/名称/行业） | 5500+ |
| `daily_k_line` | 全市场日K线行情 | 250万+ |
| `stock_pipeline` | 策略流水线（状态机） | 200-400 |
| `operation_log` | 操作审计日志 | 持续增长 |

### 5.3 状态机状态码

| 状态码 | 含义 | 说明 |
|--------|------|------|
| 0 | 初始 | 未进入策略跟踪 |
| 1 | 试盘池 | 倍量信号触发，观察期 |
| 2 | 回踩池 | 缩量洗盘确认 |
| 3 | 突破池 | 收盘站上标杆价 |
| 99 | 废弃池 | 形态破坏/超时/诱多 |

### 5.4 数据库维护

```bash
# 查看数据库大小
python -c "import os; print(f'{os.path.getsize(\"data/stock_quant.db\")/1024/1024:.1f} MB')"

# 手动执行 VACUUM 压缩数据库（释放已删除数据的空间）
python -c "import sqlite3; conn = sqlite3.connect('data/stock_quant.db'); conn.execute('VACUUM'); conn.close()"
```

## 六、API 接口一览

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | Web 总控台页面 |
| GET | `/api/task_status` | 任务执行状态 |
| GET | `/api/counts` | 各池股票数量 |
| GET | `/api/pool/{status}` | Web 端股票清单 |
| GET | `/api/pool/app/{status}` | App 端增强型清单 |
| GET | `/api/kline/{code}` | 单只股票历史K线 |
| POST | `/api/run_strategy` | 触发增量同步+策略流转 |
| POST | `/api/update_status` | 手动修改股票状态 |
| GET | `/api/logs` | 审计日志 |
| POST | `/api/reset_bootstrap` | 系统重置+全量回溯 |

## 七、定时任务

定时任务已集成在 `server.py` 中，服务启动时自动开启后台调度线程。

| 时间 | 任务 | 说明 |
|------|------|------|
| 每天 18:30 | 数据更新+策略流转 | 拉取K线 -> 策略计算 -> App同步 |

### 手动触发

通过 API 触发（等同定时任务）：

```bash
curl -X POST http://localhost:8000/api/run_strategy \
  -H "Content-Type: application/json" \
  -d "{\"source\": \"手动触发\"}"
```

## 八、App 端配置

### 8.1 服务器地址配置

App 启动后，在设置中填入后端服务地址：

- 本地测试：`http://localhost:8000`
- 局域网：`http://192.168.x.x:8000`（替换为服务器实际 IP）
- 公网：通过 ngrok/frp 等内网穿透工具暴露后，填入穿透地址

### 8.2 内网穿透（公网访问）

**方案一：ngrok**

```bash
ngrok http 8000
# 获得地址如 https://xxxx.ngrok-free.app
```

**方案二：frp**

```ini
# frpc.toml
serverAddr = "your_vps_ip"
serverPort = 7000

[[proxies]]
name = "stockquant"
type = "http"
localPort = 8000
customDomains = "your_domain.com"
```

### 8.3 App 缓存机制

- App 本地缓存有效期：4 小时
- 超过 4 小时自动清除缓存并重新拉取
- 手动刷新：点击顶部 ⟳ 按钮

## 九、日常运维

### 9.1 常用命令

```bash
# 启动服务
python server.py

# 查看数据库状态
python -c "
import sqlite3
conn = sqlite3.connect('stock_quant.db')
c = conn.cursor()
c.execute('SELECT COUNT(DISTINCT code) FROM daily_k_line')
print(f'K线股票数: {c.fetchone()[0]}')
c.execute('SELECT MAX(date) FROM daily_k_line')
print(f'最新日期: {c.fetchone()[0]}')
c.execute('SELECT status, COUNT(*) FROM stock_pipeline GROUP BY status')
for r in c.fetchall(): print(f'  status {r[0]}: {r[1]}')
conn.close()
"

# 手动增量更新K线
python update_daily.py

# 重建策略池（全量回溯）
python bootstrap_pipeline.py

# 同步App数据
python -c "from sync_app_data import sync_data_to_app_table; sync_data_to_app_table()"
```

### 9.2 日志查看

服务运行日志直接输出到控制台。如需保存到文件：

```bash
# Linux
python server.py 2>&1 | tee server.log

# Windows
python server.py > server.log 2>&1
```

审计日志（操作记录）存储在数据库 `operation_log` 表中，可通过 `/api/logs` 接口或 Web 端查看。

### 9.3 数据备份

```bash
# 备份数据库
copy data\stock_quant.db data\stock_quant_backup_20260525.db    # Windows
cp data/stock_quant.db data/stock_quant_backup_20260525.db      # Linux
```

### 9.4 端口修改

编辑 `server.py` 最后一行：

```python
uvicorn.run(app, host="0.0.0.0", port=8000)  # 修改 port 参数
```

### 9.5 定时任务时间修改

编辑 `server.py` 中的定时任务配置：

```python
schedule.every().day.at("18:30").do(daily_quant_job)  # 修改时间
```

## 十、故障排查

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| 启动报端口占用 | 8000 端口被占用 | `netstat -ano \| findstr :8000` 找到进程并终止 |
| K线数据为空 | 未执行 data_fetcher.py | 执行 `python data_fetcher.py` |
| API 请求超时 | 东方财富 API 限流 | 等待几分钟后重试，或降低并发数 |
| App 显示旧数据 | 本地缓存未过期 | 点击 ⟳ 手动刷新，或等待4小时自动刷新 |
| 策略池为空 | 未执行 bootstrap_pipeline.py | 执行 `python bootstrap_pipeline.py` |
| 数据库锁定 | 多进程同时写入 | SQLite WAL 模式已启用，正常情况不会发生 |

## 十一、项目文件结构

```
MyStockQuant/
├── server.py              # FastAPI 服务入口（含定时任务）
├── config.py              # 全局配置（DB路径 + 策略参数）
├── eastmoney_api.py       # 东方财富 API 封装（连接池+重试）
├── data_fetcher.py        # 全量历史K线拉取（并发版）
├── update_daily.py        # 智能增量K线更新
├── strategy_engine.py     # 日常状态机引擎
├── bootstrap_pipeline.py  # 全市场历史回溯扫描
├── sync_app_data.py       # App端数据同步
├── database_manager.py    # 数据库操作封装
├── requirements.txt       # Python 依赖
├── Dockerfile             # Docker 镜像构建文件
├── docker-compose.yml     # Docker Compose 编排文件
├── index.html             # Web 总控台前端
├── data/                  # 数据目录（Docker 挂载点）
│   └── stock_quant.db     # SQLite 数据库（~500MB）
└── _archive/              # 已废弃的旧脚本
```

## 十二、安全建议

1. **不要将服务直接暴露到公网**，使用 nginx 反向代理 + HTTPS
2. **定期备份数据库**，至少每周一次
3. **限制 API 访问**，如需公网访问建议加 IP 白名单或 Basic Auth
4. **监控磁盘空间**，数据库会随时间增长

---

*最后更新：2026-05-25*
