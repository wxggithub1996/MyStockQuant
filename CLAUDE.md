# MyStockQuant V3.0 - A股技术形态扫描与跟踪系统

## 项目概述

面向A股个人投资者的全自动股票技术形态扫描与跟踪系统。核心定位是**形态候选池雷达系统**，非自动交易系统。
策略模型：**倍量试盘 -> 缩量洗盘 -> 突破确认** 三级状态机流转。

## 技术栈

- Python 3.8+ / FastAPI / Uvicorn / SQLite3
- 数据源: BaoStock (日K线) + AkShare (股票列表/概念板块)
- 前端: 原生 HTML + ECharts 5.5.0 (index.html)
- 定时任务: schedule 库

## 核心文件

| 文件 | 职责 |
|------|------|
| `config.py` | 全局配置: DB路径 + 16个策略参数 |
| `server.py` | FastAPI 后端服务 (核心入口，含所有API) |
| `strategy_engine.py` | 日常增量状态机引擎 |
| `bootstrap_pipeline.py` | 全市场历史回溯扫描引擎 (V3.0核心) |
| `update_daily.py` | 智能分级增量K线更新 (VIP池优先) |
| `data_fetcher.py` | 全量历史K线数据拉取 |
| `database_manager.py` | SQLite 数据库操作封装 |
| `sync_app_data.py` | App端数据预装配同步 |
| `auto_task.py` | 定时任务调度 |
| `index.html` | Web总控台前端 (44KB, ECharts K线图) |

## 数据库 (stock_quant.db, ~494MB)

- `daily_k_line`: 全市场日K线行情 (code, date, OHLCV, amount, turn, pctChg)
- `stock_pipeline`: 策略流水线 (code主键, status状态码, test_high标杆价, entry_date等)
- `operation_log`: 审计日志 (自动/按钮/手工三种来源)
- `stock_basic`: 股票基础信息 (行业/市值)
- `stock_concept_mapping`: 概念板块映射

## 状态机状态码

- `0`: 初始 (未进入策略跟踪)
- `1`: 试盘池 (倍量信号触发，观察期)
- `2`: 回踩池 (缩量洗盘确认)
- `3`: 突破池 (收盘站上标杆价)
- `99`: 废弃池 (形态破坏/超时/诱多)

## API 端点 (server.py)

- `GET /` - Web总控台页面
- `GET /api/task_status` - 任务执行状态
- `GET /api/counts` - 各池股票数量
- `GET /api/pool/{status}` - Web端股票清单
- `GET /api/pool/app/{status}` - App端增强型清单 (BFF)
- `GET /api/kline/{code}` - 全量历史K线
- `POST /api/run_strategy` - 触发增量同步+策略流转
- `POST /api/update_status` - 手动修改状态 (上帝模式)
- `GET /api/logs` - 审计日志
- `POST /api/reset_bootstrap` - 系统重置+全量回溯

## 运行方式

```bash
# 启动服务
python server.py
# 或
uvicorn server:app --host 0.0.0.0 --port 8000

# 首次初始化
python init_basic.py        # 构建股票基础信息表
python init_concepts.py     # 构建概念板块映射
python data_fetcher.py      # 拉取全量历史K线
python bootstrap_pipeline.py # 全市场回溯扫描
```

## 代码规范

- 中文注释和日志
- SQLite 单文件数据库，无ORM
- FastAPI 使用 Pydantic 校验
- 全局任务互斥锁 (threading.Lock) 防并发
- 所有状态变更写入 operation_log 审计

## 关联项目

- `C:\software\0develop\StockQuantAPP` - UniApp 移动端 (纯视图层，调用本项目API)
