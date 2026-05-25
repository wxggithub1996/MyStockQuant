"""
东方财富 K线数据 API 封装
支持并发请求 + 连接池复用 + 自动重试
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 全局 Session (连接池复用，避免每次请求都新建TCP)
_session = None
_session_lock = threading.Lock()

def _get_session():
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                _session = requests.Session()
                retry = Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
                adapter = HTTPAdapter(max_retries=retry, pool_maxsize=10)
                _session.mount('https://', adapter)
                _session.mount('http://', adapter)
    return _session

import time as _time

def fetch_kline(code, start_date, end_date, max_retries=3):
    """
    拉取单只股票的历史K线数据 (含自动重试 + 指数退避)
    返回: [(date, code, open, high, low, close, volume, amount, turn, pctChg), ...]
    """
    secid = f"1.{code}" if code.startswith('6') else f"0.{code}"
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        'secid': secid,
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': '101',
        'fqt': '1',
        'beg': start_date.replace('-', ''),
        'end': end_date.replace('-', ''),
        'lmt': '10000',
    }
    session = _get_session()

    for attempt in range(max_retries):
        try:
            r = session.get(url, params=params, timeout=20)
            data = r.json()
            klines = data.get('data', {}).get('klines', []) if data.get('data') else []
            result = []
            for line in klines:
                parts = line.split(',')
                if len(parts) >= 11:
                    result.append((
                        parts[0], code,
                        float(parts[1]), float(parts[3]), float(parts[4]), float(parts[2]),
                        float(parts[5]), float(parts[6]), float(parts[10]), float(parts[8]),
                    ))
            return result
        except Exception:
            if attempt < max_retries - 1:
                _time.sleep(1.5 * (attempt + 1))
    return []

def fetch_kline_batch(codes_dates, max_workers=5, progress_callback=None):
    """
    并发批量拉取K线数据
    codes_dates: [(code, start_date, end_date), ...]
    返回: {code: [(date, code, open, high, low, close, volume, amount, turn, pctChg), ...]}
    """
    results = {}
    done_count = 0
    total = len(codes_dates)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}
        for code, start_date, end_date in codes_dates:
            future = executor.submit(fetch_kline, code, start_date, end_date)
            future_map[future] = code

        for future in as_completed(future_map):
            code = future_map[future]
            done_count += 1
            try:
                data = future.result()
                if data:
                    results[code] = data
            except Exception:
                pass

            if progress_callback and done_count % 100 == 0:
                progress_callback(done_count, total)

    return results
