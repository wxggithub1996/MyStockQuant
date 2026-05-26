FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 安装依赖（利用缓存层）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 复制项目文件
COPY . .

# 暴露端口
EXPOSE 8000

# 启动服务
CMD ["python", "server.py"]
