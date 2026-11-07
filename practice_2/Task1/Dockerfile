# Базовый образ с Python 3.12 (slim — без лишних пакетов)
FROM python:3.12-slim

# Логи сразу в консоль, без буферизации
ENV PYTHONUNBUFFERED=1

# Рабочая папка внутри контейнера
WORKDIR /app

# Сначала зависимости — этот слой кешируется
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Затем код пакета
COPY app ./app

