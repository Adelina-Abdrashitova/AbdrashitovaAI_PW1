# Базовый образ с Python 3.12
FROM python:3.12-slim

# Логи сразу в консоль
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app