# Общие настройки задания 2: обменник, очереди отделов и подключение

import os
import pika

EXCHANGE = "price.topic"  # topic — сообщение уходит в очереди по ключу маршрутизации

# Очередь отдела и её ключ привязки: каждый отдел выбирает свой тип данных.
# В topic: * заменяет одно слово, # — любое количество слов.
DEPARTMENTS = {
    "price.accounting": "price.*",    # все продукты
    "price.oildepot": "price.Brent",  # только Brent
    "price.analytics": "price.#",     # все продукты
}


def connect():
    # адрес и логин берём из окружения, в Docker это имя сервиса
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "app"),
            os.getenv("RABBITMQ_PASS", "app"),
        ),
    ))


def declare_exchange(channel):
    # обменник durable — переживёт перезапуск брокера
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
