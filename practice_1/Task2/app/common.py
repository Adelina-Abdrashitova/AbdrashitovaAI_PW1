# Общие настройки задания 2: обменник, очереди отделов и подключение

import os
import pika

EXCHANGE = "price.fanout"  # fanout — одно сообщение расходится копиями по всем очередям

# Очереди отделов объявляет и издатель: если отдел ещё не подключился,
# сообщение всё равно попадёт в очередь и дождётся его.
QUEUES = [
    "price.accounting",
    "price.oildepot",
    "price.analytics",
]


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
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)
