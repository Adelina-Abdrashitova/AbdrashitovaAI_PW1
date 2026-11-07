# Общие настройки задания 1: очередь и подключение к брокеру

import os
import pika

QUEUE = "well.repair.q"  # одна очередь на всех — схема "точка-точка"
DLX = "well.repair.dlx"  # обменник для недоставленных заявок
DEAD_QUEUE = "well.repair.dead"  # очередь, где лежат неудачные заявки


def connect():
    # адрес и логин берём из окружения, в Docker это имя сервиса
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "app"),
            os.getenv("RABBITMQ_PASS", "app"),
        ),
    ))


def declare_topology(channel):
    # Очередь заявок при ошибке бригады отправляет сообщение в DLQ.
    channel.exchange_declare(exchange=DLX, exchange_type="direct", durable=True)  # DLX
    channel.queue_declare(queue=DEAD_QUEUE, durable=True)  # очередь разбора
    channel.queue_bind(queue=DEAD_QUEUE, exchange=DLX, routing_key=DEAD_QUEUE)  # DLX → очередь
    channel.queue_declare(
        queue=QUEUE,
        durable=True,
        arguments={
            # Эти аргументы и включают механизм dead letter.
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": DEAD_QUEUE,
        },
    )
