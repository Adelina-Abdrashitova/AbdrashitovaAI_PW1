# Общие настройки задания 1: очередь и подключение к брокеру

import os
import pika

QUEUE = "well.repair.q"  # одна очередь на всех — схема "точка-точка"


def connect():
    # адрес и логин берём из окружения, в Docker это имя сервиса
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "app"),
            os.getenv("RABBITMQ_PASS", "app"),
        ),
    ))
