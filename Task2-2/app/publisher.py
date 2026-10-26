# Публикует изменение цены в topic-обменник и завершается

import json
import time
import uuid

import pika

from .common import DEPARTMENTS, EXCHANGE, connect, declare_exchange


class Publisher:
    def __init__(self, events=None, delay=10):
        # События разных типов: Brent и Сubic_meter
        self.events = events or [
            {"product": "Brent", "old_price": 60.4, "new_price": 65.1},
            {"product": "Сubic_meter", "old_price": 55.2, "new_price": 58.7},
            {"product": "Brent", "old_price": 65.1, "new_price": 70.8},
        ]
        self.delay = delay
        self.channel = connect().channel()

    def run(self):
        # Обменник и очереди объявляем заранее: сообщение не должно уйти в пустоту
        declare_exchange(self.channel)
        for queue, binding in DEPARTMENTS.items():
            self.channel.queue_declare(queue=queue, durable=True)
            # Каждый отдел привязывает свою очередь к нужному типу данных.
            self.channel.queue_bind(queue=queue, exchange=EXCHANGE, routing_key=binding)

        for event in self.events:
            msg = {"event_id": str(uuid.uuid4()), **event}  # добавляем id события
            self.send_msg(msg)
            time.sleep(self.delay)

    def send_msg(self, msg):
        print(
            f"{time.strftime('%H:%M:%S')} "
            f"[издатель] {msg['product']}: "
            f"{msg['old_price']} -> {msg['new_price']}",
            flush=True,
        )
        self.channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=f"price.{msg['product']}",  # тип данных в ключе маршрутизации
            body=json.dumps(msg, ensure_ascii=False).encode(),  # кириллица как есть, в байтах
            properties=pika.BasicProperties(delivery_mode=2),  # 2 — хранить на диске
        )


if __name__ == "__main__":
    Publisher().run()
