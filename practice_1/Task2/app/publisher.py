# Публикует изменение цены в fanout-обменник и завершается

import json
import time
import uuid

import pika

from .common import EXCHANGE, QUEUES, connect, declare_exchange


class Publisher:
    def __init__(self, prices=None, delay=10):
        self.prices = prices or [60.4, 65.1, 70.8, 71.6]
        self.delay = delay
        self.channel = connect().channel()

    def run(self):
        # Обменник и очереди объявляем заранее: сообщение не должно уйти в пустоту.
        declare_exchange(self.channel)
        for queue in QUEUES:
            self.channel.queue_declare(queue=queue, durable=True)
            self.channel.queue_bind(queue=queue, exchange=EXCHANGE)  # привязка к fanout

        for i in range(1, len(self.prices)):
            msg = {
                "event_id": str(uuid.uuid4()),
                "product": "Brent",
                "old_price": self.prices[i - 1],
                "new_price": self.prices[i],
            }
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
            routing_key="",  # у fanout ключ маршрутизации не учитывается
            body=json.dumps(msg, ensure_ascii=False).encode(),  # кириллица как есть, в байтах
            properties=pika.BasicProperties(delivery_mode=2),  # 2 — хранить на диске
        )


if __name__ == "__main__":
    Publisher().run()
