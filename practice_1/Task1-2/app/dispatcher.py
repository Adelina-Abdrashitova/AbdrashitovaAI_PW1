# Формирует заявки и отправляет их в очередь

import json
import time
import uuid

import pika

from .common import QUEUE, connect, declare_topology


class Dispatcher:
    def __init__(self, count=10, delay=5):
        self.count = count
        self.delay = delay
        self.channel = connect().channel()
        declare_topology(self.channel)  # заранее создаём очередь с DLX

    def run(self):
        for i in range(self.count):
            request = {
                "request_id": str(uuid.uuid4()),
                "well_id": f"W-{101 + i}",
                "force_fail": i == 4,  # одна заявка для демонстрации DLQ
            }

            print(f"{time.strftime('%H:%M:%S')} "
                  f"[диспетчер] заявка на {request['well_id']} отправлена", flush=True)
            self.channel.basic_publish(
                exchange="",            # обменник по умолчанию — публикуем прямо в очередь
                routing_key=QUEUE,      # здесь это имя очереди
                body=json.dumps(request).encode(),  # тело сообщения передаём байтами
                properties=pika.BasicProperties(delivery_mode=2),  # 2 — сообщение переживёт рестарт
            )

            time.sleep(self.delay)


if __name__ == "__main__":
    Dispatcher().run()
