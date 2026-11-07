# Подписчик: слушает свою очередь и печатает каждое пришедшее событие

import json
import os
import time

from .common import EXCHANGE, connect, declare_exchange


class Consumer:
    def __init__(self):
        self.name = os.getenv("DEPT_NAME", "отдел")        # Бухгалтерия / Нефтебаза / Аналитика
        self.queue = os.getenv("QUEUE", "price.default")   # своя очередь у каждого отдела
        self.channel = connect().channel()

    def run(self):
        # Отдельная очередь на отдел, привязанная к общему fanout — копия достанется всем.
        declare_exchange(self.channel)
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.channel.queue_bind(queue=self.queue, exchange=EXCHANGE)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self._on_message)
        print(f"{time.strftime('%H:%M:%S')} [{self.name}] ждёт новости...", flush=True)
        self.channel.start_consuming()

    def _on_message(self, ch, method, properties, body):
        event = json.loads(body)
        print(
            f"{time.strftime('%H:%M:%S')} "
            f"[{self.name}] {event['product']}: "
            f"{event['old_price']} -> {event['new_price']}",
            flush=True,
        )
        ch.basic_ack(method.delivery_tag)  # подтверждаем приём — брокер удалит сообщение


if __name__ == "__main__":
    Consumer().run()
