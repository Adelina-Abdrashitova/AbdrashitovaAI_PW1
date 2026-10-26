# Разбирает заявки, которые бригадам не удалось выполнить

import json
import os
import time

from .common import DEAD_QUEUE, connect, declare_topology


class DeadLetterConsumer:
    def __init__(self):
        self.name = os.getenv("DLQ_NAME", "разбор-заявок")
        self.channel = connect().channel()

    def run(self):
        declare_topology(self.channel)  # DLX и очередь разбора должны существовать
        self.channel.basic_qos(prefetch_count=1)  # берём по одной заявке
        self.channel.basic_consume(queue=DEAD_QUEUE, on_message_callback=self._on_message)
        print(f"{time.strftime('%H:%M:%S')} "
              f"[{self.name}] ждёт недоставленные заявки...", flush=True)
        self.channel.start_consuming()

    def _on_message(self, ch, method, properties, body):
        request = json.loads(body)
        # RabbitMQ сам добавляет заголовок x-death с причиной перемещения в DLQ.
        deaths = (properties.headers or {}).get("x-death", [])
        reason = deaths[0].get("reason", "unknown") if deaths else "unknown"
        print(f"{time.strftime('%H:%M:%S')} "
              f"[{self.name}] недоставленная заявка на {request.get('well_id')} "
              f"(причина: {reason})", flush=True)
        ch.basic_ack(method.delivery_tag)  # заявка разобрана — удаляем из DLQ


if __name__ == "__main__":
    DeadLetterConsumer().run()
