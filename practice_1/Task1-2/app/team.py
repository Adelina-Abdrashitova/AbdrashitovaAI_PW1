# Исполнитель: берёт заявку из общей очереди и подтверждает выполнение

import json
import os
import time

from .common import QUEUE, connect, declare_topology


class Team:
    def __init__(self):
        self.name = os.getenv("TEAM_NAME", "команда")  # подставляется из docker-compose
        self.channel = connect().channel()

    def run(self):
        declare_topology(self.channel)  # подключаемся к очереди с настроенным DLX
        self.channel.basic_qos(prefetch_count=1)  # одну заявку за раз, без запаса
        self.channel.basic_consume(queue=QUEUE, on_message_callback=self._on_message)
        print(f"{time.strftime('%H:%M:%S')} "
              f"[{self.name}] ждёт заявки...", flush=True)
        self.channel.start_consuming()

    def _on_message(self, ch, method, properties, body):
        request = json.loads(body)
        well_id = request["well_id"]
        # Имитируем сбой ремонта — заявка должна попасть в DLQ, а не потеряться.
        if request.get("force_fail"):
            print(f"{time.strftime('%H:%M:%S')} "
                  f"[{self.name}] не смогла выполнить {well_id} — заявка в DLQ", flush=True)
            ch.basic_nack(method.delivery_tag, requeue=False)  # без requeue — уйдёт в DLQ
            return
        print(f" {time.strftime('%H:%M:%S')} "
              f"[{self.name}] взяла заявку на {well_id}", flush=True)
        time.sleep(7)  # пауза вместо реального ремонта
        ch.basic_ack(method.delivery_tag)  # подтверждаем — брокер удалит сообщение
        print(f" {time.strftime('%H:%M:%S')} "
              f"[{self.name}] закончила {well_id}", flush=True)


if __name__ == "__main__":
    Team().run()
