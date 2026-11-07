# Маршрутизатор: направляет работу в очередь нужного исполнителя

import json
import time

import pika

from ..common import EXCHANGE_WORK, QUEUE_ROUTER, connect, declare_topology


class Router:
    def __init__(self):
        # соединение с брокером и канал для чтения задач и их пересылки
        self.channel = connect().channel()

    def run(self):
        # объявляем обменники и очереди: если их ещё нет, брокер создаст их
        declare_topology(self.channel)
        # брокер не выдаст больше одной задачи за раз
        self.channel.basic_qos(prefetch_count=1)
        # читаем очередь маршрутизатора; на каждое сообщение вызовется _on_task
        self.channel.basic_consume(queue=QUEUE_ROUTER, on_message_callback=self._on_task)
        print(f"{time.strftime('%H:%M:%S')} [router] ждёт работы...", flush=True)
        # блокирующий цикл: забирает сообщения из очереди и отдаёт их в callback
        self.channel.start_consuming()

    def _on_task(self, ch, method, properties, body):
        # body — байты, поэтому сначала разбираем JSON
        task = json.loads(body)
        work = task["work"]

        ch.basic_publish(
            exchange=EXCHANGE_WORK,       # direct-обменник сравнивает ключ с привязками
            routing_key=f"work.{work}",   # например work.drilling -> work.drilling.q
            body=json.dumps(task, ensure_ascii=False).encode(),  # пересылаем задачу как есть
            # delivery_mode=2 — persistent: задача не потеряется при перезапуске
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"{time.strftime('%H:%M:%S')} "
              f"[router] {task['well_name']} -> work.{work}", flush=True)

        # задача переслана — подтверждаем брокеру обработку
        ch.basic_ack(method.delivery_tag)


if __name__ == "__main__":
    Router().run()
