# Разделитель: превращает заявку в набор работ по всем видам

import json
import time

import pika

from ..common import EXCHANGE_SPLIT, QUEUE_SPLITTER, WORK_QUEUES, connect, declare_topology


class Splitter:
    def __init__(self):
        # соединение с брокером и канал для чтения заявок и публикации задач
        self.channel = connect().channel()

    def run(self):
        # объявляем обменники и очереди: если их ещё нет, брокер создаст их
        declare_topology(self.channel)
        # брокер не выдаст больше одной заявки за раз
        self.channel.basic_qos(prefetch_count=1)
        # читаем очередь разделителя; на каждое сообщение вызовется _on_order
        self.channel.basic_consume(queue=QUEUE_SPLITTER, on_message_callback=self._on_order)
        print(f"{time.strftime('%H:%M:%S')} [splitter] ждёт заявки...", flush=True)
        # блокирующий цикл: забирает сообщения из очереди и отдаёт их в callback
        self.channel.start_consuming()

    def _on_order(self, ch, method, properties, body):
        # body — байты, поэтому сначала разбираем JSON
        order = json.loads(body)

        # скважину обслуживают полностью, поэтому работ всегда столько же,
        # сколько видов работ в WORK_QUEUES
        for work in WORK_QUEUES:
            self._send_task(ch, order, work)

        # заявка разобрана целиком — подтверждаем брокеру обработку
        ch.basic_ack(method.delivery_tag)

    def _send_task(self, ch, order, work):
        # в каждой задаче едет номер заявки, имя скважины и вид работ
        task = {
            "order_id": order["order_id"],
            "well_name": order["well_name"],
            "work": work,
        }
        ch.basic_publish(
            exchange=EXCHANGE_SPLIT,  # fanout-обменник: задачу получат все привязки
            routing_key="",           # fanout игнорирует ключ маршрутизации
            body=json.dumps(task, ensure_ascii=False).encode(),
            # задача не потеряется при перезапуске
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"{time.strftime('%H:%M:%S')} "
              f"[splitter] {order['well_name']}: {work}", flush=True)


if __name__ == "__main__":
    Splitter().run()
