# Клиент: оформляет заявки и принимает сводные сметы

import json
import time
import uuid

import pika

from .common import EXCHANGE_IN, QUEUE_REPLY, connect, declare_topology

ORDERS = [
    {
        "order_id": str(uuid.uuid4()),  # уникальный номер заявки, по нему собирается смета
        "well_name": "W-101",
    },
    {
        "order_id": str(uuid.uuid4()),
        "well_name": "W-102",
    },
    {
        "order_id": str(uuid.uuid4()),
        "well_name": "W-103",
    },
]


class Client:
    def __init__(self):
        # открываем соединение с брокером и берём канал для публикаций и подписки
        self.channel = connect().channel()

    def run(self):
        # объявляем обменники и очереди: если их ещё нет, брокер создаст их
        declare_topology(self.channel)

        for order in ORDERS:
            self.send_order(order)
            time.sleep(3)  # пауза между заявками

        print(f"{time.strftime('%H:%M:%S')} [клиент] ждёт сводные сметы...", flush=True)
        # подписываемся на очередь ответов: сюда агрегатор кладёт готовые сметы
        self.channel.basic_consume(queue=QUEUE_REPLY, on_message_callback=self._on_summary)
        # блокирующий цикл: забирает сообщения из очереди и отдаёт их в callback
        self.channel.start_consuming()

    def send_order(self, order):
        self.channel.basic_publish(
            exchange=EXCHANGE_IN,          # публикуем в обменник заявок
            routing_key="repair.request",  # с этим ключом заявка попадёт в splitter.q
            body=json.dumps(order, ensure_ascii=False).encode(),  # тело — JSON в байтах
            # сообщение сохранится на диске брокера
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"{time.strftime('%H:%M:%S')} "
              f"[клиент] заявка на {order['well_name']} отправлена", flush=True)

    def _on_summary(self, ch, method, properties, body):
        # body — байты, поэтому сначала разбираем JSON
        summary = json.loads(body)
        print(f"{time.strftime('%H:%M:%S')} "
              f"[клиент] смета по {summary['well_name']}: "
              f"{summary['total_cost']} руб., {summary['total_days']} дн.", flush=True)
        # подтверждает брокеру, что сообщение обработано и его можно удалить
        ch.basic_ack(method.delivery_tag)


if __name__ == "__main__":
    Client().run()
