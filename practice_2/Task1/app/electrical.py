# Исполнитель по электрике: считает стоимость и срок работ

import json
import random
import time

import pika

from .common import EXCHANGE_RESULTS, WORK_QUEUES, connect, declare_topology


class Electrical:
    WORK = "electrical"        # вид работ, он же ключ в WORK_QUEUES
    COST_PER_DAY = 20000       # стоимость одного дня электромонтажа
    MIN_DAYS = 1               # минимальный срок работ
    MAX_DAYS = 15              # максимальный срок работ
    DELAY = 2                  # пауза, имитирующая расчёт

    def __init__(self):
        # соединение с брокером и канал для чтения задач и публикации результатов
        self.channel = connect().channel()
        # своя очередь: брокер выдаёт из неё задачи только этому исполнителю
        self.queue = WORK_QUEUES[self.WORK]

    def run(self):
        # объявляем обменники и очереди: если их ещё нет, брокер создаст их
        declare_topology(self.channel)
        # prefetch_count=1 — брокер не выдаст больше одной задачи за раз
        self.channel.basic_qos(prefetch_count=1)
        # читаем свою очередь; на каждую задачу вызовется _on_task
        self.channel.basic_consume(queue=self.queue, on_message_callback=self._on_task)
        print(f"{time.strftime('%H:%M:%S')} [{self.WORK}] ждёт работы...", flush=True)
        # блокирующий цикл: забирает сообщения из очереди и отдаёт их в callback
        self.channel.start_consuming()

    def _on_task(self, ch, method, properties, body):
        # body — байты, поэтому сначала разбираем JSON
        task = json.loads(body)
        print(f"{time.strftime('%H:%M:%S')} "
              f"[{self.WORK}] {task['well_name']} в работе", flush=True)

        time.sleep(self.DELAY)  # имитация расчёта
        days = random.randint(self.MIN_DAYS, self.MAX_DAYS)  # случайный срок

        # результат расчёта: номер заявки нужен агрегатору для сборки сметы
        result = {
            "order_id": task["order_id"],
            "well_name": task["well_name"],
            "work": self.WORK,
            "cost": self.COST_PER_DAY * days,
            "days": days,
        }
        ch.basic_publish(
            exchange=EXCHANGE_RESULTS,  # fanout-обменник: результат получат все привязки
            routing_key="",             # fanout игнорирует ключ маршрутизации
            body=json.dumps(result, ensure_ascii=False).encode(),
            # результат не потеряется при перезапуске
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"{time.strftime('%H:%M:%S')} "
              f"[{self.WORK}] {task['well_name']}, Стоимость: {result["cost"]}, Дней: {result["days"]}", flush=True)
        # результат отправлен — подтверждаем брокеру обработку задачи
        ch.basic_ack(method.delivery_tag)


if __name__ == "__main__":
    Electrical().run()
