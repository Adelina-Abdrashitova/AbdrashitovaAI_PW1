# Агрегатор: копит расчёты по заявке и отправляет итоговую смету

import json
import time

import pika

from ..common import QUEUE_AGGREGATOR, QUEUE_REPLY, WORK_QUEUES, connect, declare_topology


class Aggregator:
    def __init__(self):
        # соединение с брокером и канал для чтения результатов и отправки смет
        self.channel = connect().channel()
        # накопленные результаты по заявкам: order_id - {"well_name", "works"}
        self.pending = {}

    def run(self):
        # объявляем обменники и очереди: если их ещё нет, брокер создаст их
        declare_topology(self.channel)
        # prefetch_count=1 — брокер не выдаст больше одного результата за раз
        self.channel.basic_qos(prefetch_count=1)
        # читаем очередь агрегатора; на каждое сообщение вызовется _on_result
        self.channel.basic_consume(queue=QUEUE_AGGREGATOR, on_message_callback=self._on_result)
        print(f"{time.strftime('%H:%M:%S')} [aggregator] ждёт расчёты...", flush=True)
        # блокирующий цикл: забирает сообщения из очереди и отдаёт их в callback
        self.channel.start_consuming()

    def _on_result(self, ch, method, properties, body):
        # body — байты, поэтому сначала разбираем JSON
        result = json.loads(body)
        order_id = result["order_id"]

        # первая работа по заявке создаёт запись, остальные дописываются в неё
        entry = self.pending.setdefault(order_id, {
            "well_name": result["well_name"],
            "works": {},  # вид работ - результат расчёта
        })
        entry["works"][result["work"]] = result

        # смета готова только когда пришли все виды работ
        if len(entry["works"]) < len(WORK_QUEUES):
            # подтверждаем приём и ждём остальные работы
            ch.basic_ack(method.delivery_tag)
            return

        self._send_summary(ch, order_id, entry)
        # заявка закрыта — освобождаем память
        del self.pending[order_id]
        # смета отправлена — подтверждаем брокеру обработку результата
        ch.basic_ack(method.delivery_tag)

    def _send_summary(self, ch, order_id, entry):
        # смета: общая стоимость и срок плюс подробности по каждой работе
        summary = {
            "order_id": order_id,
            "well_name": entry["well_name"],
            "total_cost": sum(r["cost"] for r in entry["works"].values()),
            "total_days": sum(r["days"] for r in entry["works"].values()),
            "works": entry["works"],
        }
        ch.basic_publish(
            exchange="",             # пустая строка — публикация напрямую в очередь
            routing_key=QUEUE_REPLY,  # имя очереди, куда кладём смету
            body=json.dumps(summary, ensure_ascii=False).encode(),
            # delivery_mode=2 — persistent: смета дождётся клиента после перезапуска
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"{time.strftime('%H:%M:%S')} "
              f"[aggregator] смета по {summary['well_name']}: "
              f"{summary['total_cost']} руб., {summary['total_days']} дн.", flush=True)


if __name__ == "__main__":
    Aggregator().run()
