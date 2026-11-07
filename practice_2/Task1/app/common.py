# Общие настройки: адрес брокера и схема обменников с очередями

import os
import pika

# обменник для заявок: клиент публикует сюда, разделитель читает из очереди
EXCHANGE_IN = "repair.in"
# fanout-обменник: каждую задачу получают все привязанные к нему очереди
EXCHANGE_SPLIT = "repair.split"
# direct-обменник: работа уходит в очередь того исполнителя, чей ключ совпал
EXCHANGE_WORK = "repair.work"
# fanout-обменник для результатов: расчёт попадает и агрегатору, и в другие привязки
EXCHANGE_RESULTS = "repair.results"

# очередь, из которой читает разделитель
QUEUE_SPLITTER = "splitter.q"
# очередь, из которой читает маршрутизатор
QUEUE_ROUTER = "router.q"
# очередь, из которой читает агрегатор
QUEUE_AGGREGATOR = "aggregator.q"
# очередь, в которую агрегатор кладёт готовые сметы, а клиент их читает
QUEUE_REPLY = "reply.q"

# вид работ - очередь исполнителя; по этому словарю работают и раздача, и проверка в агрегаторе
WORK_QUEUES = {
    "drilling": "work.drilling.q",
    "electrical": "work.electrical.q",
    "logistics": "work.logistics.q",
}


def connect():
    # создаём соединение с брокером; адрес и логин берём из переменных окружения,
    # чтобы в docker-compose указать имя сервиса rabbitmq
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "app"),
            os.getenv("RABBITMQ_PASS", "app"),
        ),
    ))


def declare_topology(channel):
    # объявляем обменники; durable=True — они переживут перезапуск брокера
    channel.exchange_declare(exchange=EXCHANGE_IN, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_SPLIT, exchange_type="fanout", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_WORK, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=EXCHANGE_RESULTS, exchange_type="fanout", durable=True)

    # очередь разделителя и привязка к обменнику заявок по ключу repair.request
    channel.queue_declare(queue=QUEUE_SPLITTER, durable=True)
    channel.queue_bind(queue=QUEUE_SPLITTER, exchange=EXCHANGE_IN, routing_key="repair.request")

    # очередь маршрутизатора; у fanout привязка без ключа — получает всё подряд
    channel.queue_declare(queue=QUEUE_ROUTER, durable=True)
    channel.queue_bind(queue=QUEUE_ROUTER, exchange=EXCHANGE_SPLIT)

    # по очереди на каждый вид работ; ключ привязки work.<вид работ>
    for work, queue in WORK_QUEUES.items():
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(queue=queue, exchange=EXCHANGE_WORK, routing_key=f"work.{work}")

    # очередь агрегатора (все результаты) и очередь ответов для клиента
    channel.queue_declare(queue=QUEUE_AGGREGATOR, durable=True)
    channel.queue_bind(queue=QUEUE_AGGREGATOR, exchange=EXCHANGE_RESULTS)
    channel.queue_declare(queue=QUEUE_REPLY, durable=True)
