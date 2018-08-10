import logging
from logging.handlers import QueueHandler
from queue import Empty, Queue

from cml_pipelines.log import ZMQLogHandler, LogConsumerThread


def test_logging():
    address = "tcp://127.0.0.1:9898"
    handler = ZMQLogHandler(address)

    producer = logging.getLogger("producer")
    producer.setLevel(logging.INFO)
    producer.addHandler(handler)

    queue = Queue()
    q_handler = QueueHandler(queue)

    consumer = logging.getLogger("consumer")
    consumer.setLevel(logging.INFO)
    consumer.addHandler(q_handler)

    thread = LogConsumerThread("consumer", address)

    thread.start()
    thread._ready.wait()

    producer.info("info")
    producer.warning("warning")
    producer.error("error")

    items = []

    for _ in range(10):
        if len(items) is 3:
            break
        try:
            items.append(queue.get(timeout=0.05))
        except Empty:
            continue

    thread.stop()
    thread.join()

    assert len(items) == 3
