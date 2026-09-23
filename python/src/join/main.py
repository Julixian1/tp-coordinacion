import os
import logging
import bisect
import signal
import sys

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.fruit_top_by_client = {}
        self.count_by_client = {}
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info(
            "[JoinFilter] SIGTERM/SIGINT received. Closing connections gracefully..."
        )
        try:
            self.input_queue.close()
            self.output_queue.close()
        except Exception as e:
            logging.error(f"[JoinFilter] Error closing connections: {e}")
        sys.exit(0)

    def _process_client_top(self, client_id, partial_top):
        if client_id not in self.fruit_top_by_client:
            self.fruit_top_by_client[client_id] = []

        client_top = self.fruit_top_by_client[client_id]

        for fruit, amount in partial_top:
            amount = int(amount)
            found = False
            for i in range(len(client_top)):
                if client_top[i].fruit == fruit:
                    item_actual = client_top.pop(i)
                    nuevo_item = item_actual + fruit_item.FruitItem(fruit,amount)
                    bisect.insort(client_top,nuevo_item)
                    found = True
                    break
            if not found:
                bisect.insort(client_top,fruit_item.FruitItem(fruit,amount))

        self.count_by_client[client_id] = self.count_by_client.get(client_id,0) + 1
        current_count = self.count_by_client[client_id]

        logging.info(
            f"[JoinFilter] Received {current_count}/{AGGREGATION_AMOUNT} responses for client {client_id}"
        )

        if current_count == AGGREGATION_AMOUNT:
            fruit_chunk = list(client_top[-TOP_SIZE:])
            fruit_chunk.reverse()

            final_top = [(item.fruit, item.amount) for item in fruit_chunk]
            payload = [client_id] + final_top

            logging.info(f"[JoinFilter] Emitting final top for client {client_id}")
            self.output_queue.send(message_protocol.internal.serialize(payload))

            del self.fruit_top_by_client[client_id]
            del self.count_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("[JoinFilter] Received message")
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        partial_top = fields[1:]
        self._process_client_top(client_id, partial_top)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
