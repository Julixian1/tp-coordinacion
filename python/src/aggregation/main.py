import os
import logging
import bisect
import signal
import sys

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.eof_count_by_client = {}
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info(
            f"[AggregationFilter {ID}] SIGTERM received. Closing connections..."
        )
        try:
            self.input_exchange.stop_consuming()
        except Exception as e:
            logging.error(f"[AggregationFilter {ID}] Error stopping consumption: {e}")

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"[Client {client_id}]Processing data message")
        if client_id not in self.fruit_top_by_client:
            self.fruit_top_by_client[client_id] = []

        client_top = self.fruit_top_by_client[client_id]
        amount = int(amount)
        for i in range(len(client_top)):
            if client_top[i].fruit == fruit:
                item_actual = client_top.pop(i)
                nuevo_item = item_actual + fruit_item.FruitItem(fruit, amount)
                bisect.insort(client_top, nuevo_item)
                return

        bisect.insort(client_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info(f"[Client {client_id}] Received EOF")
        self.eof_count_by_client[client_id] = self.eof_count_by_client.get(client_id, 0) + 1
        current_eofs = self.eof_count_by_client[client_id]

        if current_eofs == SUM_AMOUNT:
            client_top = self.fruit_top_by_client.get(client_id, [])
            fruit_chunk = list(client_top[-TOP_SIZE:])
            fruit_chunk.reverse()

            payload = [client_id] + [(item.fruit, item.amount) for item in fruit_chunk]
            self.output_queue.send(message_protocol.internal.serialize(payload))

            if client_id in self.fruit_top_by_client:
                del self.fruit_top_by_client[client_id]
            del self.eof_count_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            self._process_eof(fields[0])
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    def close(self):
        logging.info(f"[AggregationFilter {ID}] Closing network connections...")
        try:
            self.input_exchange.close()
        except Exception as e:
            logging.error(f"[AggregationFilter {ID}] Error closing input exchange: {e}")

        try:
            self.output_queue.close()
        except Exception as e:
            logging.error(f"[AggregationFilter {ID}] Error closing output queue: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    try:
        aggregation_filter.start()
    finally:
        aggregation_filter.close()
    return 0


if __name__ == "__main__":
    main()
