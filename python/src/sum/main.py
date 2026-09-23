import os
import logging
import threading
import hashlib
import sys
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_listener = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["sum_control.*"]
        )
        self.control_sender = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["sum_control.eof"]
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_client = {}
        self.lock = threading.Lock()
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info(
            f"[SumFilter {ID}] SIGTERM received. Closing connections gracefully..."
        )
        try:
            self.input_queue.close()
            self.control_listener.close()
            self.control_sender.close()
            for out_exchange in self.data_output_exchanges:
                out_exchange.close()
        except Exception as e:
            logging.error(f"[SumFilter {ID}] Error closing connections: {e}")
        sys.exit(0)

    def _process_data(self, client_id, fruit, amount):
        logging.info(f" [Client {client_id}] Process data")
        with self.lock:
            if client_id not in self.amount_by_client:
                self.amount_by_client[client_id] = {}

            client_fruits = self.amount_by_client[client_id]

            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _handle_gateway_eof(self, client_id):
        logging.info(
            f"[SumFilter {ID}] Received raw EOF from Gateway for client {client_id}. Broadcasting CONTROL_EOF."
        )
        with self.lock:
            self.control_sender.send(
                message_protocol.internal.serialize([client_id])
            )

    def _process_broadcast_eof(self, client_id):
        logging.info(f" [Client {client_id}] Broadcasting data messages")
        with self.lock:
            client_fruits = self.amount_by_client.get(client_id, {})

            for final_fruit_item in client_fruits.values():
                fruit_bytes = final_fruit_item.fruit.encode("utf-8")
                aggregator_idx = (
                    int(hashlib.md5(fruit_bytes).hexdigest(), 16)
                    % AGGREGATION_AMOUNT
                )
                target_exchange = self.data_output_exchanges[aggregator_idx]
                target_exchange.send(
                    message_protocol.internal.serialize(
                        [
                            client_id,
                            final_fruit_item.fruit,
                            final_fruit_item.amount,
                        ]
                    )
                )
            logging.info(f"Broadcasting EOF message")
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message_protocol.internal.serialize([client_id]))

            if client_id in self.amount_by_client:
                del self.amount_by_client[client_id]

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            self._handle_gateway_eof(*fields)
        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 1:
            self._process_broadcast_eof(fields[0])
        ack()

    def start(self):
        control_thread = threading.Thread(
            target=lambda: self.control_listener.start_consuming(
                self.process_control_message
            ),
            daemon=True,
        )
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
