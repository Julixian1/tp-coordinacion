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

MSG_PREPARE = "PREPARE"
MSG_COUNT = "COUNT"

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
        self.msg_count_by_client = {} 
        self.expected_by_client = {}   
        self.received_by_client = {}

        self.lock = threading.Lock()
        self.control_thread = None
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info(
            f"[SumFilter {ID}] SIGTERM/SIGINT received. Stopping consumption loops..."
        )
        try:
            self.input_queue.stop_consuming()
        except Exception as e:
            logging.error(f"[SumFilter {ID}] Error stopping input queue: {e}")

        try:
            self.control_listener.stop_consuming_threadsafe()
        except Exception as e:
            logging.error(f"[SumFilter {ID}] Error stopping control listener: {e}")

    def _send_count(self, client_id, count):
        self.control_sender.send(
            message_protocol.internal.serialize([MSG_COUNT, client_id, count])
        )

    def _process_data(self, client_id, fruit, amount):
        with self.lock:
            client_fruits = self.amount_by_client.setdefault(client_id, {})
            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

            self.msg_count_by_client[client_id] = (
                self.msg_count_by_client.get(client_id, 0) + 1
            )
            if client_id in self.expected_by_client:
                self._send_count(client_id, 1)

    def _handle_gateway_eof(self, client_id, total_expected):
        logging.info(
            f"[SumFilter {ID}] EOF from Gateway, client {client_id}, N={total_expected}"
        )
        with self.lock:
            self.control_sender.send(
                message_protocol.internal.serialize(
                    [MSG_PREPARE, client_id, int(total_expected)]
                )
            )
    
    def _process_prepare(self, client_id, total_expected):
        with self.lock:
            self.expected_by_client[client_id] = int(total_expected)
            mine = self.msg_count_by_client.get(client_id, 0)
            if mine > 0:
                self._send_count(client_id, mine)
                
            if self.received_by_client.get(client_id, 0) == self.expected_by_client[client_id]:
                self._flush_data_and_eof(client_id)

    def _flush_data_and_eof(self, client_id):
        logging.info(f"[SumFilter {ID}] Flushing client {client_id} to Aggregators")
        client_fruits = self.amount_by_client.pop(client_id, {})
        self.expected_by_client.pop(client_id, None)
        self.msg_count_by_client.pop(client_id, None)
        self.received_by_client.pop(client_id, None)

        for item in client_fruits.values():
            routing_key = f"{client_id}_{item.fruit}"
            idx = int(hashlib.md5(routing_key.encode("utf-8")).hexdigest(), 16) % AGGREGATION_AMOUNT
            self.data_output_exchanges[idx].send(
                message_protocol.internal.serialize([client_id, item.fruit, item.amount])
            )
        for exchange in self.data_output_exchanges:
            exchange.send(message_protocol.internal.serialize([client_id]))
    
    def _process_count(self, client_id, count):
        with self.lock:
            self.received_by_client[client_id] = (
                self.received_by_client.get(client_id, 0) + int(count)
            )
            if (client_id in self.expected_by_client
                    and self.received_by_client[client_id] == self.expected_by_client[client_id]):
                self._flush_data_and_eof(client_id)

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == MSG_PREPARE:
            self._process_prepare(fields[1], fields[2])
        elif fields[0] == MSG_COUNT:
            self._process_count(fields[1], fields[2])
        ack()


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 2:
            self._handle_gateway_eof(*fields)
        ack()


    def start(self):
        self.control_thread = threading.Thread(
            target=lambda: self.control_listener.start_consuming(
                self.process_control_message
            ),
            daemon=True,
        )
        self.control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)

    def _safe_close(self, resource, name: str) -> None:
        if resource is None:
            return
        try:
            resource.close()
        except Exception as e:
            logging.error(f"[SumFilter {ID}] Error closing {name}: {e}")

    def close(self):
        logging.info(f"[SumFilter {ID}] Closing network connections...")
        if self.control_thread and self.control_thread.is_alive():
            self.control_thread.join(timeout=2.0)

        resources_to_close = [
            (self.input_queue, "input_queue"),
            (self.control_listener, "control_listener"),
            (self.control_sender, "control_sender"),
        ]

        for resource, name in resources_to_close:
            self._safe_close(resource, name)

        for i, out_exchange in enumerate(self.data_output_exchanges):
            self._safe_close(out_exchange, f"data_output_exchange_{i}")

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    try:
        sum_filter.start()
    finally:
        sum_filter.close()
    return 0


if __name__ == "__main__":
    main()