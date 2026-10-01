import os
import logging
import bisect
import signal

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

        self.fruit_top = {}
        self.eof_counter = {}
        self.completed_clients = set()

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        fruit_top = self.fruit_top.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated_item = fruit_top[i] + fruit_item.FruitItem(fruit, amount)
                del fruit_top[i]                      
                bisect.insort(fruit_top, updated_item) 

                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        if client_id in self.completed_clients:
            return
        
        logging.info("Received EOF")
        self.eof_counter[client_id] = self.eof_counter.get(client_id, 0) + 1
        if self.eof_counter[client_id] < SUM_AMOUNT:
            return
        
        fruit_top = self.fruit_top.pop(client_id, [])
        del self.eof_counter[client_id]
        self.completed_clients.add(client_id)

        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        result = [(fi.fruit, fi.amount) for fi in fruit_chunk]
        self.input_exchange.publish_to_queue(OUTPUT_QUEUE, message_protocol.internal.serialize([client_id, result]))

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):

        def handle_sigterm(signum, frame):
            logging.info("SIGTERM recibido, iniciando apagado")
            self.input_exchange.stop_consuming()
            try:
                self.input_exchange.close()
            except Exception as e:
                logging.error(f"Error cerrando input_exchange: {e}")

        signal.signal(signal.SIGTERM, handle_sigterm)
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
