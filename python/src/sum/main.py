import os
import logging
import signal
import zlib

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
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.input_queue.bind_and_consume_exchange(
            SUM_CONTROL_EXCHANGE, [""], self.process_control_message, exchange_type="fanout"
        )

        self.amount_by_fruit = {}
        self.completed_clients = set() 

    def _process_data(self, client_id, fruit, amount):
        client_totals = self.amount_by_fruit.setdefault(client_id, {})
        client_totals[fruit] = client_totals.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        if client_id in self.completed_clients:
            return
        self.completed_clients.add(client_id)

        client_totals = self.amount_by_fruit.pop(client_id, {})
        for final_fruit_item in client_totals.values():
            target = zlib.crc32(final_fruit_item.fruit.encode("utf-8")) % AGGREGATION_AMOUNT 
            self.data_output_exchanges[target].send(                
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
    
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            [client_id] = fields
        
            self.input_queue.publish_to_exchange(
            SUM_CONTROL_EXCHANGE, "", message_protocol.internal.serialize([client_id]), exchange_type="fanout"
        )
        ack()

    def process_control_message(self, message, ack, nack):
        [client_id] = message_protocol.internal.deserialize(message)
        self._process_eof(client_id)
        ack()

    def start(self):
        def handle_sigterm(signum, frame):
            self.input_queue.stop_consuming()

        signal.signal(signal.SIGTERM, handle_sigterm)
        self.input_queue.start_consuming(self.process_data_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()