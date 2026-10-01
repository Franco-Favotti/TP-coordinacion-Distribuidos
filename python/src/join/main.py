import os
import logging
import bisect

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

        self.partials = {}
        self.arrival_count = {}
        self.completed_clients = set()

    def _merge_partial(self, client_id, partial_top):
        merged = self.partials.setdefault(client_id, [])
        for fruit, amount in partial_top:
            for i in range(len(merged)):
                if merged[i].fruit == fruit:
                    updated_item = merged[i] + fruit_item.FruitItem(fruit, amount)
                    del merged[i]
                    bisect.insort(merged, updated_item)
                    break
            else:
                bisect.insort(merged, fruit_item.FruitItem(fruit, amount))

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")

        client_id, partial_top = message_protocol.internal.deserialize(message)

        if client_id in self.completed_clients:
            ack()
            return

        self._merge_partial(client_id, partial_top)
        self.arrival_count[client_id] = self.arrival_count.get(client_id, 0) + 1

        if self.arrival_count[client_id] < AGGREGATION_AMOUNT:
            ack()
            return

        merged = self.partials.pop(client_id, [])
        del self.arrival_count[client_id]
        self.completed_clients.add(client_id)

        final_chunk = list(merged[-TOP_SIZE:])
        final_chunk.reverse()
        result = [(fi.fruit, fi.amount) for fi in final_chunk]
        self.input_queue.publish_to_queue(OUTPUT_QUEUE, message_protocol.internal.serialize([client_id, result]))
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
