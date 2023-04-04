import asyncio
import websockets
import json
from kafka import KafkaProducer
import time
import os

# Define the Alchemy WebSocket endpoint URL
ALCHEMY_WS_URL = f'wss://eth-mainnet.g.alchemy.com/v2/tj-YBHB2LIAVXUMZRMkyyI8FKK1tEKbn'

kafka_broker = os.environ.get('KAFKA_BROKER') if os.environ.get('KAFKA_BROKER') != None else 'localhost:9092'

# create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_broker], # kafka_broker
    value_serializer=lambda m: json.dumps(m).encode('ascii'),
    buffer_memory=33554432,
    request_timeout_ms=10000
)


def parse_block(message):
    try:
        data = json.loads(message)
        block = data['params']['result']
        print(json.dumps(block, indent=4))
        return block
    except Exception as ex:
        print(ex)


async def handler(websocket):
    connection_message_received = False
    # initialize batching variables
    batch = []
    batch_size = 5
    batch_timeout = 30  # in seconds
    last_batch_sent_time = time.time()
    while True:
        try:
            message = await websocket.recv()
            if not connection_message_received:
                connection_message_received = True
                continue
            parsed_block = parse_block(message)
            batch.append(parsed_block)
            if len(batch) >= batch_size or time.time() - last_batch_sent_time >= batch_timeout:
                producer.send('block_creation', batch)
                batch = []
                last_batch_sent_time = time.time()
        except Exception as ex:
            print(ex)


async def main():
    async with websockets.connect(ALCHEMY_WS_URL) as ws:
        subscribe_msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["newHeads"]
        }
        await ws.send(json.dumps(subscribe_msg))
        await handler(ws)
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
