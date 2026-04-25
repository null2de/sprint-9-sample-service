import time
import json

from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        redis: RedisClient,
        stg_repository: StgRepository,
        batch_size: int,
        logger: Logger
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        # Имитация работы. Здесь будет реализована обработка сообщений.
        for i in range(self._batch_size):

            message = self._consumer.consume()

            if not message:
                break

            self._logger.info(f"Processing message {i + 1}: {message['object_id']}")
            # self._logger.info(f'{message}')

            self._stg_repository.order_events_insert(
                message['object_id'],
                message['object_type'],
                message['sent_dttm'],
                json.dumps(message['payload'], ensure_ascii=False)
            )

            user_name = self._redis.get(message['payload']['user']['id'])['name']
            message['payload']['user']['name'] = user_name

            restaurant_ref = self._redis.get(message['payload']['restaurant']['id'])
            message['payload']['restaurant']['name'] = restaurant_ref['name']

            menu_categories = {
                item['_id']: item['category'] for item in restaurant_ref['menu']
            }

            for order_item in message['payload']['order_items']:
                order_item['category'] = menu_categories[order_item['id']]

            message_out = {
                'object_id': message['object_id'],
                'object_type': message['object_type'],
                'payload': {
                    'id': message['object_id'],
                    'date': message['payload']['date'],
                    'cost': message['payload']['cost'],
                    'payment': message['payload']['payment'],
                    'status': message['payload']['final_status'],
                    'restaurant': message['payload']['restaurant'],
                    'user': message['payload']['user'],
                    'products': message['payload']['order_items'],                    
                }
            }

            self._producer.produce(message_out)
            # self._consumer.commit()

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
