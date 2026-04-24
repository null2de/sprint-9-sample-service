import time
from datetime import datetime
from logging import Logger


class StgMessageProcessor:
    def __init__(
        self, consumer, producer, redis, stg_repository, batch_size, logger
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
        for _ in range(self._batch_size):

            self._logger.info('1 !!!')
            message = self._consumer.consume()
            self._logger.info('2 !!!')
            self._logger.info(f'{message}')

            if not message:
                return

            self._logger.info('3 !!!')
            print(message)

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
