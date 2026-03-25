from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    def __init__(self,
                 logger: Logger,
                 ) -> None:

        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
