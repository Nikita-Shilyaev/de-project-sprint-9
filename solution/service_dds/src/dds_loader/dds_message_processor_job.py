from datetime import datetime
from logging import Logger


class DdsMessageProcessor:
    def __init__(self,
                 logger: Logger) -> None:

        self._logger = logger

        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
