import time, json
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 logger: Logger,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int = 100
                 ) -> None:

        self._logger = logger
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        start_time = time.perf_counter()
        processed_msg_cnt = 0
        duplicates_cnt = 0

        try:
            for _ in range(self._batch_size):
                msg = self._consumer.consume()

                # Если новых сообщений нет, завершаем батч раньше.
                if msg is None:
                    self._logger.info("No messages in Kafka. Batch finished early.")
                    break

                if isinstance(msg, str):
                    msg = json.loads(msg)

                # не загружаем сообщения отличные от заказов
                if msg.get("object_type") != "order_report":
                    self._logger.info("Skip non-order_report message")
                    continue

                # Подготавливаем поля для вставки в cdm.
                payload = msg["payload"]

                hk_order_pk = payload["id"]
                hk_user_pk = payload["user"]["id"]
                products = payload["products"]
                
                with self._cdm_repository.transaction() as conn:
                    inserted = self._cdm_repository.processed_order_insert(
                        conn=conn,
                        hk_order_pk=hk_order_pk
                    )

                    if not inserted:
                        duplicates_cnt += 1
                        self._logger.info(f"Skip duplicate order: {hk_order_pk}")
                        continue

                    # 1. обновляем витрину по продуктам
                    for product in products:
                        self._cdm_repository.product_insert(
                            conn=conn,
                            user_id=hk_user_pk,
                            product_id=product["id"],
                            product_name=product["name"]
                        )

                    # 2. собираем уникальные категории в рамках одного заказа
                    unique_categories = {product["category"] for product in products}

                    # 3. обновляем витрину по категориям
                    for category_name in unique_categories:
                        self._cdm_repository.category_insert(
                            conn=conn,
                            user_id=hk_user_pk,
                            category_name=category_name
                        )

                processed_msg_cnt += 1

            if processed_msg_cnt > 0 or duplicates_cnt > 0:
                self._consumer.c.commit(asynchronous=False)
                self._logger.info(f"Committed offsets for {processed_msg_cnt} messages and {duplicates_cnt} duplicates.")
        
        except Exception as e:
            self._logger.exception(f"Batch processing failed: {e}")
            raise

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        self._logger.info(f"The run took {elapsed_time:0.2f} seconds to complete.")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
