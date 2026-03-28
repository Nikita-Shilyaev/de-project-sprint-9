import time, json
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository
from dds_loader.output_message import DdsOutputMessage


class DdsMessageProcessor:
    def __init__(self,
                 logger: Logger,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 dds_output_message: DdsOutputMessage,
                 batch_size: int = 30
                 ) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._dds_output_message = dds_output_message
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        start_time = time.perf_counter()

        messages_cnt = 0

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
                if msg.get("object_type") != "order":
                    self._logger.info("Skip non-order message")
                    continue

                # Подготавливаем поля для вставки в dds.
                payload_dict = msg["payload"]
                products = payload_dict["products"]

                # 1. Пишем данные в dds
                with self._dds_repository.transaction() as conn:
                    self._dds_repository.h_order_insert(
                        conn=conn,
                        order_id=str(payload_dict["id"]),
                        order_dt=datetime.strptime(payload_dict["date"], "%Y-%m-%d %H:%M:%S")
                    )

                    self._dds_repository.h_restaurant_insert(
                        conn=conn,
                        restaurant_id=payload_dict["restaurant"]["id"]
                    )

                    self._dds_repository.h_user_insert(
                        conn=conn,
                        user_id=payload_dict["user"]["id"]
                    )

                    self._dds_repository.l_order_user_insert(
                        conn=conn,
                        order_id=str(payload_dict["id"]),
                        user_id=payload_dict["user"]["id"]
                    )

                    self._dds_repository.s_user_names_insert(
                        conn=conn,
                        user_id=payload_dict["user"]["id"],
                        username=payload_dict["user"]["name"],
                        userlogin=payload_dict["user"]["login"]
                    )

                    self._dds_repository.s_restaurant_names_insert(
                        conn=conn,
                        restaurant_id=payload_dict["restaurant"]["id"],
                        restaurant_name=payload_dict["restaurant"]["name"]
                    )

                    self._dds_repository.s_order_cost_insert(
                        conn=conn,
                        order_id=str(payload_dict["id"]),
                        payment=payload_dict["payment"],
                        cost=payload_dict["cost"]
                    )

                    self._dds_repository.s_order_status_insert(
                        conn=conn,
                        order_id=str(payload_dict["id"]),
                        status=payload_dict["status"]
                    )

                    for product in products:
                        self._dds_repository.h_product_insert(
                            conn=conn,
                            product_id=product["id"]
                        )

                        self._dds_repository.h_category_insert(
                            conn=conn,
                            category_name=product["category"]
                        )

                        self._dds_repository.l_order_product_insert(
                            conn=conn,
                            order_id=str(payload_dict["id"]),
                            product_id=product["id"]
                        )

                        self._dds_repository.l_product_restaurant_insert(
                            conn=conn,
                            product_id=product["id"],
                            restaurant_id=payload_dict["restaurant"]["id"]
                        )

                        self._dds_repository.l_product_category_insert(
                            conn=conn,
                            product_id=product["id"],
                            category_name=product["category"]
                        )

                        self._dds_repository.s_product_names_insert(
                            conn=conn,
                            product_id=product["id"],
                            product_name=product["name"]
                        )

                # 2. После успешной записи читаем заказ из DDS и собираем выходное сообщение.
                order_id = payload_dict["id"]
                output_message = self._dds_output_message.build_order_report_message(order_id)

                if output_message is None:
                    self._logger.warning(
                        f"Данные по order_id={order_id} не были сформированы."
                    )
                    continue

                # 3. Отправляем итоговое сообщение в Kafka.
                self._producer.produce(output_message)
                self._logger.info(
                    f"Сообщение по order_id={order_id} было отправлено в топик."
                )

                messages_cnt += 1

            if messages_cnt > 0:
                self._consumer.c.commit(asynchronous=False)
                self._logger.info(f"Committed offsets for {messages_cnt} messages")
        
        except Exception as e:
            self._logger.exception(f"Batch processing failed: {e}")
            raise


        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        self._logger.info(f"The run took {elapsed_time:0.2f} seconds to complete.")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
