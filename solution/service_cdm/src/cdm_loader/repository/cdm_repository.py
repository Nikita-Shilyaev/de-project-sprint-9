import uuid
from contextlib import contextmanager
from typing import Generator

from psycopg import Connection

from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    @contextmanager
    def transaction(self) -> Generator[Connection, None, None]:
        with self._db.connection() as conn:
            yield conn

    def processed_order_insert(self, conn: Connection, hk_order_pk: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.processed_orders (hk_order_pk)
                    VALUES (%(hk_order_pk)s)
                    ON CONFLICT (hk_order_pk) DO NOTHING
                    RETURNING 1;
                """,
                {"hk_order_pk": hk_order_pk}
            )
            return cur.fetchone() is not None

    def product_insert(
        self,
        conn: Connection,
        user_id: str,
        product_id: str,
        product_name: str,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.user_product_counters (
                        user_id,
                        product_id,
                        product_name,
                        order_cnt
                    )
                    VALUES (
                        %(user_id)s,
                        %(product_id)s,
                        %(product_name)s,
                        1
                    )
                    ON CONFLICT (user_id, product_id) DO UPDATE
                    SET
                        product_name = EXCLUDED.product_name,
                        order_cnt = cdm.user_product_counters.order_cnt + 1;
                """,
                {
                    "user_id": user_id,
                    "product_id": product_id,
                    "product_name": product_name
                }
            )

    def category_insert(
        self,
        conn: Connection,
        user_id: str,
        category_name: str,
    ) -> None:
        hk_category_id = str(uuid.uuid5(uuid.NAMESPACE_OID, category_name))

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.user_category_counters (
                        user_id,
                        category_id,
                        category_name,
                        order_cnt
                    )
                    VALUES (
                        %(user_id)s,
                        %(category_id)s,
                        %(category_name)s,
                        1
                    )
                    ON CONFLICT (user_id, category_id) DO UPDATE
                    SET
                        category_name = EXCLUDED.category_name,
                        order_cnt = cdm.user_category_counters.order_cnt + 1;
                """,
                {
                    "user_id": user_id,
                    "category_id": hk_category_id,
                    "category_name": category_name
                }
            )
