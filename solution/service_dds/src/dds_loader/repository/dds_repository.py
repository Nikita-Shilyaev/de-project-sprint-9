import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Generator

from lib.pg import PgConnect
from psycopg import Connection


class DdsRepository:
    LOAD_SRC = "stg-service_orders"
    UUID_NAMESPACE = uuid.NAMESPACE_OID


    def __init__(self, db: PgConnect) -> None:
        self._db = db


    def _generate_pk(self, *values) -> uuid.UUID:
        return uuid.uuid5(
            self.UUID_NAMESPACE,
            "_".join(str(v) for v in values)
        )


    @contextmanager
    def transaction(self) -> Generator[Connection, None, None]:
        with self._db.connection() as conn:
            yield conn


    def h_user_insert(self, conn: Connection, user_id: str) -> None:
        h_user_pk = self._generate_pk(user_id)

        with conn.cursor() as cur:
            cur.execute(
                    """
                        INSERT INTO dds.h_user (
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(h_user_pk)s,
                            %(user_id)s,
                            now(),
                            %(load_src)s
                        )
                        ON CONFLICT (user_id) DO NOTHING;
                    """,
                    {
                        'h_user_pk': h_user_pk,
                        'user_id': user_id,
                        'load_src': self.LOAD_SRC
                    }
            )


    def h_product_insert(self, conn: Connection, product_id: str) -> None:
        h_product_pk = self._generate_pk(product_id)

        with conn.cursor() as cur:
            cur.execute(
                    """
                        INSERT INTO dds.h_product (
                            h_product_pk,
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(h_product_pk)s,
                            %(product_id)s,
                            now(),
                            %(load_src)s
                        )
                        ON CONFLICT (product_id) DO NOTHING;
                    """,
                    {
                        'h_product_pk': h_product_pk,
                        'product_id': product_id,
                        'load_src': self.LOAD_SRC
                    }
            )


    def h_category_insert(self, conn: Connection, category_name: str) -> None:
        h_category_pk = self._generate_pk(category_name)

        with conn.cursor() as cur:
            cur.execute(
                    """
                        INSERT INTO dds.h_category (
                            h_category_pk,
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(h_category_pk)s,
                            %(category_name)s,
                            now(),
                            %(load_src)s
                        )
                        ON CONFLICT (category_name) DO NOTHING;
                    """,
                    {
                        'h_category_pk': h_category_pk,
                        'category_name': category_name,
                        'load_src': self.LOAD_SRC
                    }
            )


    def h_restaurant_insert(self, conn: Connection, restaurant_id: str) -> None:
            h_restaurant_pk = self._generate_pk(restaurant_id)

            with conn.cursor() as cur:
                cur.execute(
                        """
                            INSERT INTO dds.h_restaurant (
                                h_restaurant_pk,
                                restaurant_id,
                                load_dt,
                                load_src
                            )
                            VALUES (
                                %(h_restaurant_pk)s,
                                %(restaurant_id)s,
                                now(),
                                %(load_src)s
                            )
                            ON CONFLICT (restaurant_id) DO NOTHING;
                        """,
                        {
                            'h_restaurant_pk': h_restaurant_pk,
                            'restaurant_id': restaurant_id,
                            'load_src': self.LOAD_SRC
                        }
                )


    def h_order_insert(self, conn: Connection, order_id: str, order_dt: datetime) -> None:
        h_order_pk = self._generate_pk(order_id)

        with conn.cursor() as cur:
            cur.execute(
                    """
                        INSERT INTO dds.h_order (
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            now(),
                            %(load_src)s
                        )
                        ON CONFLICT (order_id) DO NOTHING;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_src': self.LOAD_SRC
                    }
            )


    def l_order_user_insert(self, conn: Connection, order_id: str, user_id: str) -> None:
        h_order_pk = self._generate_pk(order_id)
        h_user_pk = self._generate_pk(user_id)
        hk_order_user_pk = self._generate_pk(order_id, user_id)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.l_order_user (
                        hk_order_user_pk,
                        h_order_pk,
                        h_user_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (
                        %(hk_order_user_pk)s,
                        %(h_order_pk)s,
                        %(h_user_pk)s,
                        now(),
                        %(load_src)s
                    )
                    ON CONFLICT (hk_order_user_pk) DO NOTHING;
                """,
                {
                    'hk_order_user_pk': hk_order_user_pk,
                    'h_order_pk': h_order_pk,
                    'h_user_pk': h_user_pk,
                    'load_src': self.LOAD_SRC
                }
            )


    def l_order_product_insert(self, conn: Connection, order_id: str, product_id: str) -> None:
        h_order_pk = self._generate_pk(order_id)
        h_product_pk = self._generate_pk(product_id)
        hk_order_product_pk = self._generate_pk(order_id, product_id)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.l_order_product (
                        hk_order_product_pk,
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (
                        %(hk_order_product_pk)s,
                        %(h_order_pk)s,
                        %(h_product_pk)s,
                        now(),
                        %(load_src)s
                    )
                    ON CONFLICT (hk_order_product_pk) DO NOTHING;
                """,
                {
                    'hk_order_product_pk': hk_order_product_pk,
                    'h_order_pk': h_order_pk,
                    'h_product_pk': h_product_pk,
                    'load_src': self.LOAD_SRC
                }
            )


    def l_product_restaurant_insert(self, conn: Connection, product_id: str, restaurant_id: str) -> None:
        h_product_pk = self._generate_pk(product_id)
        h_restaurant_pk = self._generate_pk(restaurant_id)
        hk_product_restaurant_pk = self._generate_pk(product_id, restaurant_id)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.l_product_restaurant (
                        hk_product_restaurant_pk,
                        h_product_pk,
                        h_restaurant_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (
                        %(hk_product_restaurant_pk)s,
                        %(h_product_pk)s,
                        %(h_restaurant_pk)s,
                        now(),
                        %(load_src)s
                    )
                    ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                """,
                {
                    'hk_product_restaurant_pk': hk_product_restaurant_pk,
                    'h_product_pk': h_product_pk,
                    'h_restaurant_pk': h_restaurant_pk,
                    'load_src': self.LOAD_SRC
                }
            )

    def l_product_category_insert(self, conn: Connection, product_id: str, category_name: str) -> None:
        h_product_pk = self._generate_pk(product_id)
        h_category_pk = self._generate_pk(category_name)
        hk_product_category_pk = self._generate_pk(product_id, category_name)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.l_product_category (
                        hk_product_category_pk,
                        h_product_pk,
                        h_category_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (
                        %(hk_product_category_pk)s,
                        %(h_product_pk)s,
                        %(h_category_pk)s,
                        now(),
                        %(load_src)s
                    )
                    ON CONFLICT (hk_product_category_pk) DO NOTHING;
                """,
                {
                    'hk_product_category_pk': hk_product_category_pk,
                    'h_product_pk': h_product_pk,
                    'h_category_pk': h_category_pk,
                    'load_src': self.LOAD_SRC
                }
            )
    

    def s_user_names_insert(self,
                            conn: Connection,
                            user_id: str,
                            username: str,
                            userlogin: str
                        ) -> None:
        h_user_pk = self._generate_pk(user_id)
        hk_user_names_hashdiff = self._generate_pk(username, userlogin)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.s_user_names (
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src,
                        hk_user_names_hashdiff
                    )
                    VALUES (
                        %(h_user_pk)s,
                        %(username)s,
                        %(userlogin)s,
                        now(),
                        %(load_src)s,
                        %(hk_user_names_hashdiff)s
                    )
                    ON CONFLICT (h_user_pk) DO UPDATE
                    SET
                        username = EXCLUDED.username,
                        userlogin = EXCLUDED.userlogin,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src,
                        hk_user_names_hashdiff = EXCLUDED.hk_user_names_hashdiff;
                """,
                {
                    "h_user_pk": h_user_pk,
                    "username": username,
                    "userlogin": userlogin,
                    "load_src": self.LOAD_SRC,
                    "hk_user_names_hashdiff": hk_user_names_hashdiff,
                }
            )


    def s_product_names_insert(self,
                               conn: Connection,
                               product_id: str,
                               product_name: str
                            ) -> None:
            h_product_pk = self._generate_pk(product_id)
            hk_product_names_hashdiff = self._generate_pk(product_name)

            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names (
                            h_product_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_product_names_hashdiff
                        )
                        VALUES (
                            %(h_product_pk)s,
                            %(name)s,
                            now(),
                            %(load_src)s,
                            %(hk_product_names_hashdiff)s
                        )
                        ON CONFLICT (h_product_pk) DO UPDATE
                        SET
                            name = EXCLUDED.name,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src,
                            hk_product_names_hashdiff = EXCLUDED.hk_product_names_hashdiff;
                    """,
                    {
                        "h_product_pk": h_product_pk,
                        "name": product_name,
                        "load_src": self.LOAD_SRC,
                        "hk_product_names_hashdiff": hk_product_names_hashdiff,
                    }
                )

    def s_restaurant_names_insert(self,
                                  conn: Connection,
                                  restaurant_id: str,
                                  restaurant_name: str
                                ) -> None:
        h_restaurant_pk = self._generate_pk(restaurant_id)
        hk_restaurant_names_hashdiff = self._generate_pk(restaurant_name)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.s_restaurant_names (
                        h_restaurant_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_restaurant_names_hashdiff
                    )
                    VALUES (
                        %(h_restaurant_pk)s,
                        %(name)s,
                        now(),
                        %(load_src)s,
                        %(hk_restaurant_names_hashdiff)s
                    )
                    ON CONFLICT (h_restaurant_pk) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src,
                        hk_restaurant_names_hashdiff = EXCLUDED.hk_restaurant_names_hashdiff;
                """,
                {
                    "h_restaurant_pk": h_restaurant_pk,
                    "name": restaurant_name,
                    "load_src": self.LOAD_SRC,
                    "hk_restaurant_names_hashdiff": hk_restaurant_names_hashdiff,
                }
            )

    def s_order_cost_insert(self,
                            conn: Connection,
                            order_id: str,
                            payment: float,
                            cost: float
                        ) -> None:
        h_order_pk = self._generate_pk(order_id)
        hk_order_cost_hashdiff = self._generate_pk(payment, cost)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.s_order_cost (
                        h_order_pk,
                        payment,
                        cost,
                        load_dt,
                        load_src,
                        hk_order_cost_hashdiff
                    )
                    VALUES (
                        %(h_order_pk)s,
                        %(payment)s,
                        %(cost)s,
                        now(),
                        %(load_src)s,
                        %(hk_order_cost_hashdiff)s
                    )
                    ON CONFLICT (h_order_pk) DO UPDATE
                    SET
                        payment = EXCLUDED.payment,
                        cost = EXCLUDED.cost,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src,
                        hk_order_cost_hashdiff = EXCLUDED.hk_order_cost_hashdiff;
                """,
                {
                    "h_order_pk": h_order_pk,
                    "payment": payment,
                    "cost": cost,
                    "load_src": self.LOAD_SRC,
                    "hk_order_cost_hashdiff": hk_order_cost_hashdiff,
                }
            )

    def s_order_status_insert(self,
                              conn: Connection,
                              order_id: str,
                              status: str
                            ) -> None:
        h_order_pk = self._generate_pk(order_id)
        hk_order_status_hashdiff = self._generate_pk(status)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.s_order_status (
                        h_order_pk,
                        status,
                        load_dt,
                        load_src,
                        hk_order_status_hashdiff
                    )
                    VALUES (
                        %(h_order_pk)s,
                        %(status)s,
                        now(),
                        %(load_src)s,
                        %(hk_order_status_hashdiff)s
                    )
                    ON CONFLICT (h_order_pk) DO UPDATE
                    SET
                        status = EXCLUDED.status,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src,
                        hk_order_status_hashdiff = EXCLUDED.hk_order_status_hashdiff;
                """,
                {
                    "h_order_pk": h_order_pk,
                    "status": status,
                    "load_src": self.LOAD_SRC,
                    "hk_order_status_hashdiff": hk_order_status_hashdiff,
                }
            )
