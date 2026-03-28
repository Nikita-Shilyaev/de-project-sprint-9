from datetime import datetime
from typing import Any

from psycopg.rows import dict_row

from lib.pg import PgConnect


class DdsOutputMessage:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def get_order_report_rows(self, order_id: int) -> list[dict[str, Any]]:
        with self._db.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    select
                        ho.h_order_pk as order_id,
                        ho.order_dt,
                        sos.status,
                        hu.h_user_pk as user_id,
                        sun.username,
                        hp.h_product_pk as product_id,
                        spn.name as product_name,
                        hc.h_category_pk as category_id,
                        hc.category_name,
                        hr.h_restaurant_pk as restaurant_id,
                        srn.name as restaurant_name
                    from dds.h_order ho
                    join dds.s_order_status sos
                        on ho.h_order_pk = sos.h_order_pk
                    join dds.l_order_user lou
                        on ho.h_order_pk = lou.h_order_pk
                    join dds.h_user hu
                        on lou.h_user_pk = hu.h_user_pk
                    join dds.s_user_names sun
                        on hu.h_user_pk = sun.h_user_pk
                    join dds.l_order_product lop
                        on ho.h_order_pk = lop.h_order_pk
                    join dds.h_product hp
                        on lop.h_product_pk = hp.h_product_pk
                    join dds.s_product_names spn
                        on hp.h_product_pk = spn.h_product_pk
                    join dds.l_product_category lpc
                        on hp.h_product_pk = lpc.h_product_pk
                    join dds.h_category hc
                        on lpc.h_category_pk = hc.h_category_pk
                    join dds.l_product_restaurant lpr
                        on hp.h_product_pk = lpr.h_product_pk
                    join dds.h_restaurant hr
                        on lpr.h_restaurant_pk = hr.h_restaurant_pk
                    join dds.s_restaurant_names srn
                        on hr.h_restaurant_pk = srn.h_restaurant_pk
                    where ho.order_id = %(order_id)s
                    order by hp.h_product_pk
                    """,
                    {"order_id": order_id}
                )
                return cur.fetchall()

    def build_order_report_message(self, order_id: int) -> dict[str, Any] | None:
        rows = self.get_order_report_rows(order_id)

        if not rows:
            return None

        first_row = rows[0]

        products = []

        for row in rows:
            product_id = row["product_id"]
            products.append(
                {
                    "id": str(product_id),
                    "name": row["product_name"],
                    "category": row["category_name"]
                }
            )

        return {
            "object_id": str(first_row["order_id"]),
            "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "object_type": "order_report",
            "payload": {
                "id": str(first_row["order_id"]),
                "order_dt": first_row["order_dt"].strftime("%Y-%m-%d %H:%M:%S"),
                "status": first_row["status"],
                "restaurant": {
                    "id": str(first_row["restaurant_id"]),
                    "name": first_row["restaurant_name"]
                },
                "user": {
                    "id": str(first_row["user_id"]),
                    "username": first_row["username"]
                },
                "products": products
            }
        }