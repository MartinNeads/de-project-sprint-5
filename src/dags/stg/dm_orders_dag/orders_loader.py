from logging import Logger
from typing import List

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class orderObj(BaseModel):
    id: int
    order_id: str
    order_status: str
    restaurant_id: int
    user_id: int
    timestamp_id: int
    courier_id: int


class ordersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int, limit: int) -> List[orderObj]:
        with self._db.client().cursor(row_factory=class_row(orderObj)) as cur:
            cur.execute(
                """
                    SELECT distinct 
                    ooo.id,
                    ooo.order_key as order_id,
                    ooo.order_status as order_status,
                    dr.id as restaurant_id,
                    du.id as user_id,
                    dt.id as timestamp_id,
                    dc.id as courier_id 
                    FROM
                    (select distinct
                    oo.id as id
                    oo.order_key as order_key,
                    oo.order_status as order_status,
                    (oo.restaurant::JSON ->> 'id')::varchar as restaurant_id,
                    (oo.user::JSON ->> 'id')::varchar as user_id,
                    oo.ts as ts
                    from
                    (select distinct
                    id,
                    object_id as order_key,
                    (object_value::JSON ->> 'final_status')::varchar as order_status,
                    (object_value::JSON ->> 'restaurant') as restaurant,
                    (object_value::JSON ->> 'user') as user,
                    (object_value::JSON->>'date')::timestamp AS ts
                    from stg.ordersystem_orders soo
                    join (select
                    (object_value::JSON ->> 'order_id')::varchar as order_id,
                    (object_value::JSON ->> 'courier_id')::varchar as courier_id
                    from stg.deliverysystem_deliveries) sdd
                    on soo.object_id=sdd.order_id) oo) ooo
                    join
                    dds.dm_restaurants dr 
                    using (restaurant_id)
                    join
                    dds.dm_users du 
                    using (user_id)
                    join 
                    dds.dm_timestamps dt 
                    using (ts)
                    join 
                    dds.dm_couriers dc 
                    using (courier_id)
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class orderDestRepository:

    def insert_order(self, conn: Connection, order: orderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, user_id, timestamp_id, courier_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(user_id)s, %(timestamp_id)s, %(courier_id)s)
                    ;
                """,
                {
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "restaurant_id": order.restaurant_id,
                    "user_id": order.user_id,
                    "timestamp_id": order.timestamp_id,
                    "courier_id": order.courier_id,
                },
                
            )


class orderLoader:
    WF_KEY = "orders_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ordersOriginRepository(pg_origin)
        self.stg = orderDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.stg.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
