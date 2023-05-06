from logging import Logger
from typing import List

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class saleObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float
    tip_sum: float

class salesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sales(self, sale_threshold: int, limit: int) -> List[saleObj]:
        with self._db.client().cursor(row_factory=class_row(saleObj)) as cur:
            cur.execute(
                """
                    SELECT distinct 
                    do2.id as id,
                    dp.id as product_id,
                    do2.id as order_id,
                    ooo.count as count,
                    ooo.price as price,
                    ooo.total_sum as total_sum,
                    ooo.bonus_payment as bonus_payment,
                    ooo.bonus_grant as bonus_grant
                    ooo.tip_sum as tip_sum
                    from (select distinct
                    (oo.product_payments::JSON ->> 'product_id')::varchar as product_id,
                    oo.order_id as order_id,
                    (oo.product_payments::JSON ->> 'quantity')::integer as count,
                    (oo.product_payments::JSON ->> 'price')::numeric(19,5) as price,
                    (oo.product_payments::JSON ->> 'product_cost')::numeric(19,5) as total_sum,
                    (oo.product_payments::JSON ->> 'bonus_payment')::numeric(19,5) as bonus_payment,
                    (oo.product_payments::JSON ->> 'bonus_grant')::numeric(19,5) as bonus_grant
                    from
                    (select 
                    json_array_elements((event_value::JSON->>'product_payments')::JSON) as product_payments,
                    (event_value::JSON->>'order_id')::varchar as order_id
                    from
                    stg.bonussystem_events be
                    where event_type='bonus_transaction') oo
                    join (select
                    (object_value::JSON ->> 'order_id')::varchar as order_id,
                    (object_value::JSON ->> 'tip_sum')::varchar as tip_sum
                    from stg.deliverysystem_deliveries) sdd
                    on oo.order_id=sdd.order_id) ooo
                    join dds.dm_orders do2 
                    on ooo.order_id=do2.order_key 
                    join dds.dm_products dp 
                    on ooo.product_id=dp.product_id 
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": sale_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class saleDestRepository:

    def insert_sale(self, conn: Connection, sale: saleObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant, tip_sum)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s, %(tip_sum)s)
                    ;
                """,
                {
                    "product_id": sale.product_id,
                    "order_id": sale.order_id,
                    "count": sale.count,
                    "price": sale.price,
                    "total_sum": sale.total_sum,
                    "bonus_payment": sale.bonus_payment,
                    "bonus_grant": sale.bonus_grant,
                    "tip_sum": sale.tip_sum
                },
            )


class saleLoader:
    WF_KEY = "sales_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = salesOriginRepository(pg_origin)
        self.stg = saleDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_sales(self):
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
            load_queue = self.origin.list_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for sale in load_queue:
                self.stg.insert_sale(conn, sale)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
