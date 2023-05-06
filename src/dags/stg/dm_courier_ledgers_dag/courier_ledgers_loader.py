from logging import Logger
from typing import List

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class courier_ledgerObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float
    tip_sum: float

class courier_ledgersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_courier_ledgers(self, courier_ledger_threshold: int, limit: int) -> List[courier_ledgerObj]:
        with self._db.client().cursor(row_factory=class_row(courier_ledgerObj)) as cur:
            cur.execute(
                """
                    select
                    pmnt.courier_id as id,
                    pmnt.courier_id as courier_id,
                    pmnt.courier_name as courier_name,
                    pmnt.settlement_year as settlement_year,
                    pmnt.settlement_month as settlement_month,
                    count(distinct pmnt.id) as count,
                    sum(fps.total_sum) as total_sum,
                    avg(pmnt.rate) as rate_avg,
                    0.25*sum(fps.total_sum) as order_processing_fee,
                    (case 
                        when avg(pmnt.rate) < 4.0 then max(0.05*sum(fps.total_sum), 100.0)
                        when avg(pmnt.rate) between 4.0 and 4.5 then max(0.07*sum(fps.total_sum), 150.0)
                        when avg(pmnt.rate) between 4.5 and 4.9 then max(0.08*sum(fps.total_sum), 175.0)
                        when avg(pmnt.rate) >=4.9 then max(0.1*sum(fps.total_sum), 200.0)
                        else 0.0 end) as courier_order_sum,
                    sum(pmnt.tip_sum) as courier_tips_sum,
                    (case 
                        when avg(pmnt.rate) < 4.0 then max(0.05*sum(fps.total_sum), 100.0)
                        when avg(pmnt.rate) between 4.0 and 4.5 then max(0.07*sum(fps.total_sum), 150.0)
                        when avg(pmnt.rate) between 4.5 and 4.9 then max(0.08*sum(fps.total_sum), 175.0)
                        when avg(pmnt.rate) >=4.9 then max(0.1*sum(fps.total_sum), 200.0)
                        else 0.0 end) + 0.95 * sum(pmnt.tip_sum) as courier_reward_sum
                    from 
                    dds.fct_product_sales fps 
                    join
                    (select ord.id as id,
                    ord.courier_id as courier_id ,
                    ord.tip_sum as tip_sum,
                    ord.rate as rate,
                    rst.courier_name as courier_name ,
                    ts."year" as settlement_year,
                    ts."month" as settlement_month
                    from dds.dm_orders ord
                    join
                    dds.dm_timestamps ts
                    on ord.timestamp_id=ts.id 
                    join dds.dm_couriers rst 
                    on ord.courier_id=rst.id ) as pmnt
                    on fps.order_id = pmnt.id
                    
                """, {
                    "threshold": courier_ledger_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class courier_ledgerDestRepository:

    def insert_courier_ledger(self, conn: Connection, courier_ledger: courier_ledgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, count, total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(count)s, % %(total_sum)s, %(rate_avg)s, %(order_processing_fee)s, %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s)
                    group by courier_id, settlement_year, ettlement_month
                    on conflict (courier_id, settlement_year, settlement_month) 
                    do update set 
                    count=excluded.count,
                    total_sum=excluded.total_sum,
                    rate_avg=excluded.rate_avg,
                    order_processing_fee=excluded.order_processing_fee,
                    courier_order_sum=excluded.courier_order_sum,
                    courier_tips_sum=excluded.courier_tips_sum,
                    courier_reward_sum=excluded.courier_reward_sum
                    ;
                """,
                {
                    "courier_id": courier_ledger.courier_id,
                    "courier_name": courier_ledger.courier_name,
                    "settlement_year": courier_ledger.settlement_year,
                    "settlement_month": courier_ledger.settlement_month,
                    "count": courier_ledger.count,
                    "total_sum": courier_ledger.total_sum,
                    "rate_avg": courier_ledger.rate_avg,
                    "order_processing_fee": courier_ledger.order_processing_fee,
                    "courier_order_sum": courier_ledger.courier_order_sum,
                    "courier_tips_sum": courier_ledger.courier_tips_sum,
                    "courier_reward_sum": courier_ledger.courier_reward_sum,
                    
                },
            )


class courier_ledgerLoader:
    WF_KEY = "courier_ledgers_dds_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = courier_ledgersOriginRepository(pg_origin)
        self.stg = courier_ledgerDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_courier_ledgers(self):
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
            load_queue = self.origin.list_courier_ledgers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} courier_ledgers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier_ledger in load_queue:
                self.stg.insert_courier_ledger(conn, courier_ledger)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
