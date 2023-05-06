from logging import Logger
from typing import List

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime
from datetime import date
from datetime import time


class timestampObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time


class timestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, timestamp_threshold: int, limit: int) -> List[timestampObj]:
        with self._db.client().cursor(row_factory=class_row(timestampObj)) as cur:
            cur.execute(
                """
                    SELECT distinct 
                    oo.id,
                    oo.ts,
                    date_part('year', ts)::integer as year,
                    date_part('month', ts)::integer as month,
                    date_part('day', ts)::integer as day,
                    ts::date as date,
                    ts::time as time
                    from
                    (SELECT id, (object_value::JSON->>'order_ts')::timestamp AS ts
                    FROM stg.deliverysystem_deliveries
                    ) oo 
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": timestamp_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class timestampDestRepository:

    def insert_timestamp(self, conn: Connection, timestamp: timestampObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                    ;
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "date": timestamp.date,
                    "time": timestamp.time,
                },
            )


class timestampLoader:
    WF_KEY = "timestamps_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = timestampsOriginRepository(pg_origin)
        self.stg = timestampDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
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
            load_queue = self.origin.list_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for timestamp in load_queue:
                self.stg.insert_timestamp(conn, timestamp)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
