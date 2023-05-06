#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import datetime
import time
import psycopg2
import json

import requests
import json
import pandas as pd
import numpy as np

from logging import Logger
from typing import List

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class courierObj(BaseModel):
    id: int
    object_value: str

class couriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, courier_threshold: int, limit: int) -> List[courierObj]:
        nickname = "MartinNeads"
        cohort = "12"
        api_token = "25c27781-8fde-4b30-a22e-524044a7580f"

        headers = {
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
            "X-Nickname": nickname,
            "X-Cohort": cohort
            }

        params = {
            'sort_fild': 'id',
            'sort_direction' : 'asc',
            'limit' : 10,
            'offset': courier_threshold
            }

        
        response = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers', params=params, headers=headers)
        objs=response.json()
        keys=[0,1]
        print(objs)
        #objs=json2str(objs)
        lst=list()
        for i, item in enumerate(objs):
            cor=list()
            cor.insert(0,i+1)
            item=json2str(item)
            cor.insert(1,item)
            cor=tuple(cor)
            cor=dict(zip(keys,cor))
            print(cor)
            lst=lst+[cor]
            print(lst)
        print(lst)
        objs=lst
        return objs


class courierDestRepository:

    def insert_courier(self, conn: Connection, courier: courierObj) -> None:
        print(courier[1])
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(object_value)
                    VALUES (%(object_value)s)
                    ;
                """,
                {
                    "id": courier[0],
                    "object_value": courier[1]
                },
            )


class courierLoader:
    WF_KEY = "example_couriers_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = couriersOriginRepository(pg_origin)
        self.stg = courierDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
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
            load_queue = self.origin.list_couriers(last_loaded, self.BATCH_LIMIT)
            print(load_queue)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier in load_queue:
                print(courier[1])
                self.stg.insert_courier(conn, courier)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t[0] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

