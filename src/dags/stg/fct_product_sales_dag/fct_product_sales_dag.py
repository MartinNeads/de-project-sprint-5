import logging

import pendulum
from airflow.decorators import dag, task
from stg.fct_product_sales_dag.sales_loader import saleLoader
from lib.pg_connect import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_fct_product_sales_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="sales_load")
    def load_sales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = saleLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_sales()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    sales_dict = load_sales()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    sales_dict  # type: ignore


dds_fct_product_sales_dag = sprint5_dds_fct_product_sales_dag()
