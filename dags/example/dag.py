from datetime import datetime, timedelta, time
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


@task
def get_dag_params() -> dict:
    """
    Task'а на получение параметров из контекста DAG'а
    """
    context = get_current_context()
    start_date = (
        datetime.fromisoformat(context["params"]["start_date"])
        .replace(tzinfo=None)
        .isoformat()
    )
    end_date = (
        datetime.fromisoformat(context["params"]["end_date"])
        .replace(tzinfo=None)
        .isoformat()
    )
    return {
        "start_date": start_date,
        "end_date": end_date,
    }


@task
def get_sql_str(filename: str, sql_params: dict) -> str:
    """
    Таска на получение текста SQL файла с подставленными параметрами
    """
    query = ""
    sql_folder = Path(__file__).resolve().parent / "sql"
    with (sql_folder / filename).open(encoding="utf-8") as f:
        query += f.read()
        
    return query.format(**sql_params)

@task
def insert_data(**kwargs) -> None:
    """"
    Таска на получение данных из XCom и отправки их в конечную таблицую.
    Т.к. предыдущая таска является оператором - данные получаем из Xcom.
    """
    ti = kwargs['ti']

    hook = ClickHouseHook(clickhouse_conn_id='clickhouse_test')
    hook.execute(
        'INSERT INTO test_table_example VALUES', 
        params=ti.xcom_pull(task_ids='get_data')
    )


default_args = {
    "depends_on_past": False,
    # "email": [""],
    # "email_on_failure": True,
    # "email_on_retry": True,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
}

default_start_date = datetime.combine((datetime.today() - timedelta(days=1)), time.min)
default_end_date = datetime.combine(default_start_date, time.max)

with DAG(
    "example_dag",
    default_args=default_args,
    description="Пример DAG'а",
    schedule_interval="0 1 * * *", # Вписать необходимый интервал повтора
    start_date=days_ago(2),
    catchup=False,
    tags=["store", "daily", "dashboard"],
    params={    
        "start_date": Param(
            default=default_start_date.isoformat(),
            type="string",
            format='date-time'
        ),
        "end_date": Param(
            default=default_end_date.isoformat(),
            type="string",
            format='date-time'
        )
    },
) as dag:
    params = get_dag_params()

    # Подставляем параметры в запрос
    query = get_sql_str('example.sql', params)

    # Создаём таблицу для результатов, если она не существовала ранее
    check_table = ClickHouseOperator(
        task_id='check_table',
        clickhouse_conn_id='clickhouse_test',
        sql="""
        CREATE TABLE IF NOT EXISTS test_table_example
        (
            start_date Date,
            end_date Date,
            str_start_datetime String,
            str_end_datetime String,
        ) ENGINE = MergeTree
        ORDER BY start_date
        """,
    )

    # Получаем данные из продакшена
    data = ClickHouseOperator(
        task_id='get_data',
        clickhouse_conn_id='clickhouse_prod',
        sql=query,
    )

    # Помещаем данные в тестовую таблицу другого кластера
    insert_results = insert_data()

    [params >> query, check_table] >> data >> insert_results
