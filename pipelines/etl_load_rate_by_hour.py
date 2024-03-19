"""
Case A.
It seems to me that  we can use classical RDB storage like Postgres.
ETL will schedule by Airflow and will include steps below:
1. load csv file in stg_table Postgres
2. filter and transform data to final result
3. join new increment with previous rate
"""


import psycopg2
import logging
import csv
import yaml
from evacuator import NeedEvacuation, evacuator
from src.functions_for_postgres import *
log = logging.getLogger(__name__)

def entryPoint(conf: dict) -> None:
    tmp_table = conf.get('stg_tables')[0]
    stg_table = conf.get('stg_tables')[1]
    target_table = conf.get('target_tables')

    log.info("Connect to the Postgresql")
    conn = get_connection(conf, log)
    cursor = conn.cursor()

    log.info(f"Checking tables status") #IN PROD contur this step is extra
    for table in  conf.get('stg_tables'):
        check_create_stg_table(cursor, log, table)

    check_create_target_table(cursor, log, target_table)

    log.info(f"Load data from CSV to Postgresql to {tmp_table}")
    load_csv(cursor, log, conf.get('direction'), tmp_table)

    log.info(f"Transform data from {tmp_table} to {stg_table}")
    write_from_tmp_to_stg(cursor, log, tmp_table, stg_table)

    count_stg = check_count_load_stg(cursor, log, stg_table)
    if count_stg == 0:
        raise NeedEvacuation(f"No new data in {stg_table}. Skip {conf.get('direction')}]")

    log.info(f"Transform data from {stg_table} to {target_table}")
    write_from_stg_to_raw(cursor,log, stg_table, target_table)

    cursor.close()
    conn.close()

def main():
    with open('/home/user/PycharmProjects/DE_task_360T/conf/task/rate.yaml') as file:
        conf_task = yaml.safe_load(file)

    with open('/home/user/PycharmProjects/DE_task_360T/conf/datasource/all.yaml') as file:
        conf_db = yaml.safe_load(file)

    conf_task.update(conf_db)

    log.info(f'conf: {conf_task}')

    entryPoint(conf_task)


if __name__ = "__main__":
    main()

