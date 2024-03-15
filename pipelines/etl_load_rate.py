"""
It sounds like we can use classical RDB storage like Postgres to resolve case A in fastest way than by spark
ETL will schedule by Airflow and will include two steps:
1. load csv file in stg_table Postgres
2. make calcualation
"""

import psycopg2
import logging
import csv
from omegaconf import DictConfig
from src.functions_for_postgres import get_connection,load_csv, check_create_tables, check_count_load_stg, calculation

log = logging.getLogger(__name__)

def main(conf: DictConfig, task_conf) -> None:

    log.info("Connect to the database")# Connect to the database
    log.info("Connect to the database")
    conn = get_connection() #TODO reserch how to work with configs first task_conf
    cursor = conn.cursor()
    log.info(f"{check_create_tables(cursor)}")
    load_csv(cursor, task_conf["direction"])
    log.info(f"csv file count:{check_count_load_stg(cursor)}")
    #TODO check count
    log.info("check count stg table")

    log.info("2. start making calcualation")

    calculation(cursor)



    cur.execute(select_query)

    # Commit the changes and close the cursor and connection
    conn.commit()
    cur.close()
    conn.close()

if __name__ = "__main__":
    main()

