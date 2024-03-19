"""
Case B.
It seems to me that we can use previous structure of project but
in the same time we have to divide calculation by several flow
because we can have meet some kind trouble with time execution if we have only one flow.
"""

import psycopg2
import logging
import time
import csv
import yaml
from evacuator import NeedEvacuation, evacuator
from src.functions_for_postgres import *


log = logging.getLogger(__name__)

def entryPoint(conf: dict) -> None:
    flow_table = conf.get('flow_table')

    log.info("Connect to the Postgresql")
    conn = get_connection(conf, log)
    cursor = conn.cursor()


    log.info(f"Checking {flow_table} status")
    check_create_flow_table(cursor, log, flow_table)
    flows_statuses = check_flow_status(cursor, flow_table)
    flag = True

    while flag:

        for flow in flows_statuses:
            if flow[1] == 0:
                log.info(f"Close flow number {flow[0]} for calculation")
                update_flow_status(cursor, log, flow_table, flow[0], 1)

                tmp_table = f"{conf.get('stg_tables')[0]}_{flow[0]}"
                stg_table = f"{conf.get('stg_tables')[1]}_{flow[0]}"
                target_table = conf.get('target_tables')

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
                    update_flow_status(cursor, log, flow_table, flow[0], 0)
                    raise NeedEvacuation(f"No new data in {stg_table}. Skip {conf.get('direction')}]")

                log.info(f"Transform data from {stg_table} to {target_table}")
                write_from_stg_to_raw(cursor,log, stg_table, target_table)

                log.info(f"Open flow number {flow[0]} for new calculation")
                update_flow_status(cursor, log, flow_table, flow[0], 0)

                cursor.close()
                conn.close()
                flag = False
                break
        log.info(f"Waiting 2 seconds for new flow")
        time.sleep(2)

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
