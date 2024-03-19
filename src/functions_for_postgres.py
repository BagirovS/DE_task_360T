import psycopg2
from omegaconf import DictConfig

def get_connection(conf, log):
    try:
        conn_psycopg = psycopg2.connect(dbname=conf.get('gp_database'),
                                    user=conf.get('user'),
                                    password=conf.get('password'),
                                    host=conf.get('host'),
                                    port=conf.get('port'))
        conn_psycopg.autocommit = True
        log.info(f"Connect is successful")
    except Exception as e:
        log.info(f"You can't connect while have {e}")
    return conn_psycopg

def check_create_flow_table(cursor, log, flow_table) -> None:
    sql_check_create_tbl = f''' CREATE TABLE IF NOT EXISTS {flow_table} (
                        flow_number INT,
                        status INT);
                        
                        --check is it first iteration
                        
                        SELECT * FROM {flow_table}
                        '''
    cursor.execute(sql_check_create_tbl)
    records = cursor.fetchall()
    if len(records) == 0:
        sql_insert = f''' INSERT INTO {flow_table} 
                          VALUES(1,0);
                          INSERT INTO {flow_table} 
                          VALUES(2,0);
                          INSERT INTO {flow_table} 
                          VALUES(3,0);                          
                                '''
        cursor.execute(sql_insert)
    log.info(f"{flow_table} is exist")

def check_flow_status(cursor, flow_table) -> list:
    sql_create_tbl = f'''SELECT *
                         FROM {flow_table}
                        '''
    cursor.execute(sql_create_tbl)
    records = cursor.fetchall()
    return records

def update_flow_status(cursor, log, flow_table, flow_number, status) -> None:
    sql_create_tbl = f'''Update {flow_table}
                         SET status = {status}
                         WHERE flow_number = {flow_number}
                        '''
    cursor.execute(sql_create_tbl)
    log.info(f"Update {flow_table} {flow_number} = {status}")



def check_create_stg_table(cursor, log, stg_table) -> None:
    sql_create_tbl = f''' CREATE TABLE IF NOT EXISTS {stg_table} (
                        event_id BIGINT,
                        event_time BIGINT,
                        ccy_couple VARCHAR(10),
                        rate double precision);
                        '''
    cursor.execute(sql_create_tbl)
    log.info(f"{stg_table} is exist")


def check_create_target_table(cursor, log, target_table) -> None:
    sql_create_tbl = f''' CREATE TABLE IF NOT EXISTS {target_table} (
                        ccy_couple VARCHAR(10),
                        rate double precision,
                        change VARCHAR(10));
                        '''
    cursor.execute(sql_create_tbl)
    log.info(f"{target_table} is exist")

def load_csv(cursor, log, direction, tmp_table)-> None:
    sql_copy = f'''COPY {tmp_table}(event_id, event_time, ccy_couple, rate) 
    FROM '{direction}' 
    DELIMITER ',' 
    CSV HEADER;'''
    log.info(f"CSV upload successfully")
    cursor.execute(sql_copy)

def write_from_tmp_to_stg(cursor, log, tmp_table, stg_table)-> None:
    """
    :param tmp_table: data which we load from csv
    :param stg_table: transformed data from tmp_table
    --
    function should clear rows which doesn't match for our case
    before load to stg_table we should clean previous rows
    For our case it seems to me that we don't need rows with 0 values in rate column
    And we need only last rate
    """
    sql_check = f"""TRUNCATE {stg_table};
                    INSERT INTO {stg_table}
                    SELECT event_id, event_time, ccy_couple, rate
                    FROM (SELECT event_id,
                                 event_time,
                                 ccy_couple,
                                 rate,
                                 row_number()over (partition by ccy_couple order by event_time desc) as last_rate
                          FROM {tmp_table}
                          WHERE 1=1
                          AND rate > 0
                          AND event_time >= cast(floor(cast((EXTRACT(EPOCH FROM  current_timestamp::TIMESTAMP) * 1000) as float)) as bigint) - 30000) GG
                    WHERE last_rate = 1"""
    cursor.execute(sql_check)
    log.info(f"Transform data from {tmp_table} to {stg_table} completed successfully")



def check_count_load_stg(cursor, log, table_name)-> int:
    sql_check = f"select count(*) from {table_name}"
    cursor.execute(sql_check)
    check_result = cursor.fetchall()
    log.info(f"Count of rows in {table_name}: {check_result[0][0]}")
    return check_result[0][0]

def write_from_stg_to_raw(cursor, log, stg_table, target_table)-> None:
    """
    :param stg_table: data from csv file
    :param target_table: table which we want to update
    --
    function should update rote which has changed
    """
    sql_update = f"""WITH new_rate as (SELECT r.ccy_couple,
                                        s.rate,
                                        cast(round(CAST((((s.rate-r.rate)/r.rate)*100) as numeric), 3) as varchar(10))||'%' as new_change 
                                      FROM {target_table} R
                                      INNER JOIN {stg_table} S
                                      ON R.ccy_couple = S.ccy_couple
                                      AND R.rate <> S.rate)
                   UPDATE  {target_table} t
                   SET change = n.new_change,
                       rate = n.rate
                   FROM new_rate n
                   WHERE t.ccy_couple = n.ccy_couple"""
    cursor.execute(sql_update)
    log.info(f"Transform data from {stg_table} to {target_table} completed successfully")