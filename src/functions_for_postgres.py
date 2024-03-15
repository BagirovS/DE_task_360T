import psycopg2
from omegaconf import DictConfig

def get_connection(conf: DictConfig,  cfg_task={}):
    conn_psycopg = psycopg2.connect(dbname=cfg_task.get('gp_database'),
                                    user=cfg_task.get('gp_user'),
                                    password=cfg_task.get('gp_password'),
                                    host=cfg_task.get('gp_host'))
    conn_psycopg.autocommit = True
    return conn_psycopg

def check_create_tables(cursor):
    sql_stg_table = '''CREATE TABLE IF NOT EXISTS entertainment_sandbox.rate_stg (
                    event_id BigINT,
                    event_time BigINT,
                    ccy_couple VARCHAR(255),
                    rate VARCHAR(255)
    )'''
    sql_table = '''CREATE TABLE IF NOT EXISTS entertainment_sandbox.rate_main (
                    event_id BigINT Primary key,
                    event_time BigINT,
                    ccy_couple VARCHAR(255),
                    rate VARCHAR(255)
    )'''
    cursor.execute(sql_stg_table)
    cursor.execute(sql_table)
    return "Tables are exists"

def load_csv(cursor, direction)-> None:
    sql_copy = f'''COPY rates_stg(event_id, event_time, ccy_couple, rate) 
    FROM '{direction}' 
    DELIMITER ',' 
    CSV HEADER;'''
    cursor.execute(sql_copy)

def check_count_load_stg(cursor)-> int:
    sql_check = "select count(*) from entertainment_sandbox.stg_nums_3m_pay_mmts"
    cursor.execute(sql_check)
    check_result = cursor.fetchall()
    return check_result[0][0]

def calculation(cursor)-> None:
    select_query = """
                    WITH active_rates AS (
                        SELECT  ccy_couple,
                                rate,
                                ROW_NUMBER() OVER (PARTITION BY ccy_couple ORDER BY event_time DESC) AS rn
                        FROM rates_stg
                        WHERE event_time >= UNIX_TIMESTAMP(NOW()) - 30
                    )
                    SELECT ccy_couple,
                           rate AS current_rate,
                           CONCAT(ROUND((rate - lag(rate, 1) OVER (PARTITION BY ccy_couple ORDER BY rn)) / rate * 100, 3), '%') AS change
                    FROM active_rates
                    WHERE rn = 1
                    ORDER BY ccy_couple;
                    """
    cursor.execute(select_query)