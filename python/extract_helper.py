#!/usr/bin/env python3.9
import os, sys, yaml, requests, logging
from pyspark.sql import SparkSession
from pyspark.context import SparkConf

from hiped_helper import decryptNG, encrypt_with_DB_keys

def create_spark_session(job_name, warehouse_dir=None, enable_hive_support=False):
    conf=SparkConf()
    conf.set("spark.app.name", job_name)

    builder = SparkSession.builder \
      .appName(job_name) \
      .config("spark.sql.parquet.compression.code", 'uncompressed') \
      .config("spark.sql.parquet.writeLegacyFormat", "true") \

    if warehouse_dir:
        builder.config("spark.sql.warehouse.dir", warehouse_dir)

    if enable_hive_support :
        builder.enableHiveSupport()

    spark = builder.config(conf=conf).getOrCreate()

    return spark


def decrypt_driver_keys(extract_config, keys, key_cols, spark, envr, logger, hiped_jar, metadata, enc_col, column_list):
    try:
        if metadata['eot_encryption_applied'] == 'YES':
            if metadata['driver_column_encrypted'] == 'YES':

                logger.info('decrypting EOT encrypted driver keys ... ')
                keys_dec = decryptNG(extract_config, keys, key_cols, spark, envr, logger, hiped_jar)
                logger.info('completed decrypting EOT encrypted driver keys ... ')

                logger.info('Applying DB encryption to driver keys ... ')
                keys_dec = encrypt_with_DB_keys(extract_config, keys_dec, metadata, spark, envr, logger, hiped_jar)
                logger.info('DB encryption applied to driver keys ... ')

            else:
                logger.info('decrypting EOT encrytpion to driver keys ... ')
                keys_dec = decryptNG(extract_config, keys, key_cols, spark, envr, logger, hiped_jar)
                logger.info('decrypting EOT encrytpion to driver keys ... ')

        elif metadata['eot_encryption_applied'] == 'NO':
            logger.info('driver keys are DB encrypted, no encryption needs to be done')
            keys_dec = keys

        else:
            logger.error('Provide is the keys are encrypted or not')
            raise

        return keys_dec

    except Exception as e:
        logger.error(f'Error extracting data from oracle: {e}')

def get_metadata(config_load, table_request_id, table_name, logger, env):
    try:
        base_url = config_load[env]['metadata_info']['base_url']
        endpoint = config_load[env]['metadata_info']['endpoint']
        endpoint = endpoint.format(table_request_id)
        url = "https://" + base_url + endpoint
        metadata = get_api(url, logger)
        return metadata
    except Exception as e:
        logger.error(f'Error fetching metadata for table {table_name}: {e}')
        raise

def generate_api_status(table_request_id, stat, table_count):
    return{
        'table_extract_request_id': table_request_id,
        'status': stat,
        'job_type': 'EXTRACT',
        'records_processed': table_count
    }


def get_jdbc_url(dbinfo):
    host = dbinfo['marketConfig']["host_name"]
    port = dbinfo['marketConfig']["port"]
    service_name = dbinfo['marketConfig']["service_name"]
    db_name = dbinfo['marketConfig']["db_type"]
    user = dbinfo['marketConfig']["service_id"]
    JDBC = ""
    if db_name.upper() == 'ORACLE':
        JDBC=f"jdbc:{db_name}:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME={service_name})))"
    elif db_name.upper() == 'SQLSERVER':
        JDBC=f"jdbc:{db_name.lower()}://{host}:{port};database={service_name}"
    elif db_name.upper() == 'POSTGRESQL':
        JDBC = f"jdbc:{db_name.lower()}://{host}:{port}/{service_name}"
    return JDBC

def put_api(url, body, logger):
    try:
        logger.info(url)
        headers = {
            'Content-Type': 'application/json',
        }
        response = requests.post(
            url,
            headers=headers,
            json=body,
            verify=False
        )
        logger.info(f"Put call completed with response: {response}")
    except Exception as e:
        logger.error(f"Could not complete put call: {e}")

def load_secret(path, search_key):
    with open(path, 'r') as file:
        for line in file:
            #Assuming each line is in the frmat "key=value"
            if '=' in line:
                key, value = line.strip().split('=', 1)
                if key == search_key:
                    return value
    return None

def get_api(url, logger):
    try:
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            logger.info("API call completed successfully")
            return response.json()
        else:
            logger.info(f"{url} failed with code {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting API: {e}")

def load_config(file_path, logger):
    logger.info(file_path)
    with open(file_path, 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config

def get_helper_function():
    filepath = os.path.dirname(__file__).removesuffix('/extract/python')
    newpath = filepath + '/helper_function.py'+ '/lib/helper_functions-0.1-py3.9.egg'
    sys.path.append(newpath)

def set_spark_conf(spark):
    spark.conf.set('spark.main_file_path', os.path.dirname(__file__).rstrip('python'))
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    return spark

'''Creating logger'''
def logs():
    logger = logging.getLogger('main')
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    return logger