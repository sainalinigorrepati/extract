#!/usr/bin/env python3.9
import sys, os

from extract_helper import generate_api_status, create_spark_session, load_config, get_api, put_api, logs, get_metadata
from extract_oracle import extract_oracle
from constants import extract_constants
from hiped_helper import encryptNG, decrypt_with_DB_keys
from write import write_to_nas
from trigger_scrub import trigger_scrub
from pyspark.sql import SparkSession
from pyspark.context import SparkConf

'''Extract and "" PII Data from SORS, Load it to NAS and Trigger Scrub'''

def main(dbinfoId, table_extract_request_id, envr):
  global spark, logger, table_count, metadata, extract_config
  try:
    table_count = 0

    #Creating logger
    logger=logs()
    logger.info("Extract job started ...")
    logger.info("Creating Spark Session ...")
    service_id = table_extract_request_id.rsplit(':', '_').upper()
    job_name = f'{service_id}_EXTRACT'
    status = extract_constants.status_complete # only needs to change in case of failure

    spark=SparkSession=create_spark_session(job_name, None, True)
    spark.conf.set('spark.main_file_path', os.path.dirname(__file__).rstrip("python"))
    spark.conf.set('spark.service_id', table_extract_request_id.replace(':', '_').upper())
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    table_name = table_extract_request_id.split(":")[-1]
    #Load Properties
    logger.info(f"Loading {table_name}'s properties ... ")
    extract_config = load_config(f"{spark.cong.get('spark.main_file_path')}config/extract.yaml", logger)
    hiped_jar = extract_config['hiped_jar']

    logger.info("Fetching metadata from API.")
    metadata= get_metadata(extract_config, table_extract_request_id,table_name, logger, envr) # fetching metadata
    logger.info("Fetched metadata from API.")

    logger.info(f"Loading {table_name}'s properties ... ")
    user_config=get_api(f"https://{extract_config[envr]['db_info']['base_url']}fetchDBConfig?dbInfoId={dbinfoId}&env={envr}&templateId={metadata['template_id']}", logger)

    # Extract Data based on database type
    if metadata['db_type'].lower() == "oracle":
      logger.info(f"Starting {table_name} extraction ... ")
      (df, pii_list, enc_col)= extract_oracle(spark, user_config, table_name, metadata, extract_config, logger, envr, hiped_jar)
    elif metadata['db_type'].lower() == "sqlserver":
      logger.info(f"Starting {table_name} extraction ... ")
      (df, pii_list, enc_col) = extract_oracle(spark, user_config, table_name, metadata, extract_config, logger, envr,
                                                 hiped_jar)
    elif metadata['db_type'].lower() == "postgresql":
      logger.info(f"Starting {table_name} extraction ... ")
      (df, pii_list, enc_col) = extract_oracle(spark, user_config, table_name, metadata, extract_config, logger, envr,
                                                 hiped_jar)
    elif metadata['db_type'].lower() == "cassandra":
      logger.info(f"Starting {table_name} extraction ... ")
      (df, pii_list, enc_col) = extract_oracle(spark, user_config, table_name, metadata, extract_config, logger, envr,
                                                 hiped_jar)
      logger.info(f"{table_name} extraction completed ... ")
    else:
      raise Exception(f"Database type not supported: {user_config['db_type']}")

    #validate table count before writing to NAS
    # Check if table count is zero and throw an error & update API with "NO_RECORDS"


    table_count = df.count()
    #df.show(table_count, truncate=False)

    if (table_count <= 0):
      logger.info("Extraction count 0, no data to extract for the given query")
      logger.info("Skipping Encryption and Scrub Process!.....")
      logger.info(f"No files are written to NAS")
      logger.info("Updating extraction status ... ")
      json_data=generate_api_status(metadata['table_extract_request_id'], status, table_count)
      put_api(f"https://{extract_config[envr]['status']['base_url']}", json_data, logger)
      logger.info(f"Extract spark process completed with status: {status}")
    else:
      logger.info(f"Number of rows extracted:" + str(table_count))

      if len(enc_col) !=0:
        logger.info(f"Encoding columns:" + str(enc_col)).info('Decrypting DB encrypted data...')
        df = decrypt_with_DB_keys(extract_config, df, metadata, spark, envr, logger,hiped_jar, enc_col, None)
        logger.info('completed decrypting DB encrypted data ... ')
      else:
        logger.info("No DB encrypted data found for decryption")
        df = df

      #Hiped encryption for PII data
      if len(pii_list) != 0:
        logger.info('Started encryption process with EOT Keys ... ')
        enc_df = encryptNG(extract_config, df, pii_list, spark, envr, logger, hiped_jar)
        logger.info('completed encryption process with EOT Keys ... ')
      else:
        logger.info('No PII data found skipping encryption process with EOT Keys ... ')
        enc_df = df

      # write df to the path below in NAS Storage
      logger.info(f"Writing {table_name} to NAS ... ")
      output_path = f"{extract_config['output_path']['output_path_to_NAS']}{service_id}/{table_name.upper()}"
      logger.info(f"output_path: {output_path}")

      write_to_nas(enc_df, extract_config['output_path']['output_format'], output_path, logger, True)

      logger.info('Updating Extraction status ... ')
      json_data=generate_api_status(metadata['table_extract_request_id'], status, table_count)
      put_api(f"https://{extract_config[envr]['status']['base_url']}", json_data, logger)
      logger.info(f"Extract spark process completed with status: {status}")
      spark.stop()

      try:
        trigger_scrub(metadata, user_config, extract_config,envr,logger)

      except Exception as e:
        logger.error(f"error scrubbing data from {metadata['db_type']} for table {table_name}: {e}")

  except Exception as e:
    #Update with failed status and logger info out error message
    status = extract_constants.status_fail
    json_data = generate_api_status(metadata['table_extract_request_id'], status, table_count)
    put_api(f"https://{extract_config[envr]['status']['base_url']}", json_data, logger)
    logger.info(f"Error extracting data from {metadata['db_type']} for table {table_name}: {e}")
  finally:
    spark.stop()


if __name__ == "__main__":
  dbinfoId = str(sys.argv[1])
  table_extract_request_id = str(sys.argv[2])
  envr = str(sys.argv[3])
  main(dbinfoId, table_extract_request_id, envr)


