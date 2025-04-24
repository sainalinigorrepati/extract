#!/usr/bin/env python3.9

import subprocess, os, json, logging
from extract_helper import load_config, post_api
from write import write_text_file
import requests
import json


""" Trigger Scrub spark process at the end of the Extract Process """
def trigger_scrub(metadata, user_config, extract_config,envr,logger):
    logger.info("Triggering scrub process ... ")
    table_name = metadata["table_name"]
    service_id = metadata["service_id"].replace(':', '_').upper()

    main_file_path = os.path.dirname(__file__).rstrip("python")
    conf = load_config(f"{main_file_path}config/extract.yaml", logger)
    json_path = f"{conf['scrub']['base_path']}configs/{service_id.upper()}"

    if not os.path.exists(json_path):
        logger.error(f"File path does not exist: {json_path}. creating path ..")
        os.makedir(f"{json_path}")
        logger.info("path created .. ")

    logger.info(f"Writing metadata to {json_path}/{table_name.upper()}.json ... ")
    write_text_file(f"{json_path}/{table_name.upper()}.json", json.dumps(metadata), logger)

    url = "https://" + extract_config[envr]['scrub_url']['url']
    spark_master = extract_config[envr]['scrub_url']['spark_master']
    service_id_formatted = service_id.upper()
    json_file_path = f' (json_path}/{table_name.upper()}.json'
    payload = {
        "appResource": "/axp/tdm/data_masking/scrubbing/src/execute_data_masking_main.py",
        "sparkProperties": {
            "spark. executor-memory": "49",
            "spark master": f"{spark_master}",
            "spark driver memory": "4g",
            "spark driver.cores": "2",
            "spark.eventLog. enabled": "false",
            "spark-app. name": "Spark REST API - PI",
            "spark. submit. deployMode": "cluster",
            "spark. driver supervise": "true",
            "spark.driver.extraJavaptions": "-Djava.io.tmpdir=/abc/tmp",
            "spark. executor.extraJavaptions": "-Djava.io.tmpdir=/abc/tmp"
        },
        "clientSparkVersion": "3.5.0",
        "mainClass": "org. apache-spark.deploy-SparkSubmit",
        "environmentVariables": {
        "SPARK_ENV_LOADED": "1"
        },
        "action": "CreateSubmissionRequest",
        "appArgs": [
            "/abc/tdm/data_masking/scrubbing/src/execute_data_masking_main.py",
            service_id_formatted,
            json_file_path.strip()
        ]
    }
    logger.info(url)
    logger.info(payload)
    try:
        response = post_api(url, payload)
        logger.info(response)
    except Exception as e:
        logger.error(f"Failed to submit the job for {table_name}: {e}")
