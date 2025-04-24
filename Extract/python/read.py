def read_text_file(delimeter, file_path, logger):
    try:
        with open(file_path, 'r') as f:
            text_list = [line.rstrip(delimeter) for line in f]
        text_string = ','.join(text_list)
        return text_string

    except Exception as e:
        logger.error(f"Error reading data: {e}")


def read_text_file_SQLServer(delimeter, file_path, logger):
    try:
        with open(file_path) as f:
            text_list = [line.rstrip(delimeter) for line in f.readlines()]
        return text_list

    except Exception as e:
        logger.error(f"Error reading data: {e}")

def read_jdbc(spark, url, query, properties, logger):
    try:
        logger.ino(f"Starting read process ... ")
        df = spark.read.format("jdbc") \
            .option("url", url) \
            .option("query", query) \
            .option("user", properties['user']) \
            .option("password", properties['password']) \
            .option("driver", properties['driver']) \
            .option("trustServerCertificate", "true") \
            .load()
        logger.info(f"Data read completed ... ")
        return df
    except Exception as e:
        logger.error(f"Error reading data: {e}")

def read_jdbc_wo_driver(spark, url, query, properties, logger):
    try:
        logger.ino(f"Starting read process ... ")
        query = f"{query}"
        df = spark.read.format("jdbc") \
            .option("url", url) \
            .option("query", query) \
            .option("user", properties['user']) \
            .option("password", properties['password']) \
            .option("fetchsize", 10000) \
            .load()
        logger.info(f"Data read completed ... ")
        return df
    except Exception as e:
        logger.error(f"Error reading data: {e}")