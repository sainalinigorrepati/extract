from pyspark.sql import SparkSession

# Path to your JDBC driver (ojdbc8.jar)
# jdbc_jar_path = "/path/to/your/ojdbc8.jar"

# Oracle connection details
jdbc_url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCPS)(HOST=adb.us-phoenix-1.oraclecloud.com)(PORT=1522)))(CONNECT_DATA=(SERVICE_NAME=gdbeb10839997d0_testdb_high.adb.oraclecloud.com))(SECURITY=(SSL_SERVER_DN_MATCH=YES)))"

# Your Oracle database credentials
db_user = "admin"
db_password = "Nalini@12345"

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Oracle Connection") \
    .getOrCreate()

# Example query to read data from an Oracle table
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "CUSTOMERS") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("connectTimeout", "60000") \
    .load()
# Timeout in milliseconds (1 minute) # Socket timeout

# Show the dataframe content

