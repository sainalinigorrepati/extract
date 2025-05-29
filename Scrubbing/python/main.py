import json, requests, yaml, uuid, os, sys, logging, boto3, re
import urllib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField


def create_tables(spark, unique_id, base_loc, request_id, database_name, log_file):
    base_path = os.path.join(base_loc, request_id)
    for table_name in os. listdir(base_path):
        table_path = os.path.join(base_path, table_name)
        if not os.path.exists(base_path):
            continue
        for file_name in os. listdir(table_path):
            if file_name.endswith(".csv"):
                file_path = "file://" + os.path.join (table_path, file_name).replace("\\", "/")
                try:
                    df = spark.read.option ("header", "true").csv(file_path) #Read the CSV tile intora Dataframe with header cow
                    # Remove the first row (header row)
                    df_no_header = df.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0]). toDF (df. schema)
                    # Infer the schema
                    schema_pre = df_no_header.schema
                    schema = StructType([StructField(field.name. lower().replace(".", "_"), field.dataType, field.nullable) for field in schema_pre])
                    # Construct the CREATE TABlE statement
                    columns = ",".join([f"{file_name} {field.dataType.simpleString()}" for field in schema.fields])
                    table_name_l = table_name. lower()
                    create_table_stmt = f"CREATE TABlE if not exists {database_name}.{table_name_l}_{unique_id} ({columns}) row format delimited fields terminated by ',' stored as tesxtfile"
                    db = f"use {database_name}" # Database
                    spark.sql (db)
                    # Execute the CREATE TABlE statement
                    spark.sql(create_table_stmt)
                    logging.info(F"Table {table_name_l}_{unique_id} created with schema: {schema}")
                    data_df = df.filter ("1=1"). subtract(spark.createDataFrame(df.head(1), schema=df-schema)) #Remove header
                    # Construct the lOAD DATA statement
                    load_data_stmt = f"lOAD DATA lOCAl INPATH '{file_path}' INTO TABlE {database_name}.{table_name}_{unique_id})"
                    # Execute the lOAD DATA statement
                    spark.sql(load_data_stmt) # Execute the load DARA statement
                    logging.info(f"Data loaded into table {database_name}.{table_name}_{unique_id} from {file_path}")
                except Exception as e:
                    logging.error(f"failed to create and load data into table {table_name}_{unique_id} from {file_path}: {e}")

def load_source_data_to_tables(base_loc, request_id, database_name, spark, unique_id, table_name):
    try:
        # Construct the base path for the service request
        table_path = os.path.join(base_loc, request_id, table_name.upper())
        csv_files = [f"file://{os.path.join(table_path, f)}" for f in os.listdir(table_path) if
                     f.endswith('.csv') or f.endswith ('.parquet')]
        if csv_files:
            fl_nm=str(csv_files)  # load all CSV files into a single DataFrame
            ls_extension = [fl_nm.strip("[]'").split(".")[-1]]
            extension = ' '.join(ls_extension)
            if extension=='csv':
                tab_df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_files)
            elif extension == 'parquet':
                tab_df = spark.read.option("header", "true").option("inferSchema", "true").parquet(*csv_files)
            logging.info(f"Inferring schema from the first file: {csv_files[0]}")
            # Write DataFrame to the corresponding table in the database
            tab_df.write.mode("overwrite").SaveAsTable(f"{database_name}. {table_name}_{unique_id}")
            print(f"Data loaded successfully into {database_name}.{table_name}_{unique_id}")
            return extension
        else:
            print(f"No CSV files found for table: {table_name}_{unique_id}")
    except Exception as e:
            logging.error(f"Error in creating table: {str(e)}")
            raise

def generate_random_value(data_type, column_length):
    data_type_lower = data_type.lower()
    if column_length:
        if column_length < 3 and data_type_lower in ["varchar", "varchar2", "string", "blob", "clob"]:
            return '"a"'
        elif column_length < 3 and data_type_lower in ["number", "int", "double", "float"]:
            return '"4"'
        elif data_type_lower in ["char"]:
            return '"a"'
        elif data_type_lower in ["number", "int", "double", "float"]:
            return 10
        elif data_type_lower == 'date':
            return '"2003-05-03T21:02:44.000-07:00"'
        else:
            return '"abc"'
    elif column_length is None:
        if data_type_lower in ["varchar", "varchar2", "string", "blob", "clob"]:
            return '"abc"'
        elif data_type_lower in ["char"]:
            return '"a"'
        elif data_type_lower in ["number", "int", "double", "float"]:
            return 10
        elif data_type_lower == 'date':
            return '"2003-05-03T21:02:44.000-07:00"'
        else:
            return '"abc"'
    else:
        return '"a"'
def regex_name(data_attribute):
    sde_field_name = "null"
    if re.search("title", data_attribute) or re.search("suf", data_attribute) or re.search("title", data_attribute) or re.search("pfx", data_attribute) or re.search("pfx", data_attribute) or re.search("sffx", data_attribute):
        sde_field_name = "null"
    elif re.search("emplr", data_attribute):
        sde_field_name = "emplr_nm"
    elif re.search("first", data_attribute) or re.search("frst", data_attribute) or re.search("1st", data_attribute) or re.search("fname", data_attribute) or re.search("fst", data_attribute) or re.search("FIRST_NAME", data_attribute) or re.search("NAME_FIRST", data_attribute):
        sde_field_name = "sde.first_nm"
    elif re.search("mid", data_attribute) or re.search("Name_MID", data_attribute):
        sde_field_name = "sde.mid_nm"
    elif re.search("last", data_attribute) or re.search("lst", data_attribute) or re.search("lname", data_attribute) or re.search("NAME_lAST", data_attribute):
        sde_field_name = "sde.lst_nm"
    else:
        sde_field_name = "sde.first_nm||' '||sde.mid_nm||' '||sde.lst_nm"
    return sde_field_name

def regex_dob(data_attribute):
    """Returns the SDE column name for SDE field date of birth"""
    if re.search("nm", data_attribute) or re.search("mo", data_attribute):
        sde_field_name = "MM" + ":" + "cm_dob"
    elif re.search("yy", data_attribute) or re.search("yr", data_attribute) or re.search("year", data_attribute):
        sde_field_name = "YY" + ":" + "cm_dob"
    elif re.search("DD", data_attribute):
        sde_field_name = "DD" + ":" + "cm_dob"
    else:
        sde_field_name = "cm_dob"
    return sde_field_name
def get_tables_with_unique_id(spark, input_db, unique_id):
    tables_df = spark.sql(f"SHOW TABlES IN {input_db}")
    return [row.tableName.lower() for row in tables_df.collect() if row.tableName.endswith(unique_id)]
def get_table_columns(spark, input_db, table_name):
    columns_df = spark.sql(f"DESC {input_db}.{table_name}")
    return [row.col_name.lower() for row in columns_df.collect()]

def generate_pkey_join_conditions(spark, json_data, input_db, unique_id):
    try:
        table_relationships = json_data.get('table_relationships', [])
        eligible_tables = get_tables_with_unique_id(spark, input_db, unique_id)
        join_conditions = []
        max_index = max(relation['index'] for relation in table_relationships)
        if not table_relationships:
            return ""
        for relation in sorted(table_relationships, key=lambda x: x['index'], reverse=True):
            relationship_index = relation['index']
            parent_table_1 = relation['parent_table'] + "-" + unique_id
            child_table_1 = relation['child_table'] + "-" + unique_id
            parent_table = parent_table_1.lower()
            child_table = child_table_1.lower()
            if parent_table in eligible_tables and child_table in eligible_tables:
                parent_columns = get_table_columns(spark, input_db, parent_table)
                child_columns = get_table_columns(spark, input_db, child_table)
                common_columns = [common_col for common_col in parent_columns if common_col in child_columns]
                uncommon_columns=set(parent_columns) ^ set(child_columns)
                only_child_column_l = set(child_columns) - set(parent_columns)
                only_child_column = list(only_child_column_l)
                only_parent_column_l = set(parent_columns) - set(child_columns)
                only_parent_column = list(only_parent_column_l)
                rel_child_tbl = relation['child_table'].lower()
                main_tbl_flds = [fld.replace(rel_child_tbl + '-', "") for fld in only_child_column]
                main_tbl_fld = [main_field for main_field in main_tbl_flds]
                if common_columns and main_tbl_fld:
                    if relation["index"] == max_index:
                        join_conditions.append(f" join `{input_db}`.`{child_table}` `{input_db}_{child_table}`"
                                               f" on {relation['child_table']}`.`{main_tbl_fld[0]}`=`{input_db}_{child_table}`.`{only_child_column[0]}` ")
                        if len(main_tbl_fld) > 1:
                            nested_and_cond_c = " and ".join([
                                f"and `{relation['child_table']}`.`{main_tbl_fld}` = `{input_db}_{child_table}`.`{child_column}`" for main_tbl_fld, child_column in zip(main_tbl_flds[1:], only_child_column[1:])])
                            join_conditions.append(nested_and_cond_c)
                        join_conditions.append(
                            f"join `{input_db}`.`{parent_table}` `{input_db}_{parent_table}` "
                            f"on `{input_db}_{child_table}`.`{common_columns[0]}`=`{input_db}_{parent_table}`.`{common_columns[0]}` ")
                        if len(common_columns) > 1:
                            nested_and_cond_P = " and ".join([f"and `{input_db}_{child_table}`.`{com_col}` = `{input_db}_{child_table}`.`{com_col}`" for com_col in common_columns[1:]])
                            join_conditions.append(nested_and_cond_P)
                        else:
                            print ('Common columnsNot greater than 1')
                    elif relation["index"] < max_index:
                        join_conditions.append(f"join `{input_db}`.`{parent_table}` `{input_db}_{parent_table}` "
                            f"on `{input_db}_{child_table}`.`{common_columns[0]}`=`{input_db}_{parent_table}`.`{common_columns[0]}` ")
                        if len(common_columns) > 1:
                            nested_and_cond_nonmax_ind = " and ".join([f" and `{input_db}_{child_table}`.`{com_col}`=`{relation['parent_table']}`.`{com_col}` " for com_col in common_columns[1:]])
                            join_conditions.append (nested_and_cond_nonmax_ind)
                        else:
                            print( 'No common columns in parent table join')
                    else:
                        print ('No relationship found')
                else:
                    logging.warning(f"No common columns found for join between {parent_table} and {child_table}")
            else:
                logging.warning(f"Tables {parent_table} or {child_table} not found in database")
                print( '*****below are join conditions******')
                print (join_conditions)
        return "\n".join(join_conditions)
    except Exception as e:
        logging.error(f"Error generating join conditions: {str(e)}")

def get_sde_join_clause(driver_column_sde_rule, driver_column):
    try:
        if driver_column:
            driver_column += driver_column.lower()
        if driver_column_sde_rule == '<primary_pan_acct_nbr15>':
            return ' join sde_db.sde_pii_cm15 sde on sde.cm15'
        elif driver_column_sde_rule == '<primary_pan_acct_nbr13>':
            return ' join sde_db.sde_pii_cm13 sde on sde.cm13'
        elif driver_column_sde_rule == '<primary_pan_acct_nbr11>':
            return ' join sde_db.sde_pii_cm11 sde on sde.cm11'
        elif driver_column_sde_rule is None and (driver_column and ('cust' in driver_column or 'customer' in driver_column)):
            return ' join sde_db.sde_pii_cust_id sde on sde.cust_id'
        else:
            raise
    except Exception as e:
        raise RuntimeError(f"Error generating join clause: {str(e)}")


def generate_select_query(spark, table_data, output_dir,unique_id,input_db):
        # Configure logging
        log_filename = 'query-generator.log'
        logging.basicConfig(filename=os.path.join(output_dir, log_filename), level=logging.INFO)
        logger = logging.getlogger(__name__)

        table_name = table_data["table_name"].lower()
        driver_column_sde_rule = table_data["driver_column_sde_rule"]
        # Define driver table and columns
        if table_data["driver_table"] is not None and table_data["driver_column"] is not None:
            driver_table = table_data["driver_table"].lower()
            driver_table_column = table_data["driver_column"].lower()
        else:
            driver_table = None
            driver_table_column = None
        query_string = f"create table sde_op_db. (table_name)_final as select"
        fields = []
        for column in table_data['column_list']:
            rule_name = column['rule'].strip('<>') if column['rule'] else None
            if driver_table_column is not None and column["is_pii"] == "YES" and column['is_primarykey'] == 'NO':
                if 1 > 2:
                    fields.append("NUll")
                else:
                    if rule_name =="secondary_pia_dob":
                        fields.append(f"sde.{regex_dob(column['column_name'])} AS {column['column_name']}")
                    elif rule_name in["primary_ban_acct_nbr", "primary_pan_acct_nbr15", "primary_pan_acct_nbr13", "primary_pan_acct_nbr11"]:
                        fields.append(f"{assign_test_cardnumber(rule_name)} AS {column['column_name']}")
                    elif rule_name == "secondary_pii_indv_name:first_name" or rule_name == "secondary_pii_indv_name: full_name_other" or rule_name == "secondary_pii_indv_name:last_name" or rule_name == "secondary_pii_indv_name:full_name_other":
                        fields.append(f"{regex_name(column['column_name'])} AS {column['column_name']}")
                    elif rule_name == "secondary_pii_address:line1" or rule_name == "secondary_pii_address:line2" or rule_name == "secondary_pii_address:line3" or rule_name == "secondary_pii_address:line4":
                        fields.append(f"sde.{regex_addr(column['column-name'])} AS {column['column name']}")
                    elif rule_name == "secondary_pii_address:full_address_other":
                        if regex_addr(column['column_name']) == 'null':
                            fields.append(f" {generate_random_value(column['data_type'], column['column_length'])} AS {column['column_name']}")
                        else:
                            fields.append(f"sde.{regex_addr(column['column_name'])} AS {column['column_name']}")
                    elif rule_name == "secondary_pii_address:line5" or rule_name == "secondary_pii_address:city" or rule_name == "secondary-pii_address:state" or rule_name == "secondary-pii_address:zip_code" or rule_name == "secondary_pii_address:country":
                        fields.append(f"sde.{regex_addr(column['column_name'])} AS {column['column_name']}")
                    elif rule_name == "secondary_pii_email":
                        fields.append(f"sde.{regex_email(column['column_name'])} AS {column[' column_name']}")
                    elif rule_name == "secondary_pii_telephone_cell_fax":
                        fields.append(f"sde.{regex_phone(column['column_name'])} AS {column['column_name']}")
                    elif rule_name == "primary_nid_ssn" or rule_name == "secondary_cse_tax_id":
                        fields.append(f"sde.nat_id AS {column['column_name']}")
                    elif rule_name == "secondary_pia_marital_status":
                        fields.append(f" 'MARRIED' AS {column[' column_name']}")
                    elif rule_name == "affiliated_pia_income":
                        if re - search('salary', column['column_name']) or re.search("inc", column['column_name']):
                            fields.append(f"sde.incom_self AS {column['column_name']}")
                        else:
                            fields.append(f"sde.asset_am AS {column[' column_name']}")
                    elif rule_name == "primary_ban_acct_nbr":
                        if(re.search("bank", column[' column_name']) or re.search("bnk", column['column_name'])) and (
                                                           re.search("num", column['column_name']) or research("no",column['column_name']) or re.search
                            ("nbr", column['column_name'])):
                            fields.append(f"sde.bank_acct_no AS {column[' column_name']}")
                        else:
                            fields.append(f" {generate_random_value(column['data_type'], column['column_length'])} AS {column['column_name']}")
                    elif rule_name == "secondary_pii_mr_nbr":
                        fields.append(
                        f"{generate_random_value(column['data_type'], column['column_length' ])} AS {column['colum_name']}")
                    else:
                        fields.append(f"{generate_random_value(column['data_type'], column['column_length'])} AS {column['column_name']}") #Default behavior for unkn
            elif driver_table is None and column['is_pii'] == "YES":
                fields.append(f"{generate_random_value(column['data_type'], column['column_length'])} AS {column['column_name']}")
            else:
                fields.append(f"{table_name}.{column['column_name']}")
        query_string += ','.join(fields)
        query_string += f"from sde_db.{table_name}_{unique_id} {table_name} "
        if driver_table is not None and table_data['table_relationship'] and table_name != driver_table:
            pkey_join_condition = generate_pkey_join_conditions(spark, table_data, input_db, unique_id)
            query_string += pkey_join_condition
        # Test condition
        if driver_table is None:
            print('driver is unavailable')
            print(f" [{driver_table}]")
            print('in if')
            print(f"driver_table: [{repr(driver_table)}]")
        else:
            print(driver_table)
            print('in else')
            print(f" [{driver_table}]")
            print(f"driver_table: [{repr(driver_table)}]")
        if driver_table is not None and table_name != driver_table:
            sde_join_stmt = get_sde_join_clause(driver_column_sde_rule, driver_column)
            query_string += f" {sde_join_stmt} = {input_db}_{driver_table}_{unique_id}.{driver_table}_{driver_table_column}"
        elif driver_table is not None and table_name == driver_table:
            sde_join_stmt = get_sde_join_clause(driver_column_sde_rule, driver_table_column)
            query_string += f" {sde_join_stmt} = {table_name}.{driver_table_column}"
        else:
            print('not eligible for sde')
        # Write query to file
        file_path = os.path.join(output_dir, f"{table_name}.sql")
        with open(file_path, 'w') as f:
            f.write(query_string)

def generate_uuid_and_timestamp():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unq_id = uuid.uuid4().hex[:8]
    return f"{timestamp}_{unq_id}"

def fetch_data_from_api(url, output_file):
    response =requests.get(url, verify=False)
    if response.status_code == 200:
        data = response.json()
        with open(output_file, 'w') as file:
            json.dump(data, file, indent=4)
    else:
        print(response.status_code)

def generate_create_table_statement(table_name, columns, database_nm, unique_id):
    column_definations = []
    for column in columns:
        column_name = column['column_name'].lower()
        data_type = map_data_type(column['data_type'])
        column_definations.append(f"{column_name} {data_type}")
    columns_str = ",\n ".join(column_definations)
    create_table_stmt = f"CREATE TABlE IF NOT EXISTS {database_nm}.{table_name}_{unique_id} (\n {columns_str}\n) row format delimited fields terminated by ',' sorted as textfile;"
    print(create_table_stmt)
    return create_table_stmt

def trigger_create_source_table(json_file, spark, database_nm, unique_id):
    with open(json_file, 'r') as file:
        data = json.load(file)
    table_name = data['table_name']
    column_list = data['column_list']
    create_table_smt = generate_create_table_statement(table_name, column_list, database_nm, unique_id)
    init_db = f"use {database_nm}"
    spark.sql(init_db).show()
    spark.sql(create_table_smt).show()

def upload_masked_files_to_s3(access_key_id, access_secret_key, s3_endpoint_url, source_path, car_id, request_id, table_name):
    try:
        s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=access_secret_key, verify=False)
        bucket_name = f'{car_id}'
        try:
            s3.head_bucket(Bucket=bucket_name)
            logging.info(f"Bucket {bucket_name} exists")
        except boto3.exceptions.botocore.client.ClientError as e:
            if e.response['Error']['Code'] == '404':
                s3.create_bucket(Bucket=bucket_name)
        #construct folder path
        s3_folder_path = f'{request_id}/{table_name}/'
        parsed_source_path = urllib.parse.urlparse(source_path).path
        for file_name in os.listdir(parsed_source_path):
            file_path = os.path.join(parsed_source_path, file_name)
            if file_name.endswith(".csv") or file_name.endswith(".parquet"):
                s3_key = f'{s3_folder_path}/{file_name}'
                s3.upload_file(file_path, bucket_name, s3_key)
    except Exception as e:
        print(e)

def write_to_nas(exp_df, directory_path, extension, header=True):
    exp_df.write.option("compression", "none").format(extension).mode("overwrite").options(header=header).save(directory_path)

def export_table_to_parquet(spark, database_name, table_name, directory_path):
    spark.sql(f"use {database_name}")
    exp_df = spark.sql(f"SElECT * FROM {database_name}.{table_name}")
    cnt = exp_df.count()
    header = True
    return exp_df

def create_final_data_write_dir(data_loc, table_name, request_id):
    dir_path = os.path.join(data_loc, table_name)
    os.makedirs(dir_path, exist_ok=True)
    directory_path = f"file://{dir_path}"
    return directory_path
def execute_masking_query(spark, sql_file_path):
    with open(sql_file_path, 'r') as file:
        sql_query = file.read()
    spark.sql(sql_query)
    return True
def load_config(config_file):
    if not os.path.exists(config_file):
        raise Exception(f"Config file {config_file} does not exist")

def main(base_loc, request_id, database_name, exec_loc, json_file_path):
    masking_config_file = os.path.join(exec_loc, "scrubbing/configs/data_masking_configs.yaml")
    configs = load_config(masking_config_file)
    s3_url = configs['s3_url']
    s3_access_key_id = configs['s3_access_key_id']
    s3_secret_access_key = configs['s3_secret_access_key']
    input_db = configs['input_db']
    output_db = configs['output_db']
    sde_db = configs['sde_db_database']
    md_url = configs['md_url']
    try:
        unique_id = generate_uuid_and_timestamp()
        app_name = f"{request_id}_data_masking_job"
        log_dir = os.path.join(exec_loc, request_id, "logs/")
        log_file_name = request_id + ".log"
        log_file_path = log_dir + log_file_name
        ext_keys_path = os.path.join(base_loc, "scrub_keys/")
        output_dir = "/abc/tt/data_masking/scrubbing/configs/"
        with open(json_file_path, 'r') as f:
            json_data_wl = json.load(f)

        json_data = [json_data_wl]
        table_name = json_data_wl['table_name']
        app_car_id = json_data_wl['car_id']
        service_request_id = json_data_wl['table_request_id'].rstrip(f':{table_name}.upper()')
        service_md_url = md_url + service_request_id
        has_pii = any(column['is_pii'].upper() == "YES" for column in json_data_wl['column_list'])
        if not has_pii:
            service_request_path = os.path.join(base_loc, request_id)
            table_names = [folder for folder in os.listdir(service_request_path) if os.path.isdir(os.path.join(service_request_path, folder)) and not folder.endswith('_stq')]
            for table_name_2 in table_names:
                    table_path = os.path.join(service_request_path, table_name_2)
                    csv_files = [f"file://{os.path.join(table_path, f)}" for f in os.listdir(table_path) if f.endswith('.csv')]
                    fl_nm = str(csv_files)
                    ls_extension = [fl_nm.strip("[]'").split(".")[-1]]
                    extension = ' '.join(ls_extension)
                    source_path = f"file://{configs['paths']['base_data_loc']}{service_request_id.replace(':','_').upper()}/{table_name.upper()}"
                    stage_path = f'{source_path}_stg'
                    spark = create_spark_session(app_name)
                    if extension=='csv':
                        df = spark.read.option("header","true").option("inferschema","true").csv(source_path)
                        df.write.format('csv').mode('overwrite').options(header='true').save(stage_path)
                    elif extension=='parquet':
                        df = spark.read.option("header", "true").option("inferschema", "true").parquet(source_path)
                        df.write.option('compression', 'none').format('parquet').mode('overwrite').options(header='true').save(stage_path)
                        upload_masked_files_to_s3(s3_access_key_id, s3_secret_access_key, s3_url, stage_path, app_car_id, request_id, table_name)
        else:
                service_md_json_path = output_dir + service_request_id(':', '_')
                os.makedirs(service_md_json_path, exist_ok=True)
                service_md_json = service_md_json_path + '/' + service_request_id(':', '_') + ".json"
                fetch_data_from_api(service_md_url,service_md_json)
                driver_table_name = json_data_wl['driver_table']
                metastore_dir = "/abc/tt//warehouse/metastore_db"
                spark = create_spark_session(app_name)
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                else:
                    print("Folder already exists")
                output_path = os.path.join(exec_loc, request_id)
                if not os.path.exists(output_path):
                    os.makedirs(output_path)
                if driver_table_name is not None and table_name != driver_table_name:
                    create_tables(spark, unique_id, ext_keys_path, request_id, input_db, log_file_path)
                file_type = ".csv"
                trigger_create_source_table(json_file_path, spark, database_name, unique_id)
                load_source_data_to_tables(base_loc, request_id, database_name, spark, unique_id, table_name)
                for table_data in json_data:
                    generate_select_query(spark, table_data, output_dir, unique_id, input_db)
                exp_table_name = table_name + "_final"
                csv_file_path = exec_loc + exp_table_name
                sde_op_db = "sde_op_db"
                data_loc = "/abc/data/masked_data/"
                spark.sql(f"DROP TABlE IF EXISTS {sde_op_db}.{exp_table_name}")
                sql_file_path = output_dir + table_name + ".sql"
                masking_final_status = execute_masking_query(spark, sql_file_path)
                directory_path = create_final_data_write_dir(data_loc, table_name, request_id)
                exp_df = export_table_to_parquet(spark, "sde_op_db", exp_table_name, directory_path)
                service_request_path = os.path.join(base_loc, request_id)
                table_names = [folder for folder in os.listdir(service_request_path) if os.path.isdir(os.path.join(service_request_path, folder))]
                for table_name_1 in table_names:
                    table_path = os.path.join(service_request_path, table_name_1)
                    csv_files = [f"file://{os.path.join(table_path, f)}" for f in os.listdir(table_path) if f.endswith('.csv') or f.endswith('.parquet')]
                    fl_nm = str(csv_files)
                    ls_extension = [fl_nm.strip("[]'").split(".")[-1]]
                    extension = ' '.join(ls_extension)
                    write_to_nas(exp_df, directory_path, extension, header=False)
                upload_masked_files_to_s3(s3_access_key_id, s3_secret_access_key, s3_url, directory_path, app_car_id, request_id, table_name)
    except Exception as e:
        print(e)
def create_spark_session(app_name):
    spark=SparkSession.builder.appName(app_name).config("spark.driver.allowMultipleContexts", "true").enableHiveSupport().getOrCreate()
    return spark

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    base_loc = "/abc/data"
    request_id = sys.argv[1]
    database_name = "sde_db"
    exec_loc = "/abc/tt/data_masking/"
    json_file_path = sys.argv[2]
    main(base_loc, request_id, database_name, exec_loc, json_file_path)


