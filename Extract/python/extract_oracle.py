#!/usr/bin/env python3.9
from read import read_text_file, read_jdbc
from write import write_to_nas
from extract_helper import load_secret, get_jdbc_url, get_api, decrypt_driver_keys, get_metadata
from extract_processor import get_query, get_column_type, get_columns_as_str, get_in_statement
from hiped_helper import encryptNG, decrypt_with_DB_keys


import os, pandas as pd

def format_col_values(pd_col):
    unique_values = pd.unique(pd_col)
    format_col_val = pd.Series(unique_values).apply(lambda x: f"'{str(x)}'")
    return ','.join([f'(1,{v}])' for v in format_col_val.to_list()])

def encrypt_pii(extract_config, df,pii_list, spark,envr,logger):
  #Encrypt keys before writing to NAS if required
  if len(pii_list) != 0:
      logger.info('Started encryption process with EOT Keys')
      enc_df = encryptNG(extract_config, df, pii_list,spark, envr, logger, extract_config['hiped_jar'])
      logger.info('Completed encryption process with EOT Keys ... ')
  else:
      logger.info('No pii data found Skipping encryption process with EOT Keys!.....')
      enc_df = df

  return enc_df
def create_table_keys(metadata, extract_config, user_config, properties, JDBC_URL, spark, driver_cols, driver_keys, envr, logger, hiped_jar, driver_metadata):
    table_name = metadata[' table_name']
    service_id = metadata['table_extract_request_id'].rstrip(f': {table_name}.upper()').replace(':', '_').upper()
    base_filepath = f"{extract_config['scrub_keys_path']}{service_id}/"

    # if table_relationship null
    if metadata['table_relationship'] is None:
        raise ValueError(
        "No table relationshp found but extract indicator is marked as true, Please add a table relationship or request a fill extract")
    driver_column=metadata['driver _column'].upper()
    driver_table = metadata['driver_table'].upper()

    relationships = metadata["table_relationship"]
    from collections import defaultdict
    grouped = defaultdict(list)

    # Group relationships by index
    for rel in relationships:
        grouped[rel['index']].append(rel)

    # Start with the lowest index parent table
    min_index = min(grouped.keys())
    final_table = metadata["table_name"]

    # Dynamically find columns for selection from the final table
    final_columns = set()
    for rel in relationships:
        if rel['child_table'] == final_table:
            final_columns.add(rel['child_column'])
    url = f"https://{extract_config[envr]['primary_key']['base_url']}{extract_config[envr]['primary_key']['endpoint']}?templateId={metadata['template_id']}&tableName={driver_table}"
    primary_keys = get_api(url, logger)
    logger. info(primary_keys)

    select_columns =[f"' {final_table}.{col}'" for col in final_columns]
    where_clause = get_where_clause(driver_keys, driver_cols, "", logger)
    driver_select_columns = ','.join(set([rel['parent_column'] for rel in grouped[min_index]]))
    driver_select_columns += f', {driver_column}'  # Add driver column to the select statement
    for key in primary_keys:
        if key['column_name'] not in driver_select_columns:
            driver_select_columns += f", {key[' column_name']}"
    driver_query = get_query(driver_select_columns, metadata['driver_table'], metadata['schema_name'], where_clause)
    joins = []
    keys_select_columns = [f' {driver_table}.{driver_column} "{driver_table}. {driver_column}" ']
    # Get primary keys for the current extract's table
    url = f"https://{extract_config[envr]['primary_key']['base_url']}{extract_config[envr]['primary_key']['endpoint']}?templateId={metadata['template_id']}&tableName={table_name}"
    primary_keys = get_api(url, logger)
    logger.info(primary_keys)
    table_pk, table_pk_pii = [], []

    for key in primary_keys:
        table_pk += [f" {table_name}.{key[' column_name']}"]
        keys_select_columns += [f'{table_name}.{key["column_name"]}"{table_name}.{key ["column_name"]}"']
        if key ['is_pii']== "YES":
            table_pk_pii += [f"' {table_name}. {key[' column_name']}"]
    # table_pk_pii = ([f" {table_name}. {key [' column_name']}"] for key in primary_keys if keyl'is_pii'] = "YES")
    # table_pk = ([f"'(table_name). {key ['column_name']}"] for key in primary_keys)

    # Process indices in descending order
    logger.info(grouped)
    for index in sorted(grouped.keys()):
        group = grouped[index]
        logger.info(group)
        parent_table = group[0]['parent_table']
        child_table = group[0][' child_table']
        conditions = []
        keys_group_select_columns = []

        # Get parent table primary keys
        url = f"https://{extract_config[envr]['primary_key']['base_url']}{extract_config[envr]['primary_key']['endpoint']}?templateId={metadata['template_id']}&tableName={parent_table}"
        parent_primary_keys = get_api(url, logger)
        logger.info(primary_keys)

        for parent_pk in parent_primary_keys:
            if (f"{parent_table}.{parent_pk['column_name']}" not in keys_select_columns):
                keys_group_select_columns.append(f"""{parent_table}.{parent_pk['column_name']} "{parent_table}.{parent_pk['column_name']}" """)
            else:
                logger.info(
                f" {parent_table}.{parent_pk['column_name']} already in select statement: {keys_select_columns}")
        # Get child table primary keys
        url = f"https://{extract_config[envr] ['primary_key']['base_url']}{extract_config[envr]['primary_key']['endpoint']}?templateId={metadata['template_id']} & tableName = {child_table}"
        child_primary_keys = get_api(url, logger)
        logger.info(primary_keys)

        for key in child_primary_keys:
            if(f"'{child_table}. {key['column_name']}" not in keys_select_columns):
                    keys_group_select_columns.append(f""" {child_table}.{key['column_name']} "{child_table}.{key['column_name']}" """)
            else:
                    logger.info(f"'{child_table}.{key['column_name']} already in seleçt statement: {keys_select_columns}")

        # Loop through sorted relationships to create the select and join statements
        for rel in group:
                conditions.append(
                    f"{rel['parent_table']}.{rel['parent_column']} = {rel['child _table']}.{rel['child_column']}"
                )

                # Add parent and child columns to the select statement unless they are already present
                if (f"{rel['parent_table']}.{rel['parent_column']}" not in keys_select_columns):
                    keys_group_select_columns.append (
                f""" {rel['parent_table']}.{rel['parent_column']} "{rel['parent_table']}.{rel['parent_column']}" """)
                else:
                    logger.info(f"{rel['parent_table']}.{rel['parent_column']} already in select statement: {keys_select_columns}")

                if(f"{rel['child_table']}.{rel['child_table']}" not in keys_select_columns):
                    keys_group_select_columns.append(
                        f""" {rel['child_table']}.{rel['child_column']} "{rel['child_table']}.{rel['child_column']}" """
                    )
                else:
                    logger.info(f"{rel['child_table']}.{rel['child_table']} already in select statement: (keys_select_columns)")

                logger.info(f"keys_group_select_columns: {keys_group_select_columns}")

        join_condition = ' AND '.join(conditions)
        joins.append(f"JOIN {metadata['schema_name']}.{child_table} ON {join_condition}")
        keys_select_columns += keys_group_select_columns

    #concatenate the JOIN clauses
    keys_select = f"SELECT {', '.join(set(keys_select_columns))} FROM ({driver_query}) {grouped[min_index][0]['parent_table']}"
    keys_query = keys_select + '\n'.join(joins)
    # logger. info(f'===keys query: (keys_query}')
    all_keys = read_jdbc(spark, JDBC_URL, keys_query, properties, logger)
    # all_keys. show()
    df_schema = all_keys.clumns
    last_cols = [f"' {driver_table}. {driver_column}'"]
    last_pii = [f"' {driver_table}. {driver_column}'"]  # Assuming all driver columns are cms
    for index in sorted(grouped.keys()):
            group = grouped[index]
            parent_table = group[0]['parent_table']
            keys_path = f"{base_filepath} {parent_table}"
            if os.path.exists(f"{keys_path}"):
                logger.info(f"File path already exists: {keys_path}")
            else:
                logger.info(f"No file found at location: {keys_path}")
                logger.info("Creating scrub folder if needed ... ")
                if not os.path.exists(f"'{base_filepath}"):
                    logger.info(f"Folder does not exist: {base_filepath}")
                    try:
                        os.makedirs (f" {base_filepath}")
                        logger.info("Folder created ..")
                    except Exception as e:
                        logger.info(f"Error creating folder for scrub keys: {e}")
                else:
                    logger.info(f"Folder exists: {base_filepath}")
                # cols = [f"' {parent_table}. (keys ['column_name']}"" for keys in primary_keys] # columns to select from join query
                # logger. info (cols)
                # pii_list = [f"{keys ['column_name']} for keys in primary_keys, if keysl'is_pii'] == "YES"] # columns to encrypt

                logger.info("Calling primary keys API ...")
                url = f"https://{extract_config[envr]['primary_key']['base_url']}{extract_config[envr]['primary_key']['endpoint']}?templateId={metadata['template_id']}&tableName={parent_table}"
                primary_keys = get_api(url, logger)
                cols = last_cols
                pii_list = last_pii
                last_cols, last_pii = [], []

                logger.info("Selecting columns for scrub keys ...")
                for key in primary_keys:
                    if(f" {parent_table}.{key['column_name']}" in df_schema):
                        cols += [f"'{parent_table}.{key['column_name']}'"]
                        last_cols += [f"'{parent_table}. {key['column_name']}'"]  # Replace with current table's primary keys for next iteration
                        if key['is_pii'] == "YES" and key ['hipédAlgorithm'] != 'KeyBasedCryptoHash':
                            pii_list += [f"' {parent_table}.{key['column_name']}"]
                            last_pii += [f"' {parent_table}.{key['column_name']}"]
                        else:
                             logger.error(
                                 f"Primary key {parent_table}.{key['column_name']} not extracted DF schema: {df_schema}")
                             raise
                df = all_keys.select(cols)
                if driver_metadata != None:

                    if metadata['table_relationship']:
                        for i in metadata['table_relationship']:
                            if i['parent_table'] == metadata["driver_table"]:
                                for column in driver_metadata['column_list']:
                                    if column[" column_name"] == metadata["driver _column"]:
                                        if column['is_encrypted'] == "YES":
                                            enc_col = [f'{metadata["driver_table"]}.{column["column_name"]}']
                                            logger.info('decrypting DB encrypted keys')
                                            df = decrypt_with_DB_keys(extract_config, df, driver_metadata, spark, envr, logger, hiped_jar, enc_col, metadata["driver_table"])
                                            logger.info('completed decrypting DB encrypted keys')
                                        else:
                                            continue

                enc_df = encrypt_pii(extract_config, df, pii_list, spark, envr, logger)
                # enc_df. show()
                logger.info(f"Writing keys to {keys_path} ...")
                write_to_nas(enc_df, extract_config['output_path']['output_format'], keys_path, logger, True)
    cols, pii = [], []
    for key in table_pk:
        cols += [f"' {key}'"]
    cols += last_cols

    for key in table_pk_pii:
            pii += [f"' {key}'"]

    if f"''{driver_table}. {driver_column}'" in last_pii:
            last_pii.remove(f"' {driver_table}. {driver_column}*")
            logger.info("Removed driver column from pii list as it is not a primary key")

    pii += last_pii

    keys = all_keys.select(cols).distinct()
    enc_df = encrypt_pii(extract_config, keys, pii, spark, envr, logger)
    write_to_nas(enc_df, extract_config['output_path']['output_format'], f"{base_filepath}{table_name}",
                                 logger, True)
    extract_keys = all_keys.select(select_columns).distinct()
    final_columns = [column.strip("'") for column in select_columns]
    final_columns_list = list(final_columns)
    keys_pd = extract_keys.toPandas()
    key_values = [format_col_values(keys_pd[column]) for column in final_columns_list]
    # logger. info (key_values)
    return key_values, final_columns_list

#Generate the where clause based on user inputs
def get_where_clause(key_values, key_cols, filter_clause, logger):
    #logger.info(f'key_values,~key_cols, filter _clause: {key_values), {key_cols}, {filter_clause}')
    if (key_values == '' and key_cols == '' and filter_clause == ''):
        # Full extract
        return ""
    elif key_values == None or key_cols == None:
        # Relational extract marked true but issue extracting keys
        raise ValueError ("None value passed for filter_col or value_string")
    elif (key_values != '' and key_cols != '' and filter_clause == ''):
        # only relational extract
        in_clause_statements = [f"(1, {col}) IN ({key_values[indx]})" if( len(key_values[indx]) != 0) else f"{col} IN (null)" for indx, col in enumerate(key_cols) ]
        #logger. info(f'in_clause_statements: (in_clause_statements}')
        return ' AND '.join(in_clause_statements)
    elif (key_values != '' and key_cols == '' and filter_clause != ''):
        # only filter criteria
        return f"{filter_clause}"
    elif (key_values != '' and key_cols != '' and filter_clause != ''):
        # filter criteria and relational extract
        in_clause_statements = [f" (1,{col}) IN ({key_values [indx]})" if (len(key_values [indx]) != 0) else f"{col} IN (null)" for indx, col in enumerate (key_cols) ]
        return f"{' AND '.join(in_clause_statements)} AND {filter_clause}"
    else:
        raise ValueError ("Cannot generate where clause of the query with the given inputs")

def get_filter_criteria(filters, column_list, opeartor, logger):
    try:
        filter_clause = ''
        for fil in filters:
            #logger. info(f"Filter {fil}")
            # Add an AND to the phrase if its not the first filter condition
            if (filter_clause != ""):
                filter_clause += f" {opeartor} "
            # move to config
            date_format = 'yyyy-mm-dd'
            column_type = get_column_type(fil['column_name'], column_list, logger)

            # Create filter clause based on the condition value
            if (fil['condition'].upper() == 'BETWEEN'):
                logger.info("Generating BETWEEN query ")
                # throw an error if value array has more than two items
                if (len(fil['value']) != 2):
                    raise ValueError(
                        "BETWEEN condition should have exactly two objects. Please check table object above")
                if (column_type == "DATE"):
                    filter_clause += f"({fil['column_name']} BETWEEN TO_DATE('{fil['value'] [0]}', '{date_format}') AND TO_DATE('{fil['value'][1]}', '{date_format}'))"

                elif column_type in ["VARCHAR", "CHAR"]:
                    filter_clause += f"({fil['column_name']} BETWEEN '{fil['value'][0]}' AND '{fil['value'][1]}')"

                elif "TIMESTAMP" in column_type:
                    filter_clause += f"({fil['column_name']} BETWEEN TO_TIMESTAMP('{fil['value'][0]}', '{date_format}') AND TO_TIMESTAMP('{fil['value'][1]}', '{date_format}'))"
                else:
                    filter_clause += f"({fil['column_name']} BETWEEN {fil['value'] [0]} AND {fil['value'] [1]})"

            elif (fil['condition'].upper() == 'IN'):
                # Cannot use to date or to timestamp, must provide values in the correct format. Can assume it is always strings
                in_clause = get_in_statement(fil['value'])
                filter_clause += f"({fil['column_name']} IN {in_clause}"

            elif (fil['condition'] in ['=', '!=', '>', '<', '>=', '<=']):
                if (column_type == "DATE"):
                    filter_clause += f"(TO_CHAR({fil['column_name']}, '{date_format}') {fil['condition']} '{fil['value'][0]}')"

                elif column_type in ["VARCHAR", "'CHAR"]:
                    filter_clause += f"({fil['column_name']}{fil['condition']}'{fil['value'][0]}')"

                elif "TIMESTAMP" in column_type:
                    filter_clause += f"({fil['column_name']}{fil['condition']}TO_TIMESTAMP('{fil['value'][0]}', '{date_format}'))"
                else:
                    filter_clause += f"({fil['column_name']}{fil['condition']}{fil['value'][0]})"
            else:
                raise ValueError (f"Unexpected filter condition: {fil['condition']}")

        return f"({filter_clause})"

    except Exception as e:
        logger.info(f'Error generating filter_criteria: {e}')

# Create oracle properties object
def get_properties(extract_config, user_config, logger):
  try:
    properties={}
    properties['password']=load_secret(extract_config["vault_path"],user_config['marketConfig']['service_password'])
    if properties['password'] == None:
        raise ValueError(f"Unable to find vault key for key: {user_config['marketConfig']['service_password']}")

    properties['user']=user_config['marketConfig']["service_id"]
    properties['driver']=extract_config['drivers']["oracle"]
    logger.info("Properties object created")

    return properties
  except Exception as e:
      logger.error(f'Error getting properties: {e}')
      raise

def extract_oracle( spark, user_config, table_name, metadata, extract_config, logger, envr, hiped_jar):
  try:
      logger.info(f"Extracting oracle Data")

      logger.info(f"Extracting config details ... ")
      extract_data_params = extract_config['extract_data_params']
      JDBC_URL = get_jdbc_url(user_config)
      key_values = ""  #if relational extract is false
      key_cols=""  #if relational extract is false
      filter_clause = "" #if no filter criteria provided

      #create properties object for JDBC read
      logger.info(f"Creating properties object")
      properties = get_properties(extract_config, user_config, logger)
      columns_str, pii_list, enc_col = get_columns_as_str(table_name, metadata['column_list'], metadata.get('driver_column', ''), logger)

      if(metadata['relational_extract'].upper() == 'YES' and metadata['extract_type'].upper() == 'F'):
          raise ValueError(f'The scenario metadata[\'relational_extract\'] = \'YES\' and metadata[\'extract_type\'] = \'F\' is not handled currently')

      #Extract table keys if relational_extract is true
      if(metadata['relational_extract'].upper() == 'YES' or metadata['extract_type'].upper() == 'R'):
          logger.info(f"Extracting keys for {table_name}")
          service_id = metadata['table_extract_request_id'].rstrip(f':{table_name}.upper()').replace(':', '_').upper()
          key_cols = [metadata['driver_column']]
          schema_column = metadata['driver_column'].split(",")

          #Extract driver table keys
          logger.info(f"Driver Table: {metadata['driver_table']}")
          driver_keys_path = f"{extract_config['keys_path']}{service_id}/{metadata['driver_table'].upper()}/{metadata['driver_column'].upper()}.txt"
          logger.info(f"Driver Keys: {driver_keys_path}")


          keys_str = read_text_file('\n', driver_keys_path, logger)
          keys_list = list(keys_str.split(","))
          keys_data=[tuple(item.split(",")) for item in keys_list]
          keys = spark.createDataFrame(keys_data, schema=schema_column)

          driver_metadata = None

          if metadata['driver_column_encrypted'] == 'YES':
              table_name_driver = metadata['driver_table']
              servicerequest = metadata['service_id']
              driver_extract_request_id = f'{servicerequest}:{table_name_driver}'

              if metadata['table_name'] == metadata['driver_table']:
                  keys_dec = decrypt_driver_keys(extract_config, keys, key_cols, spark, envr, logger, hiped_jar, metadata, enc_col, metadata['column_list'])
              else:
                  driver_metadata = get_metadata(extract_config, driver_extract_request_id, table_name_driver, logger, envr) #fetching
                  keys_dec = decrypt_driver_keys(extract_config, keys, key_cols, spark, envr, logger, hiped_jar, driver_metadata, enc_col,
                                     metadata['column_list'])
          else:
              keys_dec = decrypt_driver_keys(extract_config, keys, key_cols, spark, envr, logger, hiped_jar, metadata,
                                             enc_col, metadata['column_list'])

          keys_dec_pd = keys_dec.toPandas().toPandas()
          key_values = [format_col_values(keys_dec_pd[column]) for column in key_cols]

          if (metadata['driver_table'] != table_name):
              (key_values, key_cols) = create_table_keys(metadata, extract_config, user_config, properties, JDBC_URL,
                                                         spark, key_cols, key_values, envr, logger, hiped_jar, driver_metadata)

          if (metadata['extract_type'].upper() == 'F'):
              if (metadata['driver_table'] != table_name):
                  (key_values, key_cols) = create_table_keys(metadata, extract_config, user_config, properties,
                                                             JDBC_URL,
                                                             spark, key_cols, key_values, envr, logger, hiped_jar,
                                                             driver_metadata)

      if metadata['filter_criteria'] is not None and len(metadata['filter_criteria']) > 0:
          logger.info(f"Generating filters for table: {table_name}")
          filters = metadata['filter_criteria']
          filter_clause = get_filter_criteria(filters, metadata['column_list'], metadata['operator'], logger)

      limit_clause = ''
      if metadata['limit'] is not None:
        limit_clause= f"FETCH FIRST {metadata['limit']} ROWS ONLY"

      where_clause = get_where_clause(key_values, key_cols, filter_clause, logger)
      logger.info("final extraction query ... ")
      columns_str, _ = get_columns_as_str(table_name, metadata['column_list'], logger)
      query = get_query(columns_str, table_name, metadata['schema_name'], where_clause, limit_clause)
      logger.info("Query Generated")

      extracted_data = read_jdbc(spark, JDBC_URL, query, properties, logger)

      return(extracted_data, pii_list, enc_col)

  except Exception as e:
    logger.error(f'error ectracting data from oracle: {e}')
