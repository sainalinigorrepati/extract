def add_enc_prefix(extract_config, column_name, env, logger, header =None, fixedIV=None,function=None, enc = 'EOT_HIPED'):
    try:
        column_name = column_name.strip("'")
        if function:
            if function == 'HipedEmbed':
                return f"hiped_embed_encrypt('{column_name}', '{env}, 'bulkcrypto', '{enc}', 'v2', '{header}', '{fixedIV}', '{extract_config['vault_path']}') as '{column_name}_enc'"
        else:
            return f"hiped_embed_encrypt('{column_name}', '{env}, 'dip', '{enc}', 'v2', 'True', '{extract_config['vault_path']}') as '{column_name}_enc'"
    except Exception as e:
        logger.error(f'Error adding prefix for encryption: {e}')
        raise


def encryptNG(extract_config, df, column_list,spark, envr, logger, hiped_jar):
    try:
        spark.sql(f"CREATE TEMPORARY FUNCTION hiped_aes_encrypt AS 'com.abc.de.bulkcrypto.udf.EncryptHipedAesUDF' USING jar '{hiped_jar}' ")
        df.createOrReplaceTempView('hive_view')
        column_order = [f"'{col}'" for col in df.columns]
        column_list = [col.replace("'","") for col in column_list]
        enc_list = [add_enc_prefix(extract_config, item, envr, logger) for item in column_list]
        list_str = ','.join(enc_list)
        logger.info(f"Encrypting columns: {list_str}")
        hiped_df = spark.sql(f"select *, {list_str} from hive_view")
        hiped_df = drop_columns(hiped_df, column_list, logger, "_enc")
        df = hiped_df.select(*column_order)
        spark.sql("DROP TEMPORARY FUNCTION IF EXISTS hiped_aes_encrypt")
        return df
    except Exception as e:
        logger.error(f"Error encryting with EOT keys: {e}")
        raise


def decrypt_with_DB_keys(extract_config, df, metadata, spark, envr, logger, hiped_jar, enc_col, table_name=None):
    try:
        for column in metadata['column_list']:
            if table_name:
                enc_col = [col.replace("'","") for col in enc_col]
                colm = f"{table_name}.{column['column_name']}"
                col = f"{table_name}.{column['column_name']}" in enc_col
                column_order = [f"'{col}'" for col in df.columns]
            else:
                colm = column['column_name']
                col = [column["column_name"]] in enc_col
                column_order = df.columns

            if col == True:
                if column['is_encrypted'] == 'YES':
                    if column['rule'] == '<primary_pan_acct_nbr15>' or column['is_primarykey'] == 'YES':
                        enc_list = list([colm, column['hipedAlgorithm'], column['encryption_sde_key'], column['hipedEmbededPANHeaderVersion'], column['encryption_sde_useIVMethod']])

                        if column['hipedAlgorithm'] == 'HipedEmbed':
                            spark.sql(f"CREATE TEMPORARY FUNCTION hiped_embed_decrypt as 'com.abc.de.bulkcrypto.udf.EncryptHipedAesUDF' USING jar '{hiped_jar}' ")
                            df.createOrReplaceTempView('hive_view')
                            dec_list = [add_dec_prefix(extract_config, enc_list[0], envr, logger, enc_list[1])]
                            column_list = [enc_list[0]]

                        elif column['hipedAlgorithm'] == 'KeyBasedCryptoHash':
                            continue
                        else:
                            logger.info('please provide correct hipedAlgorithm')
                            raise
                        list_str = ','.join(dec_list)
                        logger.info(f"Decrypting columns: {list_str}")
                        hiped_df = spark.sql(f"select *, {list_str} from hive_view")

                        hiped_df = drop_columns(hiped_df, column_list, logger, "_dec")
                        spark.sql("DROP TEMPORARY FUNCTION IF EXITS hiped_embed_decrypt")
            else:
                continue
        return df
    except Exception as e:
        logger.error(f"Error decrypting with DB keys: {e}")
        raise

def add_dec_prefix(extract_config, column_name, env, logger, function=None, enc='EOT_HIPED'):
    try:
        if function:
            if function == 'HipedEmbed':
                return f"hiped_embed_deccrypt('{column_name}', '{env}, 'dip', 'v2', '{extract_config['vault_path']}') as '{column_name}_dec'"
        else:
            return f"hiped_embed_decrypt('{column_name}', '{env}, 'dip', '{enc}', 'v2', '{extract_config['vault_path']}') as '{column_name}_decc'"
    except Exception as e:
        logger.error(f"Error adding prefix for decryption: {e}")
        raise


def drop_columns(hiped_df, column_list, logger, enc):
    try:
        hiped_df = hiped_df.drop(*column_list)
        for column_name in hiped_df.columns:
            hiped_df = hiped_df.withColumnRenamed(column_name, column_name.replace(enc,""))

        return hiped_df
    except Exception as e:
        logger.error(f"Error dropping columns: {e}")
        raise

def decryptNG(extract_config, df, column_list, spark, envr, logger, hiped_jar):
        spark.sql(
            f"CREATE TEMPORARY FUNCTION hiped_aes_encrypt AS 'com.abc.de.bulkcrypto.udf.EncryptHipedAesUDF' USING jar '{hiped_jar}' ")
        df.createOrReplaceTempView('hive_view')
        column_order = df.columns
        dec_list = [add_dec_prefix(extract_config, item, envr, logger) for item in column_list]
        list_str = ','.join(dec_list)
        logger.info(f"decrypted columns: {list_str}")
        hiped_df = spark.sql(f"select *, {list_str} from hive_view")
        hiped_df = drop_columns(hiped_df, column_list, logger, "_dec")
        df = hiped_df.select(*column_order)
        spark.sql("DROP TEMPORARY FUNCTION IF EXISTS hiped_aes_decrypt")
        return df