#!/usr/bin/env python3.9
from collections import defaultdict

from constants.rule_priority import priority_rule_dict, rule_priority_dict


def get_column_type(column_name, column_list, logger):
    try:
        for column in column_list:
            if column['column_name'] == column_name:
                return column['data_type']
    except Exception as e:
        logger.error(f'Error getting column type: {e}')

def get_primary_key(column_list):
    for column in column_list:
        if column['is_primary_key'] == "YES":
            return column['column_name']

#convert list to a string that can be used as a part of a WHERE IN statement
def get_in_statement(items):
    return f"""('{"', '".join(items), '")'}"""

def check_for_rule_duplicacy(rule_dict, logger):
    fail_flag = False
    for rule, columns in rule_dict.items():
        if len(columns) > 1:
            logger.error(f'Rule {rule} present for multiple columns: {", ".join(columns)}')
            fail_flag = True
    if fail_flag:
        raise ValueError('failing the job as same rule cannot be present for multiple columns')

def get_highest_priority_num(column_list, logger):
    highest_priority_rule_num = 0
    non_hiped_algo_rule_dict = defaultdict(list)
    for column in column_list:
        rule_num = 0
        if column['column_name'].lower() == "cust_id" and column['rule'] == '':
            rule_num = 1
            if column.get('hipedAlgorithm', '') != 'KeyBasedCryptoHash':
                non_hiped_algo_rule_dict[priority_rule_dict[rule_num]].append(column['column_name'])
        elif column['rule'] is not None:
            rule_num = rule_priority_dict.get(column['rule'], 0)
            if rule_num >0 and column.get('hipedAlgorithm', '') != 'KeyBasedCryptoHash':
                non_hiped_algo_rule_dict[priority_rule_dict[rule_num]].append(column['column_name'])
        check_for_rule_duplicacy(non_hiped_algo_rule_dict, logger)
        if rule_num > highest_priority_rule_num:
            highest_priority_rule_num = rule_num
    logger.info(f'highest priority rule num: {highest_priority_rule_num}')
    return highest_priority_rule_num





def get_columns_as_str(table_name, column_list, driver_column, logger):
    try:
        column_str = ""
        pii_list = []
        enc_col = []
        high_priority_rule_num = get_highest_priority_num(column_list, logger)
        for column in column_list:
            if column['is_encrypted'] == "YES":
                if column['rule'] == "<primary_pan_acct_nbr15>" or column['is_primarykey'] == "YES":
                    enc_col.append([column['column_name']])

            if column["is_pii"] == "YES":
                if column['rule'] == "<primary_pan_acct_nbr15>" or column['is_primarykey'] == "YES":
                    if column['hipedAlgorithm'] != 'KeyBasedCryptoHash':
                        pii_list.append(column['column_name']) #create a list of pii columns

                if column["rule"] !=priority_rule_dict.get(high_priority_rule_num,'') and column['is_primarykey'] != "YES" and column['column_name'] != driver_column:
                    if ("VARCHAR" in column['data_type']):
                        column_str += f" CAST(NULL as {column['data_type']}({column['column_length']})) as {column['column_name']}"
                    elif("CLOB" in column['data_type']):
                        column_str += f" NULL as {column['column_name']},"
                    else:
                        column_str += f" CAST(NULL as {column['data_type']}) as {column['column_name']},"
                else:
                    if column["rule"] == priority_rule_dict.get(high_priority_rule_num,''):
                        logger.info(f"not nulling {column['column_name']}, as it\'s {priority_rule_dict.get(high_priority_rule_num, '')}")
                    if column["is_primarykey"] == "YES":
                        logger.info(f"not nulling {column['column_name']}, as it\'s primary key")
                    if column['column_name'] == driver_column:
                        logger.info(f"not nulling {column['column_name']}, as it\'s driver column")
                    if "bytea" in column['data_type']:
                        column_str += f" CAST({table_name}.{column['column_name']} as VARCHAR(1000)) as {column['column_name']},"
                    else:
                        column_str += f"{table_name}.{column['column_name']},"
            else:
                if "bytea" in column['data_type']:
                    column_str += f" CAST({table_name}.{column['column_name']} as VARCHAR(1000)) as {column['column_name']},"
                else:
                    column_str += f"{table_name}.{column['column_name']},"
        return (column_str[:-1], pii_list, enc_col)
    except Exception as e:
        logger.error(f'Error generating columns for extraction: {e}')
        raise

def get_query(column_string, table_name, schema_name, where_clause, limit_clause):
    if where_clause == "":
        #Full Extract
        return f"SELECT {column_string} FROM {schema_name}.{table_name} {limit_clause}"
    elif where_clause != "":
        #partial extract
        return f"SELECT {column_string} FROM {schema_name}.{table_name} WHERE {where_clause} {limit_clause}"
    else:
        raise ValueError("cannot generate query with the given inputs")
