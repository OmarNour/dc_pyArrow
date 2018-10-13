from sqlalchemy import create_engine
import hashlib
import pandas as pd
import shutil
import datetime
import pyarrow.parquet as pq
import pyarrow as pa
from pydrill.client import PyDrill
import os, sys


def count_folders_in_dir(path):
    return len(os.listdir(path))

def rename_dataset(current_name, new_name):
    try:
        os.rename(current_name,
                  new_name)
    except:
        print(current_name, "folder not found or unexpected error:", sys.exc_info()[0])


def delete_dataset(data_set):
    try:
        shutil.rmtree(data_set)
    except:
        None


def save_to_parquet(df, dataset_root_path, partition_cols=None, string_columns=None):
    start_time = datetime.datetime.now()
    if not df.empty:

        # all_object_columns = df.select_dtypes(include='object').columns
        # print(all_object_columns)

        if string_columns is None:
            string_columns = df.columns

        for i in string_columns:
            df[i] = df[i].apply(str)

        partial_results_table = pa.Table.from_pandas(df)

        pq.write_to_dataset(partial_results_table, root_path=dataset_root_path, partition_cols=partition_cols,
                            # flavor='spark',
                            use_dictionary=False
                            )
        # flavor = 'spark'
        print("{:,}".format(len(df.index)), 'records inserted into', dataset_root_path, 'in', datetime.datetime.now() - start_time)


def read_batches_from_parquet(dataset_root_path, columns, batch_size):

    for table in (table for table in pq.read_table(dataset_root_path,
                                                      columns=columns,
                                                      nthreads=4).to_batches(batch_size)):
        df = table.to_pandas()
        yield df


def read_from_parquet(table_batches, be_ids_filter=None):

    if len(table_batches) > 0 :
        # print('read_from_parquet', dataset_root_path, chunk_size)
        # table_batches = pq.read_table(dataset_root_path, columns=columns).to_batches(chunk_size)
        # tx = pa.RecordBatch.from_arrays(dataset_root_path,['e'],)
        # print(tx)
        # print(table_batches)
        # my_list = [1,2,3,4]
        # x = (x ** 2 for x in my_list)
        # table_batches = (table for table in pq.read_table(dataset_root_path, columns=columns).to_batches(chunk_size))
        # print('table_batches2', table_batches)
        for table in table_batches:
            # print('tabletable', table)
            df = table.to_pandas()
            if be_ids_filter:
                df = df[df[be_ids_filter[0]].isin(be_ids_filter[1])]
            yield df

def single_quotes(string):
    return "'%s'" % string

def source_connection(url, schema):
    if schema is None:
        schema = ""
    url_schema = url + schema
    engine = create_engine(url_schema)
    connection = engine.connect()
    # results_proxy = connection.execute(query)
    # print('results_proxy::', results_proxy)
    return connection

def get_all_data_from_source(url, schema, query):
    # print('\n',url,schema)
    # print('url:', url)
    connection = source_connection(url, schema)
    results_proxy = connection.execute(query)

    all_results = results_proxy.fetchall()

    if all_results:
        results_keys = all_results[0].keys()
        results_df = pd.DataFrame(all_results, columns=results_keys)
    else:
        results_df = pd.DataFrame()

    return results_df

def get_chuncks_of_data_from_source(url, schema, query, fetch_many):
    # print('url:', url)
    connection = source_connection(url, schema)
    results_proxy = connection.execute(query)
    # print(results_proxy)
    more_result = True
    while more_result:
        partial_results = results_proxy.fetchmany(fetch_many)
        if not partial_results:
            more_result = False
        else:
            results_keys = partial_results[0].keys()
            results_df = pd.DataFrame(partial_results, columns=results_keys)
            yield results_df

def sha1(value):
    if type(value) != 'str':
        value = str(value)
    hash_object = hashlib.sha1(value.encode())
    hex_dig = hash_object.hexdigest()
    return str(hex_dig)


def assign_process_no(no_of_cores, index):
    process_no = index % no_of_cores
    # print(process_no)
    return process_no


def data_to_list(data):

    # print('size of datadata ',sys.getsizeof(data))
    list_data = list(data)
    return list_data


def df_to_dict(df):
    if not df.empty:
        data_dict = df.to_dict('records')
    else:
        data_dict = {}
    return data_dict


def chunk_list(list_data,chunk_size):
    chunks = (list_data[x:x + chunk_size] for x in
              range(0, len(list_data), chunk_size))
    return chunks


def integer_rep(string):
    string_int = [ord(c) for c in string]
    string_int = "".join(str(n) for n in string_int)
    string_int = int(string_int)
    return string_int


def chunk_list_loop(list_data,chunk_size):
    for chunk in chunk_list(list_data,chunk_size):
        yield chunk


def get_start_end_index(total_rows, chunk_size):
    start_index = 0
    end_index = min(chunk_size, total_rows)
    max_index = total_rows
    while start_index < end_index:
        yield start_index, end_index
        start_index = min(end_index, max_index)
        end_index = min(end_index + chunk_size, max_index)


def get_minimum_category(url, schema, table_name, be_att_id):
    min_category_query = "select min(category_no) min_category_no from "+ table_name +" where be_att_id = "+single_quotes(be_att_id)+" group by be_att_id"
    min_category_data = get_all_data_from_source(url, schema, min_category_query)

    if not min_category_data.empty:
        min_cat = min_category_data['min_category_no'].values[0]
    else:
        min_cat = 1

    # print(min_cat)

    return int(min_cat)


def get_parameter_values(config_db_url, parameter_table):
    query = "select * from " + parameter_table
    parameter_value = get_all_data_from_source(config_db_url, None, query)

    return parameter_value


def list_to_string(list, separator = None, quotes = 0):
    if separator is None:
        prefix = ""
    else:
        prefix = separator
    to_string = prefix.join((single_quotes(str(x)) if quotes == 1 else str(x)) if x is not None else "" for x in list)

    return to_string


def read_from_parquet_drill(db_root_path, schema, table, columns_list, be_ids_filter=None, drill=None):
    # drill = PyDrill(host='localhost', port=8047)

    select = 'SELECT '
    columns = list_to_string(list=columns_list, separator=',', quotes=0)
    source = '''`''' + db_root_path + '/' + schema + '/' + table + '''`'''
    from_source = ' from dfs.' + source
    if be_ids_filter:
        where = ' where bt_id in (' + list_to_string(list=be_ids_filter, separator=',', quotes=1) + ')'
    else:
        where = ''
    full_query = select + columns + from_source + where
    # print('full_query', full_query)
    try:

        query_result_df = drill.query(full_query,timeout=1000).to_dataframe()
    except:
        query_result_df = pd.DataFrame(columns=columns_list)

    return query_result_df


def get_be_core_table_names(config_db, org_business_entities, be_id):
    org_business_entities_collection_query = 'select * from ' + org_business_entities + ' where _id = ' + single_quotes(be_id)

    bt_current_collection = list_to_string(get_all_data_from_source(config_db, None, org_business_entities_collection_query)['bt_current_collection'].values)
    bt_collection = list_to_string(get_all_data_from_source(config_db, None, org_business_entities_collection_query)['bt_collection'].values)
    source_collection = list_to_string(get_all_data_from_source(config_db, None, org_business_entities_collection_query)['source_collection'].values)
    dq_result_collection = list_to_string(get_all_data_from_source(config_db, None, org_business_entities_collection_query)['dq_result_collection'].values)
    return bt_current_collection, bt_collection, source_collection, dq_result_collection


if __name__ == '__main__':
    dataset = 'C:\dc\parquet_db\DNX\BT_current_4383_10\\'
    col = ['SourceID', 'RowKey', 'AttributeID', 'AttributeValue', 'ResetDQStage']
    # dataset = 'C:\dc\parquet_db\Source_data\src_4383_10'
    # col = None
    total_rows = 0
    folders_count = count_folders_in_dir(dataset)
    for f in range(folders_count):
        complete_dataset = dataset + str(f)
        for i in read_batches_from_parquet(complete_dataset, col, 200000):
            print(type(i))
            total_rows += len(i.index)
    print('total rows:', total_rows)

#     import data_cleansing.CONFIG.Config as config
#     config_db_url = config.Config.config_db_url

#     x = get_parameter_values(config_db_url)
#
#     x_dict = x.to_dict('records')
#
#     parameters_values_dict = {}
#     for i in x_dict:
#         parameters_values_dict[i['_id']] = i['value']
