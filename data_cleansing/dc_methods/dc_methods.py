from sqlalchemy import create_engine
import hashlib
import pandas as pd
import shutil
import datetime
# import data_cleansing.CONFIG.Config as DNXConfig

import pyarrow.parquet as pq
import pyarrow as pa

def delete_dataset(data_set):
    try:
        shutil.rmtree(data_set)
    except:
        None


def save_to_parquet(df, dataset_root_path, partition_cols=None, load_table_type=None):
    start_time = datetime.datetime.now()
    if not df.empty:
        # if load_table_type == 'BT':
        # object_columns = df.select_dtypes(include='object')
        for i in df.columns:
            df[i] = df[i].apply(str)

        partial_results_table = pa.Table.from_pandas(df)

        pq.write_to_dataset(partial_results_table, root_path=dataset_root_path, partition_cols=partition_cols,
                            # flavor='spark',
                            use_dictionary=False
                            )
        # flavor = 'spark'
        print("{:,}".format(len(df.index)), 'records inserted into', dataset_root_path, 'in', datetime.datetime.now() - start_time)


def read_from_parquet(dataset_root_path, chunk_size, columns = None, be_ids_filter = None):

    if chunk_size:
        # print('read_from_parquet', dataset_root_path, chunk_size)
        table_batches = pq.read_table(dataset_root_path, columns=columns).to_batches(chunk_size)
        # tx = pa.RecordBatch.from_arrays(dataset_root_path,['e'],)
        # print(tx)
        # print(table_batches)
        for table in table_batches:
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


def get_sub_data(mongo_uri, mongo_db, collection_name,start_index, end_index, ids=None, collection_filter=None, collection_project=None):
    client = pymongo.MongoClient(mongo_uri)
    database = client[mongo_db]
    if ids is None:
        if collection_filter is None:
            collection_filter = {}
        if collection_project is None:
            collection_data = database[collection_name].find(collection_filter, no_cursor_timeout=True).skip(start_index).limit(end_index-start_index)
        else:
            collection_data = database[collection_name].find(collection_filter, collection_project, no_cursor_timeout=True).skip(start_index).limit(end_index-start_index)

        # collection_data = collection_data[start_index: end_index]
    # else:
    #     collection_data = database[collection_name].find({'_id': {'$in': ids}})
    return collection_data


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
    # print(min_category_query)
    min_category_data = get_all_data_from_source(url, schema, min_category_query)

    if not min_category_data.empty:
        min_cat = min_category_data['min_category_no'].values[0]
    else:
        min_cat = 1

    # print(min_cat)

    return min_cat

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
# if __name__ == '__main__':
#     import data_cleansing.CONFIG.Config as config
#     config_db_url = config.Config.config_db_url

#     x = get_parameter_values(config_db_url)
#
#     x_dict = x.to_dict('records')
#
#     parameters_values_dict = {}
#     for i in x_dict:
#         parameters_values_dict[i['_id']] = i['value']
