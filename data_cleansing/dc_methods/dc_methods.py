from sqlalchemy import create_engine
import hashlib
import pandas as pd
import shutil
import datetime
import pyarrow.parquet as pq
import pyarrow as pa
from pydrill.client import PyDrill
import os, sys
import dask.dataframe as dd


bt_columns = ['SourceID', 'RowKey', 'AttributeID', 'AttributeValue',
                           'HashValue', 'InsertedBy', 'ModifiedBy', 'ValidFrom', 'ValidTo',
                           'ResetDQStage', 'process_no']

bt_object_cols = ['RowKey', 'AttributeValue', 'HashValue',
                               'InsertedBy', 'ModifiedBy', 'ValidFrom', 'ValidTo', 'process_no']

bt_partioned_object_cols = ['RowKey', 'AttributeValue', 'HashValue',
                               'InsertedBy', 'ModifiedBy', 'ValidFrom', 'ValidTo', 'process_no']


# bt_partition_cols = ['batch_no', 'SourceID', 'ResetDQStage', 'AttributeID']
bt_partition_cols = ['SourceID', 'AttributeID', 'ResetDQStage', ]

result_cols = ['SourceID', 'AttributeID', 'ResetDQStage', 'bt_id', 'be_att_dr_id', 'data_rule_id', 'RowKey', 'AttributeValue',
               'data_value_pattern', 'is_issue', 'p_SourceID', 'p_AttributeID', 'p_ResetDQStage', 'p_is_issue', 'p_be_att_dr_id', 'p_data_rule_id']
result_partition_cols = ['SourceID', 'AttributeID', 'ResetDQStage', 'is_issue', 'be_att_dr_id', 'data_rule_id']
p_result_partition_cols = ['p_SourceID', 'p_AttributeID', 'p_ResetDQStage']

result_object_cols = ['SourceID', 'RowKey', 'AttributeValue', 'data_value_pattern']

def string_to_dict(sting_dict):
    if sting_dict:
        # ex: Firstname="Sita" Lastname="Sharma" Age=22 Phone=1234567890
        return eval("dict(%s)" % ','.join(sting_dict.split()))


def is_dir_exists(path):
    return os.path.exists(path)


def get_files_in_dir(path):
    files = [name for name in os.listdir(path) if os.path.isfile(os.path.join(path, name))]
    return files


def count_files_in_dir(path):
    files = get_files_in_dir(path)
    return len(files)


def folders_in_dir(path):
    return os.listdir(path)


def count_folders_in_dir(path):
    return len(os.listdir(path))


def rename_dataset(current_name, new_name):
    try:
        os.rename(current_name,
                  new_name)
    except:
        None
        # print(current_name, "folder not found or unexpected error:", sys.exc_info()[0])


def delete_dataset(data_set):
    try:
        shutil.rmtree(data_set)
    except:
        None


def xstr(s):
    if s is None:
        return ''
    return str(s)


def save_to_parquet(df, dataset_root_path, partition_cols=None, string_columns=None):
    start_time = datetime.datetime.now()
    if not df.empty:

        # all_object_columns = df.select_dtypes(include='object').columns
        # print(all_object_columns)

        if string_columns is None:
            string_columns = df.columns

        for i in string_columns:
            df[i] = df[i].apply(xstr)

        partial_results_table = pa.Table.from_pandas(df=df,nthreads=4)

        pq.write_to_dataset(partial_results_table, root_path=dataset_root_path, partition_cols=partition_cols,
                            # flavor='spark',
                            use_dictionary=False
                            )
        # flavor = 'spark'
        print("{:,}".format(len(df.index)), 'records inserted into', dataset_root_path, 'in', datetime.datetime.now() - start_time)


def read_batches_from_parquet(dataset_root_path, columns, batch_size, use_threads, filter=None):

    for table in (table for table in pq.read_table(dataset_root_path,
                                                   columns=columns,
                                                   use_threads=use_threads,
                                                   use_pandas_metadata=False).to_batches(batch_size)):
        df = table.to_pandas()

        if filter:
            for i in filter:
                df = df[df[i[0]].isin(i[1])]

        if not df.empty:
            yield df


def read_partioned_data_from_parquet(dataset_root_path, columns, filter=None):
    try:
        return pq.ParquetDataset(path_or_paths=dataset_root_path,filters=filter).read().to_pandas()[columns]
    except:
        return pd.DataFrame


def read_all_from_parquet(dataset, columns, use_threads, filter=None):
    df = pq.read_table(dataset,
                       columns=columns,
                       use_threads=use_threads,
                       use_pandas_metadata=True).to_pandas()

    if filter:
        for i in filter:
            df = df[df[i[0]].isin(i[1])]
    return df


def read_all_from_parquet_delayed(dataset, columns=None, nthreads=4, filter=None):
    df = dd.read_parquet(path=dataset, columns=columns, engine='pyarrow')
    if filter:
        for i in filter:
            df = df[df[i[0]].isin(i[1])]
    return df


def drill_source_single_quotes(source):
    return "`%s`" % source


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


def read_from_parquet_drill(db_root_path, schema, table, columns_list, be_ids_filter=None, drill=None, source=None, where='', full_query=None):
    if drill is None:
        drill = PyDrill(host='localhost', port=8047)
    if full_query is None:
        select = 'SELECT '
        columns = list_to_string(list=columns_list, separator=',', quotes=0)
        if source is None:
            source = '''`''' + db_root_path + '/' + schema + '/' + table + '''`'''
        from_source = ' from dfs.' + source
        if be_ids_filter:
            where = ' where bt_id in (' + list_to_string(list=be_ids_filter, separator=',', quotes=1) + ')'

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


def get_attribute_value_by_rowkey(bt_dataset, filters=None):
    # values = read_all_from_parquet_delayed(bt_dataset, ['RowKey', 'AttributeValue'], 4, filter=filters).compute()
    values = read_all_from_parquet(bt_dataset, ['RowKey', 'AttributeValue'], 4, filter=filters)
    if not values.empty:
        return values
    else:
        return pd.DataFrame()

    # folders_count = count_folders_in_dir(bt_dataset)
    # for f in range(folders_count):
    #     bt_src = bt_dataset + "\\" + str(f)
    #
    #     # if be_att_id:
    #     #     filter_be_att_id = ['AttributeID', [be_att_id]]
    #     #     filters = [filter_be_att_id]
    #     #
    #     # if RowKey:
    #     #     filter_rowkey = ['RowKey', [RowKey]]
    #     #     filters.append(filter_rowkey)
    #     #
    #     # if AttributeValue:
    #     #     filter_AttributeValue = ['AttributeValue', [AttributeValue]]
    #     #     filters.append(filter_AttributeValue)
    #
    #     # print('filtersfilters', filters)
    #     values = read_all_from_parquet(bt_src, ['RowKey', 'AttributeValue'], 4, filter=filters)

        # if not values.empty:
        #     return values
        # else:
        #     return pd.DataFrame()
#
if __name__ == '__main__':
    save_bt_current_data_set = "C:\\dc\\parquet_db\DNX\\test_bt_current_4383_10"


    x = [[('AttributeID', '=', 700), ],
         [('AttributeID', '=', 800), ]]

    list_of_att = [100,200,300,400,500,600,700,800,900,1000,2143]

    pa_filter = []
    for i in range(len(list_of_att)):
        tuplef = ('AttributeID', '=', list_of_att[i])
        list_tuplef = [tuplef]
        pa_filter.append(list_tuplef)

    print(pa_filter)

    pa_filter = [[('bt_id', '=', '9001520e2b30f1ae17d128db9187b36b9bdb022c6a84d44')],]
    # pa_filter = [[('AttributeID', '=', 520)], ]
    df = read_all_from_parquet2(dataset_root_path="C:\\dc\\parquet_db\\DNX\\bt_current_9898_100",
                                columns=['bt_id','AttributeID'],
                                nthreads=4, filter=pa_filter)
    print(df)

