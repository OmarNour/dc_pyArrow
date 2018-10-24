import sys
from data_cleansing.dc_methods.dc_methods import get_all_data_from_source, sha1, single_quotes, data_to_list, \
    get_chuncks_of_data_from_source, list_to_string, delete_dataset, save_to_parquet, assign_process_no, get_minimum_category,\
    read_from_parquet_drill, get_be_core_table_names, rename_dataset, read_batches_from_parquet, bt_object_cols, is_dir_exists, bt_partition_cols, \
    count_folders_in_dir
import data_cleansing.CONFIG.Config as DNXConfig
import datetime
import pandas as pd
import os

import pyarrow.parquet as pq
import pyarrow as pa
from pydrill.client import PyDrill

# from pyspark import SparkConf
# from pyspark.context import SparkContext
# from pyspark.sql import SQLContext
# sc = SparkContext.getOrCreate(SparkConf())
#
# sqlContext = SQLContext(sc)

class StartBT:
    def __init__(self):
        self.process_no = None
        pd.set_option('mode.chained_assignment', None)
        self.dnx_config = DNXConfig.Config()
        self.parameters_dict = self.dnx_config.get_parameters_values()
        # parquet_db_root_path = os.path.dirname(sys.modules['data_cleansing.BT'].__file__)+'\\'+ self.dnx_config.parquet_db_root_path
        parquet_db_root_path = self.dnx_config.parquet_db_root_path
        self.src_db_path = parquet_db_root_path + self.dnx_config.src_db_name + '\\'
        self.dnx_db_path = parquet_db_root_path + self.dnx_config.dnx_db_name + '\\'
        self.result_db_path = parquet_db_root_path + self.dnx_config.result_db_name + '\\'
        self.bt_columns = ['bt_id', 'SourceID', 'RowKey', 'AttributeID', 'BTSID', 'AttributeValue', 'RefSID',
                           'HashValue', 'InsertedBy', 'ModifiedBy', 'ValidFrom', 'ValidTo',
                           'IsCurrent', 'ResetDQStage', 'new_row', self.dnx_config.process_no_column_name]

        # self.bt_partition_cols = ['SourceID', 'ResetDQStage', 'AttributeID']
        # self.bt_partition_cols =None

    def get_source_connection_credentials(self, source_id):
        be_data_sources_query = 'select query, org_connection_id from ' + self.dnx_config.be_data_sources_collection + ' where _id = ' + single_quotes(source_id)
        be_data_sources_data = get_all_data_from_source(self.dnx_config.config_db_url, None, be_data_sources_query)
        source_query = list_to_string(be_data_sources_data['query'].values)

        org_source_id = list_to_string(be_data_sources_data['org_connection_id'].values)
        org_connections_query = 'select url, schema from ' + self.dnx_config.org_connections_collection + ' where _id = ' + single_quotes(org_source_id)
        org_connections_data = get_all_data_from_source(self.dnx_config.config_db_url, None, org_connections_query)
        source_url = list_to_string(org_connections_data['url'].values)
        source_schema = list_to_string(org_connections_data['schema'].values)

        return source_url, source_schema, source_query

    def prepare_source_df(self, source_df, row_key_column_name, process_no_column_name, no_of_cores):
        new_source_df = source_df
        new_source_df[row_key_column_name] = new_source_df[row_key_column_name].apply(sha1)
        new_source_df['_id'] = new_source_df[row_key_column_name]
        new_source_df[process_no_column_name] = new_source_df.apply(lambda x: assign_process_no(no_of_cores, x.name), axis=1)
        return new_source_df

    def get_source_column_name(self, source_id, be_id):
        columns_query = "select query_column_name,  'F'||be_att_id F_be_att_id" \
                       " from " + self.dnx_config.be_data_sources_mapping_collection + \
                       " where be_data_source_id = " + single_quotes(source_id) + \
                       " and be_att_id in (select _id from " + self.dnx_config.be_attributes_collection + " where be_id = " + single_quotes(be_id) + " and att_id != '0')"
        columns_data = get_all_data_from_source(self.dnx_config.config_db_url, None, columns_query)
        # print(columns_query)
        f_col = {}
        for i, data in columns_data.iterrows():
            f_col[data['query_column_name']] = data['F_be_att_id']
            # print('get_source_column_name', data)
        # print('f_col', f_col)
        return f_col

    def get_rowkey_column_name(self, source_id, be_id):
        row_key_column_query = "select query_column_name " \
                               " from " + self.dnx_config.be_data_sources_mapping_collection + \
                               " where be_data_source_id = " + single_quotes(source_id) + \
                               " and be_att_id = (select _id from " + self.dnx_config.be_attributes_collection + " where be_id = " + single_quotes(be_id) + " and att_id = '0')"

        row_key_column_data = get_all_data_from_source(self.dnx_config.config_db_url, None, row_key_column_query)
        row_key_column_name = list_to_string(row_key_column_data['query_column_name'].values)
        return row_key_column_name

    def load_source_data(self, no_of_cores=1):
        be_ids = self.get_be_ids()

        for i, be_id in be_ids.iterrows():
            be_id = be_id['be_id']
            be_source_ids = self.get_be_source_ids(be_id)
            core_tables = get_be_core_table_names(self.dnx_config.config_db_url, self.dnx_config.org_business_entities_collection, be_id)
            bt_current_collection, bt_collection, source_collection = core_tables[0], core_tables[1], core_tables[2]
            self.switch_bt_current_dataset(bt_current_collection)
            # print(be_source_ids)
            for i, source_id in be_source_ids.iterrows():
                source_id = source_id['_id']
                connection_credentials = self.get_source_connection_credentials(source_id)
                source_url, source_schema, source_query = connection_credentials[0], connection_credentials[1], connection_credentials[2]
                row_key_column_name = self.get_rowkey_column_name(source_id, be_id)
                # f_col = self.get_source_column_name(source_id, be_id)
                source_data_set = self.src_db_path + source_collection
                delete_dataset(source_data_set)

                count_rows = 0
                start_load_time = datetime.datetime.now()
                for file_seq, chunk_data in enumerate(get_chuncks_of_data_from_source(source_url, source_schema, source_query, int(self.parameters_dict['source_batch_size']))):
                    # print(chunk_data.info())
                    chunk_data = self.prepare_source_df(chunk_data, row_key_column_name, self.dnx_config.process_no_column_name, no_of_cores)
                    # chunk_data = chunk_data.rename(index=str, columns=f_col)
                    current_rows = len(chunk_data.index)
                    count_rows += current_rows

                    # object_columns = chunk_data.select_dtypes(include='object')
                    # for i in object_columns.columns:
                    #     chunk_data[i] = chunk_data[i].apply(str)

                    save_to_parquet(chunk_data, source_data_set, partition_cols=[self.dnx_config.process_no_column_name])

                # print("{:,}".format(count_rows), "record loaded into ", source_data_set, "in", datetime.datetime.now() - start_load_time)

    def melt_query_result(self,df_result,source_id):

        df_melt_result = pd.melt(df_result, id_vars='rowkey', var_name='AttributeName', value_name='AttributeValue')
        df_melt_result.columns = ['RowKey', 'AttributeName', 'AttributeValue']
        df_melt_result['BTSID'] = '1'
        df_melt_result['SourceID'] = source_id
        df_melt_result['new_row'] = '1'
        df_melt_result['RefSID'] = None
        df_melt_result['HashValue'] = df_melt_result['AttributeValue'].apply(sha1)
        df_melt_result['InsertedBy'] = 'ETL'
        df_melt_result['ModifiedBy'] = None
        df_melt_result['ValidFrom'] = datetime.datetime.now().isoformat()
        df_melt_result['ValidTo'] = None
        df_melt_result['IsCurrent'] = '1'
        df_melt_result['bt_id'] = 0
        df_melt_result[self.dnx_config.process_no_column_name] = self.process_no
        # df_melt_result['ResetDQStage'] = 0
        return df_melt_result

    def get_att_ids_df(self, be_data_source_id):
        data_sources_mapping = self.dnx_config.be_data_sources_mapping_collection
        query = "select query_column_name, be_att_id from " + data_sources_mapping + " where be_data_source_id = " + single_quotes(be_data_source_id)
        data_sources_mapping_data = get_all_data_from_source(self.dnx_config.config_db_url, None, query)

        data_sources_mapping_data['ResetDQStage'] = data_sources_mapping_data.apply(lambda row: get_minimum_category(self.dnx_config.config_db_url,
                                                                                                                             "",
                                                                                                                             self.dnx_config.be_attributes_data_rules_collection,
                                                                                                                             row['be_att_id']), axis=1)
        data_sources_mapping_data = data_sources_mapping_data.rename(index=str, columns={"query_column_name": "AttributeName", "be_att_id": "AttributeID"})
        return data_sources_mapping_data

    def attach_attribute_id(self, att_query_df, melt_df):
        # print(att_query_df.columns)
        # print(melt_df.columns)
        new_rows_df = melt_df.merge(att_query_df, left_on='AttributeName',
                                    right_on='AttributeName',
                                    how='left')[self.bt_columns]

        new_rows_df = new_rows_df[new_rows_df['AttributeID'].notnull()]
        new_rows_df['AttributeID'] = new_rows_df.AttributeID.astype('int64')
        new_rows_df['bt_id'] = new_rows_df['SourceID'].astype(str) + new_rows_df['AttributeID'].astype(str) + new_rows_df['RowKey']
        new_rows_df['ResetDQStage'] = new_rows_df.ResetDQStage.astype('int64')

        bt_ids = data_to_list(new_rows_df['bt_id'])
        return new_rows_df[self.bt_columns], bt_ids

    def get_source_data(self, source_id, source_data_set):
        att_query_df = self.get_att_ids_df(source_id)
        for chunk_data in read_batches_from_parquet(source_data_set, None, int(self.parameters_dict['temp_source_batch_size']), self.cpu_num_workers):
            melt_chunk_data = self.melt_query_result(chunk_data, source_id)
            attach_attribute_id_result = self.attach_attribute_id(att_query_df, melt_chunk_data)
            source_data_df, bt_ids = attach_attribute_id_result[0], attach_attribute_id_result[1]
            yield source_data_df, bt_ids

    def switch_bt_current_dataset(self, bt_current_collection):
        current_data_set = self.dnx_db_path + bt_current_collection
        current_data_set_old = current_data_set + "_old"
        delete_dataset(current_data_set_old)
        rename_dataset(current_data_set, current_data_set_old)
        return current_data_set_old

    def get_current_data(self, table_batches, bt_ids):
        current_data_df = pd.DataFrame(columns=self.bt_columns)
        be_ids_filter = [self.bt_columns[0], bt_ids]
        try:
            for chunk_data in read_from_parquet(table_batches, be_ids_filter):
                current_data_df = current_data_df.append(chunk_data)
        except:
            print("data set not found or unexpected error:", sys.exc_info()[0])

        # print(current_data_df)
        return current_data_df

    def get_delta(self, source_df, p_current_df):
        start_time = datetime.datetime.now()
        etl_occurred = -1
        current_df = p_current_df
        process_no_new = self.dnx_config.process_no_column_name+"_new"
        process_no_cbt = self.dnx_config.process_no_column_name + "_cbt"
        if not current_df.empty:
            source_df = source_df.set_index(['bt_id'])
            current_df = current_df.set_index(['bt_id'])
            merge_df = source_df.merge(current_df,
                                       left_on=['bt_id'],
                                       right_on=['bt_id'],
                                       suffixes=('_new', '_cbt'),
                                       how='left'
                                       )
            merge_df = merge_df.reset_index()

            new_data_df = merge_df.loc[(merge_df['SourceID_cbt'].isnull())]
            new_data_df = new_data_df[['bt_id', 'SourceID_new', 'RowKey_new', 'AttributeID_new', 'BTSID_new',
                                       'AttributeValue_new', 'RefSID_new', 'HashValue_new', 'InsertedBy_new',
                                       'ModifiedBy_new', 'ValidFrom_new', 'ValidTo_new', 'IsCurrent_new',
                                       'ResetDQStage_new',
                                       'new_row_new', process_no_new]]
            new_data_df.columns = self.bt_columns

            merge_df = merge_df.loc[(merge_df['SourceID_cbt'].notnull()) &
                                    (merge_df['HashValue_new'].notnull())]

            bt_modified_expired = merge_df.loc[(merge_df['HashValue_cbt'] != merge_df['HashValue_new'])]
            bt_same = merge_df.loc[(merge_df['HashValue_cbt'] == merge_df['HashValue_new'])]

            bt_modified_expired[['ValidTo_cbt', 'IsCurrent_cbt', 'ModifiedBy_cbt']] = \
                [datetime.datetime.now().isoformat(), 0, 'ETL']

            bt_modified_df = bt_modified_expired[['bt_id', 'SourceID_new', 'RowKey_new', 'AttributeID_new', 'BTSID_new',
                                                  'AttributeValue_new', 'RefSID_new', 'HashValue_new', 'InsertedBy_new',
                                                  'ModifiedBy_new', 'ValidFrom_new', 'ValidTo_new', 'IsCurrent_new',
                                                  'ResetDQStage_new',
                                                  'new_row_new', process_no_new]]
            bt_modified_df.columns = self.bt_columns

            bt_expired_data_df = bt_modified_expired[
                ['bt_id', 'SourceID_cbt', 'RowKey_cbt', 'AttributeID_cbt', 'BTSID_cbt',
                 'AttributeValue_cbt', 'RefSID_cbt', 'HashValue_cbt', 'InsertedBy_cbt',
                 'ModifiedBy_cbt', 'ValidFrom_cbt', 'ValidTo_cbt', 'IsCurrent_cbt', 'ResetDQStage_cbt',
                 'new_row_cbt', process_no_cbt]]

            bt_same_df = bt_same[
                ['bt_id', 'SourceID_cbt', 'RowKey_cbt', 'AttributeID_cbt', 'BTSID_cbt',
                 'AttributeValue_cbt', 'RefSID_cbt', 'HashValue_cbt', 'InsertedBy_cbt',
                 'ModifiedBy_cbt', 'ValidFrom_cbt', 'ValidTo_cbt', 'IsCurrent_cbt', 'ResetDQStage_cbt',
                 'new_row_cbt', process_no_cbt]]

            bt_expired_data_df.columns = self.bt_columns
            bt_same_df.columns = self.bt_columns

            # expired_ids = bt_modified_expired[['_id']]
            expired_ids = bt_modified_expired['bt_id'].values

        else:
            bt_expired_data_df = pd.DataFrame()
            bt_modified_df = pd.DataFrame()
            bt_same_df = pd.DataFrame()
            new_data_df = source_df
            expired_ids = []

        if len(bt_modified_df.index) > 0 and len(new_data_df.index) > 0:
            etl_occurred = 2
        elif len(new_data_df.index) > 0:
            etl_occurred = 1
        elif len(bt_modified_df.index) > 0:
            etl_occurred = 0

        end_time = datetime.datetime.now()
        print('---------- source df, p_current_df, bt_modified_df, new_data_df, bt_same_df:',
              len(source_df.index), ',', len(p_current_df.index), ',', len(bt_modified_df.index), ',', len(new_data_df.index), ',', len(bt_same_df.index),
              'time elapsed:', end_time - start_time)
        return bt_modified_df, bt_expired_data_df, new_data_df, etl_occurred, expired_ids, bt_same_df

    def load_data(self, p_source_data, p_current_data, bt_data_set, bt_current_data_set):

        get_delta_result = self.get_delta(p_source_data, p_current_data)

        # bt_current_data_set = self.dnx_db_path + bt_current_collection
        # bt_data_set = self.dnx_db_path + bt_collection


        same_df = get_delta_result[5]
        save_to_parquet(same_df, bt_current_data_set, bt_partition_cols, bt_object_cols)

        if get_delta_result[3] in (0,2): #etl_occurred
            assert len(get_delta_result[0]) == len(get_delta_result[1])

            modified_df = get_delta_result[0]
            expired_df = get_delta_result[1]
            expired_ids = get_delta_result[4]

            save_to_parquet(modified_df, bt_current_data_set, bt_partition_cols, bt_object_cols)
            save_to_parquet(expired_df, bt_data_set, bt_partition_cols, bt_object_cols)
            # print('expired_ids:', expired_ids)
            # manipulate = self.manipulate_etl_data(bt_collection, expired_df, expired_ids, bt_current_collection)  # expired data
            # self.parallel_data_manipulation.append(manipulate)
            #
            # manipulate = self.manipulate_etl_data(bt_current_collection, modified_df)  # modified data
            # self.parallel_data_manipulation.append(manipulate)

        if get_delta_result[3] in (1, 2):  # etl_occurred
            new_data_df = get_delta_result[2]
            save_to_parquet(new_data_df, bt_current_data_set, bt_partition_cols, bt_object_cols)
            # manipulate = self.manipulate_etl_data(bt_current_collection, new_data_df)  # new data
            # self.parallel_data_manipulation.append(manipulate)

    def get_bt_current_data(self, bt_dataset, columns, filter):
        bt_df = pd.DataFrame()
        folders_count = count_folders_in_dir(bt_dataset)
        for f in range(folders_count):
            complete_dataset = bt_dataset + "\\" + str(f)
            for df in read_batches_from_parquet(complete_dataset, columns, int(self.parameters_dict['bt_batch_size']), self.cpu_num_workers, filter=filter):
                if not df.empty:
                    bt_df = bt_df.append(df)

        print('len____bt_df', len(bt_df.index))
        return bt_df

    def etl_be(self, source_id, bt_current_collection, bt_collection, source_collection, process_no, cpu_num_workers):
        base_bt_current_data_set = self.dnx_db_path + bt_current_collection
        bt_data_set = self.dnx_db_path + bt_collection
        base_source_data_set = self.src_db_path + source_collection
        source_data_set = base_source_data_set + '\\' + self.dnx_config.process_no_column_name + '=' + process_no

        if is_dir_exists(source_data_set):
            for i, get_source_data in enumerate(self.get_source_data(source_id,source_data_set)):
                bt_current_data_set = base_bt_current_data_set + "\\" + str(i)
                source_data_df, bt_ids = get_source_data[0], get_source_data[1]
                if int(self.parameters_dict['get_delta']) == 1:
                    bt_current_collection_old = base_bt_current_data_set + "_old"
                    if is_dir_exists(bt_current_collection_old):
                        filter_bt_ids = [['bt_id', bt_ids], ]
                        print('len___filter_bt_ids', len(bt_ids))
                        bt_current_data_df = self.get_bt_current_data(bt_current_collection_old, self.bt_columns, filter_bt_ids)
                        self.load_data(source_data_df, bt_current_data_df, bt_data_set, bt_current_data_set)

                    else:
                        save_to_parquet(source_data_df, bt_current_data_set, bt_partition_cols, bt_object_cols)
                else:
                    save_to_parquet(source_data_df, bt_current_data_set, bt_partition_cols, bt_object_cols)

    def get_be_ids(self):
        be_att_ids_query = "select distinct be_att_id from " + self.dnx_config.be_attributes_data_rules_lvls_collection + " where active = 1"
        be_att_ids = get_all_data_from_source(self.dnx_config.config_db_url, None, be_att_ids_query)

        list_be_att_ids = data_to_list(be_att_ids['be_att_id'])
        in_list = list_to_string(list_be_att_ids, ", ", 1)
        be_ids_query = 'select distinct be_id from ' + self.dnx_config.be_attributes_collection + ' where _id in (' + in_list + ')'
        # print(be_ids_query)
        be_ids = get_all_data_from_source(self.dnx_config.config_db_url, None, be_ids_query)
        return be_ids

    def get_be_source_ids(self,be_id):
        mapping_be_source_ids_query = 'select distinct be_data_source_id from ' + self.dnx_config.be_data_sources_mapping_collection
        mapping_be_source_ids = get_all_data_from_source(self.dnx_config.config_db_url, None, mapping_be_source_ids_query)
        list_mapping_be_source_ids = data_to_list(mapping_be_source_ids['be_data_source_id'])
        in_list = list_to_string(list_mapping_be_source_ids, ", ", 1)

        be_source_ids_query = 'select distinct _id from ' + self.dnx_config.be_data_sources_collection + ' where active = 1 and _id in (' + in_list + ') and be_id = ' + single_quotes(be_id)
        be_source_ids = get_all_data_from_source(self.dnx_config.config_db_url, None, be_source_ids_query)
        # print('mapping_be_source_ids_query', mapping_be_source_ids_query)
        return be_source_ids

    def start_bt(self, process_no, cpu_num_workers):
        start_time = datetime.datetime.now()
        be_ids = self.get_be_ids()
        self.process_no = process_no
        self.cpu_num_workers = cpu_num_workers

        for i, be_id in be_ids.iterrows():
            be_id = be_id['be_id']

            core_tables = get_be_core_table_names(self.dnx_config.config_db_url, self.dnx_config.org_business_entities_collection, be_id)
            bt_current_collection, bt_collection, source_collection = core_tables[0], core_tables[1], core_tables[2]

            be_source_ids = self.get_be_source_ids(be_id)

            # print(be_source_ids)
            for i, source_id in be_source_ids.iterrows():
                source_id = source_id['_id']
                # print(source_id, bt_current_collection, bt_collection, source_collection)
                self.etl_be(source_id, bt_current_collection, bt_collection, source_collection, process_no, cpu_num_workers)

        print('BT elapsed time:', datetime.datetime.now() - start_time)
        # return (mapping_be_source_ids)


# if __name__ == '__main__':
#     test_bt = StartBT()
#     test_bt.load_source_data()
#     test_bt.start_bt('0', 8)