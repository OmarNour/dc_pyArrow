import data_cleansing.CONFIG.Config as DNXConfig
import pandas as pd
from data_cleansing.dc_methods.dc_methods import get_all_data_from_source, get_be_core_table_names, \
    get_files_in_dir, read_batches_from_parquet, save_to_parquet, delete_dataset, rename_dataset, single_quotes, result_cols, \
    read_all_from_parquet_delayed, read_all_from_parquet, is_dir_exists, bt_partioned_object_cols, result_object_cols, result_partition_cols, read_partioned_data_from_parquet
import data_cleansing.DQ.data_rules.rules as dr
# from pydrill.client import PyDrill
# import swifter
from dask.diagnostics import ProgressBar
from dask import compute, delayed, dataframe as dd


class StartDQ:
    def __init__(self):
        pd.set_option('mode.chained_assignment', None)
        self.dnx_config = DNXConfig.Config()
        self.parameters_dict = self.dnx_config.get_parameters_values()
        # parquet_db_root_path = os.path.dirname(sys.modules['data_cleansing.BT'].__file__)+'\\'+ self.dnx_config.parquet_db_root_path
        parquet_db_root_path = self.dnx_config.parquet_db_root_path
        self.src_db_path = parquet_db_root_path + self.dnx_config.src_db_name + '\\'
        self.dnx_db_path = parquet_db_root_path + self.dnx_config.dnx_db_name + '\\'
        self.src_f_db_path = parquet_db_root_path + self.dnx_config.src_f_db_name + '\\'
        self.result_db_path = parquet_db_root_path + self.dnx_config.result_db_name + '\\'

    def get_data_value_pattern(self, att_value):
        try:
            float_att_value = float(att_value)
            return '9' * len(str(float_att_value))
        except:
            return 'A' * len(str(att_value))

    def validate_data_rule(self, bt_current_data_df, be_att_dr_id, data_rule_id, kwargs):
        # print('validate_data_rule started', be_att_dr_id, data_rule_id, kwargs)
        result_df = pd.DataFrame()
        # bt_current_data_df.compute()
        if not bt_current_data_df.empty:
            # print('bt_current_data_df', len(bt_current_data_df.index))
            if kwargs:
                # ex: Firstname="Sita" Lastname="Sharma" Age=22 Phone=1234567890
                kwargs_dic = eval("dict(%s)" % ','.join(kwargs.split()))
                # print('kwargs_dic', kwargs_dic, kwargs_dic.keys())
            else:
                kwargs_dic = {}
            result_df = bt_current_data_df
            result_df['be_att_dr_id'] = be_att_dr_id
            result_df['data_rule_id'] = data_rule_id
            result_df['is_issue'] = result_df.apply(lambda x: dr.rules_orchestrate(data_rule_id, x['AttributeValue'], x['RowKey'], x, kwargs_dic), axis=1)
            result_df['data_value_pattern'] = result_df['AttributeValue'].apply(self.get_data_value_pattern)
        return result_df

    def get_source_categories(self):
        # t1.be_data_source_id source_id,
        source_categories_query = "select distinct t1.category_no from " + \
                           self.dnx_config.be_attributes_data_rules_collection + " t1 " +\
                           " join " +self.dnx_config.be_data_sources_collection+ " t2 on t2._id = t1.be_data_source_id and t2.active = 1" \
                           " where t1.active = 1 order by t1.be_data_source_id, t1.category_no"
        # print(source_categories_query)
        source_categories = get_all_data_from_source(self.dnx_config.config_db_url, None, source_categories_query)

        return source_categories

    def get_be_att_ids(self, category_no):
        be_att_ids_query = "select distinct t1.be_data_source_id, t1.be_att_id from " + \
                           self.dnx_config.be_attributes_data_rules_collection + " t1 " + \
                           " join " + self.dnx_config.be_data_sources_collection + " t2 on t2._id = t1.be_data_source_id and t2.active = 1" \
                                                                                   " where t1.active = 1" \
                                                                                   " and t1.category_no = " + str(category_no) + \
                           " order by be_data_source_id, t1.be_att_id"
        be_att_ids = get_all_data_from_source(self.dnx_config.config_db_url, None, be_att_ids_query)

        return be_att_ids

    def get_source_category_rules(self, category_no):
        data_rules_query = "select t1._id, t1.be_att_id, t1.be_data_source_id, join_with_f from " + \
                           self.dnx_config.be_attributes_data_rules_collection + " t1 " + \
                           " join " + self.dnx_config.be_data_sources_collection + " t2 on t2._id = t1.be_data_source_id and t2.active = 1" \
                                                                                   " where t1.active = 1" \
                           " and t1.category_no = "+str(category_no)+ \
                           " order by t1.be_data_source_id, be_att_id"
        data_rules = get_all_data_from_source(self.dnx_config.config_db_url, None, data_rules_query)

        return data_rules

    def get_tmp_rowkeys(self, result_data_set_tmp):
        # print('result_data_set_tmp', result_data_set_tmp)
        for df in read_batches_from_parquet(result_data_set_tmp, None, int(self.parameters_dict['bt_batch_size']), True):
            yield df['RowKey']

    def get_bt_current_data(self, src_f_data, bt_dataset, source_id, category_no, be_att_id, join_with_f):
        complete_dataset = bt_dataset + \
                           "\\SourceID=" + str(source_id) +\
                           "\\AttributeID=" + str(be_att_id) +\
                           "\\ResetDQStage=" + str(category_no)
        # print('complete_dataset', complete_dataset)
        if is_dir_exists(complete_dataset):
            # print('src_f_data', src_f_data.compute().columns)

            for file_name in get_files_in_dir(complete_dataset):
                pa_file_path = complete_dataset+"\\"+file_name
                # bt_current_df = read_batches_from_parquet(dataset_root_path=pa_file_path,
                #                                           columns=['bt_id', 'RowKey', 'AttributeValue'],
                #                                           batch_size=int(self.parameters_dict['bt_batch_size']),
                #                                           use_threads=True, filter=None, filter_index=True)
                bt_current_df = read_all_from_parquet_delayed(dataset=pa_file_path,
                                                              columns=['bt_id', 'RowKey', 'AttributeValue'],
                                                              filter=None)

                # src_f_data_df = src_f_data[src_f_data['rowkey'].isin(bt_current_df['RowKey'])]
                ###############
                if join_with_f == 1:
                    bt_current_df = bt_current_df.merge(src_f_data,
                                                        left_on=['RowKey'],
                                                        # left_index=True,
                                                        # right_index=True,
                                                        right_on=['rowkey'],
                                                        suffixes=('_new', '_cbt'),
                                                        how='inner'
                                                        )

                ######################
                yield bt_current_df.compute()

    def insert_result_df(self, result_df, g_result, result_data_set, next_pass, next_fail, result_data_set_tmp, source_id, category_no, be_att_id):
        if not result_df.empty:
            if g_result == 1:
                result_df['SourceID'] = source_id
                result_df['ResetDQStage'] = category_no
                result_df['AttributeID'] = be_att_id
                # 'p_SourceID', 'p_AttributeID', 'p_ResetDQStage', 'p_is_issue', 'p_be_att_dr_id', 'p_data_rule_id'
                result_df['p_SourceID'] = source_id
                result_df['p_ResetDQStage'] = category_no
                result_df['p_AttributeID'] = be_att_id
                result_df['p_is_issue'] = result_df['is_issue']
                result_df['p_be_att_dr_id'] = result_df['be_att_dr_id']
                result_df['p_data_rule_id'] = result_df['data_rule_id']

                save_to_parquet(result_df[result_cols], result_data_set, partition_cols=result_partition_cols, string_columns=result_object_cols)
            else:
                if next_pass == 1:
                    next_df = result_df[result_df['is_issue'] == 0][['RowKey']]
                    # print('next_pass', len(next_df.index))
                elif next_fail == 1:
                    next_df = result_df[result_df['is_issue'] == 1][['RowKey']]
                    # print('next_pass', len(next_df.index))
                else:
                    next_df = pd.DataFrame()
                if not next_df.empty:
                    save_to_parquet(next_df, result_data_set_tmp)

    def switch_dataset(self, dataset, suffix):
        current_dataset = dataset
        current_dataset_old = current_dataset + suffix
        delete_dataset(current_dataset_old)
        rename_dataset(current_dataset, current_dataset_old)
        return current_dataset_old

    def execute_lvl_data_rules(self, src_f_data, base_bt_current_data_set, result_data_set, result_data_set_tmp, source_id, be_att_dr_id, category_no,
                               be_att_id, rule_id, g_result, current_lvl_no, next_pass, next_fail, join_with_f, kwargs):


        # print('++++++++ source_id:', source_id, 'be_att_dr_id:', be_att_dr_id, 'category_no:', category_no)
        # print('++++++++ be_att_id:', be_att_id, 'rule_id:', rule_id, 'g_result:', g_result, 'current_lvl_no:', current_lvl_no,
        #       'next_pass:', next_pass, 'next_fail:', next_fail)

        result_data_set_tmp = result_data_set_tmp+"_"+str(be_att_dr_id)

        suffix = "_old"
        result_data_set_tmp_old = self.switch_dataset(result_data_set_tmp, suffix)

        for bt_current_data_df in self.get_bt_current_data(src_f_data, base_bt_current_data_set, source_id, category_no, be_att_id, join_with_f):
            # print('len_bt_current_data_df', len(bt_current_data_df.index))
            if not bt_current_data_df.empty:
                if current_lvl_no > 1:
                    result_df = pd.DataFrame()
                    if is_dir_exists(result_data_set_tmp_old):
                        for row_keys_df in self.get_tmp_rowkeys(result_data_set_tmp_old):  # filter with level number too!
                            bt_nxt_lvl_current_data_df = bt_current_data_df[bt_current_data_df['RowKey'].isin(row_keys_df)]

                            if not bt_nxt_lvl_current_data_df.empty:
                                result_lvl_df = self.validate_data_rule(bt_nxt_lvl_current_data_df, be_att_dr_id, rule_id, kwargs)
                                result_df = result_df.append(result_lvl_df)

                else:
                    result_df = self.validate_data_rule(bt_current_data_df, be_att_dr_id, rule_id, kwargs)
                self.insert_result_df(result_df, g_result, result_data_set, next_pass, next_fail, result_data_set_tmp, source_id, category_no, be_att_id)
        delete_dataset(result_data_set_tmp_old)

    def execute_data_rules(self, data_rule, category_no):
        # print('execute_data_rules started')
        be_att_dr_id = data_rule['_id']
        source_id = data_rule['be_data_source_id']
        join_with_f = data_rule['join_with_f']

        be_data_rule_lvls_query = "select be_att_id, rule_id, next_pass, next_fail, kwargs from " + \
                                  self.dnx_config.be_attributes_data_rules_lvls_collection + \
                                  " where active = 1 and be_att_dr_id = " + str(be_att_dr_id) + " order by level_no"
        be_data_rule_lvls = get_all_data_from_source(self.dnx_config.config_db_url, None, be_data_rule_lvls_query)
        no_of_lvls = len(be_data_rule_lvls.index)

        for current_lvl_no, data_rule_lvls in enumerate(be_data_rule_lvls.iterrows(), start=1):
            data_rule_lvls = data_rule_lvls[1]
            be_att_id = data_rule_lvls['be_att_id']
            rule_id = data_rule_lvls['rule_id']
            next_pass = data_rule_lvls['next_pass']
            next_fail = data_rule_lvls['next_fail']
            kwargs = data_rule_lvls['kwargs']

            g_result = 1 if no_of_lvls == current_lvl_no else 0
            # print('no_of_lvls', be_att_dr_id, g_result, no_of_lvls, current_lvl_no)

            be_id = self.get_be_id_by_be_att_id(str(be_att_id))
            core_tables = get_be_core_table_names(self.dnx_config.config_db_url, self.dnx_config.org_business_entities_collection, be_id)
            bt_current_collection = core_tables[0]
            source_collection = core_tables[2]
            dq_result_collection = core_tables[3]

            # print(core_tables)
            base_bt_current_data_set = self.dnx_db_path + bt_current_collection
            src_f_data_set = self.src_f_db_path + source_collection + "\\SourceID=" + str(source_id)
            result_data_set = self.result_db_path + dq_result_collection
            self.all_result_data_set.append(result_data_set) if result_data_set not in self.all_result_data_set else None
            result_data_set_tmp = result_data_set + "_tmp"

            if current_lvl_no == 1 and join_with_f == 1:
                src_f_data = read_all_from_parquet_delayed(src_f_data_set)
            else:
                src_f_data = None
                # src_f_data = src_f_data.set_index('rowkey')
            if is_dir_exists(base_bt_current_data_set):
                self.execute_lvl_data_rules(src_f_data, base_bt_current_data_set, result_data_set, result_data_set_tmp, source_id, be_att_dr_id, category_no,
                                            be_att_id, rule_id, g_result, current_lvl_no, next_pass, next_fail, join_with_f, kwargs)

    def get_next_be_att_id_category(self, source_id, be_att_id, current_category):
        next_category_query = "select min(category_no) next_category_no" \
                              " from " + self.dnx_config.be_attributes_data_rules_collection + \
                              " where be_att_id = " + str(be_att_id) + \
                              " and be_data_source_id =  " + single_quotes(str(source_id)) + \
                              " and category_no > " + str(current_category)
        # print('next_category_query', next_category_query)
        next_category = get_all_data_from_source(self.dnx_config.config_db_url, None, next_category_query)['next_category_no'].values[0]

        if next_category is None:
            next_category = current_category + 1

        return next_category

    def get_be_id_by_be_att_id(self, be_att_id):
        be_id_query = "select be_id from " + self.dnx_config.be_attributes_collection + \
                      " where _id = " + str(be_att_id)
        be_id = get_all_data_from_source(self.dnx_config.config_db_url, None, be_id_query)['be_id'].values[0]

        return be_id

    def upgrade_category(self, source_id, category_no, be_att_id):
        be_id = self.get_be_id_by_be_att_id(be_att_id)
        core_tables = get_be_core_table_names(self.dnx_config.config_db_url, self.dnx_config.org_business_entities_collection, be_id)
        bt_current_dataset = self.dnx_db_path + core_tables[0]
        if is_dir_exists(bt_current_dataset):
            next_cat = self.get_next_be_att_id_category(source_id, be_att_id, category_no)
            current_category_dataset = bt_current_dataset + "\\SourceID=" + str(source_id) + "\\AttributeID=" + str(be_att_id) + "\\ResetDQStage=" + str(category_no)
            next_category_dataset = bt_current_dataset + "\\SourceID=" + str(source_id) + "\\AttributeID=" + str(be_att_id) + "\\ResetDQStage=" + str(next_cat)
            dq_result_dataset = self.result_db_path + core_tables[3]

            partioned_dq_result_dataset = dq_result_dataset + \
                                          "\\SourceID=" + str(source_id) + \
                                          "\\AttributeID=" + str(be_att_id) + \
                                          "\\ResetDQStage=" + str(category_no) + \
                                          "\\is_issue=0"

            if is_dir_exists(partioned_dq_result_dataset):
                rowkeys = read_all_from_parquet_delayed(partioned_dq_result_dataset, ['RowKey']).compute()
                suffix = "_old"
                if is_dir_exists(current_category_dataset):
                    bt_dataset_old = self.switch_dataset(current_category_dataset, suffix)

                    rowkeys = rowkeys.set_index('RowKey')
                    parallel_delayed_upgrade_rowkeys = []
                    for bt_current in read_batches_from_parquet(bt_dataset_old, None, int(self.parameters_dict['bt_batch_size']), True):
                        self.upgrade_rowkeys(bt_current, rowkeys, current_category_dataset, next_category_dataset)
                        # delayed_upgrade_rowkeys = delayed(self.upgrade_rowkeys)(delayed(bt_current), delayed(rowkeys), current_category_dataset, next_category_dataset)
                        # parallel_delayed_upgrade_rowkeys.append(delayed_upgrade_rowkeys)
                    # compute(*parallel_delayed_upgrade_rowkeys, num_workers=self.cpu_num_workers)
                    delete_dataset(bt_dataset_old)

    def upgrade_rowkeys(self, bt_current, rowkeys, current_category_dataset, next_category_dataset):
        bt_current_passed = bt_current[bt_current['RowKey'].isin(rowkeys.index)]
        bt_current_failed = bt_current[~bt_current.index.isin(bt_current_passed.index)]

        save_to_parquet(bt_current_failed, current_category_dataset, partition_cols=None, string_columns=bt_partioned_object_cols)
        save_to_parquet(bt_current_passed, next_category_dataset, partition_cols=None, string_columns=bt_partioned_object_cols)

    def show_results(self):
        print("**********************************************************************")

        for result_path in self.all_result_data_set:
            # result_path = "C:\\dc\\parquet_db\\Result\\result_4383_10"
            if is_dir_exists(result_path):
                df1 = dd.read_parquet(path=result_path, engine='pyarrow')[['p_SourceID', 'p_AttributeID', 'p_ResetDQStage', 'p_be_att_dr_id', 'p_data_rule_id', 'p_is_issue', 'bt_id']]
                df2 = df1.reset_index()
                df2.columns = ['indx', 'SourceID', 'AttributeID', 'Category_no', 'be_att_dr_id', 'rule_id', 'is_issue', 'bt_id']
                df2 = df2.groupby(['SourceID', 'AttributeID', 'Category_no', 'be_att_dr_id', 'rule_id', 'is_issue']).agg({'bt_id': ['count']})
                df2.columns = ["cells#"]
                with ProgressBar():
                    print(df2.compute())
                print("----------------------------------------------------------------------")
        print("**********************************************************************")

    def start_dq(self, process_no, cpu_num_workers):
        self.cpu_num_workers = cpu_num_workers
        self.process_no = process_no
        self.rowkeys = pd.DataFrame()
        self.all_result_data_set = []

        source_categories = self.get_source_categories()

        for i, source_id_category_no in source_categories.iterrows():
            # source_id = source_id_category_no['source_id']
            category_no = source_id_category_no['category_no']

            source_category_rules = self.get_source_category_rules(category_no)
            parallel_execute_data_rules = []
            for j, data_rule in source_category_rules.iterrows():
                delayed_execute_data_rules = delayed(self.execute_data_rules)(data_rule, category_no)
                parallel_execute_data_rules.append(delayed_execute_data_rules)
            print('execute rules for category #', category_no)
            with ProgressBar():
                compute(*parallel_execute_data_rules, num_workers=self.cpu_num_workers)

            source_id_be_att_ids = self.get_be_att_ids(category_no)
            parallel_execute_upgrade_category = []
            for k, source_id_be_att_id in source_id_be_att_ids.iterrows():
                be_att_id = source_id_be_att_id['be_att_id']
                source_id = source_id_be_att_id['be_data_source_id']
                delayed_upgrade = delayed(self.upgrade_category)(source_id, category_no, be_att_id)
                parallel_execute_upgrade_category.append(delayed_upgrade)
            print('upgrade from category #', category_no)
            with ProgressBar():
                compute(*parallel_execute_upgrade_category, num_workers=self.cpu_num_workers)

        self.show_results()
