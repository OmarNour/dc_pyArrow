import data_cleansing.CONFIG.Config as DNXConfig
import pandas as pd
from data_cleansing.dc_methods.dc_methods import get_all_data_from_source, get_be_core_table_names, read_from_parquet_drill, \
    count_files_in_dir, get_files_in_dir, count_folders_in_dir, read_batches_from_parquet, save_to_parquet, delete_dataset, rename_dataset, single_quotes, bt_object_cols, \
    read_all_from_parquet, is_dir_exists
import data_cleansing.DQ.data_rules.rules as dr
import swifter
from pydrill.client import PyDrill

class StartDQ:
    def __init__(self):
        pd.set_option('mode.chained_assignment', None)
        self.dnx_config = DNXConfig.Config()
        self.parameters_dict = self.dnx_config.get_parameters_values()
        # parquet_db_root_path = os.path.dirname(sys.modules['data_cleansing.BT'].__file__)+'\\'+ self.dnx_config.parquet_db_root_path
        parquet_db_root_path = self.dnx_config.parquet_db_root_path
        self.src_db_path = parquet_db_root_path + self.dnx_config.src_db_name + '\\'
        self.dnx_db_path = parquet_db_root_path + self.dnx_config.dnx_db_name + '\\'
        self.result_db_path = parquet_db_root_path + self.dnx_config.result_db_name + '\\'

    def get_data_value_pattern(self, att_value):
        try:
            float_att_value = float(att_value)
            return '9' * len(str(float_att_value))
        except:
            return 'A' * len(str(att_value))

    def validate_data_rule(self, bt_current_data_df, be_att_dr_id, data_rule_id):
        print('validate_data_rule started')
        result_df = pd.DataFrame()
        if not bt_current_data_df.empty:
            result_df = bt_current_data_df
            result_df['be_att_dr_id'] = be_att_dr_id
            result_df['data_rule_id'] = data_rule_id
            result_df['is_issue'] = result_df.swifter.apply(lambda x: dr.rules_orchestrate(x['AttributeValue'], data_rule_id), axis=1)
            result_df['data_value_pattern'] = result_df['AttributeValue'].swifter.apply(self.get_data_value_pattern)
        return result_df

    def get_source_categories(self):
        source_categories_query = "select distinct t1.be_data_source_id source_id, t1.category_no from " + \
                           self.dnx_config.be_attributes_data_rules_collection +" t1 "+\
                           " join " +self.dnx_config.be_data_sources_collection+ " t2 on t2._id = t1.be_data_source_id and t2.active = 1" \
                           " where t1.active = 1 order by t1.be_data_source_id, t1.category_no"
        # print(source_categories_query)
        source_categories = get_all_data_from_source(self.dnx_config.config_db_url, None, source_categories_query)

        return source_categories

    def get_be_att_ids(self, source_id, category_no):
        be_att_ids_query = "select distinct t1.be_att_id from " + \
                           self.dnx_config.be_attributes_data_rules_collection + " t1 " + \
                           " join " + self.dnx_config.be_data_sources_collection + " t2 on t2._id = t1.be_data_source_id and t2.active = 1" \
                                                                                   " where t1.active = 1" \
                                                                                   " and t1.category_no = " + str(category_no) + " and t1.be_data_source_id = " + single_quotes(str(source_id)) + \
                           " order by t1.be_att_id"
        be_att_ids = get_all_data_from_source(self.dnx_config.config_db_url, None, be_att_ids_query)

        return be_att_ids

    def get_source_category_rules(self, source_id, category_no):
        data_rules_query = "select t1._id, t1.be_att_id, t1.be_data_source_id from " + \
                           self.dnx_config.be_attributes_data_rules_collection + " t1 " + \
                           " join " + self.dnx_config.be_data_sources_collection + " t2 on t2._id = t1.be_data_source_id and t2.active = 1" \
                                                                                   " where t1.active = 1" \
                           " and t1.category_no = "+str(category_no)+" and t1.be_data_source_id = " + single_quotes(str(source_id)) + \
                           " order by t1.be_data_source_id"
        data_rules = get_all_data_from_source(self.dnx_config.config_db_url, None, data_rules_query)

        return data_rules

    def get_tmp_rowkeys(self, result_data_set_tmp):
        print('result_data_set_tmp', result_data_set_tmp)
        for df in read_batches_from_parquet(result_data_set_tmp, None, int(self.parameters_dict['bt_batch_size']), self.cpu_num_workers):
            yield df['RowKey']

    def get_bt_current_data(self, bt_dataset, columns, filter):
        # total_rows = 0
        folders_count = count_folders_in_dir(bt_dataset)
        # print('folders_count2', folders_count)
        for f in range(folders_count):
            # complete_dataset = bt_dataset + "\\" + str(f) + '\\' + self.dnx_config.process_no_column_name + '=' + self.process_no
            complete_dataset = bt_dataset + "\\" + str(f)
            # complete_dataset = complete_dataset + "\\" + 'SourceID=' + filter['SourceID'] + "\\" + 'ResetDQStage=' + str(filter['ResetDQStage'])
            # print('complete_dataset', complete_dataset)
            for df in read_batches_from_parquet(complete_dataset, columns, int(self.parameters_dict['bt_batch_size']), self.cpu_num_workers):
                # print('dfdfdfdfdf', df)
                # print('df.columns', df.columns)
                df = df[
                    (df['SourceID'] == filter['SourceID'])
                    & (df['ResetDQStage'] == filter['ResetDQStage']) &
                     (df['AttributeID'] == filter['AttributeID'])
                    ]

                # df['ResetDQStage'] = filter['ResetDQStage']
                # df['SourceID'] = filter['SourceID']

                yield df

    def insert_result_df(self, result_df, g_result, result_data_set, next_pass, next_fail, result_data_set_tmp):
        print('insert_result_df started')
        if not result_df.empty:
            if g_result == 1:
                # print('result_data_set.columns', result_df.columns)
                partition_cols = ['SourceID', 'ResetDQStage', 'AttributeID', 'be_att_dr_id', 'data_rule_id']
                string_cols = ['SourceID', 'RowKey', 'data_value_pattern']
                save_to_parquet(result_df, result_data_set, partition_cols=partition_cols, string_columns=string_cols)
            else:
                if next_pass == 1:
                    next_df = result_df[result_df['is_issue'] == 0][['RowKey']]
                    print('next_pass', len(next_df.index))
                elif next_fail == 1:
                    next_df = result_df[result_df['is_issue'] == 1][['RowKey']]
                    print('next_pass', len(next_df.index))
                else:
                    next_df = pd.DataFrame()
                if not next_df.empty:
                    # next_df = next_df.rename(index=str, columns={"RowKey": "_id"})
                    save_to_parquet(next_df, result_data_set_tmp)

    def switch_dataset(self, dataset, suffix):
        current_dataset = dataset
        current_dataset_old = current_dataset + suffix
        delete_dataset(current_dataset_old)
        rename_dataset(current_dataset, current_dataset_old)
        return current_dataset_old

    def execute_lvl_data_rules(self, base_bt_current_data_set, result_data_set, result_data_set_tmp, source_id, be_att_dr_id, category_no,
                               be_att_id, rule_id, g_result, current_lvl_no, next_pass, next_fail):

        print('execute_lvl_data_rules started')
        columns = ['SourceID', 'RowKey', 'AttributeID', 'AttributeValue', 'ResetDQStage']
        filter = {'SourceID': source_id,
                  'ResetDQStage': category_no,
                  'AttributeID': be_att_id}

        suffix = "_old"
        result_data_set_tmp_old = self.switch_dataset(result_data_set_tmp, suffix)

        for bt_current_data_df in self.get_bt_current_data(base_bt_current_data_set, columns, filter):

            if current_lvl_no > 1:
                result_df = pd.DataFrame()
                if is_dir_exists(result_data_set_tmp_old):
                    for row_keys_df in self.get_tmp_rowkeys(result_data_set_tmp_old): # filter with level number too!
                        bt_nxt_lvl_current_data_df = bt_current_data_df[bt_current_data_df['RowKey'].isin(row_keys_df)]

                        if not bt_nxt_lvl_current_data_df.empty:
                            result_lvl_df = self.validate_data_rule(bt_nxt_lvl_current_data_df, be_att_dr_id, rule_id)
                            result_df = result_df.append(result_lvl_df)

            else:
                result_df = self.validate_data_rule(bt_current_data_df, be_att_dr_id, rule_id)
            self.insert_result_df(result_df, g_result, result_data_set, next_pass, next_fail, result_data_set_tmp)

    def execute_data_rules(self, data_rule, category_no):
        print('execute_data_rules started')
        be_att_dr_id = data_rule['_id']
        source_id = data_rule['be_data_source_id']
        be_data_rule_lvls_query = "select be_att_id, rule_id, next_pass, next_fail from " + \
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
            g_result = 1 if no_of_lvls == current_lvl_no else 0

            be_id = self.get_be_id_by_be_att_id(str(be_att_id))
            core_tables = get_be_core_table_names(self.dnx_config.config_db_url, self.dnx_config.org_business_entities_collection, be_id)
            bt_current_collection = core_tables[0]
            dq_result_collection = core_tables[3]
            # print(core_tables)
            base_bt_current_data_set = self.dnx_db_path + bt_current_collection
            result_data_set = self.result_db_path + dq_result_collection
            result_data_set_tmp = result_data_set + "_tmp"

            self.execute_lvl_data_rules(base_bt_current_data_set, result_data_set, result_data_set_tmp, source_id, be_att_dr_id, category_no,
                                        be_att_id, rule_id, g_result, current_lvl_no, next_pass, next_fail)

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

    def is_cell_passed(self, RowKey):
        return False if RowKey in self.rowkeys.index else True

    def check_cells_for_upgrade(self, r_SourceID, r_RowKey, r_AttributeID, r_ResetDQStage, next_cat, p_source_id, p_be_att_id):
        return_next_cat = r_ResetDQStage

        if r_AttributeID == p_be_att_id and r_SourceID == p_source_id:
            if self.is_cell_passed(r_RowKey):
                return_next_cat = next_cat

        # print(r_RowKey, r_AttributeID, 'return_next_cat', return_next_cat)
        return return_next_cat

    def upgrade_category(self, source_id, category_no):
        print('upgrade_category started')
        # next_category = self.get_next_be_att_id_category(source_id, 700, category_no)
        # print('next_category', next_category)
        be_att_ids = self.get_be_att_ids(source_id, category_no)
        # print('be_att_ids', be_att_ids)
        for i, be_att_id in be_att_ids.iterrows():
            be_att_id = be_att_id['be_att_id']
            be_id = self.get_be_id_by_be_att_id(be_att_id)
            core_tables = get_be_core_table_names(self.dnx_config.config_db_url, self.dnx_config.org_business_entities_collection, be_id)
            bt_current_dataset = self.dnx_db_path + core_tables[0]
            dq_result_dataset = self.result_db_path + core_tables[3]
            partioned_dq_result_dataset = dq_result_dataset+"\\"+ "SourceID="+str(source_id)+"\\ResetDQStage="+str(category_no)+"\\AttributeID="+str(be_att_id)
            # print('partioned_dq_result_dataset', partioned_dq_result_dataset)

            folders_count = count_folders_in_dir(bt_current_dataset)
            # print('folders_count', folders_count)
            next_cat = self.get_next_be_att_id_category(source_id, be_att_id, category_no)
            rowkeys = None
            if is_dir_exists(dq_result_dataset):
                self.rowkeys = read_all_from_parquet(dq_result_dataset, ['RowKey', 'is_issue'], self.cpu_num_workers)
                self.rowkeys = self.rowkeys[self.rowkeys['is_issue'] == 1].set_index('RowKey')
                for f in range(folders_count):

                    complete_bt_current_dataset = bt_current_dataset + "\\" + str(f)
                    suffix = "_old"
                    bt_dataset_old = self.switch_dataset(complete_bt_current_dataset, suffix)
                    # print('bt_dataset_old', bt_dataset_old)
                    for bt_current in read_batches_from_parquet(bt_dataset_old, None, int(self.parameters_dict['bt_batch_size']), self.cpu_num_workers):
                        # print('bt_currentbt_current', bt_current[['RowKey', 'AttributeID']])
                        print('self.rowkeys', len(self.rowkeys.index))
                        bt_current1 = bt_current[(bt_current['SourceID']==source_id) &
                                                 (bt_current['ResetDQStage'] == category_no) &
                                                 (bt_current['AttributeID'] == be_att_id) ]

                        bt_current2 = bt_current[(bt_current['SourceID'] == source_id) &
                                                 (bt_current['ResetDQStage'] == category_no) &
                                                 (bt_current['AttributeID'] != be_att_id)]

                        print('bt_current1', len(bt_current1.index))
                        print('bt_current2', len(bt_current2.index))
                        bt_current1['ResetDQStage'] = bt_current1.apply(lambda x: self.check_cells_for_upgrade(x['SourceID'],
                                                                                                               x['RowKey'],
                                                                                                               x['AttributeID'],
                                                                                                               x['ResetDQStage'],
                                                                                                               next_cat,
                                                                                                               source_id,
                                                                                                               be_att_id), axis=1)
                        save_to_parquet(bt_current1, complete_bt_current_dataset, partition_cols=None, string_columns=bt_object_cols)
                        save_to_parquet(bt_current2, complete_bt_current_dataset, partition_cols=None, string_columns=bt_object_cols)

                    delete_dataset(bt_dataset_old)


    def start_dq(self, process_no, cpu_num_workers):
        pd.set_option('mode.chained_assignment', None)
        self.dnx_config = DNXConfig.Config()
        self.parameters_dict = self.dnx_config.get_parameters_values()
        self.cpu_num_workers = cpu_num_workers
        self.process_no = process_no
        self.rowkeys = pd.DataFrame()


        delete_dataset(self.result_db_path)
        source_categories = self.get_source_categories()

        for i, source_id_category_no in source_categories.iterrows():
            source_id = source_id_category_no['source_id']
            category_no = source_id_category_no['category_no']

            source_category_rules = self.get_source_category_rules(source_id, category_no)

            for j, data_rule in source_category_rules.iterrows():
                # open multi processes here
                self.execute_data_rules(data_rule, category_no)

            self.upgrade_category(source_id, category_no)
