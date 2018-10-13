import data_cleansing.CONFIG.Config as DNXConfig
import pandas as pd
from data_cleansing.dc_methods.dc_methods import get_all_data_from_source, get_be_core_table_names, data_to_list, list_to_string,\
    count_folders_in_dir, read_batches_from_parquet, save_to_parquet, delete_dataset, rename_dataset
import data_cleansing.DQ.data_rules.rules as dr


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
        result_df = pd.DataFrame()
        if not bt_current_data_df.empty:
            result_df = bt_current_data_df
            result_df['be_att_dr_id'] = be_att_dr_id
            result_df['data_rule_id'] = data_rule_id
            result_df['is_issue'] = result_df.apply(lambda x: dr.rules_orchestrate(x['AttributeValue'], data_rule_id), axis=1)
            result_df['data_value_pattern'] = result_df['AttributeValue'].apply(self.get_data_value_pattern)
        return result_df

    def get_categories(self):
        categories_query = "select distinct category_no from " + \
                           self.dnx_config.be_attributes_data_rules_collection +\
                           " where active = 1 order by category_no"
        categories = get_all_data_from_source(self.dnx_config.config_db_url, None, categories_query)

        return categories

    def get_category_rules(self, category_no):
        data_rules_query = "select _id, be_att_id, be_data_source_id from " + \
                           self.dnx_config.be_attributes_data_rules_collection +\
                           " where active = 1 and category_no = "+str(category_no)+""
        data_rules = get_all_data_from_source(self.dnx_config.config_db_url, None, data_rules_query)

        return data_rules

    def get_tmp_rowkeys(self, result_data_set_tmp):
        for df in read_batches_from_parquet(result_data_set_tmp, None, int(self.parameters_dict['bt_batch_size'])):
            yield df['RowKey']

    def get_bt_current_data(self, bt_dataset, columns, filter):
        # total_rows = 0
        folders_count = count_folders_in_dir(bt_dataset)
        for f in range(folders_count):
            # complete_dataset = bt_dataset + "\\" + str(f) + '\\' + self.dnx_config.process_no_column_name + '=' + self.process_no
            complete_dataset = bt_dataset + "\\" + str(f) + '\\'
            for df in read_batches_from_parquet(complete_dataset, columns, int(self.parameters_dict['bt_batch_size'])):
                df = df[(df['SourceID'] == filter['SourceID']) & (df['ResetDQStage'] == filter['ResetDQStage']) & (df['AttributeID'] == filter['AttributeID'])]
                yield df

    def insert_result_df(self, result_df, g_result, result_data_set, next_pass, next_fail, result_data_set_tmp):

        if not result_df.empty:
            if g_result == 1:
                save_to_parquet(result_df, result_data_set)
            else:
                if next_pass == 1:
                    next_df = result_df[result_df['is_issue'] == 0][['RowKey']]
                elif next_fail == 1:
                    next_df = result_df[result_df['is_issue'] == 1][['RowKey']]
                else:
                    next_df = pd.DataFrame()
                if not next_df.empty:
                    # next_df = next_df.rename(index=str, columns={"RowKey": "_id"})
                    save_to_parquet(next_df, result_data_set_tmp)

    def switch_result_dataset_tmp(self, result_data_set_tmp):
        current_dataset = result_data_set_tmp
        current_dataset_old = current_dataset + "_old"
        delete_dataset(current_dataset_old)
        rename_dataset(current_dataset, current_dataset_old)
        return current_dataset_old

    def execute_lvl_data_rules(self, base_bt_current_data_set, result_data_set, result_data_set_tmp, source_id, be_att_dr_id, category_no,
                               be_att_id, rule_id, g_result, current_lvl_no, next_pass, next_fail):

        columns = ['SourceID', 'RowKey', 'AttributeID', 'AttributeValue', 'ResetDQStage']
        filter = {'SourceID': source_id,
                  'ResetDQStage': category_no,
                  'AttributeID': be_att_id}

        result_data_set_tmp_old = self.switch_result_dataset_tmp(result_data_set_tmp)
        for bt_current_data_df in self.get_bt_current_data(base_bt_current_data_set, columns, filter):
            if current_lvl_no > 1:
                result_df = pd.DataFrame()
                for row_keys_df in self.get_tmp_rowkeys(result_data_set_tmp_old): # filter with level number too!
                    bt_nxt_lvl_current_data_df = bt_current_data_df[bt_current_data_df['RowKey'].isin(row_keys_df)]

                    if not bt_nxt_lvl_current_data_df.empty:
                        result_lvl_df = self.validate_data_rule(bt_nxt_lvl_current_data_df, be_att_dr_id, rule_id)
                        result_df = result_df.append(result_lvl_df)

            else:
                print('info bt_current_data_df', bt_current_data_df.info())
                result_df = self.validate_data_rule(bt_current_data_df, be_att_dr_id, rule_id)
            self.insert_result_df(result_df, g_result, result_data_set, next_pass, next_fail, result_data_set_tmp)

    def execute_data_rules(self, data_rule, category_no):
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

            be_id_query = "select be_id from "+ self.dnx_config.be_attributes_collection +\
                          " where _id = " + str(be_att_id)
            be_id = get_all_data_from_source(self.dnx_config.config_db_url, None, be_id_query)['be_id'].values[0]
            core_tables = get_be_core_table_names(self.dnx_config.config_db_url, self.dnx_config.org_business_entities_collection, be_id)
            bt_current_collection = core_tables[0]
            dq_result_collection = core_tables[3]
            # print(core_tables)
            base_bt_current_data_set = self.dnx_db_path + bt_current_collection
            result_data_set = self.result_db_path + dq_result_collection
            result_data_set_tmp = result_data_set + "_tmp"

            self.execute_lvl_data_rules(base_bt_current_data_set, result_data_set, result_data_set_tmp, source_id, be_att_dr_id, category_no,
                                        be_att_id, rule_id, g_result, current_lvl_no, next_pass, next_fail)

    def start_dq(self, process_no, cpu_num_workers):
        pd.set_option('mode.chained_assignment', None)
        self.dnx_config = DNXConfig.Config()
        self.parameters_dict = self.dnx_config.get_parameters_values()
        self.cpu_num_workers = cpu_num_workers
        self.process_no = process_no
        delete_dataset(self.result_db_path)
        categories = self.get_categories()
        for i, category_no in categories.iterrows():
            category_no = category_no['category_no']
            category_rules = self.get_category_rules(category_no)

            for j, data_rule in category_rules.iterrows():
                # open multi processes here
                self.execute_data_rules(data_rule, category_no)
