from data_cleansing.dc_methods.dc_methods import get_parameter_values
import multiprocessing
import os
import sys


class Config:
    # config_file_path = 'D:/github/Python/Data_Cleansing/data_cleansing/dnx_config.xlsx'
    config_db_url = 'sqlite:///C:/Users/ON250000/PycharmProjects/dc_pyArrow/data_cleansing/CONFIG/dnx_config_db.db'

    parquet_db_name = 'C:\dc\parquet_db'
    parquet_db_root_path = parquet_db_name+"\\"

    drill_parquet_db_root_path = '/opt/parquet_db/'

    config_db_name = 'DNX_config'
    src_db_name = 'Source_data'
    dnx_db_name = 'DNX'
    result_db_name = 'Result'


    module_path = os.path.dirname(sys.modules['__main__'].__file__)

    tmp_result_db_name = 'tmp_Result'
    process_no_column_name = 'process_no'

    parameters_collection = 'parameters'
    organizations_collection = 'organizations'
    org_connections_collection = 'org_connections'
    org_business_entities_collection = 'org_business_entities'
    org_attributes_collection = 'org_attributes'
    be_attributes_collection = 'be_attributes'
    be_data_sources_collection = 'be_data_sources'
    be_data_sources_mapping_collection = 'be_data_sources_mapping'
    data_rules_collection = 'data_rules'
    be_attributes_data_rules_collection = 'be_attributes_data_rules'
    be_attributes_data_rules_lvls_collection = 'be_attributes_data_rules_lvls'
    run_engine_collection = 'run_engine'

    cpu_count = multiprocessing.cpu_count()
    multiprocessing_collection = 'multiprocessing'
    multiprocessing_p_no = 'p_no'
    multiprocessing_cpu_num_workers = 'cpu_num_workers'
    multiprocessing_etl = 'elt'
    multiprocessing_bt_inserts = 'bt_inserts'
    multiprocessing_bt_current_inserts = 'bt_current_inserts'
    multiprocessing_bt_current_deletes = 'bt_current_deletes'
    multiprocessing_process_alive = 'process_alive'

    def get_parameters_values(self):
        parameters_values = get_parameter_values(self.config_db_url, self.parameters_collection)

        parameters_values_dict_list = parameters_values.to_dict('records')
        # print(parameters_values_dict_list)
        parameters_values_dict = {}
        for i in parameters_values_dict_list:
            parameters_values_dict[i['_id']] = i['value']

        return parameters_values_dict
# if __name__ == '__main__':
#     x = Config()
#     print(x.get_parameters_values()['crud_operations_batch_size'])