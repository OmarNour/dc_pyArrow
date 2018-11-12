import sys, os
# sys.path.append("C:\\Users\\Omar\\PycharmProjects\\data_cleansing")
import subprocess
import multiprocessing
from data_cleansing.dc_methods.dc_methods import list_to_string, get_all_data_from_source
# from data_cleansing.build_configuration_schema.config_schema import build_config_db
# import pymongo
import data_cleansing.CONFIG.Config as DNXConfig
import datetime
import math
import data_cleansing.dc_methods.dc_methods as dc_methods
import data_cleansing.DQ.dq as dq


def get_cpu_count_cpu_num_workers(config_db_url, parameters_collection, no_of_subprocess=None):
    if no_of_subprocess is None:
        no_of_subprocess_query = "select value from " + parameters_collection + " where _id = 'no_of_subprocess' "
        no_of_subprocess = int(list_to_string(get_all_data_from_source(config_db_url, None, no_of_subprocess_query)['value'].values))


    server_cpu_count = multiprocessing.cpu_count()

    if 0 < no_of_subprocess <= server_cpu_count:
        cpu_count = no_of_subprocess
    else:
        cpu_count = server_cpu_count

    if cpu_count == server_cpu_count:
        cpu_num_workers = 1
    else:
        cpu_num_workers = math.floor(server_cpu_count / cpu_count)

    # print('no_of_subprocess', no_of_subprocess)
    # print('server_cpu_count', server_cpu_count)
    # print('cpu_count', cpu_count)
    # print('cpu_num_workers', cpu_num_workers)

    return cpu_count, cpu_num_workers


def dc_multiprocessing(to_run, no_of_subprocess=None, inputs="", desc=None, dq_type=None):

    result = get_cpu_count_cpu_num_workers(dnx_config.config_db_url, dnx_config.parameters_collection, no_of_subprocess=no_of_subprocess)
    cpu_count, cpu_num_workers = result[0], result[1]
    # to_run = '/run_engine.py'
    process_dict = {}
    process_list = []

    if dq_type is None:
        for p in range(cpu_count):

            process_list.append(p)
            process_no = str(p)
            main_inputs = " process_no="+process_no+" cpu_num_workers="+str(cpu_num_workers)+" "
            all_inputs = main_inputs + inputs
            process_dict[process_no] = subprocess.Popen(['python',
                                                         to_run,
                                                         all_inputs])
    elif dq_type == 1:
        source_category_rules = dq.StartDQ.get_source_category_rules(dnx_config.config_db_url, category_no)
        for p, data_rule in source_category_rules.iterrows():
            be_att_dr_id = data_rule['_id']
            source_id = data_rule['be_data_source_id']
            join_with_f = data_rule['join_with_f']

            process_list.append(p)
            process_no = str(p)
            main_inputs = " process_no=" + process_no + " cpu_num_workers=" + str(cpu_num_workers) + " "
            all_inputs = main_inputs + inputs + " be_att_dr_id=" + str(be_att_dr_id) + " source_id=" + str(source_id) + " join_with_f=" + str(join_with_f)
            process_dict[process_no] = subprocess.Popen(['python',
                                                         to_run,
                                                         all_inputs])
    elif dq_type == 2:
        source_id_be_att_ids = dq.StartDQ.get_be_att_ids(dnx_config.config_db_url, category_no)
        for p, source_id_be_att_id in source_id_be_att_ids.iterrows():
            be_att_id = source_id_be_att_id['be_att_id']
            source_id = source_id_be_att_id['be_data_source_id']

            process_list.append(p)
            process_no = str(p)
            main_inputs = " process_no=" + process_no + " cpu_num_workers=" + str(cpu_num_workers) + " "
            all_inputs = main_inputs + inputs + " be_att_id=" + str(be_att_id) + " source_id=" + str(source_id)
            process_dict[process_no] = subprocess.Popen(['python',
                                                         to_run,
                                                         all_inputs])

    count_finished_processes = 0
    no_of_subprocess = len(process_list)
    while process_list:
        for p_no in range(no_of_subprocess):
            if process_dict[str(p_no)].poll() is not None:
                try:
                    process_list.remove(p_no)
                    count_finished_processes += 1
                    # print('-----------------------------------------------------------')
                    print('Process no.', p_no, 'finished, total finished', count_finished_processes, 'out of', no_of_subprocess)

                except:
                    None


if __name__ == '__main__':
    initial_time = datetime.datetime.now()
    load_source_data_end_time = initial_time
    load_source_data_time = initial_time
    bt_end_time = initial_time
    bt_time = initial_time
    dq_end_time = initial_time
    dq_time = initial_time

    dq_process_dict = {}

    dnx_config = DNXConfig.Config()
    # client = pymongo.MongoClient(dnx_config.mongo_uri)
    # config_database = client[dnx_config.config_db_name]

    module_path = os.path.dirname(sys.modules['__main__'].__file__)

    run_time = datetime.datetime.now()

    result = get_cpu_count_cpu_num_workers(dnx_config.config_db_url, dnx_config.parameters_collection, no_of_subprocess=None)
    bt_cpu_count, bt_cpu_num_workers = result[0], result[1]

    result = get_cpu_count_cpu_num_workers(dnx_config.config_db_url, dnx_config.parameters_collection, no_of_subprocess=1)
    dq_cpu_count, dq_cpu_num_workers = result[0], result[1]

    run_engine_query = "select RD, BT, DQ from " + dnx_config.run_engine_collection + " where start_time = '' "
    run_engine_data = get_all_data_from_source(dnx_config.config_db_url, None, run_engine_query)

    # run_engine_data = config_database[dnx_config.run_engine_collection].find({'start_time': ''})
    # print('run_engine_data', run_engine_query, run_engine_data)
    for i, run_engine_row in run_engine_data.iterrows():
        # print(run_engine_row)
        RD = run_engine_row['RD']
        BT = run_engine_row['BT']
        DQ = run_engine_row['DQ']

        if BT == 1:
            if RD == 1:
                load_source_data_time = datetime.datetime.now()
                to_run = module_path + '/load_source_data/load_source_data.py'
                inputs = "cpu_count=" + str(bt_cpu_count)
                print('start loading data from sources ...')
                dc_multiprocessing(to_run, no_of_subprocess=1, inputs=inputs, desc=None)
                load_source_data_end_time = datetime.datetime.now()

            # config_database[dnx_config.multiprocessing_collection].drop()

            bt_time = datetime.datetime.now()
            to_run = module_path + '/run_engine.py'
            inputs = "BT=" + str(BT)
            dc_multiprocessing(to_run, no_of_subprocess=None, inputs=inputs, desc=None)
            # 65,010,912 bt current
            bt_end_time = datetime.datetime.now()

        if DQ == 1:
            dq_time = datetime.datetime.now()

            parquet_db_root_path = dnx_config.parquet_db_root_path
            result_db_path = parquet_db_root_path + dnx_config.result_db_name + '\\'
            dc_methods.delete_dataset(result_db_path)

            to_run = module_path + '/run_engine.py'
            source_categories = dq.StartDQ.get_source_categories(dnx_config.config_db_url)
            for i, source_id_category_no in source_categories.iterrows():
                category_no = source_id_category_no['category_no']
                # run rules only
                inputs = "DQ=" + str(DQ) + " dq_type=" + str(1) + " dq_category_no=" + str(category_no)
                dc_multiprocessing(to_run, no_of_subprocess=1, inputs=inputs, desc=None, dq_type=1)

                inputs = "DQ=" + str(DQ) + " dq_type=" + str(2) + " dq_category_no=" + str(category_no)
                dc_multiprocessing(to_run, no_of_subprocess=1, inputs=inputs, desc=None, dq_type=2)

            dq_end_time = datetime.datetime.now()
            dq.StartDQ.show_results(dnx_config.config_db_url, result_db_path, dnx_config.org_business_entities_collection)

        print('####################     Load soruce data time:', load_source_data_end_time - load_source_data_time, '      ####################')
        print('####################                   BT time:', bt_end_time - bt_time, '      ####################')
        print('####################                   DQ time:', dq_end_time - dq_time, '      ####################')
        print('####################                Total time:', datetime.datetime.now() - run_time, '      ####################')


