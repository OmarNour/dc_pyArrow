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

    print('no_of_subprocess', no_of_subprocess)
    print('server_cpu_count', server_cpu_count)
    print('cpu_count', cpu_count)
    print('cpu_num_workers', cpu_num_workers)

    return cpu_count, cpu_num_workers


def dc_multiprocessing(to_run, no_of_subprocess=None, inputs="", desc=None):

    result = get_cpu_count_cpu_num_workers(dnx_config.config_db_url, dnx_config.parameters_collection, no_of_subprocess=no_of_subprocess)
    cpu_count, cpu_num_workers = result[0], result[1]
    # to_run = '/run_engine.py'
    process_dict = {}
    process_list = []
    for p in range(cpu_count):
        process_list.append(p)
        process_no = str(p)

        main_inputs = " process_no="+process_no+" cpu_num_workers="+str(cpu_num_workers)+" "
        all_inputs = main_inputs + inputs
        process_dict[process_no] = subprocess.Popen(['python',
                                                     to_run,
                                                     all_inputs])

    count_finished_processes = 0
    while process_list:
        for p_no in range(cpu_count):
            if process_dict[str(p_no)].poll() is not None:
                try:
                    process_list.remove(p_no)
                    count_finished_processes += 1
                    print('-----------------------------------------------------------')
                    print('Process no.', p_no, 'finished, total finished', count_finished_processes, 'out of', cpu_count)

                except:
                    None


if __name__ == '__main__':
    # build_config_db()

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
                dc_multiprocessing(to_run, no_of_subprocess=1, inputs=inputs, desc=None)

                print('####################     load_source_data_time:', datetime.datetime.now() - load_source_data_time, '      ####################')
            # config_database[dnx_config.multiprocessing_collection].drop()

            bt_time = datetime.datetime.now()
            to_run = module_path + '/run_engine.py'
            inputs = "BT=" + str(BT)
            dc_multiprocessing(to_run, no_of_subprocess=None, inputs=inputs, desc=None)
            # 65,010,912 bt current
            print('####################     bt_time:', datetime.datetime.now() - bt_time, '      ####################')
        if DQ == 1:
            parquet_db_root_path = dnx_config.parquet_db_root_path
            result_db_path = parquet_db_root_path + dnx_config.result_db_name + '\\'
            dc_methods.delete_dataset(result_db_path)

            dq_time = datetime.datetime.now()
            to_run = module_path + '/run_engine.py'
            inputs = "DQ=" + str(DQ)
            dc_multiprocessing(to_run, no_of_subprocess=1, inputs=inputs, desc=None)
            print('####################     dq_time:', datetime.datetime.now() - dq_time, '      ####################')

        # config_database[dnx_config.run_engine_collection].update_one({'_id': i['_id']}, {'$set': {'end_time': datetime.datetime.now()}})
        print('####################     total time:', datetime.datetime.now() - run_time, '      ####################')

