import sys
# sys.path.append("C:\\Users\\Omar\\PycharmProjects\\data_cleansing")
from data_cleansing.BT.bt import StartBT
from data_cleansing.DQ.dq import StartDQ
import data_cleansing.dc_methods.dc_methods as dc_methods
import os


if __name__ == '__main__':
    all_inputs = dc_methods.string_to_dict(sys.argv[1])
    # print('all_inputs', all_inputs)
    try:
        process_no = all_inputs['process_no']#int(sys.argv[1])
    except:
        process_no = 0

    try:
        BT = all_inputs['BT']#int(sys.argv[2])
    except:
        BT = 0

    try:
        DQ = all_inputs['DQ']#int(sys.argv[3])

        try:
            dq_type = all_inputs['dq_type']
        except:
            dq_type = None

        try:
            dq_category_no = all_inputs['dq_category_no']
        except:
            dq_category_no = None

        try:
            source_id = all_inputs['source_id']
        except:
            source_id = None

        try:
            be_att_id = all_inputs['be_att_id']
        except:
            be_att_id = None

        try:
            be_att_dr_id = all_inputs['be_att_dr_id']
        except:
            be_att_dr_id = None

        try:
            join_with_f = all_inputs['join_with_f']
        except:
            join_with_f = None

    except:
        DQ = 0
        dq_type = None
        dq_category_no = None
        source_id = None
        be_att_id = None
        be_att_dr_id = None
        join_with_f = None


    try:
        cpu_num_workers = all_inputs['cpu_num_workers']#int(sys.argv[4])
    except:
        cpu_num_workers = -1
    # print(sys.modules)
    # print('module_path', os.path.dirname(sys.modules['data_cleansing.BT'].__file__))
    # BT = 1

    if BT == 1:
        # BT_time = datetime.datetime.now()
        bt = StartBT()
        # bt.load_source_data() ########### for teting only from this file
        bt.start_bt(str(process_no), cpu_num_workers)
        # print('----------------     BT_time:', datetime.datetime.now() - BT_time, '      ----------------')
    if DQ == 1:
        # DQ_time = datetime.datetime.now()
        start_dq = StartDQ()
        start_dq.start_dq(str(process_no), cpu_num_workers, dq_type, dq_category_no, source_id, be_att_id, be_att_dr_id, join_with_f)
        # print('----------------     DQ_time:', datetime.datetime.now() - DQ_time, '      ----------------')
