import sys
# sys.path.append("C:\\Users\\Omar\\PycharmProjects\\data_cleansing")
from data_cleansing.BT.bt import StartBT
import os


if __name__ == '__main__':
    try:
        process_no = int(sys.argv[1])
    except:
        process_no = 0

    try:
        BT = int(sys.argv[2])
    except:
        BT = 0

    try:
        DQ = int(sys.argv[3])
    except:
        DQ = 0

    try:
        cpu_num_workers = int(sys.argv[4])
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
        None
        # DQ_time = datetime.datetime.now()
        # start_dq = StartDQ()
        # start_dq.start_dq(cpu_num_workers)
        # print('----------------     DQ_time:', datetime.datetime.now() - DQ_time, '      ----------------')
