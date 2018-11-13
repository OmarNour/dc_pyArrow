import sys
from data_cleansing.BT.bt import StartBT
import data_cleansing.dc_methods.dc_methods as dc_methods

if __name__ == '__main__':
    all_inputs = dc_methods.string_to_dict(sys.argv[1])
    try:
        cpu_count = all_inputs['cpu_count']
        cpu_num_workers = all_inputs['cpu_num_workers']
        be_id = all_inputs['be_id']
        # cpu_count = int(sys.argv[1])
    except:
        cpu_count = 1
        cpu_num_workers = 1
        be_id = None

    start_bt = StartBT()
    start_bt.load_source_data(be_id, cpu_count, cpu_num_workers)
