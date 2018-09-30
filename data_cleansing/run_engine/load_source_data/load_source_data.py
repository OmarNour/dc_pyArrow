import sys
from data_cleansing.BT.bt import StartBT

if __name__ == '__main__':
    try:
        cpu_count = int(sys.argv[1])
    except:
        cpu_count = 1
    start_bt = StartBT()
    start_bt.load_source_data(cpu_count)
