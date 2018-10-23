import sys
from data_cleansing.dc_methods.dc_methods import save_to_parquet

if __name__ == '__main__':
    save_to_parquet(chunk_data,
                    source_data_set,
                    partition_cols=[self.dnx_config.process_no_column_name],
                    load_table_type=None)

