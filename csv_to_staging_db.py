import urllib.parse
from sqlalchemy import create_engine
import time

from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)

# filename = 'C:/project/feb24 _sample_file/MLDB_final.csv'
filename = 'C:/project/feature_box_poc_1_box_connector/fail_csv_cndb__2021-03-18_.csv'

import pandas as pd
df_prod = pd.read_csv(filename)



table_name = 'tbl_ibm_source_data_failures_staging'
start_time_copy_expert = time.time()

connection = engine.raw_connection() ## # TODO connection pooling required
cursor = connection.cursor() ## # TODO connection pooling required

# sql = "COPY " + table_name + " FROM STDIN DELIMITER \',\' CSV header;"
# cursor.copy_expert(sql, open(filename))

df_prod.to_sql(table_name, con=engine, if_exists='append', index=False)


connection.commit()
cursor.close()

end_time = time.time()

seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)