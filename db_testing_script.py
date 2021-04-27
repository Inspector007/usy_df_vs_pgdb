import urllib.parse
from sqlalchemy import create_engine
import pandas as pd

from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)
# path = 'C:\\project\\feb24 _sample_file\\staging_tble_to_csv\\'
# df_data = pd.read_sql_table(table_name_staging, con=staging)


# df_data.to_csv(path+table_name_staging+'.csv')

