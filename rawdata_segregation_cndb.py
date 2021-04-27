import pandas as pd
import os
from pathlib import Path
import numpy as np

import urllib.parse
from sqlalchemy import create_engine
import time , uuid , json, re
from datetime import datetime

from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)
def get_parent_path():
    return f"C:/Projects/Test/"
    # return f"/home/vcenteruser/ibm_new/sprint3/a_csv/27_03_data/Test/"

def get_all_files():
    # CNDB_path = f"C:/project/feb24 _sample_file/CNDB_ACCOUNT_Records_20210303.xlsx"
    CNDB_path = f"/home/vcenteruser/ibm_new/sprint3/CNDB_ACCOUNT_Records_20210303.xlsx"
    # CNDB_path = f"C:/project/feb24 _sample_file/CNDB_ACCOUNT_Records_20210325.csv"
    file_list = list()
    file_list.append(CNDB_path)
    return file_list
    

def create_dirs(parent_path, cndb_id, file_type,file_tags):
    if (parent_path != None) and (cndb_id != None) and (file_type != None)  and (file_tags != None):
        path_to_create = os.path.join(str(parent_path),str(cndb_id),str(file_type),str(file_tags))
        Path(path_to_create).mkdir(parents=True, exist_ok=True)
        return path_to_create


def get_file_type(filename):
    if isinstance(filename, str):
        filename = filename.upper()
        if filename.startswith("CNDB_ACCOUNT"):
            return "CNDB"


def get_file_tags(filename):
    if isinstance(filename, str):
        filename = filename.upper()
        if filename.startswith("CNDB_ACCOUNT"):
            return "CNDB_ACCOUNT"


def get_cndb_id_column(file_type):
    if isinstance(file_type, str):
        file_type = file_type.upper()
        if file_type == "CNDB":
            return "ACCOUNT_NUMBER"


def convert_dtype_accnNum_cndb(acc_num):
    try:
        if re.match("^[0-9 -]+$",acc_num):
            int_acc_num = int(acc_num)
            if isinstance(int_acc_num,int):
                accnum_val = 1 # if INT
                return accnum_val
        else:
            accnum_val = 0 # if NOT_INT
            return accnum_val
    except Exception as err_to_numeric:
        pass
        # print("--convert_dtype_accnNum_mldb--Exception-err_to_numeric-{acc_num}",err_to_numeric)

def process_file(filepath):
    df_cndb = pd.DataFrame()
    list_add = []
    if filepath[-3:].lower() == "csv":
        df_cndb = pd.read_csv(filepath)
    else:
        df_cndb = pd.read_excel(filepath) #sheetname may be provided later.
    import pdb;pdb.set_trace()
    df_cndb['AccNumVald'] = df_cndb['ACCOUNT_NUMBER'].apply(lambda x: convert_dtype_accnNum_cndb(x))
    df_fail_accountNum = df_cndb.query('AccNumVald==0', engine='python')
    df = df_cndb[~df_cndb.index.isin(df_fail_accountNum.index)]
    df_fail_accountNum.to_csv('df_cndb_fail_accountNum.csv')
    df.to_csv('df_cndb_success.csv')
    filename = os.path.basename(filepath)
    file_type = get_file_type(filename)
    cndb_id = get_cndb_id_column(file_type)
    file_tags = get_file_tags(filename)
    root = get_parent_path()
    distinct_cndb_ids = df[cndb_id].unique().tolist()
    for id in distinct_cndb_ids:
        pth = create_dirs(root,id,file_type,file_tags)
        df_filter = df.query(f" {cndb_id} == {id}")
        file_path = os.path.join(pth,f"{file_tags}_{id}_{datetime.now().date()}.csv")
        df_filter.to_csv(file_path, index=False)
        df_read = pd.read_csv(file_path)
        list_add.append([datetime.now(),file_type,id,file_type,file_path,None,None,None,None,None,None,None,None,None]) 
    print(f'main df shape {df_cndb.shape}')
    print(f'fail df shape {df_fail_accountNum.shape}')
    print(f'pass df shape {df.shape}')
    add_data(list_add)

def add_data(list_add):
    table_name = 'tbl_bridge_ibm_segregated_paths_blob_storage'
    start_time_copy_expert = time.time()
    columns_1 = [
                    'UpdatedOn',
                    'UpdatedBy',
                    'CndbId',
                    'DataSource',
                    'Data_source_file_1_path_on_vm',
                    'Data_source_file_2_path_on_vm',
                    'Data_source_file_3_path_on_vm',
                    'Data_source_file_4_path_on_vm',
                    'Data_source_file_5_path_on_vm',
                    'Data_source_file_1_path_on_blob',
                    'Data_source_file_2_path_on_blob',
                    'Data_source_file_3_path_on_blob',
                    'Data_source_file_4_path_on_blob',
                    'Data_source_file_5_path_on_blob'
                 ]
    df_data = pd.DataFrame(np.array(list_add),
                       columns=columns_1)
    df_data.to_csv('tbl_bridge_ibm_segregated_paths_blob_storage.csv')
    df_data.to_sql(table_name, con=engine, if_exists='append', index=False)
    cursor.close()
    connection.commit()


def main():
    file_list = get_all_files()
    for f in file_list:
        print(f)
        process_file(f)

if __name__ == "__main__":
    print("started")
    main()
    print("end")
