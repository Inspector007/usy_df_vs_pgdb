import json
import logging
import os
import time
import shutil
import urllib.parse
import glob
import zipfile
from datetime import datetime
from logging.config import fileConfig
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

fileConfig('logging.ini')
log = logging.getLogger('data_segregation')

do_clean_cedp_path = False
do_clean_swcm_path = False


# do_clean_mldb_path = False
# do_clean_cndb_path = False


def get_staging_db_connection():
    try:
        from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
        from utils import *
        """
        """

        config_data = read_config_file(CONFIGS_PATH)
        active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

        connection, cursor = data_source_connection(active_db)
        return connection
    except Exception as e:
        log.critical(f"{e}")


def get_parent_path():
    return f"C:\Projects\Test"


def get_all_files():
    # TODO - download file from box then make file list
    AIC_HW_INV_path = f'C:\Projects\Finalized Sample Data\Discovery Data\SCAN_DATA_AIC_HW_INV_20210222.csv'
    AIC_ISO_PACKAGE_path = f'C:\Projects\Finalized Sample Data\Discovery Data\SCAN_DATA_AIC_ISO_PACKAGE_20210222.csv'
    AIC_ISO_path = f'C:\Projects\Finalized Sample Data\Discovery Data\SCAN_DATA_AIC_ISO_TAG_20210222.csv'
    AIC_SW_INV_path = f'C:\Projects\Finalized Sample Data\Discovery Data\SCAN_DATA_AIC_SW_INV_20210222.csv'
    AIC_path = f'C:\Projects\Finalized Sample Data\Discovery Data\SCAN_DATA_AIC_20210222.csv'
    CNDB_path = f"C:\Projects\Finalized Sample Data\CNDB_ACCOUNT_Records_20210303.xlsx"
    MLDB_path = f"C:\Projects\Finalized Sample Data\HW_Baseline_MLDB_20220303.xlsx"
    SWCM_path = f"C:\Projects\Finalized Sample Data\SwCM_Entitlement_20210223.xlsx"

    file_list = list()
    file_list.append(AIC_HW_INV_path)
    file_list.append(AIC_ISO_PACKAGE_path)
    file_list.append(AIC_ISO_path)
    file_list.append(AIC_SW_INV_path)
    file_list.append(AIC_path)
    file_list.append(CNDB_path)
    file_list.append(MLDB_path)
    file_list.append(SWCM_path)

    return file_list


def create_dirs(parent_path, cndb_id, file_type):
    try:
        global do_clean_cedp_path
        global do_clean_swcm_path

        if (parent_path != None) and (cndb_id != None) and (file_type != None):
            path_to_create = os.path.join(str(parent_path), str(cndb_id), str(file_type))
            """
            if (file_type == "CEDP") and (do_clean_cedp_path == False):
                # shutil.rmtree(path_to_create, ignore_errors=True)
                print(do_clean_cedp_path)
                files = glob.glob(f'{path_to_create}/*')
                for f in files:
                    os.remove(f)
                do_clean_cedp_path = True
            elif (file_type == "SWCM") and (do_clean_swcm_path == False):
                files = glob.glob(f'{path_to_create}/*')
                for f in files:
                    os.remove(f)
                do_clean_swcm_path = True
            else:
                shutil.rmtree(path_to_create, ignore_errors=True)
            """
            Path(path_to_create).mkdir(parents=True, exist_ok=True)
            log.info(f"Path created - {path_to_create}")
            return path_to_create
        else:
            log.error(f"Error in creating directories")
    except Exception as e:
        log.exception(f"{e}")


def get_file_type(filename):
    if isinstance(filename, str):
        filename = filename.upper()
        if filename.startswith("SCAN_DATA_AIC"):
            return "CEDP"
        elif filename.startswith("HW_BASELINE_MLDB"):
            return "MLDB"
        elif filename.startswith("CNDB_ACCOUNT"):
            return "CNDB"
        elif filename.startswith("SWCM_ENTITLEMENT"):
            return "SWCM"
        else:
            log.error(f"Not a valid filename for CEDP/MLDB/CNDB/SWCM - {filename}")
    else:
        log.error(f"Filename - {filename} must be a string.")


def get_file_tags(filename):
    if isinstance(filename, str):
        filename = filename.upper()
        if filename.startswith("SCAN_DATA_AIC"):
            if filename.find("AIC_HW_INV") != -1:
                return "AIC_HW_INV"
            elif filename.find("AIC_ISO_PACKAGE") != -1:
                return "AIC_ISO_PACKAGE"
            elif filename.find("AIC_ISO_TAG") != -1:
                return "AIC_ISO_TAG"
            elif filename.find("AIC_SW_INV") != -1:
                return "AIC_SW_INV"
            else:
                return "AIC"
        elif filename.startswith("HW_BASELINE_MLDB"):
            return "HW_BASELINE_MLDB"
        elif filename.startswith("CNDB_ACCOUNT"):
            return "CNDB_ACCOUNT"
        elif filename.startswith("SWCM_ENTITLEMENT"):
            return "SWCM_ENTITLEMENT"
        else:
            log.error(f"Not a valid filename for CEDP/MLDB/CNDB/SWCM - {filename}")
    else:
        log.error(f"Filename - {filename} must be a string.")


def get_cndb_id_column(file_type):
    if isinstance(file_type, str):
        file_type = file_type.upper()
        if file_type == "CEDP":
            return "CNDB_ID"
        elif file_type == "MLDB":
            return "CNDB_ID"
        elif file_type == "CNDB":
            return "ACCOUNT_NUMBER"
        elif file_type == "SWCM":
            return "CUS_NUMBER"
        else:
            log.error(f"file_type is not in [CEDP,MLDB,CNDB,SWCM].")
   else:
        log.error(f"file_type - {file_type} must be a string.")


def get_config():
    config_path = "config.json"
    try:
        with open(config_path, "r") as json_file:
            json_data = json.load(json_file)
            log.info(f"config - {json_data}")
            return json_data
    except Exception as e:
        log.exception(f"{e}")
        return None


def get_chunk_size():
    config = get_config()
    if config is not None:
        return int(config["chunk_size"])
    else:
        return 10 ** 6


def do_clean_parent(parent_path):
    shutil.rmtree(parent_path, ignore_errors=True)
    return True

def process_file(filepath):
    try:
        filename = os.path.basename(filepath)
        file_type = get_file_type(filename)
        cndb_id = get_cndb_id_column(file_type)
        file_tags = get_file_tags(filename)
        root = get_parent_path()
        chunk_size = get_chunk_size()
        log.info(f"filename - {filename}")
        log.info(f"file_type - {file_type}")
        log.info(f"cndb_id - {cndb_id}")
        log.info(f"file_tags - {file_tags}")
        log.info(f"root - {root}")
        log.info(f"chunk_size - {chunk_size}")

        file_extension = filepath[-4:].lower()

        if file_extension == ".csv":
            with pd.read_csv(filepath, chunksize=chunk_size) as reader:
                for df in reader:
                    df.drop(columns=['Unnamed: 0'], inplace=True)
                    distinct_cndb_ids = df[cndb_id].unique().tolist()
                    for id in distinct_cndb_ids:
                        pth = create_dirs(root, id, file_type)
                        df_filter = df.query(f" {cndb_id} == {id}")
                        to_csv_file = os.path.join(pth, f"{file_tags}_{id}_{datetime.now().date()}.csv")
                        if not os.path.isfile(f'{to_csv_file}'):
                            df_filter.to_csv(to_csv_file, index=False)
                        else:
                            df_filter.to_csv(to_csv_file, mode='a', index=False, header=False)
                        log.info(f"File has been written - {to_csv_file}")

        elif (file_extension == ".xls") or (file_extension == "xlsx"):
            df = pd.read_excel(filepath)
            distinct_cndb_ids = df[cndb_id].unique().tolist()
            for id in distinct_cndb_ids:
                pth = create_dirs(root, id, file_type)
                df_filter = df.query(f" {cndb_id} == {id}")
                to_csv_file = os.path.join(pth, f"{file_tags}_{id}_{datetime.now().date()}.csv")
                if not os.path.isfile(f'{to_csv_file}'):
                    df_filter.to_csv(to_csv_file, index=False)
                else:
                    df_filter.to_csv(to_csv_file, mode='a', index=False, header=False)
                log.info(f"File has been written - {to_csv_file}")
        else:
            log.error(f"File format not supported - {filename}")
    except Exception as e:
        log.exception(f"{e}")


def do_zip(path):
    #rootlen = len(path) + 1
    # not working as expected - TODO
    for base, dirs, files in os.walk(path):
        if len(files) > 1:
            zipobj = zipfile.ZipFile(os.path.basename(base) + '.zip', 'w', zipfile.ZIP_DEFLATED)
            for file in files:
                fn = os.path.join(base, file)
                zipobj.write(fn)


def main():
    try:
        file_list = get_all_files()
        root = get_parent_path()
        do_clean_parent(root)
        for f in file_list:
            log.info(f"File for processing - {f}")
            process_file(f)
        #do_zip(root)
    except Exception as e:
        log.exception(f"{e}")


if __name__ == "__main__":
    start_time = time.time()
    log.info(f"main execution starting")
    main()
    log.info(f"main execution ended")
    end_time = time.time()
    log.info(f"Total time taken(seconds) in execution - {end_time - start_time} ")
