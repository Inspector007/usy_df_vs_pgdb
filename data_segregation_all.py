import json
import logging
import os
import time
import shutil
import urllib.parse
import glob
import zipfile
import re
import uuid
from uuid import UUID
from datetime import datetime
from logging.config import fileConfig
from pathlib import Path
from azure.storage.blob import BlockBlobService

import pandas as pd
from sqlalchemy import create_engine
from zipfile import ZipFile

fileConfig('logging.ini')
log = logging.getLogger('data_segregation')


class ConfigManager(object):
    def __init__(self, path_to_config="config.json"):
        if isinstance(path_to_config, str):
            if os.path.exists(path_to_config):
                self.config_path = path_to_config
                log.info(f" Config path - {self.config_path}")
            else:
                log.error(f"Path not found - {path_to_config}")
                return
        else:
            log.error(f"Config path must be string.")
            return
        self.json_config = None
        self.json_config = self.read_config()
        self.chunk_size = 10**6

    def read_config(self):
        try:
            with open(self.config_path, "r") as json_file:
                json_data = json.load(json_file)
                log.info(f"config - {json_data}")
                return json_data
        except Exception as e:
            log.exception(f"{e}")
            return None

    def get_chunk_size(self):
        if self.json_config is not None:
            return self.json_config.get("chunk_size", self.chunk_size)

    def get_source_dir(self):
        if self.json_config is not None:
            return self.json_config.get("source_dir", None)

    def get_target_dir(self):
        if self.json_config is not None:
            return self.json_config.get("target_dir", None)

    def get_path_to_replace(self):
        if self.json_config is not None:
            return self.json_config.get("local_path_str_to_replace", "")

    def get_blob_account_name(self):
        if self.json_config is not None:
            return self.json_config.get("blob_account_name", None)

    def get_blob_account_key(self):
        if self.json_config is not None:
            return self.json_config.get("blob_account_key", None)

    def get_blob_sas_token(self):
        if self.json_config is not None:
            # return urllib.parse.quote_plus(self.json_config.get("blob_sas_token", None))
            return self.json_config.get("blob_sas_token", None)

    def get_blob_container_name(self):
        if self.json_config is not None:
            return self.json_config.get("blob_container_name", None)


def get_staging_db_connection():
    try:
        from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
        from utils import *
        

        config_data = read_config_file(CONFIGS_PATH)
        active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

        connection, cursor = data_source_connection(active_db)
        #cur = connection.cursor()
        #cur.callproc()
        return connection
    except Exception as e:
        log.critical(f"{e}")


def list_all_files(dir_path):
    """
    This method will list all CSV,XLS and XLSX file recursively in a given directory
    :param dir_path: Directory to start the search.
    :return: list of all CSV/XLS/XLSX files
    """
    file_list = list()
    try:
        for folder, subfolders, filenames in os.walk(dir_path):
            if len(filenames) > 0:
                for filename in filenames:
                    if filename.lower()[-4:] in [".csv", ".xls", "xlsx"]:
                        file_list.append(os.path.join(folder, filename))
    except Exception as e:
        log.exception(f"{e}")
        return None

    return file_list


def get_all_files(source_dir):
    return list_all_files(source_dir)


def create_dirs(parent_path, cndb_id, file_type):
    try:
        if (parent_path is not None) and (cndb_id is not None) and (file_type is not None):
            path_to_create = os.path.join(str(parent_path), str(cndb_id), str(file_type))
            Path(path_to_create).mkdir(parents=True, exist_ok=True)
            log.info(f"Path created - {path_to_create}")
            return path_to_create
        else:
            log.error(f"Error in creating directories")
            return None
    except Exception as e:
        log.exception(f"{e}")
        return None


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
            return None
    else:
        log.error(f"Filename - {filename} must be a string.")
        return None


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
            return None
    else:
        log.error(f"Filename - {filename} must be a string.")
        return None


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
            return None
    else:
        log.error(f"file_type - {file_type} must be a string.")
        return None


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
    try:
        if config is not None:
            return int(config["chunk_size"])
        else:
            return 10 ** 6
    except Exception as e:
        log.exception(f"{e}")
        return 10 ** 6


def do_clean_dir(dir_path):
    shutil.rmtree(dir_path, ignore_errors=True)
    return True


def process_file(filepath, target_dir):
    try:
        filename = os.path.basename(filepath)
        file_type = get_file_type(filename)
        cndb_id = ""
        if file_type is not None:
            cndb_id = get_cndb_id_column(file_type)
        file_tags = get_file_tags(filename)
        root = target_dir
        log.info(f"filename - {filename}")

        if (file_type is None) or (cndb_id is None) or (file_tags is None):
            return False

        chunk_size = get_chunk_size()
        log.info(f"file_type - {file_type}")
        log.info(f"cndb_id - {cndb_id}")
        log.info(f"file_tags - {file_tags}")
        log.info(f"root - {root}")
        log.info(f"chunk_size - {chunk_size}")

        file_extension = filepath[-4:].lower()

        if file_extension == ".csv":
            with pd.read_csv(filepath, chunksize=chunk_size) as reader:
                for df in reader:
                    # df.drop(columns=['Unnamed: 0'], inplace=True)
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
            return False
    except Exception as e:
        log.exception(f"{e}")
        return False
    return True


def do_zip_multiple_csv(dir_path):
    try:
        for folder, subfolders, filenames in os.walk(dir_path):
            if len(filenames) > 1:
                with ZipFile(os.path.join(folder, os.path.basename(folder) + ".zip"), 'w') as zip_obj:
                    for filename in filenames:
                        if filename.lower()[-3:] == "csv":
                            file_path = os.path.join(folder, filename)
                            zip_obj.write(file_path, os.path.basename(file_path))
                            os.remove(file_path)
    except Exception as e:
        log.exception(f"{e}")
        return False
    return True


def upload_to_blob_storage(account_name, container_name, target_dir, path_to_replace, account_key=None, sas_token=None):
    try:
        from azure.storage.blob import BlockBlobService
        if account_name is not None:
                if (account_key not in ["",None]) or (sas_token is not None):
                    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key, sas_token=sas_token)
                    for root, dirs, files in os.walk(target_dir):
                        if files:
                            for file in files:
                                file_path_on_azure = os.path.join(root, file).replace(path_to_replace, "")
                                file_path_on_local = os.path.join(root, file)
                                block_blob_service.create_blob_from_path(container_name, file_path_on_azure, file_path_on_local)
                    log.info(f"Files have been uploaded to blob")
    except Exception as e:
        log.exception(f"{e}")


def get_file_source_name(blob_file_name):
    if not isinstance(blob_file_name, str):
        log.error(f"blob_file_name should be string!")
        return None
    file_name = blob_file_name.upper()
    if file_name.find("CEDP") != -1:
        return "CEDP"
    if file_name.find("MLDB") != -1:
        return "MLDB"
    if file_name.find("CNDB") != -1:
        return "CNDB"
    if file_name.find("SWCM") != -1:
        return "SWCM"
    return ""


def update_blob_url_in_db(account_name, container_name, account_key=None, sas_token=None):
    def uuid_convert(o):
        if isinstance(o, UUID):
            return o.hex
    try:
        if account_name is not None:
            if (account_key not in ["", None]) or (sas_token is not None):
                block_blob_service = BlockBlobService(account_name=account_name, sas_token=sas_token)
                print("Success")
                blob_list = block_blob_service.list_blobs(container_name=container_name)
                conn = get_staging_db_connection()
                #print(f"len of blob_list - {len(blob_list)}")
                for blob in blob_list:
                    file_name = os.path.basename(blob.name)
                    file_path = blob.name
                    file_size = blob.properties.content_length
                    print(file_name)
                    fname, fext = os.path.splitext(file_name)
                    print(fext)
                    file_type = fext[1:]
                    file_url = block_blob_service.make_blob_url(container_name, blob.name)
                    account_number = 0
                    if file_type == "zip":
                        pass

                    ac_no_grp = re.findall(r"(_)([0-9]+)(_)", file_name)
                    if len(ac_no_grp)>1:
                        account_number = ac_no_grp[0][1]
                    tenantid = uuid.uuid4()
                    source_name = get_file_source_name(file_name)

                    param = dict()
                    file_tag = dict()
                    file_tag["FileName"] = file_name
                    file_tag["FilePath"] = file_url
                    file_tag["FileSize"] = file_size
                    file_tag["FileType"] = file_type
                    file_tag["FileURL"] = file_url
                    param["UpdatedBy"] = "AtomBridge"
                    param["AccountNumber"] = int(account_number)
                    param["TenantId"] = tenantid
                    param["SourceName"] = source_name
                    param["FileTags"] = file_tag
                    print(param)
                    final_param = json.dumps(param, default=uuid_convert)
                    print(final_param)
                    proc_name = "func_bridge_update_raw_data_downloads"
                    cursor = conn.cursor()
                    result = cursor.execute(f"SELECT func_bridge_update_raw_data_downloads('{final_param}');").fetchall()
                    print(result)
                    #cursor.callproc(proc_name, final_param)
                    #results = list(cursor.fetchall())
                    #print(results)
                    cursor.close()

                conn.commit()
    except Exception as e:
        log.error(f"{e}")


def main():
    try:
        config_manager = ConfigManager("config.json")
        source_dir = config_manager.get_source_dir()
        blob_account_name = config_manager.get_blob_account_name()
        blob_account_key = config_manager.get_blob_account_key()
        blob_sas_token = config_manager.get_blob_sas_token()
        blob_container_name = config_manager.get_blob_container_name()
        local_path_str_to_replace = config_manager.get_path_to_replace()
        print(blob_account_key)
        print(blob_account_name)
        print(blob_sas_token)
        file_list = get_all_files(source_dir)

        if (file_list is None) or (len(file_list) == 0):
            log.error(f"Unable to get the file list!")
            return

        target_dir = config_manager.get_target_dir()

        ret = do_clean_dir(root)
        if ret is False:
            log.warning(f"Unable to clean the root directory before operation.")
        for f in file_list:
            log.info(f"File for processing - {f}")
            ret = process_file(f, target_dir)
            if ret is False:
                log.error(f"{f} has not been processed! Logic yet to be implemented.")
        ret = do_zip_multiple_csv(target_dir)
        if ret is False:
            log.warning(f"Unable to zip directory with multiple CSVs.")

        upload_to_blob_storage(blob_account_name, blob_container_name, target_dir, local_path_str_to_replace, account_key=blob_account_key, sas_token=blob_sas_token)
        update_blob_url_in_db(blob_account_name, blob_container_name, account_key=blob_account_key, sas_token= blob_sas_token)

    except Exception as e:
        log.exception(f"{e}")


if __name__ == "__main__":
    start_time = time.time()
    log.info(f"main execution starting")
    main()
    log.info(f"main execution ended")
    end_time = time.time()
    log.info(f"Total time taken(seconds) in execution - {end_time - start_time} ")
