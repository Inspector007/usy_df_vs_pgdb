import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
import time
from datetime import datetime


from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)

#HW_Baseline_MLDB_20220303.xlsx #TODO CHange to xlsx and code again with 
# csv_file = '/home/vcenteruser/ibm_new/sprint3/a_csv/SCAN_DATA_VM_20210325_1.CSV'
# csv_file = f'C:/project/feb24 _sample_file/SCAN_DATA_VM_20210325_1.CSV'
csv_file = f'C:/project/feb24 _sample_file/staging_load_box_data/13-04-2021/SCAN_DATA_VM_20210407_1.CSV/SCAN_DATA_VM_20210407_1.CSV'

# csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/b_csv/DF_vmware_load_staging_'+str(datetime.now().date())+'_.csv'
csv_load_staging = f'C:/project/Server_data_download/server_small_df/DF_vmware_load_staging_'+str(datetime.now().date())+'_.csv'

start_time_copy_expert = time.time()
df_original = pd.read_csv(csv_file)
#df_original.drop(df_original.columns[0], axis=1,inplace=True) # DROP COL == SR NO 
print("----df_original.shape------",df_original.shape)
#print(df_original.head(2))
ls_cols_staging = ['UpdatedOn', 'UpdatedBy', 'Source', 'StagingTags']
# cols_original = []

cols_to_drop = ['CNDB_ID','SCRIPT_TIMESTAMP','VMS_SCRIPT_NAME']

# MACHINE_TYPE -- Not to go to PRODUCTION - Only till STAGING - Viresh Mapping XLSX
# HW_SERIAL_NUMBER - Not to go to PRODUCTION - Only till STAGING - Viresh Mapping XLSX
# CAMPUS_ID - Not to go to PRODUCTION - Only till STAGING - Viresh Mapping XLSX

# df_original.drop(columns=cols_to_drop, axis=1,inplace=True)
## Dropped -- 18MARCH -- AccountNumber ... replaced with == CNDBId
## below MAPPING DICT -- dict_cols_mapping -- follows ORDER of the CSV Columns 
dict_cols_mapping = {
				"CNDB_ID": "CndbId",
                "VMS_CLUDRSAUTOLVL": "CluDrsAutoLvl",
                "VMS_CLUDRSENABLED": "CluDrsEnabled",
                "VMS_CLUHAENABLED": "CluHAEnabled",
                "VMS_CLUNAME": "CluName",
                "VMS_GTIMESTAMP": "GTimeStamp",
                "VMS_HSCPUCORES": "HsCPUCores",
                "VMS_HSCPUHYPERTHREADING": "HsCPUHyperThreading",
                "VMS_HSCPUMODEL": "HsCPUModel",
                "VMS_HSCPUSOCKETS": "HsCPUSockets",
                "VMS_HSCPUTHREADS": "HsCPUThreads",
                "VMS_HSHWTYPE": "HsHWType",
                "VMS_HSMANUFACTURER": "HsManufacturer",
                "VMS_HSMEMORYSIZEGB": "HsMemorySizeGB",
                "VMS_HSNAME": "HsName",
                "VMS_HSOS": "HsOS",
                "VMS_HSOSBLD": "HsOSbld",
                "VMS_HSOSVER": "HsOSver",
                "VMS_HSSERIAL": "HsSerial",
                "VMS_HSSTATUS": "HsStatus",
                "VMS_HSUNIQID": "HsUniqId",
                "VMS_VCENTERNAME": "VCenterName",
                "VMS_VMAUTOVMOTION": "VMAutoVMotion",
                "VMS_VMCANMIGRATE": "VmCanMigrate",
                "VMS_VMCOREPERSCKT": "VmCorePerSckt",
                "VMS_VMCPU": "VmCPU",
                "VMS_VMCPUSKTS": "VmCPUSkts",
                "VMS_VMDRSPROT": "VmDRSprot",
                "VMS_VMHAPROT": "VmHAprot",
                "VMS_VMMIGINF": "VmMigInf",
                "VMS_VMNAME": "VmName",
                "VMS_VMNAME2": "VmName2",
                "VMS_VMOS": "VmOS",
                "VMS_VMOSSOURCE": "VmOSSource",
                "VMS_VMPOWERSTATE": "VmPowerState",
                "VMS_VMSTATE": "VmState",
                "VMS_VMTOOLS": "VmTools",
                "VMS_VMTOOLSVER": "VmToolsVer",
                "VMS_VMUNIQID": "VmUniqId",
                "VMS_VMUPTIMEDAYS": "VmUptimeDays"
					}

df_original.rename(columns=dict_cols_mapping, inplace=True)
print(df_original.columns)
cols_db = []
ls_append_rows = []
for i in range(df_original.shape[0]):
	ls_append_rows.append([str(datetime.now()),'Atom_Bridge', 'VmWare Hw Inv', None])

df_staging_append = pd.DataFrame(ls_append_rows, columns = ls_cols_staging)
print("-----df_staging_append.shape--------",df_staging_append.shape)
print(df_staging_append.head(2))
#df_load_staging = pd.concat([df_staging_append,df_original],axis=1, join="inner")
df_load_staging = pd.concat([df_staging_append,df_original],axis=1)
cols_final = [
    "UpdatedOn",
    "UpdatedBy",
    "Source",
    "StagingTags",
    "CndbId",
    "VmName",
    "VmUniqId",
    "VmPowerState",
    "VmUptimeDays",
    "VmOS",
    "VmCPU",
    "VmCorePerSckt",
    "VmCPUSkts",
    "HsName",
    "HsUniqId",
    "HsStatus",
    "HsManufacturer",
    "HsHWType",
    "HsSerial",
    "HsOS",
    "HsOSver",
    "HsOSbld",
    "HsMemorySizeGB",
    "CluName",
    "CluHAEnabled",
    "VmHAprot",
    "CluDrsEnabled",
    "CluDrsAutoLvl",
    "VmDRSprot",
    "VmCanMigrate",
    "VmMigInf",
    "HsCPUModel",
    "HsCPUSockets",
    "HsCPUThreads",
    "HsCPUCores",
    "HsCPUHyperThreading",
    "GTimeStamp",
    "VmName2",
    "VmState",
    "VmTools",
    "VmToolsVer",
    "VmOSSource",
    "VCenterName",
    "VMAutoVMotion"  
]

#TODO -- cols_final - needs to come from CONFIG.ini
## Dropped -- 18MARCH -- AccountNumber ... replaced with == CNDBId
df_load_staging = df_load_staging.reindex(columns=cols_final) 
# df_load_staging.columns=cols_final
df_load_staging.to_csv(csv_load_staging,index=False)
print("----df_load_staging.shape----------",df_load_staging.shape)
print(df_load_staging.head(2))

# df_load_staging_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/DF_mldb_load_staging_'+str(datetime.now().date())+'_.csv'
# df_load_staging.to_csv(df_load_staging_csv_path,index=False)

# tbl_bridge_staging = 'tbl_bridge_ibm_vmware_hw_inventory'
tbl_bridge_staging = 'bridge.tbl_ibm_vmware_hw_inventory'
sql = "COPY " + tbl_bridge_staging + " FROM STDIN DELIMITER \',\' CSV header;"
cursor.copy_expert(sql,open(csv_load_staging))
connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()
