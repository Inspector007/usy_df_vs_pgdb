import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
import time

from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)
#HW_Baseline_MLDB_20220303.xlsx #TODO CHange to xlsx and code again with 
# csv_file = '/home/vcenteruser/ibm_new/sprint3/a_csv/SCAN_DATA_AIC_HW_INV_20210222.csv'
csv_file = f'C:/project/feb24 _sample_file/Finalized Sample Data/Discovery Data/SCAN_DATA_AIC_HW_INV_20210222.csv'

#pd.read_excel('CNDB_ACCOUNT_Records_20210303.xlsx')#, sheetname=0).to_csv('CNDB_ACCOUNT_Records_20210303.csv', index=False)
#xlrd.biffh.XLRDError: Excel xlsx file; not supported #  https://stackoverflow.com/questions/65254535/xlrd-biffh-xlrderror-excel-xlsx-file-not-supported

# csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/b_csv/Hw_inv_final.csv'
csv_load_staging = f'C:/project/feb24 _sample_file/Hw_inv_final.csv'

start_time_copy_expert = time.time()
df_original = pd.read_csv(csv_file)
print("----df_original.shape------",df_original.shape)
#print(df_original.head(2))

ls_cols_staging = ['UpdatedOn', 'UpdatedBy', ]



# val_ServerCores = {'SERVER_CORES': 0}
values = {'PVU_PER_CORE': 0,'SERVER_CORES': 0,'SERVER_ID':0,'SERVER_LOGICAL_CORES':0,'SERVER_NAME':'servername','IS_CAPPED':0,'IS_SHARED_TYPE':0,'ONLINE_VP_COUNT':0,'CLUSTER_CORES_COUNT':0}
df_original.fillna(value=values,inplace=True)
# df_original['PvuPerCore'] = df_original['PvuPerCore'].apply(lambda _: int(_))
df_original = df_original.astype({'PVU_PER_CORE':'int','SERVER_CORES': 'int','SERVER_ID':'int','SERVER_LOGICAL_CORES':'int','IS_CAPPED':'int','IS_SHARED_TYPE':'int','ONLINE_VP_COUNT':'int','CLUSTER_CORES_COUNT':'int'})


#'1234' == ClusterCoresCount

# HOSTNAME    COMPUTER_ID CNDB_ID CLUSTER_CORES_COUNT CLUSTER_NAME    COMPUTER_TYPE   DEFAULT_PVU_VALUE   NODE_TOTAL_PROCESSORS   PARTITION_CORES PROCESSOR_BRAND PROCESSOR_BRAND_STRING  PROCESSOR_MODEL PROCESSOR_TYPE  PROCESSOR_VENDOR    PVU_PER_CORE    SERVER_CORES    SERVER_ID   SERVER_LOGICAL_CORES    SERVER_NAME STATUS  SYSTEM_MODEL    ENTITLEMENT IS_CAPPED   IS_SHARED_TYPE  ONLINE_VP_COUNT SHARED_POOL_ID  SERVER_SERIAL_NUMBER    SERVER_TYPE SERVER_VENDOR   SERVER_MODEL    UPLOAD  SRC

#cols_original = ['SR_NO','CNDB_ID','CUSTOMER_NAME','MACHINE_TYPE','HW_SERIAL_NUMBER','HOSTNAME','CATEGORY','HARDWARE_STATE','SERVER_TYPE','INTERNET_FACING_HW','CAMPUS_ID','CITY','STATE','COUNTRY','ADDRESS_1',    'ADDRESS_2' ,'ADDRESS_3','ZIPCODE']

cols_to_drop = ['SR_NO',]
df_original.drop(columns=cols_to_drop, axis=1,inplace=True)
dict_cols_mapping = {
                    'HOSTNAME' :'HostName',    
                    'COMPUTER_ID' :'ComputerId',    
                    'CNDB_ID' :'CndbId',    
                    'CLUSTER_CORES_COUNT' :'ClusterCoresCount',    
                    'CLUSTER_NAME' :'ClusterName',    
                    'COMPUTER_TYPE' :'ComputerType',    
                    'DEFAULT_PVU_VALUE' :'DefaultPvuValue',    
                    'NODE_TOTAL_PROCESSORS' :'NodeTotalProcessors',    
                    'PARTITION_CORES' :'PartitionCores',    
                    'PROCESSOR_BRAND' :'ProcessorBrand',    
                    'PROCESSOR_BRAND_STRING' :'ProcessorBrandString',    
                    'PROCESSOR_MODEL' :'ProcessorModel',    
                    'PROCESSOR_TYPE' :'ProcessorType',    
                    'PROCESSOR_VENDOR' :'ProcessorVendor',    
                    'PVU_PER_CORE' :'PvuPerCore',    
                    'SERVER_CORES' :'ServerCores',    
                    'SERVER_ID' :'ServerId',    
                    'SERVER_LOGICAL_CORES' :'ServerLogicalCores',    
                    'SERVER_NAME' :'ServerName',    
                    'STATUS' :'Status',    
                    'SYSTEM_MODEL' :'SystemModel',    
                    'ENTITLEMENT' :'Entitlement',    
                    'IS_CAPPED' :'IsCapped',    
                    'IS_SHARED_TYPE' :'IsSharedType',    
                    'ONLINE_VP_COUNT' :'OnlineVpCount',    
                    'SHARED_POOL_ID' :'SharedPoolId',    
                    'SERVER_SERIAL_NUMBER' :'ServerSerialNumber',    
                    'SERVER_TYPE' :'ServerType',    
                    'SERVER_VENDOR' :'ServerVendor',    
                    'SERVER_MODEL' :'ServerModel',    
                    'UPLOAD' :'Upload',    
                    'SRC' :'Src'
                    }
df_original.rename(columns=dict_cols_mapping, inplace=True)
print(df_original.columns)
cols_db = []
ls_append_rows = []
for i in range(df_original.shape[0]):
    ls_append_rows.append(['2021-03-12','Atom_Bridge',]) # ClusterCoresCount

df_staging_append = pd.DataFrame(ls_append_rows, columns = ls_cols_staging)
print("-----df_staging_append.shape--------",df_staging_append.shape)
#print(df_staging_append.head(2))
#
#df_load_staging = pd.concat([df_staging_append,df_original],axis=1, join="inner")
df_load_staging = pd.concat([df_staging_append,df_original],axis=1)
cols_final = [
                'UpdatedOn',
                'UpdatedBy',
                'HostName',
                'ComputerId',
                'CndbId',
                'ClusterCoresCount',
                'ClusterName',
                'ComputerType',
                'DefaultPvuValue',
                'NodeTotalProcessors',
                'PartitionCores',
                'ProcessorBrand',
                'ProcessorBrandString',
                'ProcessorModel',
                'ProcessorType',
                'ProcessorVendor',
                'PvuPerCore',
                'ServerCores',
                'ServerId',
                'ServerLogicalCores',
                'ServerName',
                'Status',
                'SystemModel',
                'Entitlement',
                'IsCapped',
                'IsSharedType',
                'OnlineVpCount',
                'SharedPoolId',
                'ServerSerialNumber',
                'ServerType',
                'ServerVendor',
                'ServerModel',
                'Upload',
                'Src',
                'ServerSocket',
                'LastSuccessScan',
                'SharedPoolCore'
            ]
"""
CREATE TABLE bridge.tbl_ibm_cedp_hw_inv_staging
    "UpdatedOn","UpdatedBy","HostName","ComputerId","CndbId","ClusterCoresCount","ClusterName",
    "ComputerType","DefaultPvuValue","NodeTotalProcessors","PartitionCores","ProcessorBrand",
    "ProcessorBrandString","ProcessorModel","ProcessorType","ProcessorVendor","PvuPerCore",
    "ServerCores","ServerId","ServerLogicalCores","ServerName","Status","SystemModel",
    "Entitlement","IsCapped","IsSharedType","OnlineVpCount","SharedPoolId","ServerSerialNumber",
    "ServerType","ServerVendor","ServerModel","Upload","Src","ServerSocket","LastSuccessScan","SharedPoolCore"

"""
#TODO -- cols_final - needs to come from CONFIG.ini

df_load_staging = df_load_staging.reindex(columns=cols_final) # Satish Code - Reindex -  for mapping with the DB COLUMNS Ordering 
# df_load_staging.columns=cols_final
# df_load_staging = df_load_staging.astype({'PvuPerCore':'int'})

df_load_staging.to_csv(csv_load_staging,index=False)
print("----df_load_staging.shape----------",df_load_staging.shape)
print(df_load_staging.head(2))


# table_name = 'tbl_ibm_scan_data_aic_hw_ind_staging'
table_name = 'bridge.tbl_ibm_cedp_hw_inv_staging'

sql = "COPY " + table_name + " FROM STDIN DELIMITER \',\' CSV header;"
cursor.copy_expert(sql, open(csv_load_staging))

connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()


    


































