import sys
import urllib.parse
from sqlalchemy import create_engine
import time, uuid, json
import pandas as pd
import numpy as np
from datetime import datetime
import re, json
from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)
#177792 -  LPAR detailed report 2021-02-11.xls
#177792_LPAR_detailed_report_2021-02-11.xls

# csv_file = '/home/vcenteruser/ibm_new/sprint3/a_csv/177792_LPAR_detailed_report_2021-02-11.csv'
# csv_file = f'C:/project/feature_box_poc_1_box_connector/177792_LPAR_detailed_report_2021-02-11.csv'
csv_file = f'C:/project/feb24 _sample_file/staging_load_box_data/13-04-2021/ESW_ENTITLEMENT_DATA_20210416_1.CSV/ESW_ENTITLEMENT_DATA_20210416_1.CSV'
df_original = pd.read_csv(csv_file)
print("----df_original.shape------", df_original.shape)
# csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/b_csv/TADZ_LPAR_PreProcess_' + str(datetime.now().date()) + '_.csv'
# csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/b_csv/TADZ_LPAR_PreProcess_' + str(datetime.now().date()) + '_.csv'
start_time_copy_expert = time.time()
ls_cols_staging = ['UpdatedOn', 'UpdatedBy','StagingTags','Source']
# IsShare -- Not in the CSV - Vireshwar has added this extra column
# cols_original = ['TSID','SID','SMFID','SYSPLEX','HW_TYPE','HW_MODEL','HW_PLANT','HW_NAME','LPAR_NAME','NODE_LAST_UPDATE','CPU_SN']
#cols_to_drop = ['SR_NO']
#df_original.drop(columns=cols_to_drop, axis=1, inplace=True)

# SELECT * FROM tbl_bridge_ibm_tadz_lpar_staging;
#  UpdatedOn | UpdatedBy | TsId | SId | SmfId | SysPlex | HwType | HwModel | HwPlant | HwName | LparName | NodeLastUpdate | CpuSn | HwInventoryTags | Source
# -----------+-----------+------+-----+-------+---------+--------+---------+---------+--------+----------+----------------+-------+-----------------+--------
# (0 rows)

dict_cols_mapping = {
                    "MARKET": "Market",
                    "COUNTRY_CODE": "CountryCode",
                    "GEOGRAPHY_NAME": "GeographyName",
                    "SAP_ORDER_NO": "SapOrderNo",
                    "ITEM_NO": "ItemNo",
                    "SWO": "Swo",
                    "SWO_DESCRIPTION": "SwoDescription",
                    "SAP_CUSTNO": "SapCustno",
                    "COUNTRY": "Country",
                    "SOLD_TO_CUSTNO": "SoldToCustno",
                    "SAP_BILLTO_CUSTNO": "SapBilltoCustno",
                    "CMR_BILLTO_CUSTNO": "CmrBilltoCustno",
                    "SAP_INST_CUSTNO": "SapInstCustno",
                    "CMR_INST_CUSTNO": "CmrInstCustno",
                    "SAP_SHIPTO_CUSTNO": "SapShiptoCustno",
                    "CMR_SHIPTO_CUSTNO": "CmrShiptoCustno",
                    "DESIG_MACHINE": "DesigMachine",
                    "PROD_DIV_CODE": "ProdDivCode",
                    "ORDER_CREATE_DATE": "OrderCreateDate",
                    "SERIAL_NO": "SerialNo",
                    "EQUIPMENT_NO": "EquipmentNo",
                    "IW_IBASE_STATUS": "IwIbaseStatus",
                    "VALUE_METRIC_DESCR": "ValueMetricDescr",
                    "LICENSE_TYPE": "LicenseType",
                    "ORDER_TYPE": "OrderType",
                    "CHARGE_OPTION": "ChargeOption",
                    "CHG_VALUE_INTEGER": "ChgValueInteger",
                    "LIC_EFF_DATE": "LicEffDate",
                    "REMOVAL_DATE": "RemovalDate",
                    "SWO_DESCR_UP": "SwoDescrUp",
                    "EE_DESCR_UP": "EeDescrUp",
                    "EEVM_DESCR_UP": "EevmDescrUp",
                    "CID_DESCR_UP": "CidDescrUp",
                    "CNDB_ID": "CndbId"
                     }
df_original.rename(columns=dict_cols_mapping, inplace=True)
ls_append_rows = []
for i in range(df_original.shape[0]):
    ls_append_rows.append([str(datetime.now().date()), 'Atom_Bridge', '{"1TF": "Inprocess"}', 'ESW'])
df_staging_append = pd.DataFrame(ls_append_rows, columns=ls_cols_staging)
print("-----df_staging_append.shape---", df_staging_append.shape)

# df_load_staging = pd.concat([df_staging_append,df_original],axis=1, join="inner")
df_load_staging = pd.concat([df_staging_append, df_original], axis=1)
cols_final = ["UpdatedOn",
                "UpdatedBy",
                "Source",
                "StagingTags",
                "Market",
                "CountryCode",
                "GeographyName",
                "SapOrderNo",
                "ItemNo",
                "Swo",
                "SwoDescription",
                "SapCustno",
                "Country",
                "SoldToCustno",
                "SapBilltoCustno",
                "CmrBilltoCustno",
                "SapInstCustno",
                "CmrInstCustno",
                "SapShiptoCustno",
                "CmrShiptoCustno",
                "DesigMachine",
                "ProdDivCode",
                "OrderCreateDate",
                "SerialNo",
                "EquipmentNo",
                "IwIbaseStatus",
                "ValueMetricDescr",
                "LicenseType",
                "OrderType",
                "ChargeOption",
                "ChgValueInteger",
                "LicEffDate",
                "RemovalDate",
                "SwoDescrUp",
                "EeDescrUp",
                "EevmDescrUp",
                "CidDescrUp",
                "CndbId"]
# TODO -- cols_final - needs to come from CONFIG.ini
## Dropped -- 18MARCH -- AccountNumber ... replaced with == CNDBId
df_load_staging = df_load_staging.reindex(columns=cols_final)
# df_load_staging.to_csv(csv_load_staging,index=False)
print("----df_load_staging.shape-------QA TEAM DEMO --- 30MARCH-", df_load_staging.shape)

# df_load_staging_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/DF_tadz_load_staging_' + str(
#     datetime.now().date()) + '_.csv'
df_load_staging_csv_path = f'C:/project/feature_box_poc_1_box_connector/DF_tadz_load_staging_' + str(
    datetime.now().date()) + '_.csv'

df_load_staging.to_csv(df_load_staging_csv_path, index=False)
sql = "COPY tbl_ibm_entitlements_esw_d FROM STDIN DELIMITER \',\' CSV header;"
cursor.copy_expert(sql, open(df_load_staging_csv_path))
connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert", seconds_copy_expert)
cursor.close()


