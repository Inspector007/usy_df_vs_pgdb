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
csv_file = f'C:/project/feb24 _sample_file/staging_load_box_data/13-04-2021/DSW_ENTITLEMENT_DATA_20210408_1.csv/DSW_ENTITLEMENT_DATA_20210408_1.csv'
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
                    "ACCOUNT_NUMBER": "AccountNumber",
                "UNIQUE_ID": "UniqueId",
                "SAP_SALES_ORG_CODE": "SapSalesOrgCode",
                "CNTRY_CODE": "CntryCode",
                "SOLD_TO_CUST_NUM": "SoldToCustNum",
                "Master File Comment": "MasterFileComment",
                "SAP_SALES_ORD_NUM": "SapSalesOrdNum",
                "LINE_ITEM_SEQ_NUM": "LineItemSeqNum",
                "SAP_MATL_GRP_5_COND_CODE": "SapMatlGrp5CondCode",
                "PART_NUM": "PartNum",
                "PART_DSCR_FULL": "PartDscrFull",
                "SAP_MATL_TYPE_CODE": "SapMatlTypeCode",
                "SAP_MATL_TYPE_CODE_DSCR": "SapMatlTypeCodeDscr",
                "PART_QTY": "PartQty",
                "CU_UOM_CODE": "CuUomCode",
                "CU_DSCR": "CuDscr",
                "START_DATE": "StartDate",
                "END_DATE": "EndDate",
                "CONFIGRTN_ID": "ConfigrtnId",
                "QUOTE_NUM": "QuoteNum",
                "QUOTE_LINE_ITEM_SEQ_NUM": "QuoteLineItemSeqNum",
                "BILL_TO_PURCH_ORD_NUM": "BillToPurchOrdNum",
                "RSEL_ORD_DATE": "RselOrdDate",
                "ORDG_METHOD_CODE": "OrdgMethodCode",
                "ORDG_METHOD_CODE_DSCR": "OrdgMethodCodeDscr",
                "SOLD_TO_SAP_CNT_ID": "SoldToSapCntId",
                "PAYER_CUST_NUM": "PayerCustNum",
                "PAYER_SAP_CNT_ID": "PayerSapCntId",
                "BILL_TO_CUST_NUM": "BillToCustNum",
                "RSEL_CUST_NUM": "RselCustNum",
                "SHIP_TO_CUST_NUM": "ShipToCustNum",
                "SAP_CTRCT_NUM": "SapCtrctNum",
                "CMR_CUST_NUM": "CmrCustNum",
                "CMR_SYS_LOC_CODE": "CmrSysLocCode",
                     }
df_original.rename(columns=dict_cols_mapping, inplace=True)
ls_append_rows = []
for i in range(df_original.shape[0]):
    ls_append_rows.append(
        [str(datetime.now().date()), 'Atom_Bridge', '{"1TF": "Inprocess"}', 'DSW'])
df_staging_append = pd.DataFrame(ls_append_rows, columns=ls_cols_staging)
print("-----df_staging_append.shape---", df_staging_append.shape)

# df_load_staging = pd.concat([df_staging_append,df_original],axis=1, join="inner")
df_load_staging = pd.concat([df_staging_append, df_original], axis=1)
cols_final = [
                "UpdatedOn",
                "UpdatedBy",
                "Source",
                "StagingTags",
                "AccountNumber",
                "UniqueId",
                "SapSalesOrgCode",
                "CntryCode",
                "SoldToCustNum",
                "MasterFileComment",
                "SapSalesOrdNum",
                "LineItemSeqNum",
                "SapMatlGrp5CondCode",
                "PartNum",
                "PartDscrFull",
                "SapMatlTypeCode",
                "SapMatlTypeCodeDscr",
                "PartQty",
                "CuUomCode",
                "CuDscr",
                "StartDate",
                "EndDate",
                "ConfigrtnId",
                "QuoteNum",
                "QuoteLineItemSeqNum",
                "BillToPurchOrdNum",
                "RselOrdDate",
                "OrdgMethodCode",
                "OrdgMethodCodeDscr",
                "SoldToSapCntId",
                "PayerCustNum",
                "PayerSapCntId",
                "BillToCustNum",
                "RselCustNum",
                "ShipToCustNum",
                "SapCtrctNum",
                "CmrCustNum",
                "CmrSysLocCode"]
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
sql = "COPY tbl_ibm_entitlements_dsw_d FROM STDIN DELIMITER \',\' CSV header;"
cursor.copy_expert(sql, open(df_load_staging_csv_path))
connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert", seconds_copy_expert)
cursor.close()


