import urllib.parse
from sqlalchemy import create_engine
import time , uuid , json
import pandas as pd

"""tbl_ibm_mldb_staging Columns -->
['UpdatedOn', 'UpdatedBy', 'Source', 'Category', 'HardwareState', 'AccountNumber', 
'CustomerName', 'CampusId', 'State','Address1', 'Address2',
'Address3','City', 'ZipCode','InternetFacingHw', 'HostName', 'ServerType', 'MldbTags']
"""
db_user_staging = urllib.parse.quote_plus("devuser1@usedadvsampql01")
db_pass_staging = urllib.parse.quote_plus("Devuser1@3")
engine_staging = create_engine(f'postgresql+psycopg2://{db_user_staging}:{db_pass_staging}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )
table_name_staging = 'tbl_ibm_mldb_staging'
tbl_name_cndb = 'tbl_ibm_cndb_staging'
tbl_name_mldb = 'tbl_ibm_mldb_staging'
tbl_fail = 'tbl_ibm_source_data_failures_staging'

"""tbl_it_asset_baseline Columns -->
['Id', 'UpdatedOn', 'UpdatedBy', 'IsDeleted', 'TenantId', 'HostName', 'MacAddress', 'BoTInstalled', 
'ITTags', 'BoTCommDate', 'AdhocScan', 'AssetStatus', 'AssetLocation', 'AccountNumber', 
'AccountName', 'SupportingTags']
"""
db_user_production = urllib.parse.quote_plus("postgres")
db_pass_production = urllib.parse.quote_plus("portaluser@8877")
engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@atomclient.westcentralus.cloudapp.azure.com:5432/ibm_atomdeployment_db?sslmode=', client_encoding='utf8' )
engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@atomclient.westcentralus.cloudapp.azure.com:5432/ey_atombase_db?sslmode=', client_encoding='utf8' )

table_name_production = 'tbl_it_asset_baseline'

start_time_copy_expert = time.time()
df_cndb = pd.read_sql_table(tbl_name_cndb, con=engine_staging)
df_mldb = pd.read_sql_table(tbl_name_mldb, con=engine_staging)

def validations_check(df_cndb):
    df_accnt_num = len(df_cndb['AccountNumber'])
    df_dtypes = df_cndb.dtypes#get_dtype_counts() #TODO - Validation MetaData_Table_Reporting
    sr_dtype = df_cndb['AccountNumber'].dtypes #TODO - Validation MetaData_Table_Reporting

import re 
def convert_dtype_accnNum_cndb(acc_num):
    try:
        if re.match("^[0-9 -]+$", acc_num):
            int_acc_num = int(acc_num)
            if isinstance(int_acc_num,int):
                print("-convert_dtype_accnNum_cndb--- NUMERIC_ACCNUM [CNDB_ID] in CNDB_StagingTable =",int_acc_num)
                accnum_val = 1 # if INT 
                return accnum_val
        else:
            print("-convert_dtype_accnNum_cndb---NON_NUMERIC_ACCNUM [CNDB_ID] in CNDB_StagingTable =",acc_num)
            accnum_val = 0 # if NOT_INT 
            return accnum_val
    except Exception as err_to_numeric:
        print("--convert_dtype_accnNum_cndb--Exception-err_to_numeric-",err_to_numeric)

def wrap_check_accnNum_cndb(df_cndb):
    df_cndb['AccNumVald'] = df_cndb['AccountNumber'].apply(lambda x: convert_dtype_accnNum_cndb(x)) 

def get_indexFailure_Row(df_cndb):
    fail_df = df_cndb.query('AccNumVald==0')
    print("----fail_df.shape----",fail_df.shape)
    fail_df.to_csv("fail_df.csv")
    df_cndb_1 = df_cndb.query('AccNumVald==1')
    print("----df_cndb_1.shape----",df_cndb_1.shape)
    df_cndb_1.to_csv("df_cndb_1.csv")

def cnbd_id_vald():
    """ cndb id """
    df_mldb['cndb_id_vald'] = (1).where(df_cndb['AccountNumber'].isin(df_mldb['AccountNumber']),other=0)  
    #TODO - need actual CNDBID + JSONB 

def check_empty():
    """ hostname """
    df_mldb['host_empty_vald'] = df_cndb['AccountNumber'].isnull()

#validations_check(df_cndb)
wrap_check_accnNum_cndb(df_cndb)
df_cndb.to_csv("df_cndb_Final.csv")
get_indexFailure_Row(df_cndb)
