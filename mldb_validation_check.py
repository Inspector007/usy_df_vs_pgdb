import urllib.parse
from sqlalchemy import create_engine
import time , uuid , json
import pandas as pd
import numpy as np
from datetime import datetime
import re , json


from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)


db_user_staging = urllib.parse.quote_plus("")
db_pass_staging = urllib.parse.quote_plus("")
engine_staging = create_engine(f'', client_encoding='utf8' )

tbl_name_cndb = 'tbl_bridge_ibm_cndb_staging'
tbl_bridge_ibm_mldb_staging = 'tbl_bridge_ibm_mldb_staging'
tbl_fail = 'tbl_ibm_source_data_failures_staging'
tbl_golden_master = 'tbl_golden_m'

db_user_production = urllib.parse.quote_plus("")
db_pass_production = urllib.parse.quote_plus("")
engine_production = create_engine(f'', client_encoding='utf8' )
engine_production_golden = create_engine(f'', client_encoding='utf8' )

connection = engine_production.raw_connection() #TODO connection pooling required
cursor = connection.cursor() #TODO connection pooling required

start_time_copy_expert = time.time()
try:
    df_cndb_inprocess = pd.read_sql_query('select "AccountNumber" from tbl_bridge_ibm_cndb_staging', con=engine_staging)
    df_mldb_inprocess = pd.read_sql_query('select * from '+tbl_bridge_ibm_mldb_staging+' WHERE \"MldbTags\"->>\'1TF\'=\'Inprocess\'', con=engine_staging)
    # df_mldb_inprocess_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/df_mldb_inprocess__'+str(datetime.now().date())+'_.csv'
    df_mldb_inprocess_csv_path = f'C:/project/Server_data_download/server_small_df/df_mldb_inprocess__'+str(datetime.now().date())+'_.csv'
    df_mldb_inprocess.to_csv(df_mldb_inprocess_csv_path,index=False)
    print("----df_mldb_inprocess.shape----",df_mldb_inprocess.shape)
    print("----df_cndb_inprocess.shape----",df_cndb_inprocess.shape)
    ####Read golden master from ey_atombase_db using tbl_golden_m
    df_golden_m = pd.read_sql_query('select "GoldenValue" from '+tbl_golden_master+' WHERE "GoldenKey" = \'AssetDesignation\'', con=engine_production_golden)
except Exception as err_read_df_mldb_inprocess:
    print("-----err_read_df_mldb_inprocess-",err_read_df_mldb_inprocess)

def mldb_success_to_prod(df_data):
    db_user_production = urllib.parse.quote_plus("")
    db_pass_production = urllib.parse.quote_plus("")
    engine_production = create_engine(f'')
    table_name_production = 'tbl_it_asset_baseline'

    myId = uuid.uuid4()
    df_prod = pd.DataFrame()
    # df_prod['Id'] = df_data['AccountNumber']
    df_prod['UpdatedOn'] = df_data['UpdatedOn']
    df_prod['UpdatedBy'] = df_data['UpdatedBy']
    df_prod['IsDeleted'] = False
    df_prod['TenantId'] = myId
    df_prod['HostName'] = df_data['HostName']
    df_prod['MacAddress'] = 'MacAddress'
    df_prod['BoTInstalled'] = False
    df_prod['BoTCommDate'] = '2021-03-07'
    df_prod['AdhocScan'] = False
    df_prod['AssetStatus'] = df_data['HardwareState']
    df_prod['AccountNumber'] = df_data['CNDBId']
    df_prod['AccountName'] = df_data['CustomerName']
    
    dict_SupportingTags = {
    'Category' : df_data['Category'].tolist(),
    'CampusId' : df_data['CampusId'].tolist(),
    'State' : df_data['State'].tolist(),
    'Address1' : df_data['Address1'].tolist(),
    'Address2' : df_data['Address2'].tolist(),
    'Address3' : df_data['Address3'].tolist(),
    'City' : df_data['City'].tolist(),
    'ZipCode' : df_data['ZipCode'].tolist(),
    'InternetFacingHw' : df_data['InternetFacingHw'].tolist()
    }
    df_data_SupportingTags = pd.DataFrame.from_dict(dict_SupportingTags)
    #df_data_SupportingTags["NewAccountFLAG"]= df_mldb.apply(lambda x: get_vald_str(x), axis=1) # get_vald_str()
    SupportingTags_Jsonb = df_data_SupportingTags.to_json(orient='records',lines=True)
    SupportingTags_list = []
    AssetLocation_list = []
    ITTags_list = []
    for data_jsonb in SupportingTags_Jsonb.split('\n')[:-1]:
        SupportingTags_list.append(data_jsonb)
        AssetLocation_list.append(data_jsonb)
        ITTags_list.append(data_jsonb)
    df_prod['SupportingTags'] = SupportingTags_list #jsonb
    df_prod['AssetLocation'] = AssetLocation_list #jsonb #df_mldb_inprocess["NewAccountFLAG"]= df_mldb.apply(lambda x: get_vald_str(x), axis=1) # get_vald_str()
    df_prod['ITTags'] = ITTags_list #jsonb
    
    df_prod.to_sql(table_name_production, con=engine_production, if_exists='append', index=False)


def convert_dtype_accnNum_mldb(acc_num):
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
        print("--convert_dtype_accnNum_mldb--Exception-err_to_numeric-",err_to_numeric)

# def wrap_check_accnNum_mldb(df_mldb_inprocess):
#   df_mldb_inprocess['AccNumVald'] = df_mldb_inprocess['CNDBId'].apply(lambda x: convert_dtype_accnNum_mldb(x))

def get_indexFailure_Row(df_mldb_inprocess):
    df_mldb_inprocess['AccNumVald'] = df_mldb_inprocess['CNDBId'].apply(lambda x: convert_dtype_accnNum_mldb(x))
    
    
    #New Account FLAG == df_mldbAccNum_MissingIn_cndbid
    # df_mlAcNum_missing = df_mldb_validac_success[~df_mldb_inprocess['CNDBId'].isin(df_cndb_inprocess['AccountNumber'])] #~ NOT IN
    # print(df_mlAcNum_missing.shape)

    ### VALIDATIONS for FAILURE
    df_fail = pd.DataFrame()
    df_fail_accountNum = df_mldb_inprocess.query('AccNumVald==0 or HostName.isnull()', engine='python') #or (AmSwStatus not in ["ACTIVE","INACTIVE"]) or AccountName.isnull()
    ## Validated - HostName and AccNumber

    #df_cndb_nonFAIL = pd.eval('df_fail_accountNum'-'df_cndb_inprocess') 
    #TODO - use eval() + cPython cdef , cpdef , strict Typing etc >> Time_It 
    start_time_df_mldb_nonFAIL = time.time()
    connection_staging = engine_staging.raw_connection() #TODO connection pooling required
    cursor_staging = connection_staging.cursor() #TODO connection pooling required

    df_mldb_success = df_mldb_inprocess[~df_mldb_inprocess.index.isin(df_fail_accountNum.index)] #~ NOT IN
    print('df_mldb_success----> {0}'.format(df_mldb_success.shape))

    
    ### add flag to not matched CNDBID step 2 pending CNDBID -- Flag
    # print('df_mldb_ac_mismatch ----> {0}'.format(df_mlAcNum_missing.shape))
    # df_mldb_success = df_mldb_validac_success[~df_mldb_success.index.isin(df_mlAcNum_missing.index)] #~ NOT IN
    # print('df_mldb_success ----> {0}'.format(df_mldb_success.shape))    
    ### add flag for mismatched ServerType against tbl_golden_m ---> GoldenValue
    
    
    #mldb_success_to_prod(df_mldb_success)
    
    print("---INSERTED --into PROD -mldb_success_to_prod(df_mldb_success)=========")
    sql_query = ""
    sql_query = 'UPDATE tbl_bridge_ibm_mldb_staging set "MldbTags" = \'{"1TF":"Processed"}\' where \"MldbTags\"->>\'1TF\'=\'Inprocess\';'
    # cursor_staging.execute(sql_query)
    print('-----int(cursor_staging.rowcount)-------',int(cursor_staging.rowcount))
    connection_staging.commit()
    cursor_staging.close()
    #df_cndb_nonFAIL["CndbTags"] = '{"1TF":"Processed"}' #TODO - update query for only Updating the VALUE and Not overWriting the KEY 
    print("-----DF_NonFailingROWS.shape-------",df_mldb_success.shape)
    # df_mldb_success_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/DF_mldb_successROWS_'+str(datetime.now().date())+'_.csv'
    df_mldb_success_csv_path = f'C:/project/Server_data_download/server_small_df/DF_mldb_successROWS_'+str(datetime.now().date())+'_.csv'
    df_mldb_success.to_csv(df_mldb_success_csv_path,index=False)
    df_mldb_nonFAIL_end_time = time.time()
    time_taken = start_time_df_mldb_nonFAIL - df_mldb_nonFAIL_end_time
    print("time taken seconds-- DF_NonFailingROWS-- Inserted to CNDB PROD Table",time_taken)

    df_fail["UpdatedOn"] = df_fail_accountNum["UpdatedOn"]
    df_fail["UpdatedBy"] = df_fail_accountNum["UpdatedBy"]
    df_fail["TenantId"] = "" #TODO - IsDeleted and TenantId values - ADESH Sir will confirm.
    df_fail["IsDeleted"] = False
    df_fail["AccountNo"] = None #TODO - to be NULL for the Failure ROWS with REASON - 
    #TODO - AccountNumber will be STRING as this is a VALIDATION FAILED ROW - till 18th MARCH we had CONSTRAINT as INT here 
    df_fail["Source"] = df_fail_accountNum["Source"]
    df_fail["FailureReason"] = df_fail_accountNum.apply(lambda x: get_failure_reason(x), axis=1)

    print("        "*90)
    print("-------df_fail_FailureReason-----",df_fail["FailureReason"])
    print("        "*90)

    df_fail["DataTags"] = df_fail_accountNum.apply(lambda x: str(x.to_json(orient="columns", date_format='iso')), axis=1)
    print("df_fail_accountNum  shape",df_fail_accountNum.shape)
    print("Failure table shape",df_fail.shape)
    rows_count = int(df_fail.shape[0])
    df_fail.insert(0, 'Id', range(0, 0 + rows_count)) #TODO - AutoIncrement ID
    fail_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/fail_csv_cndb__'+str(datetime.now().date())+'_.csv'
    df_fail.to_csv(fail_csv_path,index=False)
    print("Failure table\n",df_fail.head())
    print("DataTags\n", df_fail["DataTags"])
    sql = "COPY " + tbl_fail + " FROM STDIN DELIMITER \',\' CSV header;"
    # cursor.copy_expert(sql, open(fail_csv_path))
    connection.commit()
    end_time = time.time()
    seconds_copy_expert = end_time - start_time_copy_expert
    print("time taken seconds - copy_expert",seconds_copy_expert)
    cursor.close()
    
    #df_cndb_AccNumVald = df_cndb_inprocess.query('AccNumVald==1') #and (AmSwStatus=="ACTIVE" or AmSwStatus == "INACTIVE")

# def check_account_status(df_cndb_inprocess):
#   df_failAcctStatus = df_cndb_inprocess.query('AmSwStatus!= "ACTIVE" and AmSwStatus!="INACTIVE"')
#   print('fail df {0}'.format(df_failAcctStatus.head(5)))
#   print('fail df shape final {0}'.format(df_failAcctStatus.shape))
#   df_failAcctStatus["FailureReason"] = "AccStatusInvalid"
#   df_cndb_2 = df_status.query('AmSwStatus=="ACTIVE" or AmSwStatus == "INACTIVE"')
#   print('fail df shape final {0}'.format(df_cndb_2.shape))
#   df_cndb_2['CndbTags'] =  "{'1TF':'Processed'}"
#   df_cndb_2.to_csv("df_cndb_2.csv")
#   return df_failAcctStatus

def cnbd_id_vald():
    """ cndb id """
    df_mldb['cndb_id_vald'] = (1).where(df_cndb_inprocess['AccountNumber'].isin(df_mldb['CNDBId']),other=0)
    #TODO - need actual CNDBID + JSONB

def check_empty():
    """ hostname """
    df_mldb['host_empty_vald'] = df_cndb_inprocess['AccountNumber'].isnull()

def get_vald_str(df_mldb=df_mldb_inprocess):
    #df_cndb_inprocess == Global DF From Above 
    vald_mldb = ""
    print("---if df_mldb['CNDBId'].isin------")
    if df_mldb['CNDBId'].isin(df_cndb_inprocess['AccountNumber']):
        vald_mldb += "CNDBId_matched_"
    # else:
    #     vald_mldb += "CNDBId_Not_matched_"
    # if df_mldb['ServerType'].isin(df_golden_master['ServerType']):
    #     vald_mldb += "ServerType_matched_"
    return vald_mldb[:-1] # drop the Underscore

# cols_final = ['UpdatedOn', 'UpdatedBy', 'Source', 'Category', \
#           'HardwareState', 'CNDBId', 'CustomerName', \
#           'CampusId', 'State','Address1', 'Address2',
#           'Address3','City', 'ZipCode','InternetFacingHw', \
#           'HostName', 'ServerType', 'MldbTags','HardwareSerialNumber',\
#           'IsShare','Country','MachineType']

def get_failure_reason(df_mldb=df_mldb_inprocess):
    """
    :param df:DataFrame
    :return:str
    """
    fail_reason = ""
    # if ServerType
    if df_mldb["CNDBId"] == None:
        fail_reason += "CNDBId_Empty_"
    if df_mldb["AccNumVald"] == 0:
        fail_reason +="CNDBId_NotInt_"
    if df_mldb["HostName"] == None:
        fail_reason +="HostName_Empty_"
    return fail_reason[:-1]


#wrap_check_accnNum_mldb(df_mldb_inprocess)
get_indexFailure_Row(df_mldb_inprocess)
