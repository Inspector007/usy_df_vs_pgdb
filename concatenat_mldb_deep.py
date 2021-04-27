
import urllib.parse
from sqlalchemy import create_engine
import time , uuid , json
import pandas as pd
import numpy as np
from datetime import datetime
import re , json

db_user_staging = urllib.parse.quote_plus("")
db_pass_staging = urllib.parse.quote_plus("")
engine_staging = create_engine(f'postgresql+psycopg2://{db_user_staging}:{db_pass_staging}@:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )

tbl_name_cndb = 'tbl_bridge_ibm_cndb_staging'
tbl_bridge_ibm_mldb_staging = 'tbl_bridge_ibm_mldb_staging'
tbl_fail = 'tbl_ibm_source_data_failures_staging'
tbl_golden_master = 'tbl_golden_m'

db_user_production = urllib.parse.quote_plus("")
db_pass_production = urllib.parse.quote_plus("")
engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@:5432/ibm_atombridge_db?sslmode=', client_encoding='utf8' )
engine_production_golden = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@:5432/ey_atombase_db?sslmode=', client_encoding='utf8' )

connection = engine_production.raw_connection() #TODO connection pooling required
cursor = connection.cursor() #TODO connection pooling required

start_time_copy_expert = time.time()
try:
    df_cndb_inprocess = pd.read_sql_query('select "AccountNumber" from tbl_bridge_ibm_cndb_staging', con=engine_staging)
    df_mldb_inprocess = pd.read_sql_query('select * from '+tbl_bridge_ibm_mldb_staging+' WHERE \"MldbTags\"->>\'1TF\'=\'Inprocess\'', con=engine_staging)
    df_mldb_inprocess_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/df_mldb_inprocess__'+str(datetime.now().date())+'_.csv'
    df_mldb_inprocess.to_csv(df_mldb_inprocess_csv_path,index=False)
    print("----df_mldb_inprocess.shape----",df_mldb_inprocess.shape)
    ####Read golden master from ey_atombase_db using tbl_golden_m
    df_golden_m = pd.read_sql_query('select "GoldenValue" from '+tbl_golden_master+' WHERE "GoldenKey" = \'AssetDesignation\'', con=engine_production_golden)
except Exception as err_read_df_mldb_inprocess:
    print("-----err_read_df_mldb_inprocess-",err_read_df_mldb_inprocess)

def mldb_success_to_prod(df_data):
    db_user_production = urllib.parse.quote_plus("")
    db_pass_production = urllib.parse.quote_plus("")
    engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@:5432/ibm_atomdeployment_db?sslmode=', client_encoding='utf8' )
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
    df_prod['AccountNumber'] = df_data['CNDBID']
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
    SupportingTags_Jsonb = df_data_SupportingTags.to_json(orient='records',lines=True)
    SupportingTags_list = []
    AssetLocation_list = []
    ITTags_list = []
    for data_jsonb in SupportingTags_Jsonb.split('\n')[:-1]:
        SupportingTags_list.append(data_jsonb)
        AssetLocation_list.append(data_jsonb)
        ITTags_list.append(data_jsonb)
    df_prod['SupportingTags'] = SupportingTags_list #jsonb
    '''
    {
    "HWBoxName": "Box Name40",
    "PVUPerCore": "300 PVUs",
    "TotalPhysicalCore": "60"
    }
    '''
    df_prod['AssetLocation'] = AssetLocation_list #jsonb
    '''{
    "City": "NAVI MUMBAI",
    "State": "MH",
    "ZipCode": "400708",
    "Address1": "RELIABLE TECH PARK",
    "Address2": "RELIABLE TECH PARK",
    "Address3": "RELIABLE TECH PARK",
    "CampusId": "INMRELI",
    "Category": "MLDB",
    "InternetFacingHw": "InternetFacingHw"
    }'''
    df_prod['ITTags'] = ITTags_list #jsonb

    '''{
    "SwVersion": "4.6",
    "ClusterName": "Sector1",
    "ProcessorModel": "Pentium platinum2",
    "OperatingSystem": "Linux",
    "AssetDesignation": "Designation30",
    "OracleCoreFactor": "max200",
    "HostTypeAsAssetType": "Guest-server",
    "SwFileNameDiscovered": "File Name69"
    }'''
    
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
    #New Account FLAG

    df_fail = pd.DataFrame()
    df_fail_accountNum = df_mldb_inprocess.query('AccNumVald==0 or HostName.isnull()', engine='python') #or (AmSwStatus not in ["ACTIVE","INACTIVE"]) or AccountName.isnull()

    ### step 1 and 3 completed
    #df_cndb_nonFAIL = pd.eval('df_fail_accountNum'-'df_cndb_inprocess') 
    #TODO - use eval() + cPython cdef , cpdef , strict Typing etc >> Time_It 
    start_time_df_mldb_nonFAIL = time.time()

    connection_staging = engine_staging.raw_connection() #TODO connection pooling required
    cursor_staging = connection_staging.cursor() #TODO connection pooling required

    df_mldb_validac_success = df_mldb_inprocess[~df_mldb_inprocess.index.isin(df_fail_accountNum.index)] #~ NOT IN


    print('df_mldb_validac_success ----> {0}'.format(df_mldb_validac_success.shape))

    df_mldb_ac_mismatch = df_mldb_validac_success[~df_mldb_validac_success['CNDBId'].isin(df_cndb_inprocess['AccountNumber'])] #~ NOT IN
    ### add flag to not matched CNDBID step 2 pending CNDBID -- Flag

    print('df_mldb_ac_mismatch ----> {0}'.format(df_mldb_ac_mismatch.shape))

    df_mldb_success = df_mldb_validac_success[~df_mldb_validac_success.index.isin(df_mldb_ac_mismatch.index)] #~ NOT IN

    print('df_mldb_success ----> {0}'.format(df_mldb_success.shape))    


    ### add flag for mismatched ServerType against tbl_golden_m ---> GoldenValue

    # mldb_success_to_prod(df_mldb_success)
    
    print("---INSERTED --into PROD -mldb_success_to_prod(df_mldb_success)=========")
    sql_query = ""
    sql_query = 'UPDATE tbl_bridge_ibm_mldb_staging set "MldbTags" = \'{"1TF":"Processed"}\' where \"MldbTags\"->>\'1TF\'=\'Inprocess\';'
    # cursor_staging.execute(sql_query)
    print('-----int(cursor_staging.rowcount)-------',int(cursor_staging.rowcount))
    connection_staging.commit()
    cursor_staging.close()
    #df_cndb_nonFAIL["CndbTags"] = '{"1TF":"Processed"}' #TODO - update query for only Updating the VALUE and Not overWriting the KEY 
    print("-----DF_NonFailingROWS.shape-------",df_mldb_success.shape)
    df_mldb_success_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/DF_mldb_successROWS_'+str(datetime.now().date())+'_.csv'
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

def get_vald_str(df_cndb,df_mldb,df_golden_master):
    vald_mldb = ""
    if df_cndb['CNDBId'].isin(df_mldb['CNDBId']):
        vald_mldb += "CNDBId_matched_"
    if df_mldb['ServerType'].isin(df_golden_master['ServerType']):
        vald_mldb += "ServerType_matched_"
    return vald_mldb[:-1] # drop the Underscore

# cols_final = ['UpdatedOn', 'UpdatedBy', 'Source', 'Category', \
#           'HardwareState', 'CNDBId', 'CustomerName', \
#           'CampusId', 'State','Address1', 'Address2',
#           'Address3','City', 'ZipCode','InternetFacingHw', \
#           'HostName', 'ServerType', 'MldbTags','HardwareSerialNumber',\
#           'IsShare','Country','MachineType']

def get_failure_reason(df_mldb):
    """
    :param df:DataFrame
    :return:str
    """
    fail_reason = ""
    # if ServerType
    if df["CNDBId"] == None:
        fail_reason += "CNDBId_Empty_"
    if df["AccNumVald"] == 0:
        fail_reason +="CNDBId_NotInt_"
    if df["HostName"] == None:
        fail_reason +="HostName_Empty_"
    return fail_reason[:-1]


#wrap_check_accnNum_mldb(df_mldb_inprocess)
get_indexFailure_Row(df_mldb_inprocess)
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
import time

### CT TEAM STAGIING DB 
# db_user = urllib.parse.quote_plus("devuser1@usedadvsampql01")
# db_pass = urllib.parse.quote_plus("Devuser1@3")
# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )

### DEEPAK LAPTOP DB 
db_user = urllib.parse.quote_plus("postgres")
db_pass = urllib.parse.quote_plus("pgadmin")
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@localhost:5432/ibm_atombridge_db?sslmode=', client_encoding='utf8' )

connection = engine.raw_connection() #TODO connection pooling required
cursor = connection.cursor() #TODO connection pooling required

#HW_Baseline_MLDB_20220303.xlsx #TODO CHange to xlsx and code again with 
# csv_file = '/home/vcenteruser/ibm_new/sprint3/a_csv/HW_Baseline_MLDB_20210218.csv'
# csv_file = f'C:/project/Server_data_download/server_original/HW_Baseline_MLDB_20210218_1.csv'
csv_file = f'C:/project/feb24 _sample_file/hw_baseline.csv'

#pd.read_excel('CNDB_ACCOUNT_Records_20210303.xlsx')#, sheetname=0).to_csv('CNDB_ACCOUNT_Records_20210303.csv', index=False)
#xlrd.biffh.XLRDError: Excel xlsx file; not supported
# https://stackoverflow.com/questions/65254535/xlrd-biffh-xlrderror-excel-xlsx-file-not-supported

# csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/b_csv/MLDB_final.csv'
csv_load_staging = f'C:/project/Server_data_download/server_small_df/MLDB_final.csv'

start_time_copy_expert = time.time()
df_original = pd.read_csv(csv_file)
df_original.drop(df_original.columns[0], axis=1,inplace=True) # DROP COL == SR NO 
print("----df_original.shape------",df_original.shape)
#print(df_original.head(2))

ls_cols_staging = ['UpdatedOn', 'UpdatedBy', 'Source', 'MldbTags']

ls_append_rows = []
for i in range(df_original.shape[0]):
    ls_append_rows.append(['2021-03-12','Atom_Bridge', 'Source', "{'1TF':'Inprocess'}"])

df_staging_append = pd.DataFrame(ls_append_rows, columns = ls_cols_staging)
print("-----df_staging_append.shape--------",df_staging_append.shape)
#print(df_staging_append.head(2))
#
df_load_staging = pd.concat([df_staging_append,df_original],axis=1, join="inner")
cols_final = ['UpdatedOn', 'UpdatedBy', 'Source', 'Category', \
				'HardwareState', 'AccountNumber', 'CustomerName', \
				'CampusId', 'State','Address1', 'Address2',
				'Address3','City', 'ZipCode','InternetFacingHw', \
				'HostName', 'ServerType', 'MldbTags']
#TODO = 'ClientDirNumber', to be REMOVED ? 
#TODO - SAtish Code - 'SystemType' mapped in middle 
#TODO -- cols_final - needs to come from CONFIG.ini

df_load_staging = df_load_staging.reindex(columns=cols_final) #TODO Satish Code - Why Reindex ? 
# df_load_staging.columns=cols_final
df_load_staging.to_csv(csv_load_staging,index=False)
print("----df_load_staging.shape----------",df_load_staging.shape)
print(df_load_staging.head(2))

table_name = 'tbl_ibm_mldb_staging'

sql = "COPY " + table_name + " FROM STDIN DELIMITER \',\' CSV header;"
cursor.copy_expert(sql, open(csv_load_staging))

connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()
