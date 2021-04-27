import urllib.parse
from sqlalchemy import create_engine
import time , uuid , json
import pandas as pd
import numpy as np


"""tbl_ibm_mldb_staging Columns -->
['UpdatedOn', 'UpdatedBy', 'Source', 'Category', 'HardwareState', 'AccountNumber', 
'CustomerName', 'CampusId', 'State','Address1', 'Address2',
'Address3','City', 'ZipCode','InternetFacingHw', 'HostName', 'ServerType', 'MldbTags']
# """
# db_user_staging = urllib.parse.quote_plus("devuser1@usedadvsampql01")
# db_pass_staging = urllib.parse.quote_plus("Devuser1@3")
# engine_staging = create_engine(f'postgresql+psycopg2://{db_user_staging}:{db_pass_staging}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )


db_user = urllib.parse.quote_plus("postgres")
db_pass = urllib.parse.quote_plus("pgadmin")
engine_staging = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@localhost:5432/ibm_atombridge_db?sslmode=', client_encoding='utf8' )

#table_name_staging = 'tbl_ibm_mldb_staging'
tbl_name_cndb = 'tbl_bridge_ibm_cndb_staging'
#tbl_name_mldb = 'tbl_ibm_mldb_staging'
tbl_fail = 'tbl_bridge_ibm_source_data_failures_staging'

db_user_production = urllib.parse.quote_plus("")
db_pass_production = urllib.parse.quote_plus("")
#engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@atomclient.westcentralus.cloudapp.azure.com:5432/ibm_atomdeployment_db?sslmode=', client_encoding='utf8' )
engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@ibm_atombridge_db?sslmode=', client_encoding='utf8' )
# engine_staging = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@atomclient.westcentralus.cloudapp.azure.com:5432/ibm_atombridge_db_2?sslmode=', client_encoding='utf8' )

connection = engine_production.raw_connection() #TODO connection pooling required
cursor = connection.cursor() #TODO connection pooling required


connection_staging = engine_staging.raw_connection() #TODO connection pooling required
cursor_staging = connection_staging.cursor() #TODO connection pooling required

start_time_copy_expert = time.time()
# df_cndb = pd.read_sql_table(tbl_name_cndb, con=engine_staging)
# select * from public.tbl_it_asset_baseline WHERE "ITTags"->>'AssetDesignation'='Designation1'
df_cndb_inprocess = pd.read_sql_query('select * from '+tbl_name_cndb+' WHERE \"CndbTags\"->>\'1TF\'=\'Inprocess\'', con=engine_staging)
#df_fail = pd.read_sql_query('select * from '+tbl_fail,con=engine_staging)
# import pdb;pdb.set_trace()
df_fail = pd.DataFrame()
#df_mldb = pd.read_sql_table(tbl_name_mldb, con=engine_staging)
print("----cndb_actual_df.shape----",df_cndb_inprocess.shape)

def cndb_success_to_prod(df_data):
    db_user_production = urllib.parse.quote_plus("")
    db_pass_production = urllib.parse.quote_plus("")
    engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@?sslmode=', client_encoding='utf8' )
    table_name_production = 'tbl_accounts'


    myId = uuid.uuid4()
    df_prod = pd.DataFrame()
    # df_prod['Id'] = df_data['AccountNumber']
    df_prod['UpdatedOn'] = df_data['UpdatedOn']
    df_prod['UpdatedBy'] = df_data['UpdatedBy']
    df_prod['IsDeleted'] = False
    df_prod['TenantId'] = myId
    # df_prod['AccountTags'] = '' #jsonb
    df_prod['AccountName'] = 'AccountName' #text
    df_prod['ServiceType'] = 'ServiceType' #text
    df_prod['Source'] = 'Source' #text
    df_prod['ClientDirNumber'] = df_data['ClientDirNumber']
    # df_prod['AddressTags'] = '' #jsonb
    # df_prod['DpeDetailTags'] = '' #jsonb
    df_prod['AccountNumber'] = df_data['AccountNumber']

    dict_AccountTags = {
    'AccountTypeName' : df_data['AccountTypeName'].tolist(),
    'AmSwStatus' : df_data['AmSwStatus'].tolist(),
    'SystemType' : df_data['SystemType'].tolist(),
    'IndustryName' : df_data['IndustryName'].tolist(),
    'SectorName' : df_data['SectorName'].tolist(),
    }

    df_data_AccountTags = pd.DataFrame.from_dict(dict_AccountTags)
    AccountTags_Jsonb = df_data_AccountTags.to_json(orient='records',lines=True)
    AccountTags_list = []
    for data_jsonb in AccountTags_Jsonb.split('\n')[:-1]:
        AccountTags_list.append(data_jsonb)
    df_prod['AccountTags'] = AccountTags_list

    dict_AddressTags = {
    'CountryCode' : df_data['CountryCode'].tolist(),
    'GeographyName' : df_data['GeographyName'].tolist(),
    'RegionName' : df_data['RegionName'].tolist(),
    }
    df_data_AddressTags = pd.DataFrame.from_dict(dict_AddressTags)
    AddressTags_Jsonb = df_data_AddressTags.to_json(orient='records',lines=True)
    AddressTags_list = []
    for data_jsonb in AddressTags_Jsonb.split('\n')[:-1]:
        AddressTags_list.append(data_jsonb)
    df_prod['AddressTags'] = AddressTags_list

    dict_DpeDetailTags = {
    'DpeContactEmail' : df_data['DpeContactEmail'].tolist(),
    'DpeContactName' : df_data['DpeContactName'].tolist(),
    }
    df_data_DpeDetailTags = pd.DataFrame.from_dict(dict_DpeDetailTags)
    DpeDetailTags_Jsonb = df_data_DpeDetailTags.to_json(orient='records',lines=True)
    DpeDetailTags_list = []
    for data_jsonb in DpeDetailTags_Jsonb.split('\n')[:-1]:
        DpeDetailTags_list.append(data_jsonb)
    df_prod['DpeDetailTags'] = DpeDetailTags_list

    df_prod.to_sql(table_name_production, con=engine_production, if_exists='append', index=False)


import re
def convert_dtype_accnNum_cndb(acc_num):
    try:
        if re.match("^[0-9 -]+$", acc_num):
            int_acc_num = int(acc_num)
            if isinstance(int_acc_num,int):
                # print("-convert_dtype_accnNum_cndb--- NUMERIC_ACCNUM [CNDB_ID] in CNDB_StagingTable =",int_acc_num)
                accnum_val = 1 # if INT
                return accnum_val
        else:
            # print("-convert_dtype_accnNum_cndb---NON_NUMERIC_ACCNUM [CNDB_ID] in CNDB_StagingTable =",acc_num)
            accnum_val = 0 # if NOT_INT
            return accnum_val
    except Exception as err_to_numeric:
        print("--convert_dtype_accnNum_cndb--Exception-err_to_numeric-",err_to_numeric)

def wrap_check_accnNum_cndb(df_cndb_inprocess):
    df_cndb_inprocess['AccNumVald'] = df_cndb_inprocess['AccountNumber'].apply(lambda x: convert_dtype_accnNum_cndb(x))

def get_indexFailure_Row(df_cndb_vald_AccNumVald):
    df_fail_accountNum = df_cndb_inprocess.query('AccNumVald==0 or (AmSwStatus not in ["ACTIVE","INACTIVE"]) or AccountName.isnull()', engine='python')

    # final_col = pd.concat(df_cndb_inprocess, df_fail_accountNum).drop_duplicates(keep=False)
    df_success = df_cndb_inprocess[~df_cndb_inprocess.index.isin(df_fail_accountNum.index)]
    import pdb;pdb.set_trace()

    cndb_success_to_prod(df_success)

    sql_query= ""
    sql_query = 'UPDATE tbl_bridge_ibm_cndb_staging set "CndbTags" = \'{"1TF":"Processed"}\' where \"CndbTags\"->>\'1TF\'=\'Inprocess\';'

    cursor_staging.execute(sql_query)
    
    df_fail["UpdatedOn"] = df_fail_accountNum["UpdatedOn"]
    df_fail["UpdatedBy"] = df_fail_accountNum["UpdatedBy"]
    df_fail["TenantId"] = ""
    df_fail["IsDeleted"] = False
    df_fail["AccountNo"] = 67776 #df_fail_accountNum["AccountNumber"]
    df_fail["Source"] = df_fail_accountNum["Source"]
    df_fail["FailureReason"] = df_fail_accountNum.apply(lambda x: get_failure_reason(x), axis=1) #get_failure_reason(df_fail_accountNum)
    
    ### DEEPAK CODE STARTS ----------
    print("df_fail_accountNum  shape",df_fail_accountNum.shape)
    print("df_fail_accountNum head \n",df_fail_accountNum.head())
    
    df_fail["DataTags"] = df_fail_accountNum.apply(lambda x: str(x.to_json(orient="columns", date_format='iso')), axis=1)
    
    print("Failure table shape",df_fail.shape)
    print("Failure table\n",df_fail.head())
    print("DataTags\n", df_fail["DataTags"])
    try:
        df_fail.to_sql(tbl_fail, con=engine_production, if_exists='append', index=False) #connection
        connection.commit()
        cursor.close()
        connection_staging.commit()
        cursor_staging.close()
    except Exception as err_copy_data_staging:
        print("-----err_copy_data_staging-----",err_copy_data_staging)  


def cnbd_id_vald():
    """ cndb id """
    df_mldb['cndb_id_vald'] = (1).where(df_cndb_inprocess['AccountNumber'].isin(df_mldb['AccountNumber']),other=0)
    #TODO - need actual CNDBID + JSONB

def check_empty():
    """ hostname """
    df_mldb['host_empty_vald'] = df_cndb_inprocess['AccountNumber'].isnull()

def get_failure_reason(df):
    """
    :param df:DataFrame
    :return:str
    """
    fail_reason = ""
    if df["AccountNumber"] == None:
        fail_reason += "AccNumEmpty_"
    if df["AccNumVald"] == 0:
        fail_reason +="AccNumNotInt_"
    if df["AccountName"] == None:
        fail_reason +="AccNameEmpty_"
    if df["AmSwStatus"] not in ["ACTIVE","INACTIVE"]:
        fail_reason += "AccStatusInvalid_"
    print("-------fail_reason[:-1]------",fail_reason[:-1]) 
    return fail_reason[:-1]

#validations_check(df_cndb)
wrap_check_accnNum_cndb(df_cndb_inprocess)
get_indexFailure_Row(df_cndb_inprocess)
# df_cndb.to_csv("df_cndb_Final.csv")






