import urllib.parse
from sqlalchemy import create_engine
import time
import pandas as pd
import uuid
import json

"""tbl_ibm_mldb_staging Columns -->
['UpdatedOn', 'UpdatedBy', 'Source', 'Category', 'HardwareState', 'AccountNumber', 
'CustomerName', 'CampusId', 'State','Address1', 'Address2',
'Address3','City', 'ZipCode','InternetFacingHw', 'HostName', 'ServerType', 'MldbTags']
 """
db_user_staging = urllib.parse.quote_plus("devuser1@usedadvsampql01")
db_pass_staging = urllib.parse.quote_plus("Devuser1@3")
engine_staging = create_engine(f'postgresql+psycopg2://{db_user_staging}:{db_pass_staging}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )
table_name_staging = 'tbl_ibm_mldb_staging'

"""tbl_it_asset_baseline Columns -->
['Id', 'UpdatedOn', 'UpdatedBy', 'IsDeleted', 'TenantId', 'HostName', 'MacAddress', 'BoTInstalled', 
'ITTags', 'BoTCommDate', 'AdhocScan', 'AssetStatus', 'AssetLocation', 'AccountNumber', 
'AccountName', 'SupportingTags']
 """
db_user_production = urllib.parse.quote_plus("postgres")
db_pass_production = urllib.parse.quote_plus("portaluser@8877")
engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@atomclient.westcentralus.cloudapp.azure.com:5432/ibm_atomdeployment_db?sslmode=', client_encoding='utf8' )
table_name_production = 'tbl_it_asset_baseline'

start_time_copy_expert = time.time()
df_data = pd.read_sql_table(table_name_staging, con=engine_staging)

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
df_prod['AccountNumber'] = df_data['AccountNumber']
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
df_prod['AssetLocation'] = AssetLocation_list #jsonb
df_prod['ITTags'] = ITTags_list #jsonb

df_prod.to_sql(table_name_production, con=engine_production, if_exists='append', index=False)


