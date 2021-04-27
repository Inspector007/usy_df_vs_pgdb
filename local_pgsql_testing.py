import urllib.parse
from sqlalchemy import create_engine
import time
import pandas as pd
import json
import ast
import uuid

myId = uuid.uuid4()



table_name_production = 'tbl_golden_m'

start_time_copy_expert = time.time()
df_data = pd.read_sql_table(table_name_production, con=engine_production)

from datetime import datetime
csv_load_staging = f'C:/project/Server_data_download/server_small_df/tbl_golden_m_'+str(datetime.now().date())+'_.csv'
print(df_data.columns.tolist())
df_data.to_csv(csv_load_staging,index=False)
#mapping
# ['Id', 'UpdatedOn', 'UpdatedBy', 'IsDeleted', 'TenantId', 'HostName', 'MacAddress', 'BoTInstalled', 'ITTags', 'BoTCommDate', 'AdhocScan', 'AssetStatus', 'A
# ssetLocation', 'AccountNumber', 'AccountName', 'SupportingTags']

# ['UpdatedOn', 'UpdatedBy', 'Source', 'Category', 'HardwareState', 'AccountNumber', 'CustomerName', 'CampusId', 'State', 'Address1', 'Address2', 'Address3',
 # 'City', 'ZipCode', 'InternetFacingHw', 'HostName', 'ServerType', 'MldbTags']

# df_data1 = pd.read_sql_table(table_name_production, con=engine_staging)


# df_prod = pd.DataFrame()

# df_prod['Id'] = df_data['AccountNumber']
# df_prod['UpdatedOn'] = df_data['UpdatedOn']
# df_prod['UpdatedBy'] = df_data['UpdatedBy']
# df_prod['IsDeleted'] = False
# df_prod['TenantId'] = myId
# df_prod['HostName'] = df_data['HostName']
# df_prod['MacAddress'] = 'MacAddress'
# df_prod['BoTInstalled'] = False
# df_prod['BoTCommDate'] = '2021-03-07'
# df_prod['AdhocScan'] = False
# df_prod['AssetStatus'] = df_data['HardwareState']
# df_prod['AccountNumber'] = df_data['AccountNumber']
# df_prod['AccountName'] = df_data['CustomerName']

# df_data1 = pd.read_sql_table(table_name_production, con=engine_staging)
# print(df_data.columns.tolist())
# dict_SupportingTags = {
# 'Category' : df_data['Category'].tolist(),
# 'CampusId' : df_data['CampusId'].tolist(),
# 'State' : df_data['State'].tolist(),
# 'Address1' : df_data['Address1'].tolist(),
# 'Address2' : df_data['Address2'].tolist(),
# 'Address3' : df_data['Address3'].tolist(),
# 'City' : df_data['City'].tolist(),
# 'ZipCode' : df_data['ZipCode'].tolist(),
# 'InternetFacingHw' : df_data['InternetFacingHw'].tolist()
# }
# df_data_SupportingTags = pd.DataFrame.from_dict(dict_SupportingTags)
# print(type(df_data_SupportingTags))
# SupportingTags_Jsonb = df_data_SupportingTags.to_json(orient='records',lines=True)
# SupportingTags_list = []
# AssetLocation_list = []
# ITTags_list = []
# for data_jsonb in SupportingTags_Jsonb.split('\n')[:-1]:
#     SupportingTags_list.append(data_jsonb)
#     AssetLocation_list.append(data_jsonb)
#     ITTags_list.append(data_jsonb)
# df_prod['SupportingTags'] = SupportingTags_list #jsonb
# df_prod['AssetLocation'] = AssetLocation_list #jsonb
# df_prod['ITTags'] = ITTags_list #jsonb

# print(df_data1.columns.tolist())
# df_data['FileDownloadTags'] = df_data['FileDownloadTags'].apply(ast.literal_eval)
# df_data['FileDownloadTags'] = df_data['FileDownloadTags'].astype(str)
# df_data['FileDownloadTags'] = df_data['FileDownloadTags'].apply(json.loads)
# df_data['FileDownloadTags'] = df_data['FileDownloadTags'].apply(lambda _:json.dumps(_))

# df_prod.to_sql(table_name_production, con=staging, if_exists='append', index=False)

