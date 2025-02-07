import urllib.parse
from sqlalchemy import create_engine
import time
import pandas as pd
import uuid
import json

from settings import read_config_file, CONFIGS_PATH, PROJECT_PATH
from utils import *
"""
"""

config_data = read_config_file(CONFIGS_PATH)
active_db = config_data['db_environment'][config_data['active_db_env']]['atom_core_db']

connection, cursor = data_source_connection(active_db)
table_name_production = 'tbl_accounts'


start_time_copy_expert = time.time()
# df_data = pd.read_sql_table(table_name_staging, con=engine_staging)

myId = uuid.uuid4()


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


