import sys
import os
import urllib.parse
import re, json
from os import listdir
from os.path import isfile, join
from sqlalchemy import create_engine
import time, uuid, json
import pandas as pd
import numpy as np
from datetime import datetime
from settings import CONFIGS_PATH, read_config_file, FILE_DOWNLOAD_PATH
from utilities.logger import setup_logger

box_logger = setup_logger(module_name='connectors', folder_name=str('box'))

def mldb_load_staging():
	"""
	"""
	### CT TEAM STAGIING DB 
	# db_user = urllib.parse.quote_plus("devuser1@usedadvsampql01")
	# db_pass = urllib.parse.quote_plus("Devuser1@3")
	config_data = read_config_file(CONFIGS_PATH)
	active_db = config_data['db_environment'][config_data['active_db_env']]

	# db_user_production = urllib.parse.quote_plus("postgres")
	# db_pass_production = urllib.parse.quote_plus("portaluser@8877")
	#
	# db_user_staging = urllib.parse.quote_plus("devuser1@usedadvsampql01")
	# db_pass_staging = urllib.parse.quote_plus("Devuser1@3")
	print("mldb_load_staging : Started")
	if active_db['ssl'] is True:
		engine = create_engine(f'postgresql+psycopg2://{active_db["username"]}:{active_db["password"]}@{active_db["host"]}:{active_db["port"]}/{active_db["database"]}?sslmode=require', client_encoding='utf8' )
	else:
		engine = create_engine(
			f'postgresql+psycopg2://{active_db["username"]}:{active_db["password"]}@{active_db["host"]}:{active_db["port"]}/{active_db["database"]}', client_encoding='utf8')

	# engine = create_engine(
	# 	f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@atomclient.westcentralus.cloudapp.azure.com:5432/ibm_atombridge_db?sslmode=',
	# 	client_encoding='utf8')

	# engine = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@usedadvsampql01.postgres.database.azure.com:5432/ibm_atombridge_db_2?sslmode=require', client_encoding='utf8' )
	# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )
	connection = engine.raw_connection()
	cursor = connection.cursor()
	# HW_Baseline_MLDB_20220303.xlsx #TODO CHange to xlsx and code again with 

	#params_init_conn["engine_staging"] = 
	# params_init_conn["engine_staging"] = create_engine(f'postgresql+psycopg2://{db_user_staging}:{db_pass_staging}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )
	# print('-------type(params_init_conn["engine_staging"])')
	# print(type(params_init_conn["engine_staging"]))

	mldb = config_data['data_sources']['mldb']
	mldb_folder = os.path.join(FILE_DOWNLOAD_PATH, mldb['folder_name'])
	if os.path.exists(mldb_folder) is False:
		box_logger.info(f'Folder {mldb["folder_name"]} does not exist')
		return

	mldb_files = [f for f in listdir(mldb_folder) if isfile(join(mldb_folder, f))]
	for mldb_file in mldb_files:
		file_name, file_type = mldb_file.rsplit('.',1)
		if file_type.lower() not in mldb['file_type']:
			continue

		if not file_name.lower().startswith(mldb['file_name_starts_with'].lower()):
			continue

		# csv_file = '/home/vcenteruser/ibm_new/sprint3/a_csv/HW_Baseline_MLDB_20220303.csv'
		print("Processing file: "  + file_name )
		box_logger.info(f'Processing mldb files: {file_name}')
		df_original = pd.read_csv(os.path.join(mldb_folder, mldb_file))
		print("----df_original.shape------", df_original.shape)
		# csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/b_csv/MLDB_PreProcess_' + str(datetime.now().date()) + '_.csv'
		start_time_copy_expert = time.time()
		# ls_cols_staging = ['UpdatedOn', 'UpdatedBy', 'Source', 'MldbTags', 'IsShare']
		# df_original = [str(datetime.now().date()), 'Atom_Bridge', 'Source_mldbData', '{"1TF": "Inprocess"}', 'IsShare']
		df_original['UpdatedOn'] = str(datetime.now().date())
		df_original['UpdatedBy'] = 'Atom_Bridge'
		df_original['Source'] = 'Source_mldbData'
		df_original['MldbTags'] = '{"1TF": "Inprocess"}'
		df_original['IsShare'] = 'IsShare'
		# IsShare -- Not in the CSV - Vireshwar has added this extra column
		# cols_original = ['SR_NO', 'CNDB_ID', 'CUSTOMER_NAME', 'MACHINE_TYPE', 'HW_SERIAL_NUMBER', 'HOSTNAME', 'CATEGORY',
		# 				 'HARDWARE_STATE', 'SERVER_TYPE',
		# 				 'INTERNET_FACING_HW', 'CAMPUS_ID', 'CITY', 'STATE', 'COUNTRY', 'ADDRESS_1', 'ADDRESS_2',
		# 				 'ADDRESS_3', 'ZIPCODE']
		cols_to_drop = ['SR_NO']
		# MACHINE_TYPE -- Not to go to PRODUCTION - Only till STAGING - Viresh Mapping XLSX
		# HW_SERIAL_NUMBER - Not to go to PRODUCTION - Only till STAGING - Viresh Mapping XLSX
		# CAMPUS_ID - Not to go to PRODUCTION - Only till STAGING - Viresh Mapping XLSX
		if 'SR_NO' in df_original.columns:
			df_original.drop(columns=cols_to_drop, axis=1, inplace=True)

		## Dropped -- 18MARCH -- AccountNumber ... replaced with == CNDBId
		## below MAPPING DICT -- dict_cols_mapping -- follows ORDER of the CSV Columns
		dict_cols_mapping = {"CNDB_ID": "CNDBId",
							 "CUSTOMER_NAME": "CustomerName",
							 "MACHINE_TYPE": "MachineType",
							 "HW_SERIAL_NUMBER": "HardwareSerialNumber",
							 "HOSTNAME": "HostName",
							 "CATEGORY": "Category",
							 "HARDWARE_STATE": "HardwareState",
							 "SERVER_TYPE": "ServerType",
							 "INTERNET_FACING_HW": "InternetFacingHw",
							 "CAMPUS_ID": "CampusId",
							 "CITY": "City",
							 "STATE": "State",
							 "COUNTRY": 'Country',
							 "ADDRESS_1": "Address1",
							 "ADDRESS_2": "Address2",
							 "ADDRESS_3": "Address3",
							 "ZIPCODE": "ZipCode"
							 }
		df_original.rename(columns=dict_cols_mapping, inplace=True)
		# ls_append_rows = []
		# for i in range(df_original.shape[0]):
		# 	ls_append_rows.append(
		# 		[str(datetime.now().date()), 'Atom_Bridge', 'Source_mldbData', '{"1TF": "Inprocess"}', 'IsShare'])
		# df_staging_append = pd.DataFrame(ls_append_rows, columns=ls_cols_staging)
		# print("-----df_staging_append.shape---", df_staging_append.shape)

		# df_load_staging = pd.concat([df_staging_append,df_original],axis=1, join="inner")
		# df_load_staging = pd.concat([df_staging_append, df_original], axis=1)
		cols_final = ['UpdatedOn', 'UpdatedBy', 'Source', 'Category', \
					  'HardwareState', 'CNDBId', 'CustomerName', \
					  'CampusId', 'State', 'Address1', 'Address2',
					  'Address3', 'City', 'ZipCode', 'InternetFacingHw', \
					  'HostName', 'ServerType', 'MldbTags', 'HardwareSerialNumber', \
					  'IsShare', 'Country', 'MachineType']
		# TODO -- cols_final - needs to come from CONFIG.ini
		## Dropped -- 18MARCH -- AccountNumber ... replaced with == CNDBId
		df_original = df_original.reindex(columns=cols_final)
		# df_load_staging.to_csv(csv_load_staging,index=False)
		print("----df_load_staging.shape-------QA TEAM DEMO --- 25MARCH-", df_original.shape)
		df_load_staging_csv_path = os.path.join(mldb_folder,mldb_file)
		df_original.to_csv(df_load_staging_csv_path, index=False)
		sql = "COPY bridge.tbl_ibm_mldb_staging FROM STDIN DELIMITER \',\' CSV header;"
		cursor.copy_expert(sql, open(df_load_staging_csv_path))
		connection.commit()
		end_time = time.time()
		seconds_copy_expert = end_time - start_time_copy_expert
		print("time taken seconds - copy_expert", seconds_copy_expert)
		cursor.close()
		box_logger.info(f'Processing end mldb files: {file_name}')

		# Delete the processed file


if __name__ == '__main__':
	mldb_load_staging()