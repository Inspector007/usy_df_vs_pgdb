
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, String, DateTime
import pandas as pd
from io import StringIO
import time , psycopg2
from datetime import datetime
from pgcopy import CopyManager
import urllib.parse
from sqlalchemy import Table, Column, MetaData, Integer, Computed
import psycopg2.extras
import os
import configparser
import logging
import ast
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, String, DateTime
from io import StringIO
import time
from datetime import datetime
from pgcopy import CopyManager
import psycopg2
from pathlib import Path

cur_dir = os.getcwd() # TODO - check path on Ubuntu VM
config_aic = configparser.ConfigParser()
config_aic.read(cur_dir+'/'+'config.ini')

# db_name = config_aic['DEFAULT']['db_name']
# # engine = config_aic['DEFAULT']['engine']
# user = config_aic['DEFAULT']['user']
# password = config_aic['DEFAULT']['password']
# # print(password)
# # print(engine)


db_user = urllib.parse.quote_plus("")
db_pass = urllib.parse.quote_plus("")
## verbose - sslmode=require
#engine = create_engine('postgresql+psycopg2://postgres:portaluser@8877@atomclient.westcentralus.cloudapp.azure.com:5432/ey_atombridge_db', client_encoding='utf8' )#, echo=True) ## Dont Echo may take more time

#engine_postgres = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@usedadvsampql01.postgres.database.azure.com:5432/postgres?sslmode=require', client_encoding='utf8' )
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@', client_encoding='utf8' )
#print(type(engine))
#print (engine_postgres.table_names()) # []
print (engine.table_names()) # list of Tables


aic_package_dbname = config_aic['SCAN_DATA_AIC_PACKAGE_DATA']['db_name']
aic_package_fileformat = config_aic['SCAN_DATA_AIC_PACKAGE_DATA']['file_format']
aic_package_table_name = config_aic['SCAN_DATA_AIC_PACKAGE_DATA']['table_name']
print("aic_package_table_name----",aic_package_table_name)
aic_package_csv_headers = ast.literal_eval(config_aic.get("SCAN_DATA_AIC_PACKAGE_DATA", "csv_headers"))


aic_tag_table_name = config_aic['SCAN_DATA_AIC_ISO_TAG']['table_name']
print("aic_package_table_name----",aic_package_table_name)
aic_tag_csv_headers = ast.literal_eval(config_aic.get("SCAN_DATA_AIC_ISO_TAG", "csv_headers"))





# ast - ast.literal_eval - done to get a LIST from the config.ini file

aic_hw_inv = config_aic['SCAN_DATA_AIC_HW_INV']
aic_hw_inv_fileformat = config_aic['SCAN_DATA_AIC_HW_INV']['file_format']
aic_hw_inv_csv_headers = ast.literal_eval(config_aic.get("SCAN_DATA_AIC_HW_INV", "csv_headers"))
aic_hw_table_name = config_aic['SCAN_DATA_AIC_HW_INV']['table_name']

aic_sw_inv = config_aic['SCAN_DATA_AIC_SW_INV']
aic_sw_inv_fileformat = config_aic['SCAN_DATA_AIC_SW_INV']['file_format']
aic_sw_inv_csv_headers = ast.literal_eval(config_aic.get("SCAN_DATA_AIC_SW_INV", "csv_headers"))
aic_sw_table_name = config_aic['SCAN_DATA_AIC_SW_INV']['table_name']

logging.basicConfig(filename='uload_to_staging.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.ERROR)

ls_SCAN_DATA_AIC = []
ls_SCAN_DATA_AIC_HW_INV = []
ls_SCAN_DATA_AIC_SW_INV = []
ls_SCAN_DATA_AIC_ISO_TAG = []
ls_SCAN_DATA_AIC_PACKAGE_DATA = []
ls_VM_Script_Report_ACCOUNT_NO = []

# count = 0


outer_dir = Path(__file__).parent / "../box_connector/sample_data_download"
#outer_dir = 'C:/project/ibmnew/EY.SAM.ATOM.IBM/EY.Atom.Bridge/apps/connectors/box_connector/sample_data_download'
# TODO - change on Azure VM = outer_dir
# ls_dir_name = []
# ls_dir_1 = []

ls_SCAN_DATA_AIC_PACKAGE_DATA_path = []
ls_SCAN_DATA_AIC_ISO_TAG_path = []
ls_SCAN_DATA_AIC_HW_INV_path = []
ls_SCAN_DATA_AIC_SW_INV_path = []

for dir_path,sub_dir_name,dir_files in os.walk(outer_dir):
	for file_name in dir_files:
		if file_name.endswith(".csv"):
			if 'AIC' in file_name:
				#if 'PACKAGE' in file_name:
				# if 'deepak' and 'PACKAGE' in file_name:
				# 	ls_SCAN_DATA_AIC_PACKAGE_DATA.append(file_name)
				# 	ls_SCAN_DATA_AIC_PACKAGE_DATA_path.append(os.path.join(dir_path, file_name))
				# if 'deepak' and 'TAG' in file_name:
				# 	ls_SCAN_DATA_AIC_ISO_TAG.append(file_name)
				# 	ls_SCAN_DATA_AIC_ISO_TAG_path.append(os.path.join(dir_path, file_name))
				if 'deepak' and 'HW' in file_name:
					ls_SCAN_DATA_AIC_HW_INV.append(file_name)
					ls_SCAN_DATA_AIC_HW_INV_path.append(os.path.join(dir_path, file_name))
				if 'deepak' and 'SW' in file_name:
					ls_SCAN_DATA_AIC_SW_INV.append(file_name)
					ls_SCAN_DATA_AIC_SW_INV_path.append(os.path.join(dir_path, file_name))


#print('ls_SCAN_DATA_AIC_PACKAGE_DATA --- {0}'.format(ls_SCAN_DATA_AIC_PACKAGE_DATA))
#print('ls_SCAN_DATA_AIC_ISO_TAG - {0}'.format(ls_SCAN_DATA_AIC_ISO_TAG))
#print('ls_SCAN_DATA_AIC_ISO_TAG_path - {0}'.format(ls_SCAN_DATA_AIC_ISO_TAG_path))
print('ls_SCAN_DATA_AIC_PACKAGE_DATA_path - {0}'.format(ls_SCAN_DATA_AIC_PACKAGE_DATA_path))
print('ls_SCAN_DATA_AIC_PACKAGE_DATA_path - {0}'.format(ls_SCAN_DATA_AIC_ISO_TAG_path))


def csv_headers(filename):
	df_csv = pd.read_csv(outer_dir+'/'+filename)
	cols_csv = list(df_csv)
	if 'AIC' and 'PACKAGE' in file_name:
		# Get from ini
		if aic_package_csv_headers == cols_csv:
			# import pdb;pdb.set_trace()
			logging.error('Validating headeres with csv schema for file -> %s',filename)
	if 'AIC' and 'TAG' in file_name:
		# Get from ini
		if aic_tag_csv_headers == cols_csv:
			# import pdb;pdb.set_trace()
			logging.error('Validating headeres with csv schema for file -> %s', filename)



# TODO - Use params
# params = {}
# params['filename'] = 'filename'
# params['dbname'] = 'dbname'
# params['tablename'] = 'tablename'

connection = engine.raw_connection() ## # TODO connection pooling required
cursor = connection.cursor() ## # TODO connection pooling required

def csv_to_db(filename):#,outer_dir):
	# TODO - call a method for adding - 2 Headers - UpdatedOn , UdatedBy - to upload to DB and Match the Staging DB Schema
	#start_time = time.time()
	start_time_copy_expert = time.time()
	if 'AIC' and 'PACKAGE' in filename:
		sql = "COPY "+aic_package_table_name+" FROM STDIN DELIMITER \',\' CSV header;"
		cursor.copy_expert(sql, open(filename))
	if 'AIC' and 'TAG' in filename:
		sql = "COPY " + aic_tag_table_name + " FROM STDIN DELIMITER \',\' CSV header;"
		cursor.copy_expert(sql, open(filename))
	if 'AIC' and 'HW' in filename:
		sql = "COPY " + aic_hw_table_name + " FROM STDIN DELIMITER \',\' CSV header;"
		cursor.copy_expert(sql, open(filename))
	if 'AIC' and 'SW' in filename:
		sql = "COPY " + aic_sw_table_name + " FROM STDIN DELIMITER \',\' CSV header;"
		cursor.copy_expert(sql, open(filename))
	#cursor.copy_expert(sql,data_buff)
	connection.commit()
	end_time = time.time()
	seconds_copy_expert = end_time - start_time_copy_expert

	print("time taken seconds - copy_expert",seconds_copy_expert)
	cursor.close()
	print("-finally --cursor.close()----")


for iter_name in ls_SCAN_DATA_AIC_HW_INV_path:
	# print(iter_name)
	# print("   "*90)
	if 'deepak' and 'HW' in iter_name:
		csv_to_db(iter_name)


# end_time_all = time.time()
# seconds_all = end_time_all - start_time
# print("time taken seconds - conn create to copy_expert",seconds_all)

