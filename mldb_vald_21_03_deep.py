import sys
import urllib.parse
from sqlalchemy import create_engine
import time , uuid , json
import pandas as pd
import numpy as np
from datetime import datetime
import re , json


print("Choice = 1 :- Load MLDB to Staging Tables ")
print("Choice = 2 :- Pre Process MLDB  ")
print("Choice = 3 :- Post Process MLDB - Run Validations  ")
choice = int(input("Enter a number: "))

def mldb_load_staging():
	"""
	"""
	connection = engine.raw_connection() 
	cursor = connection.cursor()
	#HW_Baseline_MLDB_20220303.xlsx #TODO CHange to xlsx and code again with 
	csv_file = '/home/vcenteruser/ibm_new/sprint3/a_csv/HW_Baseline_MLDB_20220303.csv'
	df_original = pd.read_csv(csv_file)
	print("----df_original.shape------",df_original.shape)
	csv_load_staging = '/home/vcenteruser/ibm_new/sprint3/b_csv/MLDB_PreProcess_'+str(datetime.now().date())+'_.csv'
	start_time_copy_expert = time.time()
	ls_cols_staging = ['UpdatedOn', 'UpdatedBy', 'Source', 'MldbTags','IsShare']
	# IsShare -- Not in the CSV - Vireshwar has added this extra column
	cols_original = ['SR_NO','CNDB_ID','CUSTOMER_NAME','MACHINE_TYPE','HW_SERIAL_NUMBER','HOSTNAME','CATEGORY','HARDWARE_STATE','SERVER_TYPE',
	'INTERNET_FACING_HW','CAMPUS_ID','CITY','STATE','COUNTRY','ADDRESS_1','ADDRESS_2','ADDRESS_3','ZIPCODE']
	cols_to_drop = ['SR_NO']
	# MACHINE_TYPE -- Not to go to PRODUCTION - Only till STAGING - Viresh Mapping XLSX
	# HW_SERIAL_NUMBER - Not to go to PRODUCTION - Only till STAGING - Viresh Mapping XLSX
	# CAMPUS_ID - Not to go to PRODUCTION - Only till STAGING - Viresh Mapping XLSX
	df_original.drop(columns=cols_to_drop, axis=1,inplace=True)
	## Dropped -- 18MARCH -- AccountNumber ... replaced with == CNDBId
	## below MAPPING DICT -- dict_cols_mapping -- follows ORDER of the CSV Columns 
	dict_cols_mapping = {"CNDB_ID":"CNDBId",
						"CUSTOMER_NAME":"CustomerName",
						"MACHINE_TYPE":"MachineType",
						"HW_SERIAL_NUMBER":"HardwareSerialNumber",
						"HOSTNAME":"HostName",
						"CATEGORY":"Category",
						"HARDWARE_STATE":"HardwareState", 
						"SERVER_TYPE":"ServerType",
						"INTERNET_FACING_HW":"InternetFacingHw",
						"CAMPUS_ID":"CampusId",
						"CITY":"City",
						"STATE":"State",
						"COUNTRY":'Country',
						"ADDRESS_1":"Address1",
						"ADDRESS_2":"Address2",
						"ADDRESS_3":"Address3",
						"ZIPCODE":"ZipCode"
										}
	df_original.rename(columns=dict_cols_mapping, inplace=True)
	ls_append_rows = []
	for i in range(df_original.shape[0]):
		ls_append_rows.append([str(datetime.now().date()),'Atom_Bridge', 'Source_mldbData','{"1TF": "Inprocess"}','IsShare'])
	df_staging_append = pd.DataFrame(ls_append_rows, columns = ls_cols_staging)
	print("-----df_staging_append.shape---",df_staging_append.shape)
	
	#df_load_staging = pd.concat([df_staging_append,df_original],axis=1, join="inner")
	df_load_staging = pd.concat([df_staging_append,df_original],axis=1)
	cols_final = ['UpdatedOn', 'UpdatedBy', 'Source', 'Category', \
				'HardwareState', 'CNDBId', 'CustomerName', \
				'CampusId', 'State','Address1', 'Address2',
				'Address3','City', 'ZipCode','InternetFacingHw', \
				'HostName', 'ServerType', 'MldbTags','HardwareSerialNumber',\
				'IsShare','Country','MachineType']
	#TODO -- cols_final - needs to come from CONFIG.ini
	## Dropped -- 18MARCH -- AccountNumber ... replaced with == CNDBId
	df_load_staging = df_load_staging.reindex(columns=cols_final) 
	#df_load_staging.to_csv(csv_load_staging,index=False)
	print("----df_load_staging.shape-",df_load_staging.shape)
	
	df_load_staging_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/DF_mldb_load_staging_'+str(datetime.now().date())+'_.csv'
	df_load_staging.to_csv(df_load_staging_csv_path,index=False)
	sql = "COPY tbl_bridge_ibm_mldb_staging FROM STDIN DELIMITER \',\' CSV header;"
	cursor.copy_expert(sql,open(csv_load_staging))
	connection.commit()
	end_time = time.time()
	seconds_copy_expert = end_time - start_time_copy_expert
	print("time taken seconds - copy_expert",seconds_copy_expert)
	cursor.close()
	#select * from tbl_bridge_ibm_mldb_staging WHERE "MldbTags"->>'1TF'='Inprocess'; #\"CndbTags\"=\'{\"1TF\":\"Processed\"}\'
	#select * from tbl_bridge_ibm_mldb_staging WHERE "MldbTags"->>'1TF'='Processed';
	#select * from tbl_bridge_ibm_mldb_staging WHERE "MldbTags"->>'1TF'='Inprocess';
	#select count(*) from tbl_bridge_ibm_mldb_staging WHERE "MldbTags"->>'1TF'='Inprocess';

def init_conn():
	"""
	"""
	params_init_conn ={}
	try:
		db_user_production = urllib.parse.quote_plus("")
		db_pass_production = urllib.parse.quote_plus("")

		db_user_staging = urllib.parse.quote_plus("")
		db_pass_staging = urllib.parse.quote_plus("")
		
		#engine_staging = create_engine(f'postgresql+psycopg2://{db_user_staging}:{db_pass_staging}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )
		params_init_conn["engine_staging"] = create_engine(f'postgresql+psycopg2://{db_user_staging}:{db_pass_staging}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )
		print('-------type(params_init_conn["engine_staging"])')
		print(type(params_init_conn["engine_staging"]))

		#engine_staging = params_init_conn["engine_staging"]
		params_init_conn["connection_staging"] = params_init_conn["engine_staging"].raw_connection() #TODO connection pooling required
		params_init_conn["cursor_staging"] = params_init_conn["connection_staging"].cursor() 
		tbl_name_cndb = 'tbl_bridge_ibm_cndb_staging'
		
		#tbl_golden_master = 'tbl_golden_m'

		###tbl_fail = 'tbl_ibm_source_data_failures_staging'
		engine_prod_failure = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@:5432/ibm_atombridge_db?sslmode=', client_encoding='utf8' )
		params_init_conn["conn_prod_ibm_atombridge_db"] = engine_prod_failure.raw_connection() 
		params_init_conn["cursor_ibm_atombridge_db"] = params_init_conn["conn_prod_ibm_atombridge_db"].cursor() 
		
		#engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@atomclient.westcentralus.cloudapp.azure.com:5432/ibm_atomdeployment_db?sslmode=', client_encoding='utf8' )
		params_init_conn["engine_production_golden"] = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@:5432/ey_atombase_db?sslmode=', client_encoding='utf8' )
		engine_prod_ibm_atomdeployment_db = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@:5432/ibm_atomdeployment_db?sslmode=', client_encoding='utf8' )
		#conn_prod_ibm_atomdeployment_db = engine_prod_ibm_atomdeployment_db.raw_connection() 
		params_init_conn["conn_prod_ibm_atomdeployment_db"] = engine_prod_ibm_atomdeployment_db.raw_connection() 
		params_init_conn["cursor_ibm_atomdeployment_db"] = params_init_conn["conn_prod_ibm_atomdeployment_db"].cursor() 
		return params_init_conn
	except Exception as err_init_conn:
		print("----err_init_conn------",err_init_conn)

def init_tables():
	"""
	"""
	params_init_conn = init_conn()
	engine_staging = params_init_conn["engine_staging"]
	engine_prod_golden_m = params_init_conn["engine_production_golden"]
	#tbl_bridge_ibm_mldb_staging = 'tbl_bridge_ibm_mldb_staging'
	params_init_tables = {}
	try:
		df_cndb_inprocess = pd.read_sql_query('select "AccountNumber" from tbl_bridge_ibm_cndb_staging', con=engine_staging)
		params_init_tables["df_cndb_inprocess"] = df_cndb_inprocess
		df_mldb_inprocess = pd.read_sql_query('select * from tbl_bridge_ibm_mldb_staging WHERE \"MldbTags\"->>\'1TF\'=\'Inprocess\'', con=engine_staging)
		params_init_tables["df_mldb_inprocess"] = df_mldb_inprocess
		df_mldb_inprocess_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/df_mldb_inprocess__'+str(datetime.now().date())+'_.csv'
		df_mldb_inprocess.to_csv(df_mldb_inprocess_csv_path,index=False)
		print("----df_mldb_inprocess.shape----",df_mldb_inprocess.shape)
		print("----df_cndb_inprocess.shape----",df_cndb_inprocess.shape)
		###Read golden master from ey_atombase_db using tbl_golden_m
		df_golden_m = pd.read_sql_query('select "GoldenValue" from tbl_golden_m WHERE "GoldenKey" = \'AssetDesignation\'', con=engine_prod_golden_m)
		#print('------df_golden_m-------',df_golden_m)
		return params_init_tables
	except Exception as err_read_df_mldb_inprocess:
		print("-----err_read_df_mldb_inprocess-",err_read_df_mldb_inprocess)

def mldb_success_to_prod(df_data):
	"""
	"""
	params_init_conn = init_conn()
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
	start_time_get_vald_str = time.time()
	df_data_SupportingTags["NewAccountFLAG"]= df_data.apply(lambda x: get_vald_str(x), axis=1) # get_vald_str()
	print("df_data.shape ---",df_data.shape)
	print("df_data_SupportingTags.shape -- ", df_data_SupportingTags.shape)
	# df_data.shape --- (49108, 23)
	# df_data_SupportingTags.shape --  (49108, 10)

	print(df_data_SupportingTags["NewAccountFLAG"])
	print("   "*90)

	df_data_SupportingTags.to_csv("df_data_SupportingTags.csv")
	end_time_get_vald_str = time.time()
	seconds_get_vald_str = end_time_get_vald_str - start_time_get_vald_str
	#print("time_secs- get_vald_str",seconds_get_vald_str)
	SupportingTags_Jsonb = df_data_SupportingTags.to_json(orient='records',lines=True)
	SupportingTags_list = []
	AssetLocation_list = []
	ITTags_list = []
	## TODO -- MAKE BELOW Similar == df_fail["DataTags"] = df_fail_accountNum.apply(lambda x: str(x.to_json(orient="columns", date_format='iso')), axis=1)
	for data_jsonb in SupportingTags_Jsonb.split('\n')[:-1]:
		SupportingTags_list.append(data_jsonb)
		AssetLocation_list.append(data_jsonb)
		ITTags_list.append(data_jsonb)
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
	df_prod['ITTags'] = ITTags_list #As discussed with ADESH sir- no data to be sent in ITTags
	df_prod['BoTCommDate'] = '2021-03-07'
	df_prod['AdhocScan'] = False
	df_prod['AssetStatus'] = df_data['HardwareState']
	df_prod['AssetLocation'] = AssetLocation_list #jsonb #df_mldb_inprocess["NewAccountFLAG"]= df_mldb.apply(lambda x: get_vald_str(x), axis=1) # get_vald_str()
	df_prod['AccountNumber'] = df_data['CNDBId']
	df_prod['AccountName'] = df_data['CustomerName']
	df_prod['SupportingTags'] = SupportingTags_list #jsonb
	# slice_df = df_prod.head(20)
	# row_cnt_slice_df = int(slice_df.shape[0])
	row_cnt_prod_df = int(df_prod.shape[0])
	df_prod.insert(0, 'Id', range(0, 0 + row_cnt_prod_df)) #TODO - AutoIncrement ID -- DEEPAK Code is done integrate
	"""
	psycopg2.errors.InvalidTextRepresentation: invalid input syntax for type integer: "2021-03-19"
	CONTEXT:  COPY tbl_it_asset_baseline, line 2, column Id: "2021-03-19"
	"""
	### 21MARCH_1520h -- NOW Truncating - it_asset_baseline
	mldb_prod_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/mldb_prod_csv_cndb_'+str(datetime.now().date())+'_.csv'
	#mldb_prod_csv_path = '/home/vcenteruser/ibm_new/sprint3/mldb_prod_21_03.csv'
	df_prod.to_csv(mldb_prod_csv_path,index=False)
	sql_mldb_prod = "COPY tbl_it_asset_baseline FROM STDIN DELIMITER \',\' CSV header;"

	params_init_conn["cursor_ibm_atomdeployment_db"].copy_expert(sql_mldb_prod, open(mldb_prod_csv_path))
	params_init_conn["conn_prod_ibm_atomdeployment_db"].commit()
	# end_time = time.time()
	# seconds_copy_expert = end_time - start_time_copy_expert
	# print("time taken seconds - mldb_prod_csv_path",seconds_copy_expert)
	print('---ROWS inserted in PROD TABLE -- tbl_it_asset_baseline --',int(cursor_production.rowcount))
	params_init_conn["cursor_ibm_atomdeployment_db"].close()


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

def mldb_processed():
	"""
	"""
	params_init_conn = init_conn()
	# cursor_staging = params_init_conn["cursor_staging"]
	# connection_staging = params_init_conn["connection_staging"]
	sql_query = ""
	sql_query = 'UPDATE tbl_bridge_ibm_mldb_staging set "MldbTags" = \'{"1TF":"Processed"}\' where \"MldbTags\"->>\'1TF\'=\'Inprocess\';'
	cursor_staging.execute(sql_query)
	print('--int(cursor_staging.rowcount)--',int(params_init_conn["cursor_staging"].rowcount))
	params_init_conn["connection_staging"].commit()
	params_init_conn["cursor_staging"].close()

def fail_vald(df_mldb_inprocess,df_fail_accountNum):
	"""
	"""
	params_init_conn = init_conn()
	df_fail = pd.DataFrame()
	# df_mldb_inprocess - is the ORIGINAL DF IN STAGING 
	# df_fail_accountNum - We are taking FAILURE ROWS out of the ORIGINAL and creating SUCESS DF == df_mldb_success , to INSERT in - tbl_it_asset_baseline
	# ACCEPTANCE_CRITERIA - STATUS_DONE = Remove the line items which failed the validations from MLDB staging to failure table
	df_mldb_success = df_mldb_inprocess[~df_mldb_inprocess.index.isin(df_fail_accountNum.index)] #~ NOT IN
	### df_mldb_success -- is DF in which ACC_NUM are VALID and HOSTNAME is Not NULL
	print('df_mldb_success----> {0}'.format(df_mldb_success.shape))
	#df_mldb_success----> (49108, 23)
	mldb_success_to_prod(df_mldb_success)
	print("---INSERTED -mldb_success_to_prod-")
	# ACCEPTANCE_CRITERIA - Mark all items InProgress and then to Processed. [as per agreed flag]
	mldb_processed() # mark all Non Failure ROWS as PROCESSED
	df_mldb_nonFAIL_end_time = time.time()
	time_taken = start_time_df_mldb_nonFAIL - df_mldb_nonFAIL_end_time
	print("time taken seconds-- DF_NonFailingROWS-- Inserted to CNDB PROD Table",time_taken)


	### add flag to not matched CNDBID step 2 pending CNDBID -- Flag
	# print('df_mldb_ac_mismatch ----> {0}'.format(df_mlAcNum_missing.shape))
	# df_mldb_success = df_mldb_validac_success[~df_mldb_success.index.isin(df_mlAcNum_missing.index)] #~ NOT IN
	# print('df_mldb_success ----> {0}'.format(df_mldb_success.shape))    
	### add flag for mismatched ServerType against tbl_golden_m ---> GoldenValue

	df_fail["UpdatedOn"] = df_fail_accountNum["UpdatedOn"]
	df_fail["UpdatedBy"] = df_fail_accountNum["UpdatedBy"]
	df_fail["TenantId"] = "" #TODO - IsDeleted and TenantId values - ADESH Sir will confirm.
	df_fail["IsDeleted"] = False
	df_fail["AccountNo"] = None #TODO - to be NULL for the Failure ROWS with REASON - 
	#TODO - AccountNumber will be STRING as this is a VALIDATION FAILED ROW - till 18th MARCH we had CONSTRAINT as INT here 
	df_fail["Source"] = df_fail_accountNum["Source"]
	# ACCEPTANCE_CRITERIA - STATUS_DONE = Hostname should not be empty any checks for garbage values ?
	# ACCEPTANCE_CRITERIA - STATUS_DONE = Account Number should not be empty and will have numeric value
	df_fail["FailureReason"] = df_fail_accountNum.apply(lambda x: get_failure_reason(x), axis=1)
	#print("-------df_fail_FailureReason-----",df_fail["FailureReason"])


	df_fail["DataTags"] = df_fail_accountNum.apply(lambda x: str(x.to_json(orient="columns", date_format='iso')), axis=1)
	print("df_fail_accountNum  shape",df_fail_accountNum.shape)
	print("Failure table shape",df_fail.shape)
	rows_count = int(df_fail.shape[0])
	df_fail.insert(0, 'Id', range(0, 0 + rows_count)) #TODO - AutoIncrement ID -- DEEPAK Code is done integrate
	fail_csv_path = '/home/vcenteruser/ibm_new/sprint3/b_csv/MLDB_FAIL_'+str(datetime.now().date())+'_.csv'
	df_fail.to_csv(fail_csv_path,index=False)
	print("Failure table\n",df_fail.head(2))
	print("DataTags\n", df_fail["DataTags"])
	sql = "COPY tbl_ibm_source_data_failures_staging FROM STDIN DELIMITER \',\' CSV header;"
	params_init_conn["cursor_ibm_atombridge_db"].copy_expert(sql, open(fail_csv_path))
	params_init_conn["conn_prod_ibm_atombridge_db"].commit() 
	end_time = time.time()
	seconds_copy_expert = end_time - start_time_copy_expert
	print("time taken seconds - copy_expert",seconds_copy_expert)
	params_init_conn["cursor_ibm_atombridge_db"].close()
	#df_cndb_AccNumVald = df_cndb_inprocess.query('AccNumVald==1') #and (AmSwStatus=="ACTIVE" or AmSwStatus == "INACTIVE")


def mldb_valid_main(df_mldb_inprocess):
	"""
	df_mldb_success----> (12277, 23)
	"""
	start_time_df_mldb_nonFAIL = time.time()
	params_init_conn = init_conn()
	params_init_tables = init_tables()
	connection_staging = params_init_conn["connection_staging"]
	cursor_staging = params_init_conn["cursor_staging"]
	# ACCEPTANCE_CRITERIA Validation Checks: Account Number should not be empty and will have numeric value
	df_mldb_inprocess['AccNumVald'] = df_mldb_inprocess['CNDBId'].apply(lambda x: convert_dtype_accnNum_mldb(x))
	#print("-------AA--df_mldb_inprocess---",df_mldb_inprocess['AccNumVald'].head(2))
	### VALIDATIONS for FAILURE - Validated - HostName and AccNumber
	
	df_fail_accountNum = df_mldb_inprocess.query('AccNumVald==0 or HostName.isnull()', engine='python') #or (AmSwStatus not in ["ACTIVE","INACTIVE"]) or AccountName.isnull()
	""" dont delete below lines - we are getting the FLAG in df_data_SupportingTags["NewAccountFLAG"] 
	but we can get the DF with New Accounts as below if required """
	# ### New Account FLAG == df_mldbAccNum_missing in existing LIST of CNBDId's - thus its a NEW ACCOUNT NUMBER
	# df_mldb_validac_success = df_mldb_inprocess[~df_mldb_inprocess.index.isin(df_fail_accountNum.index)] #~ NOT IN
	# df_mldbAccNum_missing = df_mldb_validac_success[~df_mldb_inprocess['CNDBId'].isin(df_cndb_inprocess['AccountNumber'])] #~ NOT IN
	# print("----df_mldbAccNum_missing.shape------",df_mldbAccNum_missing.shape)
	#df_cndb_nonFAIL = pd.eval('df_fail_accountNum'-'df_cndb_inprocess') 
	#TODO - use eval() + cPython cdef , cpdef , strict Typing etc -- then time.time() 
	fail_vald(df_mldb_inprocess,df_fail_accountNum)


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


params_init_tables = init_tables()
df_cndb_inprocess = params_init_tables["df_cndb_inprocess"]
print("----called --- df_cndb_inprocess-------")

def get_vald_str(df_mldb):
	result_get_vald_str = {}
	#vald_mldb = ""
	if df_mldb['CNDBId'] != "" or  math.isnan(df_mldb['CNDBId'])==False:
		if df_mldb['CNDBId'] in list(df_cndb_inprocess['AccountNumber']):
			result_get_vald_str["CNDBId_match"] = "CNDBId_Matched"
		if df_mldb['CNDBId'] not in list(df_cndb_inprocess['AccountNumber']):
			result_get_vald_str["CNDBId_match"] = "CNDBId_Not_Matched"
		# if df_mldb['ServerType'].isin(df_golden_master['ServerType']):
		#     vald_mldb += "ServerType_matched_"
	return result_get_vald_str # drop the Underscore

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
	if df_mldb["CNDBId"] == None:
		fail_reason += "CNDBId_Empty_"
	if df_mldb["AccNumVald"] == 0:
		fail_reason +="CNDBId_NotInt_"
	if df_mldb["HostName"] == None:
		fail_reason +="HostName_Empty_"
	return fail_reason[:-1]


#wrap_check_accnNum_mldb(df_mldb_inprocess)
if choice == 1 :
	mldb_load_staging()
elif choice == 2 :
	params_init_tables = init_tables()
	print("------type(params_init_tables)---------",type(params_init_tables))
	df_mldb_inprocess = params_init_tables["df_mldb_inprocess"]
	mldb_valid_main(df_mldb_inprocess)
elif choice == 3:
	print("-----Not Done --WIP------")
else : 
	print("Invalid input")
	sys.exit()


