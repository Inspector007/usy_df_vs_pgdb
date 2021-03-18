
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

db_user = urllib.parse.quote_plus("devuser1@usedadvsampql01")
db_pass = urllib.parse.quote_plus("Devuser1@3")
## verbose - sslmode=require
#engine = create_engine('postgresql+psycopg2://postgres:portaluser@8877@atomclient.westcentralus.cloudapp.azure.com:5432/ey_atombridge_db', client_encoding='utf8' )#, echo=True) ## Dont Echo may take more time 

#engine_postgres = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@usedadvsampql01.postgres.database.azure.com:5432/postgres?sslmode=require', client_encoding='utf8' )
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )
#print(type(engine))
#print (engine_postgres.table_names()) # []
print (engine.table_names()) # list of Tables
tables =  ['tbl_ibm_source_data_failures_staging','tbl_ibm_scan_data_aic_hw_ind_staging', 
         'tbl_ibm_scan_data_aic_iso_tag_staging', 
          'tbl_ibm_scan_data_aic_package_data_staging', 
           'tbl_ibm_scan_data_aic_sw_inc_staging', 
            'tbl_ibm_scan_data_aicstaging',
            # 'tbl_bridge_ibm_vmware_hw_inventory', 
            # 'tbl_bridge_ibm_entitlements_esw', 
            # 'tbl_bridge_ibm_tadz_lpar_staging'
            ]

for t in tables:
    connection = engine.raw_connection()
    cursor = connection.cursor()
    sql_df_aic = "Select * FROM "+t+" where 1=2"
    df_fromSQL = pd.read_sql_query(sql_df_aic,connection)
    df_fromSQL = df_fromSQL.transpose()
    df_fromSQL.to_csv(t+".csv")
    connection.close()
#cursor.execute("Select * FROM tbl_ibm_cndb_staging LIMIT 0")
#'tbl_ibm_cndb_staging', 'tbl_ibm_mldb_staging',
#colnames = [desc[0] for desc in cursor.description]
#print(colnames)
#sql_df = "Select * FROM tbl_ibm_swcm_staging LIMIT 2"

#sql_df_aic = "Select * FROM tbl_ibm_cndb_staging" 
#tbl_ibm_mldb_staging
#sql_df_aic = "Select * FROM tbl_ibm_mldb_staging" 
#sql_df = "Select * FROM tbl_ibm_scan_data_aic_hw_ind_staging LIMIT 2"
# sql_df1 = "Select * FROM tbl_ibm_scan_data_aic_iso_tag_staging LIMIT 2"
# sql_df2 = "Select * FROM tbl_ibm_scan_data_aic_package_data_staging LIMIT 2"
# sql_df3 = "Select * FROM tbl_ibm_scan_data_aic_sw_inc_staging LIMIT 2"
# sql_df4 = "Select * FROM tbl_ibm_scan_data_aicstaging LIMIT 2"

#sql_df = "Select * FROM tbl_ibm_mldb_staging LIMIT 2"
#df_fromSQL = pd.read_sql_query(sql_df_aic,connection)
# df_fromSQL1 = pd.read_sql_query(sql_df1,connection)
# df_fromSQL2 = pd.read_sql_query(sql_df2,connection)
# df_fromSQL3 = pd.read_sql_query(sql_df3,connection)
# df_fromSQL4 = pd.read_sql_query(sql_df4,connection)

# print(type(df_fromSQL))
# print(pd.DataFrame.head(df_fromSQL))
#df_fromSQL.to_csv("tbl_ibm_swcm_staging_.csv")

#df_fromSQL.to_csv("mldb_staging_.csv")
# df_fromSQL1.to_csv("tbl_ibm_scan_data_aic_iso_tag_staging_.csv")
# df_fromSQL2.to_csv("tbl_ibm_scan_data_aic_package_data_staging_.csv")
# df_fromSQL3.to_csv("tbl_ibm_scan_data_aic_sw_inc_staging_.csv")
# df_fromSQL4.to_csv("tbl_ibm_scan_data_aicstaging_.csv")

#df_fromSQL.to_csv("tbl_ibm_mldb_staging_.csv")
#
# cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
# cursor.execute("""select *
#              from information_schema.columns
#              where table_schema NOT IN ('information_schema', 'pg_catalog')
#              order by table_schema, table_name""")
# #
# for row in cursor:
#   # table_schema = row['table_schema']
#   # print(table_schema)
#   table_name = row['table_name']
#   if table_name == "tbl_ibm_cndb_staging":
#   #if table_name == "tbl_ibm_mldb_staging":
#       #print(table_name)
#       column_name = row['column_name']
#       print(column_name)
#       data_type = row['data_type']
#       print(data_type)

#print(ey_atombridge_db.tbl_ibm_swcm_staging.__table__.columns.keys())
# messages = Table('tbl_ibm_swcm_staging', meta, autoload=True, autoload_with=engine)
# print(messages)
#print (engine.table_names())

# def metadata_dump(sql, *multiparams, **params):
#     # print or write to log or file etc
#     print(sql.compile(dialect=engine.dialect))
# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' ), strategy='mock', executor=metadata_dump)
# metadata.create_all(engine)    

# from sqlalchemy.schema import CreateTable
# with engine.connect() as conn:
#   conn.execute(CreateTable(tbl_ibm_swcm_staging))

# from sqlalchemy import MetaData
# metadata = MetaData()
# print("-----here----",metadata)
# #-----here---- MetaData(bind=None)
# for t in metadata.sorted_tables:
#   print(t.name)

# from sqlalchemy.schema import CreateTable
# #print(CreateTable(tbl_ibm_swcm_staging))
# print(CreateTable(Model.__tbl_ibm_swcm_staging__).compile(dialect=postgresql.dialect()))

# (ibm_venv) vcenteruser@vcentervm:~/manoj/EY.SAM_ATOM/EY.Atom.Bridge/apps/connectors/vcenter$ python ram_demo_18feb.py
# <class 'sqlalchemy.engine.base.Engine'>
# ['__EFMigrationsHistory', 'tbl_audit_trail', 'tbl_bridge_pre_staging', 'tbl_connector_config', 
#'tbl_connector_execution', 'tbl_ibm_cndb_staging', 'tbl_ibm_mldb_staging', 'tbl_ibm_swcm_staging', 
#'tbl_notification', 'tbl_sccm_add_remove_program', 'tbl_sccm_gs_system_console_usage_max_grp', 'tbl_sccm_gs_virtual_application', 'tbl_sccm_gs_work_status', 'tbl_sccm_hs_add_remove_program', 'tbl_sccm_hs_add_remove_program_64', 'tbl_sccm_r_system', 'tbl_sccm_ra_system_ip_address', 'tbl_sccm_ra_system_ip_subnet', 'tbl_sccm_ra_system_name', 'tbl_sccm_recently_used_app', 'tbl_sccm_software_file', 'tbl_sccm_system_software_file', 'tbl_sccm_system_software_product', 'tbl_sccm_virtual_application_package']
# (ibm_venv) vcenteruser@vcentervm:~/manoj/EY.SAM_ATOM/EY.Atom.Bridge/apps/connectors/vcenter$ pwd
# /home/vcenteruser/manoj/EY.SAM_ATOM/EY.Atom.Bridge/apps/connectors/vcenter


