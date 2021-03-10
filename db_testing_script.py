import urllib.parse
from sqlalchemy import create_engine
import pandas as pd

# db_user_production = urllib.parse.quote_plus("postgres")
# db_pass_production = urllib.parse.quote_plus("portaluser@8877")
# engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@atomclient.westcentralus.cloudapp.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )
# table_name_production = 'tbl_it_asset_baseline'
# df_data1 = pd.read_sql_table(table_name_production, con=engine_production)

db_user = urllib.parse.quote_plus("postgres")
db_pass = urllib.parse.quote_plus("pgadmin")
staging = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@localhost:5432/ibm_atombridge_db?sslmode=', client_encoding='utf8' )
table_name_staging = 'tbl_ibm_swcm_staging'

path = 'C:\\project\\feb24 _sample_file\\staging_tble_to_csv\\'
df_data = pd.read_sql_table(table_name_staging, con=staging)


df_data.to_csv(path+table_name_staging+'.csv')
