import urllib.parse
from sqlalchemy import create_engine
import time

# db_user = urllib.parse.quote_plus("devuser1@usedadvsampql01")
# db_pass = urllib.parse.quote_plus("Devuser1@3")
# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@usedadvsampql01.postgres.database.azure.com:5432/ey_atombridge_db?sslmode=require', client_encoding='utf8' )

db_user = urllib.parse.quote_plus("postgres")
db_pass = urllib.parse.quote_plus("pgadmin")
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@localhost:5432/ibm_atombridge_db?sslmode=', client_encoding='utf8' )
# filename = 'C:/project/Server_data_download/server_original/CNDB_ACCOUNT_Records_20210308_1.csv'


# db_user = urllib.parse.quote_plus("postgres")
# db_pass = urllib.parse.quote_plus("portaluser@8877")
#engine_production = create_engine(f'postgresql+psycopg2://{db_user_production}:{db_pass_production}@atomclient.westcentralus.cloudapp.azure.com:5432/ibm_atomdeployment_db?sslmode=', client_encoding='utf8' )
# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@atomclient.westcentralus.cloudapp.azure.com:5432/ibm_atombridge_db?sslmode=', client_encoding='utf8' )



# filename = 'C:/project/feb24 _sample_file/MLDB_final.csv'
filename = 'C:/project/feature_box_poc_1_box_connector/fail_csv_cndb__2021-03-18_.csv'

import pandas as pd
df_prod = pd.read_csv(filename)


import pdb;pdb.set_trace()
table_name = 'tbl_ibm_source_data_failures_staging'
start_time_copy_expert = time.time()

connection = engine.raw_connection() ## # TODO connection pooling required
cursor = connection.cursor() ## # TODO connection pooling required

# sql = "COPY " + table_name + " FROM STDIN DELIMITER \',\' CSV header;"
# cursor.copy_expert(sql, open(filename))

df_prod.to_sql(table_name, con=engine, if_exists='append', index=False)


connection.commit()
cursor.close()

end_time = time.time()

seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)