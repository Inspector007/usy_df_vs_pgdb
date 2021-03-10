import urllib.parse
from sqlalchemy import create_engine
import time

# db_user = urllib.parse.quote_plus("")
# db_pass = urllib.parse.quote_plus("")
# engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@', client_encoding='utf8' )
db_user = urllib.parse.quote_plus("postgres")
db_pass = urllib.parse.quote_plus("pgadmin")
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@localhost:5432/ibm_atombridge_db?sslmode=', client_encoding='utf8' )

connection = engine.raw_connection() ## # TODO connection pooling required
cursor = connection.cursor() ## # TODO connection pooling required
filename = 'C:\project\Server_data_download\server_original\CNDB_ACCOUNT_Records_12_Feb_2021_MASKED.csv'
table_name = 'tbl_ibm_cndb_staging'
start_time_copy_expert = time.time()
sql = "COPY " + table_name + " FROM STDIN DELIMITER \',\' CSV header;"
cursor.copy_expert(sql, open(filename))
connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()