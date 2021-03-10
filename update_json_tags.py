import urllib.parse
from sqlalchemy import create_engine
import time
import pandas as pd

db_user = urllib.parse.quote_plus("")
db_pass = urllib.parse.quote_plus("")
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}', client_encoding='utf8' )
connection = engine.raw_connection() ## # TODO connection pooling required
cursor = connection.cursor() ## # TODO connection pooling required
table_name = 'tbl_ibm_cndb_staging'

start_time_copy_expert = time.time()
df_data=pd.DataFrame()
df_data.to_sql(table_name, con=engine, if_exists='append', index=False)
cursor.close()
connection.commit()
end_time = time.time()
seconds_copy_expert = end_time - start_time_copy_expert
print("time taken seconds - copy_expert",seconds_copy_expert)
cursor.close()