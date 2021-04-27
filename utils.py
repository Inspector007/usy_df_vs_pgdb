import time
from sqlalchemy import create_engine
# from utilities.logger import setup_logger

# box_logger = setup_logger(module_name='general', folder_name='bridge')


# def time_func():
#     logger_obj = setup_logger('timer')

#     def wrap(f):
#         def wrapped_f(*args, **kwargs):
#             ts = time.time()
#             result = f(*args, **kwargs)
#             te = time.time()
#             logger_obj.info(f'Execution Time for {f.__name__}() {round(te - ts, 2)} sec')
#             return result

#         return wrapped_f

#     return wrap


def data_source_connection(active_db):
    try:
        print(f'Establishing data source connection')
        if active_db['ssl'] is True:
            engine = create_engine(
                f'postgresql+psycopg2://{active_db["username"]}:'
                f'{active_db["password"]}@{active_db["host"]}:'
                f'{active_db["port"]}/{active_db["database"]}?sslmode=require',
                client_encoding='utf8')
        else:
            engine = create_engine(
                f'postgresql+psycopg2://{active_db["username"]}:'
                f'{active_db["password"]}@{active_db["host"]}:'
                f'{active_db["port"]}/{active_db["database"]}',
                client_encoding='utf8')

        connection = engine.raw_connection()
        cursor = connection.cursor()
        print(f'Connection established successfully')
        return connection, cursor
    except Exception as ex:
        print(f'Error in establishing connection, Error: {ex}')