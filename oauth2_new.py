import logging
import time
from boxsdk import OAuth2,JWTAuth, Client
# from folder_structure import *
import os
from datetime import datetime
import pandas as pd
from pathlib import Path

def parse_json():
    df_from_json = pd.read_json("index.json", orient='records')
    print(df_from_json)
    return df_from_json

def get_parent_path():
    # return f"C:/Projects/Test/"
    return f"/home/vcenteruser/satish/OriginalData/"
    # return f"/home/vcenteruser/ibm_new/sprint3/a_csv/27_03_data/Test/"

def create_dirs(parent_path, folder_name):
    if (parent_path != None) and (folder_name != None):
        path_to_create = os.path.join(str(parent_path),str(folder_name))
        Path(path_to_create).mkdir(parents=True, exist_ok=True)
        return path_to_create



cur_dir = os.getcwd()
# config = JWTAuth.from_settings_file(cur_dir + '/798124088_vw9mlb0a_config.json')
logging.basicConfig(filename='box.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.ERROR)
config = OAuth2(
            client_id='kdg4gmqqyzhtdmpuplewxyylzq30ds4h',
            client_secret='nSlQmW65lE7lCwoGCjwcB9XEn2G4tGcD',
            access_token='NecmoSDdoWeR4ucFvgAXENvi29a3A3mm',  # expire in every 60 minutes
            # refresh_token : rEmOhNolAOWHD2JSuq6RZTOtDT8Jh6ydoizknN7psMqZeeGCHQiW4cQksC3zcpcS
            # access_token : RRGDWYg4k6xeiyal9sVomgh8SBrlO82W
        )
# change to info level
logging.basicConfig(filename='box.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.ERROR)
start_time_box = time.time()
try:
    logging.error('**' * 50)
    client = Client(config)
    # import pdb;pdb.set_trace()
    users = client.users(user_type='all')
    # usert = client.user().get()
    # print('{0} (Usert ID: {1}) type {2}'.format(usert.name, usert.id, usert.type))
    user1 = None
    for user in users:
        print('{0} (User ID: {1}) type {2}'.format(user.name, user.id, user.type))
        user1 = user
    folder_id = '0'
    # user_id = '15418425574'
    # user1 = client.user(user_id=user_id).get()

    # auth_user = config.authenticate_user(user1)
    # sdk = JWTAuth.from_settings_file(cur_dir + '/798124088_vw9mlb0a_config.json', access_token=auth_user)
    # client = Client(sdk)
    # print('The current user ID is {0}'.format(user1.id)) 
    root = get_parent_path()
    # json_data = parse_json()
    
    folder1 = client.folder(folder_id).get_items()
    for i in folder1:
        file_id = i.id
        file_name = i.name
        # extension = file_name.split('.')[-1]
        if i.type == 'folder':
            # print(f'folder name - {file_name}')
            folder2 = client.folder(file_id).get_items()
            for tx in folder2:
                # print(f'{file_name} -- {tx.name}')
                if tx.type == 'folder':
                    file_id_tx = tx.id
                    file_name_tx = tx.name
                    print('...\\'+file_name_tx)
                    # if '...\\'+file_name_tx in json_data.Folder.values:
                    # path = create_dirs(root,file_name_tx)            
                    path = get_parent_path()           
                    print(f'folder name 2- {file_name_tx}')
                    folder3 = client.folder(tx.id).get_items()
                    for i_tx in folder3:
                        file_id_tx1 = i_tx.id
                        file_name_tx1 = i_tx.name
                        if tx.name == 'SC_DIS_CEDP_DATA' and file_name_tx1 in ['AIC_ISO_TAG_20210325_1.CSV']:
                            file_path = os.path.join(path,f"{file_name_tx1}")
                            output_file = open(f'{file_path}', 'wb')
                            print(f'--file_path----{file_path}')
                            client.file(file_id_tx1).download_to(output_file)
        else:
            print(f'------{i.name}')
            # output_file = open(f'C:/BoxData/{file_name}', 'wb')
            # client.file(file_id).download_to(output_file)
        # File Downloading in main directory path
        # downloading_path = file_mapped_folder(file_name)
        # file_content = client.file(file_id).content()
        # output_file = open('C:\project\ibmnew\EY.SAM.ATOM.IBM\EY.Atom.Bridge\\apps\connectors\\box_connector'+ '/' + file_name, 'wb')
        # client.file(file_id).download_to(output_file)
        logging.error('%s file successfully downloaded', file_name)
except Exception as e:
    # print("Exception occurred BOX SDK credential expired "+e)
    logging.error(f"Exception occurred BOX SDK credential expired {e}", exc_info=True)

end_time = time.time()
seconds_box_to_vm = end_time - start_time_box
logging.error(" time taken seconds - from BOX to AzureVM ----> %d seconds", seconds_box_to_vm)