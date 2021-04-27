from boxsdk import OAuth2,JWTAuth, Client
from fastapi import FastAPI
import uvicorn , os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from routers import connectors
import pandas as pd
from datetime import datetime
from pathlib import Path
import numpy as np
import logging

app = FastAPI()
app.include_router(connectors.router)

logging.basicConfig(filename='autogenerate_refresh_token.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.ERROR)

def parse_json():
    try:
        df_from_json = pd.read_json("meta_data_.json", orient='records')
        logging.error(f'DataFrame from ParsedJSON--saved to meta_data_df_from_json.csv-')
        print('df_from_json---------\n',df_from_json)
        df_from_json.to_csv("meta_data_df_from_json.csv")
        logging.error(f'Logging the PARSED_JSON_DF from Parsed JSON File ...')
        logging.error('\t'+ df_from_json.to_string().replace('\n', '\n\t'))
        return df_from_json
    except Exception as exc_parse_json:
        logging.error(f"EXCEPTION from - PARSE_JSON - {exc_parse_json}")

def get_parent_path():
    #TODO -- paths of DIR to come from --- Config.ini --- Not in the Python Code 
    #parent_path = f"/home/vcenteruser/ibm_new/sprint3/box_connector_loggerUpdated_13MARCH/dir_26Demo/"
    parent_path = f"/home/vcenteruser/ibm_new/sprint3/demo31/1_Box/demo31/"
    logging.error(f'Path of Parent Directory---coming from --Config.ini- {parent_path} .')
    return parent_path

def create_dirs(parent_path, folder_name):
    if (parent_path != None) and (folder_name != None):
        path_to_create = os.path.join(str(parent_path), str(folder_name))
        Path(path_to_create).mkdir(parents=True, exist_ok=True)
        return path_to_create

def read_token_from_csv():
    df_token = pd.read_csv("df_token_append.csv")
    print("----ROW -- Last + COL-3  ----")
    print(df_token.iloc[-1:,3])
    print(type(df_token.iloc[-1:,3])) #<class 'pandas.core.series.Series'>
    token_val = list(df_token.iloc[-1:,3])
    token_val = token_val[0]
    access_token = list(df_token.iloc[-1:,0])
    access_token = access_token[0]
    logging.error(f'Started the REFRESH TOKEN AUTO generate App .')
    # HardCode the values below for Initial RUN 
    access_token = 'SV1UMH7te8u75NSfybPEcD0MleaRVKHn'
    token_val = 'IfTadYPBRGfkeQjFI3LaP6LSFPaDnUL8umb7Fb1YfZKirVhrZW9NV8aDaPlRzVVn'
    return token_val , access_token

def download_files():
    """
    """
    logging.error(f'Logging started --download_files ...')
    ls_cnt_actualFiles = []
    ls_cnt_actualDIRs = []
    ls_not_got_metaData = []
    ls_got_metaData = []
    dict_count_actualFiles = {}
    root = get_parent_path()
    json_data = parse_json()
    df_counter = pd.DataFrame()
    df_metaData_vald = pd.DataFrame()

    ls_file_cnt = json_data["File_Count"].tolist()
    ls_zeroIdx = [i for i, e in enumerate(ls_file_cnt) if e == 0]
    non_zero_df = json_data.drop(json_data.index[ls_zeroIdx])
    logging.error(f'Logging the NON ZERO DF from Parsed JSON File ...')
    logging.error('\t'+ non_zero_df.to_string().replace('\n', '\n\t'))
    
    df_meta_data_compare = pd.DataFrame()
    df_meta_data_compare["Meta_DataFile_DIR_Names"] = non_zero_df["Folder"]
    df_meta_data_compare["Meta_DataFile_File_Count"] = non_zero_df["File_Count"]
    df_meta_data_compare["Meta_DataFile_File_Names"] = non_zero_df["FileSet"]

    token_val , access_token = read_token_from_csv()
    access_token_config = OAuth2(
            client_id='kdg4gmqqyzhtdmpuplewxyylzq30ds4h',
            client_secret='nSlQmW65lE7lCwoGCjwcB9XEn2G4tGcD',
            access_token= str(access_token),
            refresh_token= str(token_val)
        )
    #logging.basicConfig(filename='box.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.ERROR)
    #start_time_box = time.time()

    #try:
    #logging.error('**' * 50)
    client = Client(access_token_config)
    users = client.users(user_type='all')
    user1 = None
    for user in users:
        print('{0} (User ID: {1}) type {2}'.format(user.name, user.id, user.type))
        user1 = user
    folder_id = '0'

    # auth_user = config.authenticate_user(user1)
    # sdk = JWTAuth.from_settings_file(cur_dir + '/798124088_vw9mlb0a_config.json', access_token=auth_user)
    # client = Client(sdk)
    # print('The current user ID is {0}'.format(user1.id))

    folder1 = client.folder(folder_id).get_items()
    for i in folder1:
        file_id = i.id
        file_name = i.name
        if i.type == 'folder':
            folder2 = client.folder(i.id).get_items()
            for tx in folder2:
                if tx.type == 'folder':
                    file_id_tx = tx.id
                    actualDIR_name = tx.name
                    if '...\\' + actualDIR_name in non_zero_df.Folder.values:
                        path = create_dirs(root, actualDIR_name)
                        ls_cnt_actualDIRs.append(actualDIR_name)
                        #print(ls_cnt_actualDIRs)
                        non_zero_df["Actual_BOX_Dir_Names"] = pd.Series(ls_cnt_actualDIRs)
                        folder3 = client.folder(file_id_tx).get_items()
                        for i_tx in folder3:
                            file_id_tx1 = i_tx.id
                            actualFILE_name = i_tx.name
                            #print("------ls_fileSet..tolist()--------\n")
                            ls_fileSet = non_zero_df.FileSet.values 
                            if actualFILE_name in str(ls_fileSet.tolist()):
                                ls_got_metaData.append(actualFILE_name)
                                logging.error(f'FILE_VALIDATION:- File in BOX also listed in META_DATA_Json == {actualFILE_name} .')
                                if "zip" in str(actualFILE_name) or "json" in str(actualFILE_name):
                                    file_path = os.path.join(path, f"{actualFILE_name}")
                                    logging.error(f'PATH_DOWNLOAD_DIR ---- {file_path} .')
                                    output_file = open(f'{file_path}', 'wb')
                                    client.file(file_id_tx1).download_to(output_file)
                                if  "zip" in str(actualFILE_name):
                                    try:
                                        # TODO -- from Config.ini 
                                        password_unzip = "fusion123$" # TODO -- password to Config.ini
                                        import pyminizip
                                        logging.error(f'DECRYPT_FILE:- Decrypting the File :- {actualFILE_name} .')
                                        logging.error(f'UN_COMPRESS_FILE:- Unzipping the File :- {actualFILE_name} .')
                                        #pyminizip.uncompress(file_path, password_unzip, "/home/vcenteruser/ibm_new/sprint3/box_connector_loggerUpdated_13MARCH/dir_26Demo/uncompressed_csv_files",0)#, int(withoutpath))
## DEEPAK CHANGE BELOW LINE 
                                        #pyminizip.uncompress(file_path, password_unzip, "/home/vcenteruser/ibm_new/sprint3/demo31/1_Box/demo31/uncompressed_csv_files",0)
                                    except Exception as err_init_conn:
                                        print(err_init_conn)
                                        logging.error(f'ERROR:- Error while Decrypting the File :- {actualFILE_name} .')
                            else:
                                ls_not_got_metaData.append(actualFILE_name)
                                logging.error(f'FILE_VALIDATION:- File in BOX , NOT listed in META_DATA_Json == {actualFILE_name} .')
                        
        else:
            print(f'-EXCEPTION:- Got a Non DIR Object within ROOT DIR , FileName--{i.name}')
            logging.error(f'EXCEPTION:- Got a Non DIR Object within ROOT DIR , FileName :- {i.name} .')
            pass 

def get_new_tokens():
    token_val , access_token = read_token_from_csv()
    import requests
    import json
    from datetime import datetime 
    dict_new_tokens = {}

    url = "https://api.box.com/oauth2/token"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = {
        'client_id': 'kdg4gmqqyzhtdmpuplewxyylzq30ds4h',
        'client_secret': 'nSlQmW65lE7lCwoGCjwcB9XEn2G4tGcD',
        'refresh_token': str(token_val),#' fOreg2QrkO8TAsMK5GMPV9ipkqWDlBTT0RtDpDguN7Z7JwxbL0ti0ZQR8hc4Ciz7',
        'grant_type': 'refresh_token'
    }
    response = requests.post(url, headers=headers, data=data)

    if response :
        print("----response-------",response)
        access_token_json = json.loads(response.text)
        print("----access_token_json----from efresh File ---",access_token_json)
        df_counter = pd.DataFrame()
        access_token_json["datetime"] = str(datetime.now())
        df_counter = df_counter.append(access_token_json, ignore_index=True)
        print("----df_counter----",df_counter)
        print(str(access_token_json["refresh_token"]))
        print(f'refresh_token : {access_token_json["refresh_token"]}')
        print(f'access_token : {access_token_json["access_token"]}')
        print("  "*90)
        print(response) # 200 
        df_counter.to_csv("df_token_append.csv", mode='a', index=False, header=False)
        return dict_new_tokens

box_scheduler = BackgroundScheduler()
box_scheduler.start()

# box_getToken = box_scheduler.add_job(get_new_tokens, trigger='cron', minute='*/1')
# print("--started === box_schedule r.add_job(get_token --- next run at---",box_getToken.next_run_time)

box_downLoadFiles = box_scheduler.add_job(download_files, trigger='cron', minute='*/1')
print("--started === box_scheduler.add_job(download_files --- next run at---",box_downLoadFiles.next_run_time)

@app.get("/")
async def root():
    return {"message": "Hello!"}

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8888)



 



