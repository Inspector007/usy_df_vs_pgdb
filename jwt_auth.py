import logging
import time
from boxsdk import JWTAuth, Client
from folder_structure import *

cur_dir = os.getcwd()
config = JWTAuth.from_settings_file(cur_dir+'/801216798_5hnrf8a2_config.json')
# config = JWTAuth.from_settings_file(cur_dir+'/798124088_vw9mlb0a_config.json')

#change to info level
logging.basicConfig(filename='box12.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.ERROR)
start_time_box = time.time()
try:
    logging.error('**' * 50)
    client = Client(config)
    user = client.user().get()
    folder_id = '0'
    print('The current user ID is {0}'.format(user.id))
    folder1 = client.folder(folder_id).get_items()
    for i in folder1:
        file_id = i.id
        file_name = i.name
        extension = file_name.split('.')[-1]
        # File Downloading in main directory path
        # downloading_path = file_mapped_folder(file_name)
        file_content = client.file(file_id).content()
        # output_file = open(downloading_path + '/' + file_name, 'wb')
        # output_file = open(f'C:/Projects/Test/{file_name}', 'wb') 
        # client.file(file_id).download_to(output_file)
        file_content = client.file(file_id).content()
        output_file = open('C:/Projects/Test/' + file_name, 'wb')
        client.file(file_id).download_to(output_file)
        logging.error('%s file successfully downloaded', file_name)
except Exception as e:
    # print("Exception occurred BOX SDK credential expired "+e)
    logging.error("Exception occurred BOX SDK credential expired ", exc_info=True)

end_time = time.time()
seconds_box_to_vm = end_time - start_time_box
logging.error(" time taken seconds - from BOX to AzureVM ----> %d seconds",seconds_box_to_vm)
