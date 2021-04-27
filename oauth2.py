import logging
from boxsdk import OAuth2, Client
from folder_structure import *
import time

#change to info level
logging.basicConfig(filename='box.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.ERROR)

# '%(asctime)s - %(message)s'
start_time_box = time.time()
try:
    auth = OAuth2(
        client_id='ejaskhl0cx0dw1nelk5thp97kxzyfpls',
        client_secret='4St8PBcQEikoC5tboTmlguBw1HOysJYI',
        access_token='rxtefovBqarEFjcfqF5V0Ol5qMc6nT7O', # expire in every 60 minutes
    )
    logging.info('************************************'
                  '***************************************'
                  '******************************')
    client = Client(auth)
    user = client.user().get()
    print('The current user ID is {0}'.format(user.id))
    folder1 = client.folder(folder_id='131994037351').get_items()
    for i in folder1:
        file_id = i.id
        file_name = i.name
        # File Downloading in main directory path
        downloading_path = file_mapped_folder(file_name)
        extension = file_name.split('.')[-1]
        
        file_content = client.file(file_id).content()
        output_file = open(downloading_path + '/' + file_name, 'wb')
        client.file(file_id).download_to(output_file)
        logging.error('%s file successfully downloaded',file_name)
except Exception as e:
    # print("Exception occurred BOX SDK credential expired "+e)
    logging.error("Exception occurred BOX SDK credential expired ",exc_info=True)

end_time = time.time()
seconds_box_to_vm = end_time - start_time_box
logging.error(" time taken seconds - from BOX to AzureVM %d",seconds_box_to_vm)

