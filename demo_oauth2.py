import logging
from boxsdk import OAuth2, Client
from folder_structure import *
import time

#change to info level
logging.basicConfig(filename='demo_box.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.ERROR)

# '%(asctime)s - %(message)s'
start_time_box = time.time()
try:
    auth = OAuth2(
        client_id='jmagkr1x8s5061gbpxv9972r913jyvwl',
        client_secret='LaALIrTHXYFRqIfb3gZgrzbSNW5H0Pya',
        access_token='fQaHyWhEpn9GU3qPD6A8aITFOqVJzykV', # expire in every 60 minutes
    )
    logging.error('**'*50)
    client = Client(auth)
    user = client.user().get()
    print('The current user ID is {0}'.format(user.id))
    folder1 = client.folder(folder_id='132673428544').get_items()
    for i in folder1:
        file_id = i.id
        file_name = i.name
        # File Downloading in main directory path
        downloading_path = fun_main_dir_path()
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

