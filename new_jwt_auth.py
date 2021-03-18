import logging
import time
from boxsdk import JWTAuth, Client
from folder_structure import *

cur_dir = os.getcwd()
config = JWTAuth.from_settings_file(cur_dir + '/798124088_vw9mlb0a_config.json')
# change to info level
logging.basicConfig(filename='box.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.ERROR)
start_time_box = time.time()
try:
    logging.error('**' * 50)
    client = Client(config)
    # import pdb;pdb.set_trace()
    users = client.users(user_type='all')
    usert = client.user().get()
    print('{0} (Usert ID: {1}) type {2}'.format(usert.name, usert.id, usert.type))
    user1 = None
    for user in users:
        print('{0} (User ID: {1}) type {2}'.format(user.name, user.id, user.type))
        user1 = user
    folder_id = '133299161697'
    user_id = '15418425574'
    user1 = client.user(user_id=user_id).get()

    auth_user = config.authenticate_user(user1)
    sdk = JWTAuth.from_settings_file(cur_dir + '/798124088_vw9mlb0a_config.json', access_token=auth_user)
    client = Client(sdk)
    print('The current user ID is {0}'.format(user1.id))
    folder1 = client.folder(folder_id).get_items()
    for i in folder1:
        file_id = i.id
        file_name = i.name
        # extension = file_name.split('.')[-1]
        if i.type == 'folder':
            print(file_name)
            for tx in i.get_items():
                print(tx.name)
        else:
            print(i.name)
        # File Downloading in main directory path
        # downloading_path = file_mapped_folder(file_name)
        # file_content = client.file(file_id).content()
        # output_file = open('C:\project\ibmnew\EY.SAM.ATOM.IBM\EY.Atom.Bridge\\apps\connectors\\box_connector'+ '/' + file_name, 'wb')
        # client.file(file_id).download_to(output_file)
        logging.error('%s file successfully downloaded', file_name)
except Exception as e:
    # print("Exception occurred BOX SDK credential expired "+e)
    logging.error("Exception occurred BOX SDK credential expired ", exc_info=True)

end_time = time.time()
seconds_box_to_vm = end_time - start_time_box
logging.error(" time taken seconds - from BOX to AzureVM ----> %d seconds", seconds_box_to_vm)