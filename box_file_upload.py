from boxsdk import OAuth2, Client
import os
from folder_structure import *
auth = OAuth2(
    client_id='ejaskhl0cx0dw1nelk5thp97kxzyfpls',
    client_secret='4St8PBcQEikoC5tboTmlguBw1HOysJYI',
    access_token='MpNAMxJnbanRdql29h1EtW2vnzugxNo4', # expire in every 60 minutes
)
client = Client(auth)
user = client.user().get()
print('The current user ID is {0}'.format(user.id))

folder_id = '131994037351'
upload_file_path = '/home/vcenteruser/deepak/bloat_2gb.csv'
new_file = client.folder(folder_id).upload(upload_file_path)
print(new_file.name)

# Upload a file to Box!
# from io import StringIO
#
# stream = StringIO()
# stream.write('Box Python SDK test!')
# stream.seek(0)
# box_file = client.folder('0').upload_stream(stream, 'box-python-sdk-test.txt')
# print(box_file.name)

