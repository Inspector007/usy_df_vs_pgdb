import requests
import json
import logging
from datetime import datetime
from boxsdk import OAuth2
from boxsdk import Client

logging.basicConfig(filename='credentials.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.ERROR)


client_id= 'kdg4gmqqyzhtdmpuplewxyylzq30ds4h'
client_secret= 'nSlQmW65lE7lCwoGCjwcB9XEn2G4tGcD'

auth_url_t = ''
csrf_token_t = ''

a = None
b = None
def store_tokens(access_token, refresh_token):
	# store the tokens at secure storage (e.g. Keychain)
	print(access_token)
	print(refresh_token)

def auth_user():
	oauth = OAuth2(
	client_id=client_id,
	client_secret=client_secret
	# store_tokens=root,
	)

	auth_url, csrf_token = oauth.get_authorization_url('http://localhost:8888/')
	# import pdb;pdb.set_trace()
	# auth_url_test = oauth.get_authorization_url('https://usedadvsamwap01.azurewebsites.net/')


	print(f'---------auth_url - {auth_url}')
	print(f'-------csrf_token - {csrf_token}')
	auth_url_t = auth_url
	csrf_token_t = csrf_token


def request_access_token(auth_url_t,csrf_token_t):
	assert 'THE_CSRF_TOKEN_YOU_GOT' == csrf_token_t
	access_token, refresh_token = oauth.authenticate('YOUR_AUTH_CODE')
	client = Client(oauth)


# http://localhost:8888/?code=DSVRTWnOkhcNydS5eYcxRyC3ufhiP4gT


##  http://localhost:8888/?code=DSVRTWnOkhcNydS5eYcxRyC3ufhiP4gT
# request_access_token(auth_url_t, csrf_token_t)

def refresh_token_old():
	url = "https://api.box.com/oauth2/token"
	
	headers = {
	    'Content-Type': 'application/x-www-form-urlencoded',
	}
	
	data = {
	  'client_id': client_id,
	  'client_secret': client_secret,
	  'refresh_token': 'WBgt82SssUguQjbw4x4U9moFMiOfgGUz',
	  'grant_type': 'refresh_token'
	}
	
	response = requests.post(url, headers=headers, data=data)
	
	st = json.loads(response.text)
	
	logging.error(f'refresh_token : {st["refresh_token"]}')
	logging.error(f'access_token : {st["access_token"]}')
	logging.error(f'DateTime : {datetime.now()}')
	logging.error(f'**'*50)
	
	print(response)


def refresh_token():
	# https://account.box.com/api/oauth2/get_authorization_urlize?client_id=kdg4gmqqyzhtdmpuplewxyylzq30ds4h&redirect_uri=http://localhost:8888&response_type=code
	oauth = OAuth2(
		client_id=client_id,
		client_secret=client_secret
		# store_tokens=root,
		)
	access_token1, refresh_token1 = oauth.authenticate('0Ab1plT44IrNAnNOW0MFyxoy2pmSe2pv') 
	print(access_token1)
	print(refresh_token1)



refresh_token()
	# client = Client(oauth)




# auth_user()



