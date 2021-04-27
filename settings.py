import json
import os
import sys

PROJECT_PATH = os.path.dirname(os.path.realpath(__file__))

if getattr(sys, 'frozen', False):
    PROJECT_PATH = os.path.dirname(sys.executable)
elif __file__:
    PROJECT_PATH = os.path.dirname(__file__)

CONFIGS_PATH = os.path.join(PROJECT_PATH, "configs.json")
LOGS_PATH = os.path.join(PROJECT_PATH, "logs")
FILE_DOWNLOAD_PATH = os.path.join(PROJECT_PATH, "box_files")
BLOB_FOLDER_FILES_PATH = os.path.join(PROJECT_PATH, "blob_folder_files")
PROCESSED_FILES = os.path.join(PROJECT_PATH, "processed_files")
CHUNKS_FILES = os.path.join(PROJECT_PATH, "file_chunks")
os.makedirs(FILE_DOWNLOAD_PATH, exist_ok=True)
os.makedirs(BLOB_FOLDER_FILES_PATH, exist_ok=True)
os.makedirs(CHUNKS_FILES, exist_ok=True)

BOX_RUN_TIME = None


def read_config_file(path):
    """
        This method is to read local config file

        Args:
            path: config file path

        Returns:
            dict:
                * config data

        Raises:
            FileNotFoundError: File not found error

    """
    try:
        with open(path, "r") as json_file:
            json_data = json.load(json_file)
            return json_data
    except FileNotFoundError as e:
        print("Config File Not Found Please add config file in configs folder")


def write_config_file(path, config_data):
    """
        This method is to write local config file

        Args:
            path: config file path
            config_data: config data

        Returns:
            dict:
                * config data

        Raises:
            FileNotFoundError: File not found error

    """
    try:
        with open(path, "w") as json_file:
            json.dump(config_data, json_file, indent=4, sort_keys=True)
    except FileNotFoundError as e:
        print("Config File Not Found Please add config file in configs folder")


CONFIG_DATA = read_config_file(CONFIGS_PATH)
FILE_ENCODING = CONFIG_DATA['file_encoding']
FILE_SEPARATOR = CONFIG_DATA['file_separator']
DB_TO_USE = CONFIG_DATA.get('active_db_env')
BOX_AUTHENTICATOR_METHOD = CONFIG_DATA.get('box_authenticator_method')
IS_BOX_RUNNING = False
OAUTH2 = CONFIG_DATA.get('oauth2')
CONFIG_DATA['box_running'] = False
write_config_file(CONFIGS_PATH, CONFIG_DATA)
LOG_LEVEL = CONFIG_DATA.get('log_level')

