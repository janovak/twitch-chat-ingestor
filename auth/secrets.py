import json
import os

# Constants for paths
SECRETS_BASE_PATH = 'secrets'
TWITCH_API_BASE_PATH = os.path.join(SECRETS_BASE_PATH, 'twitch-api')
ASTRA_DB_BASE_PATH = os.path.join(SECRETS_BASE_PATH, 'astra-db')

def load_secrets(filepath):
    with open(filepath) as f:
        return json.load(f)

def load_secrets_once(func):
    def wrapper(*args, **kwargs):
        if not hasattr(func, 'has_run'):
            func.secrets = func(*args, **kwargs)
            func.has_run = True
        return func.secrets
    return wrapper

@load_secrets_once
def load_twitch_api_secrets():
    return load_secrets(os.path.join(TWITCH_API_BASE_PATH, 'twitch-api-app-secret.json'))

@load_secrets_once
def load_astra_db_secrets():
    cloud_config = {
        'secure_connect_bundle': os.path.join(ASTRA_DB_BASE_PATH, 'secure-connect-live-stream-data.zip')
    }
    secrets = load_secrets(os.path.join(ASTRA_DB_BASE_PATH, 'live_stream_data-token.json'))
    return secrets, cloud_config

def get_twitch_api_client_id():
    return load_twitch_api_secrets()["clientId"]

def get_twitch_api_secret():
    return load_twitch_api_secrets()["secret"]

def get_chat_db_client_id():
    return load_astra_db_secrets()[0]["clientId"]

def get_chat_db_secret():
    return load_astra_db_secrets()[0]["secret"]

def get_astra_db_cloud_config():
    return load_astra_db_secrets()[1]
