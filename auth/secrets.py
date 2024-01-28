import json
import os

# Define the base path for secrets
secrets_base_path = 'secrets'

def _load_secrets(filepath):
    with open(filepath) as f:
        secrets = json.load(f)
    return secrets

def load_twitch_api_secrets():
    # if we have we already loaded the secrets then just return them
    if hasattr(load_twitch_api_secrets, 'has_run'):
        return load_twitch_api_secrets.secrets

    twitch_api_base_path = os.path.join(secrets_base_path, 'twitch-api')

    load_twitch_api_secrets.secrets = _load_secrets(os.path.join(twitch_api_base_path, 'twitch-api-app-secret.json'))

    # record that the function run and we already loaded the secrets
    load_twitch_api_secrets.has_run = True

    return load_twitch_api_secrets.secrets

def load_astra_db_secrets():
    # if we have we already loaded the secrets then just return them
    if hasattr(load_astra_db_secrets, 'has_run'):
        return load_astra_db_secrets.secrets, load_astra_db_secrets.cloud_config

    astra_db_base_path = os.path.join(secrets_base_path, 'astra-db')

    # Your cloud_config with the secure_connect_bundle
    load_astra_db_secrets.cloud_config = {
        'secure_connect_bundle': os.path.join(astra_db_base_path, 'secure-connect-live-stream-data.zip')
    }

    load_astra_db_secrets.secrets = _load_secrets(os.path.join(astra_db_base_path, 'live_stream_data-token.json'))

    # record that the function run and we already loaded the secrets
    load_astra_db_secrets.has_run = True

    return load_astra_db_secrets.secrets, load_astra_db_secrets.cloud_config

def get_twitch_api_client_id():
    secrets = load_twitch_api_secrets()
    return secrets["clientId"]

def get_twitch_api_secret():
    secrets = load_twitch_api_secrets()
    return secrets["secret"]

def get_chat_db_client_id():
    secrets, _ = load_astra_db_secrets()
    return secrets["clientId"]

def get_chat_db_secret():
    secrets, _ = load_astra_db_secrets()
    return secrets["secret"]

def get_astra_db_cloud_config():
    _, cloud_config = load_astra_db_secrets()
    return cloud_config
