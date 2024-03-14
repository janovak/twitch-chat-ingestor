import json
import os

# Constants for paths
SECRETS_BASE_PATH = "secrets"
TWITCH_API_BASE_PATH = os.path.join(SECRETS_BASE_PATH, "twitch-api")
ASTRA_BASE_PATH = os.path.join(SECRETS_BASE_PATH, "astra")
REDIS_BASE_PATH = os.path.join(SECRETS_BASE_PATH, "redis")
NEON_BASE_PATH = os.path.join(SECRETS_BASE_PATH, "neon")
CLOUDAMQP_BASE_PATH = os.path.join(SECRETS_BASE_PATH, "cloudamqp")


def load_secrets(filepath):
    with open(filepath) as f:
        return json.load(f)


def load_secrets_once(func):
    def wrapper(*args, **kwargs):
        if not hasattr(func, "has_run"):
            func.secrets = func(*args, **kwargs)
            func.has_run = True
        return func.secrets

    return wrapper


@load_secrets_once
def load_twitch_api_secrets():
    return load_secrets(
        os.path.join(TWITCH_API_BASE_PATH, "twitch-api-app-secret.json")
    )


@load_secrets_once
def load_astra_secrets():
    cloud_config = {
        "secure_connect_bundle": os.path.join(
            ASTRA_BASE_PATH, "secure-connect-live-stream-data.zip"
        )
    }
    secrets = load_secrets(os.path.join(ASTRA_BASE_PATH, "live_stream_data-token.json"))
    return secrets, cloud_config


@load_secrets_once
def load_redis_secrets():
    return load_secrets(os.path.join(REDIS_BASE_PATH, "redis-secret.json"))


@load_secrets_once
def load_neon_secrets():
    return load_secrets(os.path.join(NEON_BASE_PATH, "neon-secret.json"))


@load_secrets_once
def load_cloudamqp_secrets():
    return load_secrets(os.path.join(CLOUDAMQP_BASE_PATH, "cloudamqp-secret.json"))


def get_twitch_api_client_id():
    return load_twitch_api_secrets()["clientId"]


def get_twitch_api_secret():
    return load_twitch_api_secrets()["secret"]


def get_astra_client_id():
    return load_astra_secrets()[0]["clientId"]


def get_astra_secret():
    return load_astra_secrets()[0]["secret"]


def get_astra_cloud_config():
    return load_astra_secrets()[1]


def get_redis_host_url():
    return load_redis_secrets()["host"]


def get_redis_host_port():
    return load_redis_secrets()["port"]


def get_redis_host_password():
    return load_redis_secrets()["password"]


def get_neon_url():
    return load_neon_secrets()["neonURL"]


def get_cloudamqp_url():
    return "localhost"  # load_cloudamqp_secrets()["cloudAMQPURL"]
