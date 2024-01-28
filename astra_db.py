from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import auth.secrets as secrets

def get_session(keyspace):
    auth_provider = PlainTextAuthProvider(secrets.get_chat_db_client_id(), secrets.get_chat_db_secret())
    cluster = Cluster(cloud=secrets.get_astra_db_cloud_config(), auth_provider=auth_provider)
    session = cluster.connect(keyspace)
    return session