import os
import json
import requests
import logging
import ssl
from pymongo import MongoClient

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s/%(module)s @ %(threadName)s: %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class ConfigurationException(Exception):
    pass


def parse_bool(s):
    if s is None:
        return False
    if isinstance(s, bool):
        return s
    return s.lower() == 'true'


class Repository(object):
    def __init__(self, api_root, api_key):
        self.api_root = api_root
        self.api_key = api_key

    def get_connection(self, repository_id):
        headers = {'X-CogScale-Key': self.api_key}
        r = requests.get("%s/v1/repository/%s/connection" % (self.api_root, repository_id), headers=headers)
        r.raise_for_status()
        return r.json()

    def get_client(self, repository_id):
        conn = self.get_connection(repository_id)
        return Repository._create_client(conn)

    def get_database(self, repository_id):
        conn = self.get_connection(repository_id)
        db_name = conn["database"]
        return Repository._create_client(conn)[db_name]

    @staticmethod
    def _create_client(conn):
        if conn.get("username") && conn.get("password"):
            mongo_uri = "mongodb://{}:{}@{}:{}/{}".format(
                  conn["username"]
                , conn["password"]
                , conn["server"]["host"]
                , conn["server"]["port"]
                , conn["database"]
            )
        else:
            mongo_uri = "mongodb://{}:{}/{}".format(
                  conn["server"]["host"]
                , conn["server"]["port"]
                , conn["database"]
            )

        opts = conn["server"].get("options", {})
        if parse_bool(opts.get("ssl", False)):
            return MongoClient(mongo_uri, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
        else:
            return MongoClient(mongo_uri)


class Observers(object):
    def __init__(self, api_root, api_key, observers):
        self.api_root = api_root
        self.api_key = api_key
        self.observers = observers or {}

    def on_error(self, error):
        error_observers = self.observers.get('error')
        if error_observers:
            for queue in error_observers:
                self._post_message(queue, error)

    def on_completion(self, msg):
        completion_observers = self.observers.get('completion')
        if completion_observers:
            for queue in completion_observers:
                self._post_message(queue, msg)

    def _post_message(self, queue, msg):
        queue_msg = {
            "messages": [{
                "body": json.dumps(msg)
            }]
        }

        headers = {'X-CogScale-Key': self.api_key}

        r = requests.post("%s/v1/queues/%s/messages" % (self.api_root, queue), json=queue_msg, headers=headers)
        if r.status_code != 201:
            logger.error('Observer failed to post message to queue %s; error was "%s"' % (queue, r.text))


class Foundation(object):
    """
    Create a Foundation connection.

    Configuration is loaded in the following order:
    1. Keyword args passed here
    2. Payload (PAYLOAD_FILE)
    3. Config JSON (CONFIG_FILE) set at job deploy time
    4. ENV variables set at job deploy time

    Keywords override payload, payload overrides config JSON, config JSON overrides environment.
    """
    def __init__(self, **kwargs):
        payload = Foundation._load_json_from_env('PAYLOAD_FILE')
        config = Foundation._load_json_from_env('CONFIG_FILE')

        self.api_root = kwargs.get('foundation_api_root',
                                   payload.get('foundation_api_root',
                                               config.get('foundation_api_root', os.getenv('FOUNDATION_API_ROOT'))))

        if not self.api_root:
            raise ConfigurationException(
                'Foundation API root not configured: see http://docs.foundation.insights.ai for details')

        self.api_key = kwargs.get('foundation_api_key',
                                  payload.get('foundation_api_key',
                                              config.get('foundation_api_key', os.getenv('FOUNDATION_API_KEY'))))

        if not self.api_key:
            raise ConfigurationException(
                'Foundation API key not configured: see http://docs.foundation.insights.ai for details')

    def repository(self):
        return Repository(self.api_root, self.api_key)

    def observers(self, observers):
        return Observers(self.api_root, self.api_key, observers)

    @staticmethod
    def _load_json_from_env(env_var):
        file_path = os.getenv(env_var)
        if file_path and os.path.isfile(file_path):
            with open(file_path) as f:
                return json.load(f)
        else:
            return {}
