"""
Connection to RabbitMQ server management
"""
from pyrabbit.api import Client

CONNECTION = None


def get_connection():
    global CONNECTION
    if CONNECTION is None or not CONNECTION.is_alive():
        CONNECTION = Connection.connect_rabbit_server()
    if not CONNECTION.is_alive():
        raise Exception("Connection is not alive")
    return CONNECTION


def close_connection():
    pass


class Connection(object):

    @staticmethod
    def connect_rabbit_server():
        conn = Client('localhost:15672', 'guest', 'guest')
        return conn