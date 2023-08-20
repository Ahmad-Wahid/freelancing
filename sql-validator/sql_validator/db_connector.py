from configparser import ConfigParser
import psycopg2
import os


def read_config():
    config = ConfigParser()
    config_path = os.path.expanduser("~/config.ini")
    config.read(config_path)
    return config


def connect_db():
    try:
        config_data = read_config()
        if "database" in config_data:
            database = config_data.get("database", "name")
            user = config_data.get("database", "user")
            password = config_data.get("database", "password")
            host = config_data.get("database", "host", fallback="localhost")
            port = config_data.get("database", "port", fallback="5432")

            conn = psycopg2.connect(
                database=database, user=user, password=password, host=host, port=port
            )
            return conn
        else:
            print("Database section not found in config.ini")
    except Exception as e:
        print("An error occurred:", e)
