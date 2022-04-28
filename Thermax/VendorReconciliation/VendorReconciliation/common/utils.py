import os
import sys
import json

class clsDbConfig:
    _DB_SERVER_NAME = ""
    _DB_USER_NAME = ""
    _DB_PASSWORD = ""
    _DB_DATABASE_NAME = ""
    _DB_PORT = ""
    _DB_ENGINE = ""


    def __init__(self, db_file, module):
        try:
            if os.path.exists(db_file):
                print("Reading DB Config File:", db_file)
                self.read_config(db_file, module)
            else:
                print("Error!! Database config file not found..", db_file)
        except Exception as e:
            print(e)
            sys.exit()

    def read_config(self, db_file, module):
        try:
            with open(db_file, "r") as f:
                config_items =json.load(f)

            for k,v in config_items.items():
                for items in v:
                    if items["module"] == module:
                        credentials = items['credentials']
                        self._DB_SERVER_NAME = credentials['server']
                        self._DB_USER_NAME = credentials['user']
                        self._DB_PASSWORD = credentials['password']
                        self._DB_DATABASE_NAME = credentials['database']
                        self._DB_PORT = credentials['port']
                        self._DB_ENGINE = credentials['engine']

            print("DB Settings initialized for module:", module)

        except Exception as e:
            print(e)

    def server(self):
        return self._DB_SERVER_NAME

    def user(self):
        return  self._DB_USER_NAME

    def password(self):
        return self._DB_PASSWORD

    def port(self):
        return self._DB_PORT

    def dbname(self):
        return self._DB_DATABASE_NAME

    def engine(self):
        return self._DB_ENGINE