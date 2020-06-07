import os
import sys
import pickle
from moz_sql_parser import parse
from utils.SQLParser import SQLParser
from utils.ConvertDB import ConvertDB


class CLI:
    def __init__(self):
        pass

    @staticmethod
    def help():
        print("Usage sql2cypher [options]\n"
              "Listing of options: \n"
              "\t-s\t\tConvert sample sql into cypher\n"
              "\t-c\t\tConvert the mysql database into cypher\n"
              "")

    @staticmethod
    def transfer_sql():
        """
        transfer the sql to cypher
        :return:
        """
        print("Please input some sql languages: ")
        lines = sys.stdin.readlines()
        for sql in lines:
            sql_parser = SQLParser()
            sql_parser.generate_cypher(parse(sql))
            print(sql_parser.get_cypher())

    @staticmethod
    def convert_db():
        """
        convert the whole database in mysql
                db = "employees"
                user = "lsy"
                password = "li1998"
                cypher_user = "neo4j"
                cypher_password = "li1998"
        :return:
        """
        def set_config():
            """
            add some config value
            :return:
            """
            config = {}
            db = input("Please input the sql database which you want to convert: ")
            user = input("Please input the sql user which you want to convert: ")
            password = input("Please input the sql password which you want to convert: ")
            cypher_user = input("Please enter you cypher user: ")
            cypher_password = input("Please enter you cypher password: ")

            config = {
                'db': db,
                'user': user,
                'password': password,
                'cypher_user': cypher_user,
                'cypher_password': cypher_password
            }
            return config

        filepath = os.getcwd() + "/data/config.pickle"
        config = {}
        try:
            files = open(filepath, "rb")
            data = pickle.load(files)
            if type(data) is dict:
                config = data
        except FileNotFoundError:
            pass

        if len(config) != 0:
            default = input("Do you want to use the default value [y/n]: ")

            if default.lower() == 'n':
                config = set_config()
        else:
            config = set_config()

        files = open(filepath, "wb")
        pickle.dump(config, files)

        cb = ConvertDB(config['db'], config['user'], config['password'],
                       config['cypher_user'], config['cypher_password'])
        # print(cb.execute_sql("show tables", ()))
        # cb.read_relations()
        cb.export_tables()
