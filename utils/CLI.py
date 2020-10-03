import os
import sys
from moz_sql_parser import parse
from utils.SQLParser import SQLParser
from utils.ConvertDB import ConvertDB
from configparser import ConfigParser, ParsingError, NoSectionError


class CLI:
    _config_path = "conf/db.ini"

    def __init__(self):
        self.config = self._load_config()
        self.cb = self._load_convert()

    def _load_config(self):
        """
        load the config file. Set it as private function
        :return: the config Parser
        """
        try:
            config = ConfigParser()
            config.read(self._config_path)
            return config
        except ParsingError as err:
            raise FileNotFoundError("Can not find config file in ./conf/db.ini")

    def _load_convert(self):
        try:
            mysql_config = self.config["mysql"]
            neo4j_config = self.config["neo4j"]
        except NoSectionError as err:
            print("Can not find the section {} in db.ini".format(err))
            raise KeyError(err.section)
        finally:
            print("Something wrong with the config")

        cb = ConvertDB(mysql_config['database_name'], mysql_config['username'], mysql_config['password'],
                       neo4j_config['username'], neo4j_config['password'])
        return cb

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
            sql_parser.generate_cypher(parse(sql), sql)
            print(sql_parser.get_cypher())

    def convert_db(self):
        """
        convert the whole database in mysql
                db = "employees"
                user = "lsy"
                password = "li1998"
                cypher_user = "neo4j"
                cypher_password = "li1998"
        :return:
        """
        # print(cb.execute_sql("show tables", ()))
        # cb.read_relations()
        self.cb.export_tables()
