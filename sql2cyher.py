"""
init the python file
Author: Hongtai Zhang & Shunyang Li
Date: 21/05/2020
Description:
    Convert SQL to cypher
"""
import sys
from moz_sql_parser import parse
from utils.SQLParser import SQLParser
from utils.ConvertDB import ConvertDB

if __name__ == '__main__':
    # sql = "SELECT company.* FROM Company as company;"
    convert_type = input("You can choose the format of converting sql to cypher: \n"
                         "\t1. Simple convert without join table\n"
                         "\t2. Convert the whole database to cypher\n")

    if convert_type not in ['1', '2'] or len(convert_type) > 1:
        raise ValueError("Incorrect number!")

    if convert_type == '1':
        print("Please input some sql: ")
        lines = sys.stdin.readlines()
        for sql in lines:
            sql_parser = SQLParser()
            sql_parser.generate_cypher(parse(sql))
            print(sql_parser.get_cypher())
    else:
        # db = input("Please input the sql database which you want to convert: ")
        # user = input("Please input the sql user which you want to convert: ")
        # password = input("Please input the sql password which you want to convert: ")
        # cypher_user = input("Please enter you cypher user: ")
        # cypher_password = input("Please enter you cypher password: ")
        db = "test"
        user = "lsy"
        password = "li1998"
        cypher_user = "neo4j"
        cypher_password = "li1998"

        cb = ConvertDB(db,user,password,cypher_user,cypher_password)
        print(cb.execute_sql("show tables", ()))
