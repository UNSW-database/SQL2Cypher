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

if __name__ == '__main__':
    # sql = "SELECT company.* FROM Company as company;"
    lines = sys.stdin.readlines()
    for sql in lines:
        sql_parser = SQLParser()
        sql_parser.generate_cypher(parse(sql))
        print(sql_parser.get_cypher())
