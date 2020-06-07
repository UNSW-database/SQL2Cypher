"""
init the python file
Author: Hongtai Zhang & Shunyang Li
Date: 21/05/2020
Description:
    Convert SQL to cypher
"""
import sys
from utils.CLI import CLI

if __name__ == '__main__':
    # sql = "SELECT company.* FROM Company as company;"
    # get the command line
    if len(sys.argv) > 2:
        print("Please have a look the help: python3 sql2cypher.py --help")
        exit()

    command = sys.argv[1]
    cli = CLI()
    if command == "--help":
        cli.help()
    elif command == "-s":
        cli.transfer_sql()
    elif command == "-c":
        cli.convert_db()
    else:
        raise ValueError("Invalid command")

