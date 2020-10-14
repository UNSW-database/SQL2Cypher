"""
init the python file
Author: Hongtai Zhang & Shunyang Li
Date: 21/05/2020
Description:
    Convert SQL to cypher
"""
import os
import sys
import argparse
from view import app
import view.route
from utils.CLI import CLI

if __name__ == '__main__':
    # sql = "SELECT company.* FROM Company as company;"
    # get the command line
    parser = argparse.ArgumentParser(description="Welcome to use sql2cypher for managing your database",
                                     epilog="Enjoy the middleware")
    parser.add_argument('--translate', '-t', action="store_true", help="Translate SQL query to Cypher query")
    parser.add_argument('--migrate', '-m', action="store_true", help="Migrate RDBMS to Graph database")
    parser.add_argument('--database', '-db', help="The specific relational database(mysql or psql). "
                                                  "The default value is mysql")
    parser.add_argument('--load_method', '-lm', help="The load method, provide csv and cypher. "
                                                     "The default method is csv and cypher together")
    parser.add_argument('--clean_cache', '-cc', help="To clean the cache information")
    parser.add_argument('--web_ui', '-wu', action="store_true", help="Start the web ui model")
    args = parser.parse_args()

    if len(sys.argv) < 2:
        parser.print_help()

    supports = ['mysql', 'psql']
    if args.migrate:
        db = args.database
        db_name = 'mysql'
        if db is not None and db in supports:
            db_name = db
        print(db_name)
        cli = CLI(db_name=db_name)
        cli.convert_db()
        sys.exit()

    if args.translate:
        cli = CLI()
        cli.transfer_sql()
        sys.exit()

    if args.clean_cache is not None:
        os.remove(os.getcwd() + '/cache/relation.pickle')

    if args.web_ui:
        app.run()
    # if len(sys.argv) > 2:
    #     print("Please have a look the help: python3 sql2cypher.py --help")
    #     exit()
    #
    # command = sys.argv[1]
    # cli = CLI()
    # # cli.cb.get_tables()
    # if command == "--help":
    #     cli.help()
    # elif command == "-t":
    #     cli.transfer_sql()
    # elif command == "-m":
    #     cli.convert_db()
    # else:
    #     raise ValueError("Invalid command")

