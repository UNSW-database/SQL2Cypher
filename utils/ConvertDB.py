"""
This section code working on convert the whole database to cypher
"""
import os
import sys
import pickle
import mysql.connector
from neomodel import db


class ConvertDB:
    def __init__(self, db, user, password, cypher_user, cypher_password, cypher_ip=None, ip=None):
        self.db = db
        self.user = user
        self.password = password
        self.cypher_user = cypher_user
        self.cypher_password = cypher_password
        self.ip = ip if ip is not None else "localhost"
        self.cypher_ip = cypher_ip if cypher_ip is not None else "localhost"
        self.set_up_cypher()

    def set_up_cypher(self):
        """
        set up the cypher server
        :return:
        """
        db.set_connection('bolt://{}:{}@{}:7687'.format(self.cypher_user, self.cypher_password, self.cypher_ip))

    def execute_sql(self, query, args=()):
        """
        execute the sql query language
        :param query: sql query language
        :param args: args in sql
        :return: all the values get from db
        """
        mydb = mysql.connector.connect(
            host=self.ip,
            user=self.user,
            password=self.password,
            database=self.db,
            auth_plugin='mysql_native_password'
        )

        mycursor = mydb.cursor()

        mycursor.execute(query, args)
        res = [dict((mycursor.description[idx][0], value)
                    for idx, value in enumerate(row)) for row in mycursor.fetchall()]
        return res

    def read_relations(self):
        """
        get the tables relation by user typing
        :return: nothing
        """
        # get all the tables
        filepath = os.getcwd() + "/data/relation.pickle"
        all_table = self.execute_sql("SHOW TABLES;")
        tables = []
        for t in all_table:
            for k, v in t.items():
                tables.append(t[k])

        query = """
            SELECT `TABLE_NAME`,  `REFERENCED_TABLE_NAME`, `REFERENCED_COLUMN_NAME`
            FROM `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` 
            WHERE `TABLE_SCHEMA` = SCHEMA() 
                AND `REFERENCED_TABLE_NAME` IS NOT NULL;
        """

        relation = []
        try:
            files = open(filepath, "rb")
            data = pickle.load(files)
            print(data)
            if type(data) is list:
                relation = data
        except FileNotFoundError:
            pass

        visited_tables = set()
        relation_tables = self.execute_sql(query)
        for rt in relation_tables:
            r = {}
            re = input("Please enter the relation between {}->{}: ".format(rt['REFERENCED_TABLE_NAME'], rt['TABLE_NAME']))
            re = "{}_{}".format(rt['REFERENCED_TABLE_NAME'], rt['TABLE_NAME']) if re is "" else re

            r[rt['REFERENCED_TABLE_NAME']] = {
                'to': rt['TABLE_NAME'],
                'on': rt['REFERENCED_COLUMN_NAME'],
                'relation': re
            }
            visited_tables.add(rt['REFERENCED_TABLE_NAME'])
            visited_tables.add(rt['TABLE_NAME'])
            relation.append(r)

        for table in tables:
            r = {}
            if table not in visited_tables:
                r[table] = None
                relation.append(r)

        # now try to solve the relation to pickle
        files = open(filepath, "wb")
        pickle.dump(relation, files)


