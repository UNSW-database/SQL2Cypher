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

        print("\nThe function can not detect the relation between tables.\n"
              "Please enter the relation in format: a->b: relationship, a<->b: relationship. There are the tables: \n")
        for table in tables:
            print(table)

        relation = {}
        try:
            files = open(filepath, "rb")
            data = pickle.load(files)
            print(data)
            if type(data) is dict:
                relation = data
        except FileNotFoundError:
            pass

        lines = sys.stdin.readlines()
        for line in lines:
            t = line.split("<->")
            # one to one relation
            if len(t) != 0 and "<->" in line:
                # r.0 is table name r.1 is relation name
                t1 = t[0]
                r = t[1].split(": ")
                t2 = r[0]
                r = r[1]
                relation[t1] = {
                    t2: r
                }
                relation[t2] = {
                    t2: r
                }
            else:
                t = line.split("->")
                if len(t) == 0:
                    raise ValueError("Incorrect relation type")
                r = t[1].split(": ")
                relation[t[0]] = {
                    r[0]: r[1]
                }

        # now try to solve the relation to pickle
        files = open(filepath, "wb")
        pickle.dump(relation, files)


