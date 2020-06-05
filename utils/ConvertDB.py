"""
This section code working on convert the whole database to cypher
"""
import os
import pickle
import pandas as pd
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
        self.filepath = os.getcwd() + "/data/"
        self.export_path = '/var/lib/neo4j/import'

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

        mydb.commit()
        mydb.close()
        return res

    def load_pickle(self):
        """
        load pickle files
        :return: pickle info
        """
        filepath = self.filepath + "/relation.pickle"
        try:
            files = open(filepath, "rb")
            data = pickle.load(files)
            if type(data) is list:
                return data
        except FileNotFoundError:
            return None
        return None

    def read_relations(self):
        """
        get the tables relation by user typing
        :return: nothing
        """
        # get all the tables
        filepath = self.filepath + "/relation.pickle"
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

        # all the relations which stored in pickle
        data = self.load_pickle()
        relation = [] if data is None else data

        # for this time which need to be export
        export_tables = []
        visited_tables = set()
        relation_tables = self.execute_sql(query)
        for rt in relation_tables:
            r = {}
            re = input("Please enter the relation between {}->{}: ".format(rt['REFERENCED_TABLE_NAME'], rt['TABLE_NAME']))
            re = "{}_{}".format(rt['REFERENCED_TABLE_NAME'], rt['TABLE_NAME']) if re == "" else re

            r[rt['REFERENCED_TABLE_NAME']] = {
                'to': rt['TABLE_NAME'],
                'on': rt['REFERENCED_COLUMN_NAME'],
                'relation': re
            }
            visited_tables.add(rt['REFERENCED_TABLE_NAME'])
            visited_tables.add(rt['TABLE_NAME'])
            export_tables.append(r)

        for table in tables:
            r = {}
            if table not in visited_tables:
                r[table] = None
                export_tables.append(r)

        # now try to solve the relation to pickle
        relation += export_tables
        files = open(filepath, "wb")
        pickle.dump(relation, files)

        return export_tables

    def export_tables(self):
        """
        export the table data into csv ready to load into database
        :return:
        """
        # export_tables = self.read_relations()
        export_tables = [{'employees': None}]
        for table in export_tables:
            key = list(table.keys())[0]
            if table[key] is None:
                # it means the table is independent
                data = self.execute_sql("SELECT * FROM %s;" % key)
                cols = data[0].keys()
                df = pd.DataFrame(data, columns=cols)
                df.to_csv(self.export_path + '/{}.csv'.format(key), index=False)
            else:
                # means it should have some relation
                pass




