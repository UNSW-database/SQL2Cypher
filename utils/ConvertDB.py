"""
This section code working on convert the whole database to cypher
"""
import os
import pickle
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
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
        self.filepath = os.getcwd() + "/data/"
        self.export_path = '/var/lib/neo4j/import'
        # self.export_path = '/home/heldon/Directory/neo4j-community-4.0.4/import'

    def execute_cypher(self, query):
        """
        set up the cypher server
        db connect:
            db.set_connection('bolt://{}:{}@{}:7687'.format(self.cypher_user, self.cypher_password, self.cypher_ip))
        :return:
        """
        db.set_connection('bolt://{}:{}@{}:7687'.format(self.cypher_user, self.cypher_password, self.cypher_ip))
        db.cypher_query(query)

    def execute_sql(self, query, args=()):
        """
        execute the sql query language
        :param query: sql query language
        :param args: args in sql
        :return: all the values get from db
        """
        try:
            mydb = mysql.connector.connect(
                host=self.ip,
                user=self.user,
                password=self.password,
                database=self.db,
                auth_plugin='mysql_native_password'
            )
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                raise ValueError("Something is wrong with your user name or password!")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                raise IOError("Database dose not exist!")
            else:
                raise ValueError("err")

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
            SELECT `TABLE_NAME`, `COLUMN_NAME`, `REFERENCED_TABLE_NAME`, `REFERENCED_COLUMN_NAME`
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
            re = input(
                "Please enter the relation between {}->{}: ".format(rt['REFERENCED_TABLE_NAME'], rt['TABLE_NAME']))
            re = "{}_{}".format(rt['REFERENCED_TABLE_NAME'], rt['TABLE_NAME']) if re == "" else re

            r[rt['REFERENCED_TABLE_NAME']] = {
                'to': rt['TABLE_NAME'],
                't1_on': rt['COLUMN_NAME'],
                't2_on': rt['REFERENCED_COLUMN_NAME'],
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
        LOAD CSV FROM 'file:///data.csv' AS row
        WITH row[0] AS Code, row[1] AS Name, row[2] AS Address, row[3] AS Zip, row[4] as Country
        MERGE (company:Company {Code: Code})
          SET company.Name = Name, company.Address = Address, company.Zip = Zip, company.Country = Country
        :return:
        """

        def generate_cypher(key):
            """
            generate the comment part for cypher
            :param key:
            :return:
            """
            data = self.execute_sql("SELECT * FROM %s LIMIT 10;" % key)
            if len(data) == 0:
                raise ValueError("Please insert at least one data in your table")
            cols = data[0].keys()
            df = pd.DataFrame(data, columns=cols)
            df.to_csv(self.export_path + '/{}.csv'.format(key), index=False)

            query = "LOAD CSV FROM 'file:///{}.csv' AS row WITH ".format(key)
            table_schema = self.execute_sql("show columns from %s;" % key)

            query += ", ".join("row[{}] AS {}".format(index, name['Field'])
                               for index, name in enumerate(table_schema))

            # get the primary key
            primary_key = self.execute_sql("show columns from {} where `Key` = 'PRI';".format(key))
            if len(primary_key) != 0:
                primary_key = primary_key[0]['Field']
            else:
                # raise ValueError("The table {} does not have primary key".format(key))
                # if can not find primary key
                primary_key = self.execute_sql("show columns from {};".format(key))[0]['Field']

            query += " MERGE ({}:{} ".format(str(key).lower(), key)
            query += "{"
            query += "{}: {} ".format(primary_key, primary_key)
            query += "}) SET "
            # query += "MERGE ({}:{} {{}: {}}) SET ".format(str(key).lower(), key, primary_key, primary_key)
            del table_schema[0]
            query += ", ".join("{}.{} = {}".format(str(key).lower(), name['Field'], name['Field'])
                               for index, name in enumerate(table_schema))
            return query

        export_tables = self.read_relations()
        cypher_query = []
        print("Starting export csv files for TABLES! Please wait for a while ...")

        for table in export_tables:
            key = list(table.keys())[0]
            if table[key] is None:
                query = generate_cypher(key)
                self.execute_cypher("MATCH ({}:{})DETACH DELETE {};".format(str(key).lower(), key, str(key).lower()))
                cypher_query.append(query)
            else:
                # means it should have some relation, key means table name
                t1 = key
                t2 = table[key]['to']
                t1_on = table[key]['t1_on']
                t2_on = table[key]['t2_on']
                relation = table[key]['relation']

                q1 = generate_cypher(t1)
                q2 = generate_cypher(t2)
                cypher_query.append(q1)
                cypher_query.append(q2)

                primary_key_t1 = self.execute_sql("show columns from {} where `Key` = 'PRI';".format(t1))
                if len(primary_key_t1) != 0:
                    primary_key_t1 = primary_key_t1[0]['Field']

                primary_key_t2 = self.execute_sql("show columns from {} where `Key` = 'PRI';".format(t2))
                if len(primary_key_t1) != 0:
                    primary_key_t2 = primary_key_t2[0]['Field']

                data = self.execute_sql("SELECT {}.{}, {}.{} FROM {}, {} WHERE {}.{} = {}.{} LIMIT 10".
                                        format(t1, primary_key_t1, t2, primary_key_t2, t1, t2, t1, t1_on, t2, t2_on))

                cols = data[0].keys()
                df = pd.DataFrame(data, columns=cols)
                df.to_csv(self.export_path + '/{}_{}.csv'.format(t1, t2), index=False)

                query = "LOAD CSV WITH HEADERS FROM 'file:///{}_{}.csv' AS row ".format(t1, t2)

                query += "MATCH ({}:{} ".format(str(t1).lower(), t1)
                query += "{"

                query += "{}: row.{}".format(primary_key_t1, primary_key_t1)
                query += "}) "

                query += "MATCH ({}:{} ".format(str(t2).lower(), t2)
                query += "{"

                query += "{}: row.{}".format(primary_key_t2, primary_key_t2)
                query += "}) "

                query += "MERGE ({})-[r:{}]->({})".format(str(t1).lower(), relation, str(t2).lower())

                cypher_query.append(query)
                self.execute_cypher("MATCH ({}:{})DETACH DELETE {};".format(str(t1).lower(), t1, str(t1).lower()))
                self.execute_cypher("MATCH ({}:{})DETACH DELETE {};".format(str(t2).lower(), t2, str(t2).lower()))

        for cypher in cypher_query:
            print("Execute: {}".format(cypher))
            self.execute_cypher(cypher)
        print("Export finished!")
