"""
This section code working on convert the whole database to cypher
"""
import os
import sys
import time
import pickle
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from neomodel import db,config

config.ENCRYPTED_CONNECTION = False


class ConvertDB:
    _neo4j_export_path = '/var/lib/neo4j/import'
    _cache_path = os.getcwd() + '/cache/'

    def __init__(self, db, user, password, cypher_user, cypher_password, cypher_ip=None, ip=None):
        # self.__neo4j_export_path = None
        self.db = db
        self.user = user
        self.delete_files = []
        self.password = password
        self.cypher_user = cypher_user
        self.cypher_password = cypher_password
        self.ip = ip if ip is not None else "localhost"
        self.cypher_ip = cypher_ip if cypher_ip is not None else "localhost"

    def _execute_cypher(self, query):
        """
        set up the cypher server
        db connect:
            db.set_connection('bolt://{}:{}@{}:7687'.format(self.cypher_user, self.cypher_password, self.cypher_ip))
        :return:
        """
        db.set_connection('bolt://{}:{}@{}:7687'.format(self.cypher_user, self.cypher_password, self.cypher_ip))
        db.cypher_query(query)

    def _execute_sql(self, query, args=()):
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

    def _load_pickle(self):
        """
        load pickle files
        :return: pickle info
        """
        filepath = self._cache_path + "/relation.pickle"
        try:
            files = open(filepath, "rb")
            data = pickle.load(files)
            if type(data) is list:
                return data
        except FileNotFoundError:
            return None
        return None

    def get_tables(self):
        """
        return all the tables in the db
        :return: all the tables name
        """
        # get all the tables as a node
        tables = self._execute_sql("SELECT table_name as id FROM "
                                  "information_schema.tables "
                                  "where table_schema='{}';".format(self.db))
        return tables

    def get_relations(self, only_table=False):
        """
        get all the relationship between tables
        :return: array of relations
        """
        if not only_table:
            query = "SELECT `TABLE_NAME`, `COLUMN_NAME`, `REFERENCED_TABLE_NAME`, `REFERENCED_COLUMN_NAME`"
        else:
            query = "SELECT `TABLE_NAME`,`REFERENCED_TABLE_NAME`"
        query += "FROM `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` WHERE `TABLE_SCHEMA` = SCHEMA() " \
                 "AND `REFERENCED_TABLE_NAME` IS NOT NULL;"
        relation_tables = self._execute_sql(query)

        return relation_tables

    def read_relations(self):
        """
        get the tables relation by user typing,
        the cache relation like: {
            'db name': {
                'src': {
                    'on': 'xx',
                    'dst': 'xx',
                    'dst_on': 'xxx',
                    'label': 'xxx'
                }
            }
        }
        :return: nothing
        """
        # get all the tables
        filepath = self._cache_path + "/relation.pickle"
        all_table = self._execute_sql("SHOW TABLES;")
        tables = []
        for t in all_table:
            tables.append(t['Tables_in_{}'.format(self.db)])

        # all the relations which stored in pickle
        data = self._load_pickle()
        # set the relation dict is empty if no data
        relation = {} if data is None else data

        # for this time which need to be export
        relationship = {}
        visited_tables = set()

        # read the relationship between tables
        relation_tables = self.get_relations()
        for rt in relation_tables:
            label = input(
                "Please enter the relation between {}->{}: ".format(rt['REFERENCED_TABLE_NAME'], rt['TABLE_NAME']))
            label = "{}_{}".format(rt['REFERENCED_TABLE_NAME'], rt['TABLE_NAME']) if label == "" else label

            if rt['REFERENCED_TABLE_NAME'] not in relationship:
                relationship[rt['REFERENCED_TABLE_NAME']] = []
            # it cloud have multiple relationship then use an array to store that
            relationship[rt['REFERENCED_TABLE_NAME']].append({
                'src_key': rt['COLUMN_NAME'],
                'dst': rt['TABLE_NAME'],
                'dst_key': rt['REFERENCED_COLUMN_NAME'],
                'label': label

            })
            visited_tables.add(rt['REFERENCED_TABLE_NAME'])
            visited_tables.add(rt['TABLE_NAME'])

        # add the single table
        for table in tables:
            if table not in visited_tables:
                print(table)
                relationship[table] = None

        # now try to solve the relation to pickle
        # print(relation)
        relation[self.db] = relationship
        files = open(filepath, "wb")
        pickle.dump(relation, files)

        return relationship

    def _load_with_csv(self, table_name, data):
        """
        load the data with csv model if the count less than 100000
        otherwise load with cypher model
        :return:
        """
        # export the csv file into neo4j export path
        cols = data[0].keys()
        df = pd.DataFrame(data, columns=cols)
        filepath = self._neo4j_export_path + '/{}.csv'.format(table_name)
        df.to_csv(filepath, index=False)

        query = "LOAD CSV WITH HEADERS FROM 'file:///{}.csv' AS row ".format(table_name)
        table_schema = self._execute_sql("show columns from %s;" % table_name)

        # get the primary key
        primary_key = self._execute_sql("show columns from {} where `Key` = 'PRI';".format(table_name))
        if len(primary_key) != 0:
            primary_key = primary_key[0]['Field']
        else:
            # raise ValueError("The table {} does not have primary key".format(key))
            # if can not find primary key
            primary_key = self._execute_sql("show columns from {};".format(table_name))[0]['Field']

        query += " MERGE ({}:{} ".format(str(table_name).lower(), table_name)
        query += "{"
        query += "{}: row.{}, ".format(primary_key, primary_key)

        del table_schema[0]
        query += ", ".join("{}: coalesce(row.{}, \"Unknown\")".format(name['Field'], name['Field'])
                           for index, name in enumerate(table_schema))
        query += "}); "
        self.delete_files.append(filepath)
        return query

    def _load_with_cypher(self, table_name, data):
        """
        load with cypher query if the count >= 100000
        load with a small group size can be used to finish the process
        :return:
        """
        total = len(data)
        for index, row in enumerate(data):
            query = "CREATE ({}:{} ".format(table_name, table_name)
            query += "{ "
            query += ", ".join(
                "{}: \"{}\"".format(col, str(row[col]).replace('"', '\'').replace('&', '&.').replace('""', '\'')) for
                col in row)
            query += "});"
            self._progress(index, total, status='Generating cypher query for table: {}'.format(table_name))
            # db.run(query)
            # print(query)

    def _isvalid_load(self, table_name, data):
        """
        check whether data is valid and then load the data into the database
        :param data:
        :return:
        """
        if len(data) == 0:
            raise ValueError("Please insert at least one data in your table")
        if len(data) >= 100000:
            self._execute_cypher("MATCH ({}:{})DETACH DELETE {};".format(str(table_name).lower(),
                                                                         table_name, str(table_name).lower()))
            self._load_with_cypher(table_name, data)
        else:
            return self._load_with_csv(table_name, data)

        return None

    def _progress(self, count, total, status=''):
        """
        add the process bar for python
        :param count:
        :param total:
        :param status:
        :return:
        """
        bar_len = 60
        filled_len = int(round(bar_len * count / float(total)))
        percents = round(100.0 * count / float(total), 1)
        bar = '=' * filled_len + '-' * (bar_len - filled_len)

        sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
        sys.stdout.flush()

    def export_tables(self):
        """
        export the table data into csv ready to load into database by using two methods
        1. load csv,
        2. load with cypher query
        :return:
        """

        export_tables = self.read_relations()
        cypher_query = []
        # to record whether the tables data already converted
        exported = []
        print("Starting export csv files for TABLES! Please wait for a while ...")

        print(export_tables)
        # table is the key name
        for table in export_tables:
            if export_tables[table] is None:
                data = self._execute_sql("SELECT * FROM %s;" % table)
                if len(data) == 0:
                    raise ValueError("Please insert at least one data in your table")

                exported.append(table)
                # remove the old data firstly
                self._execute_cypher(
                    "MATCH ({}:{}) DETACH DELETE {};".format(str(table).lower(), table, str(table).lower()))
                # if the dataset is too large then use cypher query to load
                if len(data) >= 100000:
                    self._load_with_cypher(table, data)
                else:
                    query = self._load_with_csv(table, data)
                    cypher_query.append(query)
            else:
                # means it should have some relation, key means table name
                src = table
                if src not in exported:
                    src_data = self._execute_sql("SELECT * FROM %s;" % src)
                    result = self._isvalid_load(src, src_data)
                    if result is not None:
                        cypher_query.append(result)
                # to record the converted table
                exported.append(src)

                for t in export_tables[table]:
                    dst = t['dst']
                    src_key = t['src_key']
                    dst_key = t['dst_key']
                    label = t['label']

                    if dst not in exported:
                        dst_data = self._execute_sql("SELECT * FROM %s;" % dst)
                        result = self._isvalid_load(dst, dst_data)
                        if result is not None:
                            cypher_query.append(result)

                    exported.append(dst)
                    query = "MATCH ({}:{}), ".format(str(src).lower(), src)
                    query += "({}:{}) ".format(str(dst).lower(), dst)
                    query += "WHERE {}.{} = {}.{} ".format(src, src_key, dst, dst_key)
                    query += "MERGE ({})-[r:{}]->({})".format(str(src).lower(), label, str(dst).lower())

                    cypher_query.append(query)

        # add progress bar in the terminal
        total = len(cypher_query)
        for index, cypher in enumerate(cypher_query):
            self._progress(index, total, status='Execute cypher query')
            print("Execute: {}".format(cypher))
            # self._execute_cypher(cypher)
        print("Export finished!")

        # after exporting the data then delete the csv files which cached in the neo4j directory
        print("Start cleaning the cache file...")
        for file in self.delete_files:
            os.remove(file)
