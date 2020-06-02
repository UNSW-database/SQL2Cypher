"""
This section code working on convert the whole database to cypher
"""
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

