"""
init the python file
Author: Hongtai Zhang & Shunyang Li
Date: 21/05/2020
Description:
    Convert SQL to cypher
"""
import json
from moz_sql_parser import parse


def handle_select_from(data, handle_type="FROM"):
    """
    handle the from data, because it includes many from values
    :param data: dict data or list
    :param handle_type: the handle type it only can be `select` or `from`
    :return:
        a string contain the match data
    """
    # if it was select then return `return` else `MATCH`
    result = "MATCH" if handle_type == "FROM" else "RETURN"
    if data is None:
        raise ValueError("The target can not be None")

    if type(data) == dict:
        # if the type of data is dict that means it only has one source
        # assume the select format: from xx as x
        result += " ({}:{})".format(data['name'], data['value']) if handle_type == "FROM" \
            else " {}".format(data['value'])

    if type(data) == list:
        # if the type of data is list that means it has more than one source
        pass

    return result


def parse_sql(query):
    """
    parse the sql query into ast tree format
    assume the sql language format:
        SELECT p.* FROM Product as p;
    :param query: sql language
    :return:
        list of elements of sql
    """
    result = ""
    # because we need to check the format of sql
    try:
        data = parse(query)
    except ValueError:
        raise ValueError("The sql query incorrect, please check the format for sql")
    result = handle_select_from(data['from']) + " "
    result += handle_select_from(data['select'], handle_type="SELECT")

    print(result)


if __name__ == '__main__':
    sql = "SELECT p.* FROM products as p;"
    # sql = "SELECT p.ProductName, p.UnitPrice FROM products AS p WHERE p.ProductName = 'Chocolade'; "
    # sql = "SELECT p.ProductName, sum(od.UnitPrice * od.Quantity) AS Volume FROM customers AS c LEFT OUTER JOIN orders AS o ON (c.CustomerID = o.CustomerID) LEFT OUTER JOIN order_details AS od ON (o.OrderID = od.OrderID) LEFT OUTER JOIN products AS p ON (od.ProductID = p.ProductID) WHERE c.CompanyName = 'Drachenblut Delikatessen' GROUP BY p.ProductName ORDER BY Volume DESC;"
    parse_sql(sql)
