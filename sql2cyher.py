"""
init the python file
Author: Hongtai Zhang & Shunyang Li
Date: 21/05/2020
Description:
    Convert SQL to cypher
"""
import sys
from moz_sql_parser import parse


def handle_select_from(data, from_type=True):
    """
    handle the from data, because it includes many from values
    :param data: dict data or list
    :param from_type: the handle type it only can be `select` or `from`
    :return:
        a string contain the match data
    """
    # if it was select then return `return` else `MATCH`
    result = "MATCH " if from_type else "RETURN "
    if data is None:
        raise ValueError("The target can not be None")

    if type(data) == dict:
        # if the type of data is dict that means it only has one source
        # assume the select format: from xx as x
        # result += "({}:{})".format(data['name'], data['value']) if from_type \
        #     else "{}".format(data['value'])
        if from_type:
            result += "({}:{})".format(data['name'], data['value'])
        else:
            # if type data is dict, that means it should be keywords
            if type(data['value']) == dict:
                result += ",".join(" {} {} ".format(str(key).upper(), str(value).replace('.*', ''))
                                   for key, value in data['value'].items())
            else:
                result += "{}".format(str(data['value']).replace('.*', ''))

    if type(data) == list:
        # if the type of data is list that means it has more than one source
        if not from_type:
            values = [x['value'] for x in data]
            result += ", ".join(values)
        else:
            pass

    return result


def and_or_case(data, operators, keyword='AND'):
    """
    handle and case for the where condition
    :param data: list
    :param operators: the operations, eg: >, <, >=, <=, =, !=
    :param keyword: AND, OR
    :return:
        A string of query language
    """
    result = ""
    for case in data:
        if result != "":
            result += keyword
        for key, value in case.items():
            # get the operation first
            ope = operators[key]
            # value is a list type
            values = []
            find_literal = False
            for v in value:
                if type(v) != dict:
                    values.append(v)
                else:
                    values.append(v['literal'])
                    find_literal = True
            result += " {} {} {} ".format(values[0], ope, values[1]) if not find_literal \
                else " {} {} '{}' ".format(values[0], ope, values[1])
    return result


def handle_in_case(data):
    """
    handle in case in where conditions
    eg: {'in': ['p.ProductName', {'literal': ['a', 'b']}]}
    ['p.ProductName', [1, 2]]}
    when index = 0, it is a keyword
    :param data: dict type data
    :return:
        A string
    """
    values = []
    result = "{} IN [".format(data[0])
    is_literal = False
    for i in range(1, len(data)):
        if type(data[i]) != dict:
            values = [i for i in data[i]]
        else:
            is_literal = True
            values = [i for i in data[i]['literal']]

    if is_literal:
        result += ", ".join("'{}'".format(i) for i in values)
    else:
        result += ", ".join(str(i) for i in values)

    return result + "] "


def handle_where(data):
    """
    handle where query
    we need to consider the condition is and or or
    and is >, <, >=, <=, =, !=
    and the where dict is like 'and': [], 'or': []
    :param data: dict data or list
    :return:
        A string
    """
    result = "WHERE "
    operators = {
        'neq': '!=',
        'eq': '=',
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<='
    }

    for key in data:
        if str(key).lower() == 'and':
            # handle and case
            result += and_or_case(data[key], operators)
        elif str(key).lower() == 'or':
            # handle or case
            result += and_or_case(data[key], operators, keyword='OR')
        elif str(key).lower() == 'in':
            # handle in case
            result += handle_in_case(data[key])
    return result


def handle_order(data):
    """
    handle order by
    :param data: list or dict
    :return:
        A string
    """
    if data is None:
        return ""
    result = " ORDER BY"
    # means only one order by
    if type(data) == dict:
        result += " {} {}".format(data['value'], data['sort'].upper())
    else:
        result += ",".join(" {} {}".format(order['value'], order['sort'].upper()) for order in data)

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
        # print(data)
    except ValueError:
        raise ValueError("The sql query incorrect, please check the format for sql")
    result = handle_select_from(data['from']) + " "
    if "where" in data:
        result += handle_where(data['where'])
    result += handle_select_from(data['select'], from_type=False)

    # handle order by
    result += handle_order(data['orderby']) if 'orderby' in data else ""
    result += " LIMIT {}".format(data['limit']) if 'limit' in data else ""
    return result + ";"


if __name__ == '__main__':
    # sql = "SELECT company.* FROM Company as company;"
    # sql = "SELECT p.ProductName, p.UnitPrice FROM products AS p WHERE p.ProductName != 'Chocolade' AND p.x <= 0;"
    # sql = "SELECT p.ProductName, p.UnitPrice FROM products as p ORDER BY p.UnitPrice DESC, p.UnitPrice ASC LIMIT 10;"
    lines = sys.stdin.readlines()
    for sql in lines:
        print(parse_sql(sql))
