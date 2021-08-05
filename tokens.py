import sqlparse
from sqlparse import sql

query = "UPDATE Customers c SET c.ContactName = 'Alfred Schmidt', c.City= 'Frankfurt' WHERE c.CustomerID = 1;"

query_tokens = sqlparse.parse(query)[0].tokens


def count_tables(tokens):
    """
    count how many tables
    :param tokens:
    :return:
    """
    count = 0
    for token in tokens:
        if token.ttype is None and type(token) is sql.Identifier:
            if token.get_name() == token.get_real_name():
                raise Exception("Please use a different alias name")
            count += 1
    return count


def value_condition(tokens):
    """
    extract value condition in the tokens
    :param tokens: sql queries token
    :return:
    """
    values = " SET "
    match = "MATCH "
    where = " "
    for token in tokens:
        # print('token[%s] type[%s]' % (token, token.ttype) )
        if token.ttype is None and isinstance(token, sql.IdentifierList):
            for id in token.get_identifiers():
                if values != " SET ":
                    values += ", " + str(id)
                else:
                    values += str(id)
        elif token.ttype is None and type(token) is sql.Identifier:
            # tables
            match += "({}:{}) ".format(str(token.get_name()), str(token.get_real_name()))
        elif token.ttype is None and type(token) is sql.Where:
            # where condition
            where += str(token).replace(";", "")
    return match + where + values


def parse(tokens):
    if count_tables(tokens) == 1:
        query = value_condition(tokens)
        print(query)
    else:
        raise Exception("Can not parse relationships now")


if __name__ == '__main__':
    parse(query_tokens)

# for token in query_tokens:
#     print('token[%s] type[%s] id: [%s]' % (token, token.ttype, type(token)))
#
# # print(sql.IdentifierList.value)
# for token in query_tokens:
#     # print('token[%s] type[%s]' % (token, token.ttype) )
#     if token.ttype is None and isinstance(token, sql.IdentifierList):
#         print('Identifierlist:')
#         for id in token.get_identifiers():
#             print(id)
#
#
# for token in query_tokens:
#     # print('token[%s] type[%s]' % (token, token.ttype) )
#     if token.ttype is None and type(token) is sql.Identifier:
#         print('Table:')
#         print(token.get_name(), token.get_real_name())
#
#
# for token in query_tokens:
#     # print('token[%s] type[%s]' % (token, token.ttype) )
#
#     if token.ttype is None and type(token) is sql.Where:
#         print('Where:')
#         print(token)
