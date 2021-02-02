"""
parse sql query and convert it into cypher
"""
# import sqlparse
#
# raw = "SELECT a, b FROM title_basic tb, title_rating tr, title_crew tc, title_akas ta WHERE tb.tconst = tc.tconst and tb.tconst = tr.tconst and tb.tconst = ta.titleId GROUP BY tr.tconst LIMIT 100000 ;"


# parse = sqlparse.parse(raw)
#
# for p in parse[0].tokens:
#     print(p.ttype, p.value)

from moz_sql_parser import parse


def parse_from(parses):
    """
    parse the from status, then converting them into cypher
    :param parses: sql parse
    :return: string cypher
    """
    if "from" not in parses.keys():
        print("no from status")
        return None

    command = "MATCH "
    if type(parses["from"]) is list:
        # add the value together
        for kv in parses["from"]:
            command += "(" + kv["name"] + ":" + kv["value"] + ") "
    else:
        command += "(" + parses["from"]["name"] + ":" + parses["from"]["value"] + ") "

    return command


def parse_where(parses):
    """
    load where condition for parses based on the relationship storage
    :param parses: sql parse
    :return: where condition with cypher query
    """
    pass


def parse_limit(parses):
    """
    parse the limit number
    :param parses:
    :return:
    """
    if "limit" in parses.keys():
        if not parses["limit"].isdigit():
            raise Exception("SQL limit incorrect")
        else:
            return "LIMIT " + parses["limit"]

    return None


def parse_head(values):
    """
    parse the first part of sql (like select, update, delete)
    :param values: sql parse
    :return: string
    """
    # means it is select query
    if "select" in values.keys():
        # if it is select then we need to consider whether it include some functions
        if type(values["select"]) is list:
            # means select multiple values, it can be value or functions
            # for double check the sql format
            if any(type(i) != dict for i in values["select"]):
                raise Exception("Incorrect SQL format")
            # handle the multiple values case
            return "return " + ", ".join([s["value"] for s in values["select"]]) + ";"
        else:
            # means select only one value
            return "return " + values["select"] + ";"
    elif "update" in values.keys():
        # means it is update query, nodes update and rel update
        pass
    elif "delete" in values.keys():
        # means it is a delete query
        pass


if __name__ == '__main__':
    raw = "SELECT a, b FROM title_basic tb, title_rating tr, title_crew tc, title_akas ta WHERE tb.tconst = tc.tconst and tb.tconst = tr.tconst and tb.tconst = ta.titleId GROUP BY tr.tconst LIMIT 100000 ;"
    values = parse(raw)
    print(values)
    # parse_head(values)

