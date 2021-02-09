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


storage = [{
    'src': 'title_basic',
    'dst': 'title_crew',
    'src_key': 'tconst',
    'dst_key': 'tconst',
    'type': 'no',
    'label': 'KNOWN',
}]


def parse_from(parses):
    """
    parse the from status, then converting them into cypher
    :param parses: sql parse
    :return: string cypher
    """
    tables = {}
    if "from" not in parses.keys():
        print("no from status")
        return None

    command = "MATCH "
    if type(parses["from"]) is list:
        # add the value together
        for kv in parses["from"]:
            tables[kv["name"]] = kv["value"]
            # command += "(" + kv["name"] + ":" + kv["value"] + ") "

        command += ", ".join("({}:{})".format(kv["name"], kv["value"]) for kv in parses["from"])
    else:
        tables[parses["from"]["name"]] = parses["from"]["value"]
        command += "(" + parses["from"]["name"] + ":" + parses["from"]["value"] + ") "

    return tables, command


def parse_where(parses):
    """
    load where condition for parses based on the relationship storage
    :param parses: sql parse
    :return: where condition with cypher query
    """
    # get all the tables as dict format
    tables, command = parse_from(parses)
    print(tables)
    ope = {
        'neq': '!=',
        'eq': '=',
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<='
    }
    relation = ""
    condition = " WHERE "
    # record all the visited node in the node graph
    visited = []

    # to record all the src and dst node
    # src = [v["src"] for v in storage]
    # dst = [v['dst'] for v in storage]

    if "where" in parses.keys():
        # where is should be a dict format
        if type(parses["where"]) is not dict:
            raise Exception("SQL incorrect format")

        # get where conditions
        where = parses["where"]

        # loop the operations and join them together
        """
        if it is and, then it should be the list with {'eq': ['tb.tconst', 'tc.tconst']}
        """
        for op in where.keys():
            if type(where[op]) is dict:
                # means one condition
                pass
            elif type(where[op]) is list:
                if op == "and":
                    # there can be a relationship
                    """
                    1. extract eq operations firstly
                    2. consider the relationships
                    """
                    relationships = []
                    conditions = []
                    for operations in where[op]:
                        for key in operations.keys():
                            # put all the eq together at the first
                            if key == "eq":
                                relationships.append(operations[key])
                            else:
                                # just combine together for and conditions
                                conditions.append(" {} ".format(ope[key]).join([str(i) for i in operations[key]]))

                    # now consider the relationships
                    for re in relationships:
                        key1, key2 = re[0], re[1]
                        t1, pk1 = key1.split(".")
                        t2, pk2 = key2.split(".")

                        # check whether relation
                        origin_table_name1 = tables[t1]
                        origin_table_name2 = tables[t2]

                        # consider relationships
                        for st in storage:
                            if origin_table_name1 == st["src"] and origin_table_name2 == st["dst"]:
                                # 1->2
                                relation += ", ({})-[:{}]->({}) ".format(t1, st["label"], t2)
                            elif origin_table_name2 == st["src"] and origin_table_name1 == st["dst"]:
                                #  2->1
                                relation += ", ({})-[:{}]->({}) ".format(t2, st["label"], t1)
                            elif origin_table_name1 == st["src"] and st["type"] == "isLabel":
                                # join relationship, there should have a a->b->c relationship
                                for tr in relationships:
                                    if tr[0] == key2:
                                        # means should have a join relationship
                                        if origin_table_name2 == st["src_key"].split(" ")[0]:
                                            # add the relationship
                                            relation += ", ({})-[{}:{}]->({})".\
                                                format(t1, t2, st["label"], tr[1].split(" ")[0])
                                            # remove the join conditions
                                            del tr
                                        else:
                                            # can not find correct relationship
                                            conditions.append("{}={}".format(key1, key2))
                            elif origin_table_name2 == st["src"] and st["type"] == "isLabel":
                                # join relationship
                                # join relationship, there should have a a->b->c relationship
                                for tr in relationships:
                                    if tr[0] == key1:
                                        # means should have a join relationship
                                        if origin_table_name1 == st["src_key"].split(" ")[0]:
                                            # add the relationship
                                            relation += ", ({})-[{}:{}]->({})". \
                                                format(t2, t1, st["label"], tr[1].split(" ")[0])
                                            # remove the join conditions
                                            del tr
                                        else:
                                            # can not find correct relationship
                                            conditions.append("{} = {}".format(key1, key2))
                            else:
                                conditions.append("{} = {}".format(key1, key2))

                    condition += " AND ".join(conditions)
                else:
                    # there can not be a relationship
                    conditions = []
                    for operations in where[op]:
                        for key in operations.keys():
                            conditions.append(" {} ".format(ope[key]).join([str(i) for i in operations[key]]))

                    condition += " OR ".join(conditions)
    print(condition)
    print(relation)
    return command + relation + condition


def parse_limit(parses):
    """
    parse the limit number
    :param parses:
    :return:
    """
    if "limit" in parses.keys():
        if type(parses["limit"]) is not int:
            raise Exception("SQL limit incorrect")
        else:
            return " LIMIT " + str(parses["limit"])

    return None


def parse_head(values):
    """
    parse the first part of sql (like select, update, delete)
    :param values: sql parse
    :return: string
    """
    # means it is select query
    if "select" in values.keys():
        command = parse_where(values)
        # if it is select then we need to consider whether it include some functions
        if type(values["select"]) is list:
            # means select multiple values, it can be value or functions
            # for double check the sql format
            if any(type(i) != dict for i in values["select"]):
                raise Exception("Incorrect SQL format")
            # handle the multiple values case
            command += " return " + ", ".join([s["value"] for s in values["select"]])
        else:
            # means select only one value
            command += " return " + values["select"]
        command += parse_limit(values)
        return command
    elif "update" in values.keys():
        # means it is update query, nodes update and rel update
        pass
    elif "delete" in values.keys():
        # means it is a delete query
        pass


if __name__ == '__main__':
    raw = "SELECT a, b FROM title_basic tb, title_rating tr, title_crew tc, title_akas ta WHERE tb.tconst = tc.tconst " \
          "AND tc.tconst = ta.tconst AND ta.a >= 1 LIMIT 100000 ;"
    values = parse(raw)
    print(parse_head(values))
    # parse_where(values)


    # parse_head(values)

