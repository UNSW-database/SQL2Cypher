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
    'src': 't1',
    'dst': 't2',
    'src_key': 't3 k1',
    'dst_key': 't3 k2',
    'type': 'isLabel',
    'label': 'relationship label',
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
            command += "(" + kv["name"] + ":" + kv["value"] + ") "
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
    tables = parse_from(parses)
    ope = {
        'neq': '!=',
        'eq': '=',
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<='
    }
    relation = ""
    condition = ""
    # record all the visited node in the node graph
    visited = []

    # to record all the src and dst node
    src = [v["src"] for v in storage]
    dst = [v['dst'] for v in storage]
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
                # means and or or
                for val in where[op]:
                    for k in val.keys():
                        # means eq or nep something
                        """
                        check eq to get whether exists relationship
                        only this can be relationship
                        """
                        if k == "eq":
                            # get two keys
                            key1, key2 = val[k][0], val[k][1]
                            t1, pk1 = key1.split(".")
                            t2, pk2 = key2.split(".")

                            # check whether relation
                            origin_table_name1 = tables[t1]
                            origin_table_name2 = tables[t2]

                            # must has a start node, means there is a relationship
                            # then check whether exist
                            if origin_table_name1 in src and origin_table_name2 in dst:
                                # 1->2
                                pass
                            elif origin_table_name2 in src and origin_table_name1 in dst:
                                #  2->1
                                pass

                            # to storage the relationship
                            # for st in storage:
                            #     if origin_table_name1 == st["src"] and pk1 == st["src_key"]:
                            #         if origin_table_name2 == st["dst"] and pk2 == st["dst_key"]:
                            #             # means relationship 1->2
                            #             relation += t1 + "-" + "[:{}]".format(st["label"]) + t2
                            #     elif origin_table_name2 == st["src"] and pk2 == st["src_key"]:
                            #         if origin_table_name1 == st["dst"] and pk1 == st["dst_key"]:
                            #             # means relationship 2->1
                            #             relation += t2 + "-" + "[{}]".format(st["label"]) + t1


                        else:
                            # just join the relation
                            pass

        # means it should be a connectable table
        if " " in storage["src_key"] and " " in storage["dst_key"]:
            # check whether exist in the where condition and src_table == dst_table
            src_table, src_key = storage["src_key"].split(" ")
            dst_table, dsk_key = storage["dst_key"].split(" ")

    return None


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
    raw = "SELECT a, b FROM title_basic tb, title_rating tr, title_crew tc, title_akas ta WHERE tb.tconst = tc.tconst LIMIT 100000 ;"
    values = parse(raw)
    print(values)
    # parse_head(values)

