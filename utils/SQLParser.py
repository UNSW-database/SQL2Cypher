from utils.OrderBy import OrderBy
from utils.Select import Select
from utils.From import From
from utils.Where import Where
from utils.Limit import Limit


class SQLParser:
    def __init__(self):
        self.cypher = ""
        self.Orderby = OrderBy("ORDER BY ")
        self.Select = Select("RETURN ")
        self.From = From("MATCH ")
        self.Where = Where("WHERE ")
        self.Limit = Limit("LIMIT ")

    def generate_cypher(self, data):
        if "from" in data:
            self.From.handle_sql(data['from'])
            self.cypher += self.From.get_cypher()

        if "where" in data:
            self.Where.handle_sql(data['where'])
            self.cypher += self.Where.get_cypher()

        if "select" in data:
            self.Select.handle_sql(data['select'])
            self.cypher += self.Select.get_cypher()

        if 'orderby' in data:
            self.Orderby.handle_sql(data['orderby'])
            self.cypher += self.Orderby.get_cypher()

        if 'limit' in data:
            self.Limit.handle_sql(data['limit'])
            self.cypher += self.Limit.get_cypher()

    def get_cypher(self):
        return self.cypher + ";"

