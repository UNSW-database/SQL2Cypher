from utils.AbsSQL import AbsSQL


class From(AbsSQL):

    def handle_sql(self, data):
        """
        handle the from data, because it includes many from values
        :param data: dict data or list
        :return:
             a string contain the match data
        """
        self.check_data(data)

        if type(data) == dict:
            self.cypher += "({}:{}) ".format(data['name'], data['value'])
        elif type(data) == list:
            pass
