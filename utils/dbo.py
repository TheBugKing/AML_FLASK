class Dbo:
    def __init__(self):
        self.cursor = self.connect()

    def connect(self):
        # TO DO
        # write to database connection code here and return a cursor object
        return None

    def run_select_query(self, params=None):
        # TO DO
        # write code to execute select queries with and without params and return the results
        # use self.cursor
        pass

    def run_insert_query(self, params=None ):
        # TO DO
        # write code to execute insert queries with and without params and return the results
        # use self.cursor
        pass

    def run_sql_query(self, params=None):
        # TO DO
        # write code to execute generic queries and SPs with and without params and return the results
        # use self.cursor
        pass
