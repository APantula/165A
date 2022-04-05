import os

from lstore.config import Config
from lstore.table import Table


class Database:
    def __init__(self):
        self.tables = {}
        self.config = Config()

    def open(self, storage_path):
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)

        for dir in os.listdir(storage_path):
            table_def = self.config.get_table_def(dir)
            self.create_table(table_def['name'], table_def['num_columns'], table_def['key_index'] - 4, storage_path)

    def close(self):
        self.save_tables()
        self.config.write_config()

    def save_tables(self):
        self.config.write_tables(self.tables.values())
        for table in self.tables.values():
            table.save_table(self.config.get_config_string('db_location') + "/" + table.name)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key_index, storage_path=None):

        table = Table(name, num_columns, key_index, storage_path)
        if name not in self.tables:
            self.tables[name] = table
        else:
            print("Database already contains " + name + ". Choose a different key.\n")
            pass
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        del self.tables[name]
        return self.tables

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        return self.tables[name]


if __name__ == '__main__':
    database = Database()
    database.open('./ECS165')
    thingy = database.config.get_table_def('grades')
    database.close()
    # if not os.path.exists("./ECS165/hello/iam/coding2/rn"):
    #     os.makedirs("./ECS165/hello/iam/coding2/rn")
