import ast
import configparser


class Config:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.initialize_config()

    def initialize_config(self):
        try:
            with open('config.ini') as f:
                self.config.read_file(f)
        except IOError:
            self.generate_default_values()

    def generate_default_values(self):
        self.config['lstore'] = {}
        self.config['lstore']['db_location'] = 'ECS165'
        pass

    def write_config(self):
        with open('config.ini', 'w') as configfile:
            self.config.write(configfile)

    def write_tables(self, tables):
        table_dict = {}
        for table in tables:
            table_dict[table.name] = {'name': table.name, 'num_columns': table.num_columns,
                                      'key_index': table.key_column}
        self.config['tables'] = table_dict

    def get_table_def(self, table_name):
        return ast.literal_eval(self.config['tables'][table_name])

    def get_config_string(self, key):
        try:
            return self.config['lstore'][key]
        except KeyError:
            self.generate_default_values()
            return self.get_config_string(key)


# For testing of the index
if __name__ == '__main__':
    config = Config()
    config.write_config()
