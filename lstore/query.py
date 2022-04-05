from lstore.Record import Record
from lstore.table import Table


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    # delete method successful
    def delete(self, primary_key):

        try:
            self.table.get_record(primary_key)
            self.table.delete_record(primary_key)
        except IndexError:
            print("primary key does not exist in current records.")
            return False
        except Exception as e:
            print("other exception:", e)
            return False

        return True

    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """

    # insert method successful
    def insert(self, *columns):
        # TA says schema encoding is to mark which columns have been updated
        schema_encoding = '0' * self.table.num_columns_internal
        cols = []
        for i in columns: cols.append(i)
        columns = cols

        if len(columns) != self.table.num_columns_internal - 4:
            print("this table has " + self.table.num_columns_internal + " columns, but " + len(
                columns) + " columns was entered.")
            return False

        try:
            key = columns[self.table.key_column]
            self.table.add_record(key, columns)
        except:
            print("an error occured with the insertion attempt.")
            return False

        return True

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    # select method successful
    def select(self, index_value, index_column, query_columns):

        if len(query_columns) + 4 != self.table.num_columns_internal:
            print("this table has " + self.table.num_columns_internal + " columns, but query size was " + len(
                query_columns) + ".")
            return []

        # get all records of the given value
        records_list = self.table.get_multiple_records(index_value, index_column)

        # update each column of a record according to values in query_columns
        query_list = []
        for record in records_list:
            sliced_record = record.columns[4:]
            key = sliced_record[self.table.key_column]
            queried_columns = []
            for column_value, query_selection in zip(sliced_record, query_columns):
                if query_selection == 0:
                    queried_columns.append(None)
                queried_columns.append(column_value)
            new_record = Record(record.columns[1], key, queried_columns)
            query_list.append(new_record)

        return query_list

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    # update method failed
    def update(self, primary_key, *columns):
        cols = []
        for i in columns: cols.append(i)
        columns = cols
        try:
            self.table.update_record(primary_key, columns)
        except:
            print("update failed")
            return False
        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    # sum method successful
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            sum = self.table.sum(start_range, end_range, aggregate_column_index)
        except:
            print("Error with sum.")
            return False
        return sum

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key_column, [1] * self.table.num_columns_internal)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns_internal
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False


if __name__ == '__main__':
    table = Table("hello", 5, 0)
    query = Query(table)
    for i in range(10):
        query.insert(906659671 + i, 93, 0, 0, 0)
    print("test insert")

    query.delete(906659674)
    print("test delete 1")

    query.delete(906659680)
    print("test delete 2")

    query.delete(100)
    print("test delete 3")

    # select currently doesn't work
    query.select(906659671, 0, [1, 1, 1, 1, 1])
    print("test query 1")

    query.select(906659672, 0, [0, 0, 0, 0, 0])
    print("test query 2")

    # update doesn't work
    # query.update(906659672, [None, None, None, None, 99])
    # print("test update 1")

    # query.update(906659699, [None, None, None, None, None])
    # print("test update 2")

    start_value = 906659671
    end_value = start_value + 5
    result = query.sum(start_value, end_value, 0)
    print("test sum 1:", result)

    start_value = 1
    end_value = start_value + 5
    result = query.sum(start_value, end_value, 0)
    print("test sum 2:", result)
