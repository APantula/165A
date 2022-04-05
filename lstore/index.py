"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
import os
import threading

from lstore.bTree import BTree

TREE_ORDER = 12


class Index:
    def __init__(self, table, column_key):
        # One index for each table. All our empty initially.
        self.indices = [None] * (table.num_columns_internal + 4)
        self.read_lock = threading.Lock()
        self.write_lock = threading.Lock()
        if column_key > 0:
            self.indices[column_key] = BTree(TREE_ORDER)

    def get_count_records_indexed(self):
        for i in range(len(self.indices)):
            if self.indices[i] is not None:
                return self.indices[i].get_count_elements()

    def save_b_trees(self, path):
        for i in range(len(self.indices)):
            if self.indices[i] is not None:
                self.indices[i].write_tree_in_order(path + "/" + str(i) + ".index")

    def read_from_saved_index(self, path):
        for file_or_dir in os.listdir(path):
            if file_or_dir.__contains__(".index"):
                index = int(file_or_dir.split('.')[0])
                b_tree = BTree(TREE_ORDER)
                b_tree.read_tree_from_path(path + "/" + file_or_dir)
                self.indices[index] = b_tree

    def add_record(self, record, recordlocation):
        self.read_lock.acquire()
        self.write_lock.acquire()
        for i in range(len(record.columns)):
            if self.indices[i] is not None:
                self.indices[i].insert(record.columns[i], recordlocation)
        self.write_lock.release()
        self.read_lock.release()

    def remove_record(self, record):
        self.read_lock.acquire()
        self.write_lock.acquire()
        for i in range(len(self.indices)):
            if self.indices[i] is not None:
                self.indices[i].delete(record.columns[i], record.rid)
        self.write_lock.release()
        self.read_lock.release()

    def locate_single_element(self, column, value):
        assert self.indices[column] is not None
        return self.indices[column].find_single(value)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        return self.locate_range(value, value, column)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        self.read_lock.acquire()
        assert self.indices[column] is not None
        result = self.indices[column].find_all_between(begin, end)
        self.read_lock.release()
        return result

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.read_lock.acquire()
        if self.indices[column_number] is None:
            self.indices[column_number] = BTree(TREE_ORDER)
        self.read_lock.release()

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None


# For testing of the index
if __name__ == '__main__':
    tree = BTree(6)
    for i in range(50):
        tree.insert(i, i)
        tree.print_tree_in_order()
        tree.print_tree()
    for i in range(50):
        tree.delete(i, i)
        tree.print_tree_in_order()
        tree.print_tree()
