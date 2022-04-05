import os
import pickle

from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class BasePage:
    def __init__(self, num_columns, page_type, storage_path, table, from_file=False):
        self.offset_dict = {}
        self.storage_path = storage_path + "/pages/"
        self.table = table
        self.page_type = page_type
        self.max_pagesize = 512
        self.tps = 0
        self.max_size = 4096  # 4KB
        self.num_columns = num_columns
        self.COLUMN_OFFSET = 4
        self.page_list = []
        self.num_records = 0
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)
        for i in range(self.num_columns):
            new_storage_page = self.storage_path + str(i)
            if not os.path.exists(new_storage_page):
                os.makedirs(new_storage_page)
            self.page_list.append(Page(new_storage_page, self.table, from_file))

    def is_full(self):
        return self.num_records >= self.max_pagesize

    def set_tps(self, rid):
        self.tps = max(self.tps, rid)

    def add_record_merge(self, record, position):
        for i in range(len(self.page_list)):
            self.page_list[i].update(record.columns[i], position)

    def add_record(self, record):
        self.offset_dict[record.rid] = self.num_records  # adds row number of each rid in basepage to offset_dict
        for i in range(len(self.page_list)):
            self.page_list[i].write(record.columns[i])  # writes record columns to pages
        self.num_records += 1

    def write_record(self, rid, cols):
        row_offset = self.get_row(rid)
        for i in range(self.COLUMN_OFFSET, len(self.page_list)):
            self.page_list[i].update(cols[i-self.COLUMN_OFFSET], row_offset)

    def get_row(self, rid):
        return self.offset_dict[rid]


    def get_record(self, rid, query_columns):
        """Gets data from each physical page
        """
        cols = []
        row_offset = self.get_row(rid)
        for i in range(len(self.page_list)):
            if query_columns == None:
                cols.append(self.page_list[i].read(row_offset))
            elif i > 3 and query_columns[i - 4] == 0:
                cols.append(0)
            else:
                cols.append(self.page_list[i].read(row_offset))
        return cols

    def delete_record(self, rid):
        """ the base record will be invalidated by setting the RID of itself,
         and all its tail records to a special value.
        """
        row_offset = self.get_row(rid)
        self.page_list[RID_COLUMN].delete(row_offset)

    def change_indirection(self, original_rid, latest_rid):
        row_offset = self.get_row(original_rid)
        self.page_list[INDIRECTION_COLUMN].update(latest_rid, row_offset)
        # 0 is the indirection page
        # find record in base pages and in the tail pages

    def save_base_page(self, storage_path):
        # we just pickle the page list etc
        with open(storage_path + '/base_page_fields.pickle', 'wb') as base_page_fields_file:
            base_page_fields = {'offset_dict': self.offset_dict, 'page_type': self.page_type,
                                'max_pagesize': self.max_pagesize, 'max_size': self.max_size,
                                'num_columns': self.num_columns, 'COLUMN_OFFSET': self.COLUMN_OFFSET}
            pickle.dump(base_page_fields, base_page_fields_file)

    def restore_base_page(self, storage_path):

        with open(storage_path + '/base_page_fields.pickle', 'rb') as base_page_fields_file:
            loaded_base_page_fields = pickle.load(base_page_fields_file)
            self.offset_dict = loaded_base_page_fields['offset_dict']
            self.page_type = loaded_base_page_fields['page_type']
            self.max_pagesize = loaded_base_page_fields['max_pagesize']
            self.max_size = loaded_base_page_fields['max_size']
            self.num_columns = loaded_base_page_fields['num_columns']
            self.COLUMN_OFFSET = loaded_base_page_fields['COLUMN_OFFSET']

        pages_storage_path = storage_path + '/pages'
        dirs = os.listdir(pages_storage_path)

        if len(self.page_list) < len(dirs):
            for i in range(len(dirs) - len(self.page_list)):
                self.page_list.append(Page(pages_storage_path + '/' + str(i), self.table, True))
