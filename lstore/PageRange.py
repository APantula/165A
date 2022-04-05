import os
import pickle

from lstore.BasePage import BasePage
from lstore.RecordLocation import RecordLocation


class PageRange:  # rid to basepage
    """
    :param pagerange_index: int   Index value of PageRange
    :param num_columns: int       Number of data columns in the PageRange
    """

    def __init__(self, pagerange_index, num_columns, storage_path, table, from_file=False):
        self.base_pages = []
        self.storage_path = storage_path + "/page_ranges/" + str(pagerange_index) + "/base_pages/"
        self.table = table
        self.pagerange_index = pagerange_index
        self.max_basepages = 16
        self.num_columns = num_columns
        for i in range(self.max_basepages):  # add bases pages
            self.base_pages.append(BasePage(num_columns, 'base', self.storage_path + str(i), self.table, from_file))
        self.base_pages.append(
            BasePage(num_columns, 'tail', self.storage_path + str(self.max_basepages), self.table,
                     from_file))  # add tail page

    def pagerange_full(self):
        return self.base_pages[self.max_basepages - 1].is_full()

    def set_base_pages(self, new_pages):
        self.base_pages = []
        for i in range(len(new_pages)):
            self.base_pages.append(new_pages[i])

    def get_index_open_basepage(self):
        for i in range(self.max_basepages):
            if not self.base_pages[i].is_full():
                return i
        return "Page Range Full"  # Check this later

    def add_record(self, record):
        basepage_index = self.get_index_open_basepage()
        self.base_pages[basepage_index].add_record(record)
        return RecordLocation(record.rid, basepage_index, self.pagerange_index)

    def get_record(self, record_location, query_columns):
        return self.base_pages[record_location.basepage_index].get_record(
            record_location.rid, query_columns)  # returns first location of record in list of records

    def delete_record(self, recordLocation):
        basepage_index = recordLocation.basepage_index
        return self.base_pages[basepage_index].delete_record(recordLocation.rid)

    def get_tail_page_index(self):
        for i in range(self.max_basepages, len(self.base_pages)):
            if not self.base_pages[i].is_full():
                return i

        self.base_pages.append(
            BasePage(self.num_columns, 'tail', self.storage_path + str(len(self.base_pages)), self.table))
        return len(self.base_pages) - 1

    def update_record(self, record):
        basepage_index = self.get_tail_page_index()  # gives tailpage
        self.base_pages[basepage_index].add_record(record)  # adds to tailpage
        return RecordLocation(record.rid, basepage_index, self.pagerange_index)

    def change_indirection(self, record_location, latest_rid):
        # needs to go to the tail page
        basepage_index = record_location.basepage_index
        self.base_pages[basepage_index].change_indirection(record_location.rid, latest_rid)  # just updates indirection

    def save_page_range(self, storage_path):
        with open(storage_path + '/page_range_fields.pickle', 'wb') as page_range_fields_file:
            page_range_fields = {'pagerange_index': self.pagerange_index,
                                 'max_basepages': self.max_basepages,
                                 'num_columns': self.num_columns}
            pickle.dump(page_range_fields, page_range_fields_file)

        base_page_storage_path = storage_path + '/base_pages'
        if not os.path.exists(base_page_storage_path):
            os.makedirs(base_page_storage_path)

        for i in range(len(self.base_pages)):
            base_page = self.base_pages[i]
            base_page_path = base_page_storage_path + '/' + str(i)
            if not os.path.exists(base_page_path):
                os.makedirs(base_page_path)
            base_page.save_base_page(base_page_path)

    def restore_page_range(self, storage_path):
        with open(storage_path + 'page_range_fields.pickle', 'rb') as page_range_fields_fields_file:
            loaded_page_range_fields = pickle.load(page_range_fields_fields_file)
            self.max_basepages = loaded_page_range_fields['max_basepages']
            self.pagerange_index = loaded_page_range_fields['pagerange_index']
            self.num_columns = loaded_page_range_fields['num_columns']

        base_page_storage_path = storage_path + 'base_pages'
        dirs = os.listdir(base_page_storage_path)
        self.base_pages = []
        for i in range(self.num_columns):
            self.base_pages.append(
                BasePage(self.num_columns, 'base', storage_path + "base_pages/" + str(i), self.table, from_file=True))
        for i in range(self.num_columns, len(dirs)):
            self.base_pages.append(
                BasePage(self.num_columns, 'tail', storage_path + "base_pages/" + str(i), self.table, from_file=True))
        for dir in dirs:
            self.base_pages[int(dir)].restore_base_page(base_page_storage_path + '/' + dir)
