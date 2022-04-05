# from lstore.page import Page
import queue


# granularity of a single page
import threading


class Slot:
    def __init__(self, slot_id):
        self.slot_id = slot_id
        self.pin = 0
        self.dirty_bit = 0
        self.access_count = 0

    def pin_page(self):
        self.pin += 1
        self.access_count += 1

    def unpin_page(self):
        self.pin -= 1


# number of slots to be decided upon initialization
class Cache:
    def __init__(self, num_slots, table_name):
        self.num_slots = num_slots
        self.name = table_name
        # page_dict maps a page to its corresponding slot
        self.page_dict = {}
        # pages are pushed to queue in first come first serve order
        self.queue = queue.Queue(num_slots)
        self.slot_id = 0
        self.lock = threading.Lock()

    # loads a page that does not exist in cache
    def load_page(self, page):
        # check if queue is full
        self.lock.acquire()
        if self.at_capacity():
            # FIFO rule to evict pages
            to_evict = self.queue.get()
            # wait until pin_count becomes 0 to evict page
            pin_count = self.page_dict[to_evict].pin
            while pin_count != 0:
                pin_count = self.page_dict[to_evict].pin
            self.evict_page(to_evict)
        # add the page to the queue
        self.queue.put(page)
        # pair the page object with a slot object in page_dict
        new_slot = Slot(self.slot_id)
        self.page_dict[page] = new_slot
        self.slot_id += 1
        self.lock.release()

    def evict_page(self, page):
        dirty_bit = self.page_dict[page].dirty_bit
        if dirty_bit == 1:
            self.commit_page(page)
        # mark page not loaded in cache
        page.loaded_in_cache = False
        # remove page as key from page_dict
        self.page_dict.pop(page, None)
        return True

    def commit_page(self, page):
        page.save_page(page.path)

    # will return true if cache is full
    def at_capacity(self):
        return self.queue.qsize() == self.num_slots

    # checks whether a page object is in the cache
    def is_page_in_pool(self, page):
        # look for the slot object that the page occupies in the page_dict
        slot = self.page_dict.get(page)
        if slot is not None:
            return True
        return False

    def commit_all_pages(self):
        while not self.queue.empty():
            self.evict_page(self.queue.get())
