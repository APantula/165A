class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.table = None
        # check if we have locks on a specific rid
        self.lock_dictionary = {}

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, table, *args):
        self.table = table
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        self.lock_dictionary = {}
        primary_key = self.table.key_column - self.table.COLUMN_OFFSET
        for query, args in self.queries:
            # write locks:update, increment, delete
            # need to process sum
            if query == query.__self__.select:
                record_location = self.table.index.locate(self.table.key_column, args[primary_key])[0]
                rid = record_location.rid
                if rid not in self.lock_dictionary:
                    if self.table.lock_manager.acquire_read_lock(rid):
                        self.lock_dictionary[rid] = True
                    else:
                        return self.abort()
            elif query == query.__self__.update or query == query.__self__.increment or query == query.__self__.delete:
                record_location = self.table.index.locate(self.table.key_column, args[primary_key])[0]
                rid = record_location.rid
                if rid not in self.lock_dictionary:
                    if self.table.lock_manager.acquire_write_lock(rid):
                        self.lock_dictionary[rid] = False
                    else:
                        return self.abort()
                elif self.lock_dictionary[rid]:
                    self.table.lock_manager.release_read_lock(rid)
                    if self.table.lock_manager.acquire_write_lock(rid):
                        self.lock_dictionary[rid] = False
                    else:
                        return self.abort()
            # for sum
            elif query == query.__self__.sum:
                record_locations = self.table.index.locate_range(self.table.key_column, args[primary_key])
                for record_location in record_locations:
                    rid = record_location.rid
                    if rid not in self.lock_dictionary:
                        if self.table.lock_manager.acquire_read_lock(rid):
                            self.lock_dictionary[rid] = True
                        else:
                            return self.abort()

            # read lock:select, sum
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        for rid in self.lock_dictionary:
            if self.lock_dictionary[rid]:
                self.table.lock_manager.release_read_lock(rid)
            else:
                self.table.lock_manager.release_write_lock(rid)
        return False

    def commit(self):
        for rid in self.lock_dictionary:
            if self.lock_dictionary[rid]:
                self.table.lock_manager.release_read_lock(rid)
            else:
                self.table.lock_manager.release_write_lock(rid)
        return True
