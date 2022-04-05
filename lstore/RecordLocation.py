class RecordLocation:
    def __init__(self, rid, basepage_index, pagerange_index):
        self.rid = rid
        self.basepage_index = basepage_index  # basepage
        self.pagerange_index = pagerange_index  # pagerange

    def write_to_byte_array(self):
        data = bytearray(24)
        data[0:8] = self.rid.to_bytes(8, byteorder='big')
        data[8:16] = self.basepage_index.to_bytes(8, byteorder='big')
        data[16:24] = self.pagerange_index.to_bytes(8, byteorder='big')
        return data

    def from_bytes(self, byte_array):
        self.rid = int.from_bytes(bytes=byte_array[0:8], byteorder='big')
        self.basepage_index = int.from_bytes(bytes=byte_array[8:16], byteorder='big')
        self.pagerange_index = int.from_bytes(bytes=byte_array[16:24], byteorder='big')

    def __eq__(self, other):
        return other.rid == self.rid and other.basepage_index == self.basepage_index and other.pagerange_index == self.pagerange_index


if __name__ == '__main__':
    original = RecordLocation(1, 2, 3)
    other_record = RecordLocation(0, 0, 0)
    other_record.from_bytes(original.write_to_byte_array())
    assert other_record == original
