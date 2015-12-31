import csv
import os

class CsvSplitter():
    def __init__(self, filename, size, in_file=None):
        self.size = size
        self.filename = filename
        self.in_file = in_file
        self.chunks = []
        self.writer = None
        self.current_chunk = 0
        self.out_file = None

    def generate_rows_from_file(self, filename):
        if not self.in_file:
            self.in_file = open(filename, "rU")
        reader = csv.reader(self.in_file)
        for row in reader:
            yield row

    def name_chunk(self, filename, num):
        basename = os.path.splitext(filename)[0]
        return "%s_chunk%d.csv" % (basename, num)

    def close_file(self):
        if self.out_file:
            self.out_file.close()

    def get_writer(self, chunk_num):
        if chunk_num > self.current_chunk:
            self.close_file()
            filename = self.name_chunk(self.filename, chunk_num)
            self.chunks.append(filename)
            self.out_file = open(filename, "w")
            self.writer = csv.writer(self.out_file)
            self.current_chunk = chunk_num
        return self.writer

    def write_row_to_chunk(self, row, chunk_num):
        writer = self.get_writer(chunk_num)
        writer.writerow(row)

    def split(self):
        chunk_num = 1
        line_num = 1
        for row in self.generate_rows_from_file(self.filename):
            self.write_row_to_chunk(row, chunk_num)
            line_num += 1
            if line_num > self.size:
                chunk_num += 1
                line_num = 1

        self.close_file()
        return self.chunks




