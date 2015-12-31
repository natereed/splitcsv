import csv
import os

class CsvSplitter():
    def __init__(self, filename, chunk_size, in_file=None, output_header_row=False, input_header_row=False):
        self.chunk_size = chunk_size
        self.filename = filename
        self.in_file = in_file
        self.chunks = []
        self.writer = None
        self.current_chunk = 0
        self.out_file = None
        self.output_header_row = output_header_row
        self.input_header_row = input_header_row

    def generate_rows_from_file(self, filename):
        if not self.in_file:
            self.in_file = open(filename, "rU")
        reader = csv.reader(self.in_file)
        for row in reader:
            yield row

    def read_fieldnames(self):
        if not self.in_file:
            self.in_file = open(self.filename, "rU")
        reader = csv.reader(self.in_file)
        self.fieldnames = reader.next()

    def name_chunk(self, filename, num):
        basename = os.path.splitext(filename)[0]
        return "%s_chunk%d.csv" % (basename, num)

    def close_file(self):
        if self.out_file:
            self.out_file.close()

    def init_writer(self, chunk_num):
        self.close_file()
        filename = self.name_chunk(self.filename, chunk_num)
        self.chunks.append(filename)
        self.out_file = open(filename, "w")
        self.writer = csv.writer(self.out_file)
        self.current_chunk = chunk_num
        if self.output_header_row and self.fieldnames:
            self.writer.writerow(self.fieldnames)

    def get_writer(self, chunk_num):
        if chunk_num > self.current_chunk:
            self.init_writer(chunk_num)
        return self.writer

    def write_row_to_chunk(self, row, chunk_num):
        writer = self.get_writer(chunk_num)
        writer.writerow(row)

    def split(self):
        chunk_num = 1
        line_num = 1

        if self.input_header_row:
            self.read_fieldnames()

        for row in self.generate_rows_from_file(self.filename):
            self.write_row_to_chunk(row, chunk_num)
            line_num += 1
            if line_num > self.chunk_size:
                chunk_num += 1
                line_num = 1

        self.close_file()
        return self.chunks




