import csv
import os
import StringIO
import unittest

from csvsplitter import CsvSplitter

class CsvSplitterTests(unittest.TestCase):

    def setUp(self):
        self.persist_example = False

    def tearDown(self):
        if self.persist_example:
            os.remove("example.csv")
        if self.chunks:
            for chunk in self.chunks:
                os.remove(chunk)

    def generate_example(self, num_cols, num_rows, include_header_row=True, persist=False):
        self.persist_example = persist
        example = ""
        if include_header_row:
            example += ','.join(["col %d" % i for i in range(num_cols)])
            example += "\n"
        for row in range(num_rows):
            example +=  ",".join(["data-#%d.%d" % (row, i) for i in range(num_cols)])

            example += "\n"
        self.example = example

        if persist:
            f = open("example.csv", "w")
            f.write(example)
            f.close()

        return self.example

    def verify_chunk_size(self, file, expected_length, output_includes_header_row):
        length = 0
        with open(file, "r") as csv_in:
            reader = csv.reader(csv_in)
            length = 0
            for row in reader:
                length += 1

        total_length_verified = length - (1 if output_includes_header_row else 0)
        self.assertEqual(expected_length, total_length_verified)

        return total_length_verified


    def verify_chunks(self, chunks, chunk_size, total_expected_num_lines, output_includes_header_row):
        num_verified_lines = 0
        for chunk in chunks[:-1]:
            num_verified_lines += self.verify_chunk_size(chunk, chunk_size, output_includes_header_row)
        if total_expected_num_lines % chunk_size == 0:
            expected_remaining_length = chunk_size
        else:
            expected_remaining_length = total_expected_num_lines % num_verified_lines

        self.assertEqual(expected_remaining_length,
                         self.verify_chunk_size(chunks[-1],
                                                expected_remaining_length,
                                                output_includes_header_row))


    def run_test_with_splitter(self, splitter, expected_num_chunks):
        num_input_header_lines = 1 if splitter.input_header_row else 0
        total_num_lines = len(self.example.strip().split("\n"))
        total_expected_num_lines = total_num_lines - num_input_header_lines

        self.chunks = splitter.split()

        self.assertTrue(self.chunks and len(self.chunks) == expected_num_chunks)
        self.verify_chunks(self.chunks,
                           splitter.chunk_size,
                           total_expected_num_lines,
                           splitter.output_header_row)

    def test_verify_chunks(self):
        chunk1 = '''col1,col2
1234,4567
2345,6788
'''
        with open("example_chunk1.csv", "w") as out_csv:
            out_csv.write(chunk1)

        chunk2 = '''col1,col2
7658,8767
2374,2987
'''
        with open("example_chunk2.csv", "w") as out_csv:
            out_csv.write(chunk2)

        self.chunks = ["example_chunk1.csv", "example_chunk2.csv"]
        self.verify_chunks(self.chunks, 2, 4, True)

    def test_split_file_with_file_handle(self):
        self.generate_example(8, 11, persist=False)
        self.run_test_with_splitter(CsvSplitter("example.csv", 5, StringIO.StringIO(self.example), input_header_row=True), 3)

    def test_split_file_with_filename(self):
        self.generate_example(8,11, persist=True)
        self.run_test_with_splitter(CsvSplitter("example.csv", 5, input_header_row=True), 3)

    def test_split_file_with_header_row_output_header_row(self):
        self.generate_example(8,11, persist=True)
        self.run_test_with_splitter(CsvSplitter("example.csv", 5, output_header_row=True, input_header_row=True), 3)

    def test_split_file_with_header_row_ignore(self):
        self.generate_example(8,11, persist=True)
        self.run_test_with_splitter(CsvSplitter("example.csv", 5, output_header_row=False, input_header_row=True), 3)

    def test_split_file_into_equal_size_chunks(self):
        self.generate_example(8,10, persist=True)
        self.run_test_with_splitter(CsvSplitter("example.csv", 5, output_header_row=False, input_header_row=True), 2)

if __name__ == '__main__':
    unittest.main()


