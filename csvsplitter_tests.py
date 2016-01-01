import csv
import os
import StringIO
import unittest

from csvsplitter import CsvSplitter

class CsvSplitterTests(unittest.TestCase):

    def tearDown(self):
        if self.persist_example:
            os.remove("example.csv")
        os.remove("example_chunk1.csv")
        os.remove("example_chunk2.csv")
        os.remove("example_chunk3.csv")

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


    def verify_chunks(self, chunks, chunk_size, expected_num_lines, output_includes_header_row):
        num_verified_lines = 0
        for chunk in chunks[:-1]:
            print "Verifying chunk %s" % chunk
            num_verified_lines += self.verify_chunk_size(chunk, chunk_size, output_includes_header_row)
        print "Num verified lines: %d" % num_verified_lines
        print "Expected number lines: %d " % expected_num_lines
        print "Verified %d lines" % num_verified_lines
        print "Verifying last chunk..."
        self.assertEqual(expected_num_lines - num_verified_lines,
                         self.verify_chunk_size(chunks[-1], expected_num_lines % num_verified_lines, output_includes_header_row))

    def run_test_with_splitter(self, splitter):
        expected_num_lines = len(self.example.strip().split("\n"))
        chunks = splitter.split()
        print "%d chunks" % len(chunks)
        self.assertTrue(chunks and len(chunks) == 3)
        self.verify_chunks(chunks,
                           splitter.chunk_size,
                           expected_num_lines - (1 if splitter.input_header_row else 0), # num lines net input header row
                           splitter.output_header_row)

    def test_split_file_with_file_handle(self):
        self.generate_example(8, 11)
        self.run_test_with_splitter(CsvSplitter("example.csv", 5, StringIO.StringIO(self.example), input_header_row=True))

    def test_split_file_with_filename(self):
        self.generate_example(8,11, persist=True)
        self.run_test_with_splitter(CsvSplitter("example.csv", 5, input_header_row=True))

    def test_split_file_with_header_row_output_header_row(self):
        self.generate_example(8,11, persist=True)
        self.run_test_with_splitter(CsvSplitter("example.csv", 5, output_header_row=True, input_header_row=True))

    def test_split_file_with_header_row_ignore(self):
        self.generate_example(8,11, persist=True)
        self.run_test_with_splitter(CsvSplitter("example.csv", 5, output_header_row=False, input_header_row=True))

if __name__ == '__main__':
    unittest.main()


