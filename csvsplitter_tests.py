import csv
import StringIO
import unittest

from csvsplitter import CsvSplitter

class CsvSplitterTests(unittest.TestCase):

    def setUp(self):
        self.example_file = '''****6331,School Health Corporation,2 - Non DNBi,Inside Sales NCA Telesales,24906331,School Health Corporation,$810.59 ,$967.77
****1898,"Resilux America, LLC",1 - DNBi,Inside Sales Customer Success,24911898,"Resilux America, LLC","$4,880.47 ","$4,880.47 "
****7606,"On A Roll Trucking, Inc.",1 - DNBi,Inside Sales Customer Success,24917606,"On A Roll Trucking, Inc.","$4,218.80 ","$4,556.30 "
****2619,"Pharmachem Laboratories, Inc.",1 - DNBi,Inside Sales RMS Telesales,24922619,"Pharmachem Laboratories, Inc.","$15,500.00 ","$15,500.00 "
****4058,Paws Chicago,5 - Other,Inside Sales S&MS Telesales,24924058,Paws Chicago,,$0.00
****8343,The Gsi Group LLC,1 - DNBi,National Accounts,618938260,AGCO Corporation,"$19,070.57 ","$20,977.62 "
****6747,Dave Thomas Foundation For Adoption,4a - Prospecting,Inside Sales S&MS Telesales,24936747,Dave Thomas Foundation For Adoption,"$3,595.00 ",
****9127,Dachis Corporation,4a - Prospecting,Indirect,24939127,Dachis Corporation,"$14,062.50 ",
****1077,"Mapco Express, Inc.",1 - DNBi,Inside Sales RMS Telesales,532313715,DELEK GROUP LTD.,"$22,813.36 ","$23,269.07 ",
****2265,"Stereotaxis, Inc.",1 - DNBi,Inside Sales Customer Success,24942265,"Stereotaxis, Inc.","$1,008.00 ",
****0826,"Symmons Industries, Inc.",1 - DNBi,Inside Sales Customer Success,1010826,"Symmons Industries, Inc.","$1,780.25 ","$1,869.27 "
'''
    def verify_chunk_size(self, file, expected_length=None):
        length = 0
        with open(file, "r") as csv_in:
            reader = csv.reader(csv_in)
            length = 0
            for row in reader:
                length += 1

        print "--- Read %d lines" % length
        print "--- Expected: %d" % expected_length
        if expected_length:
            self.assertEqual(expected_length, length)
        return length


    def verify_chunks(self, chunks, size, expected_num_lines):
        num_verified_lines = 0
        for chunk in chunks[:-1]:
            num_verified_lines += self.verify_chunk_size(chunk, 5)
        self.assertEqual(expected_num_lines - num_verified_lines,
                         self.verify_chunk_size(chunks[-1], expected_num_lines % num_verified_lines))

    def test_split_file_with_file_handle(self):
        num_verified_lines = 0
        expected_num_lines = len(self.example_file.strip().split("\n"))

        splitter = CsvSplitter("example.csv", 5, StringIO.StringIO(self.example_file))
        chunks = splitter.split()
        self.assertTrue(chunks and len(chunks) == 3)
        self.verify_chunks(chunks, 5, expected_num_lines)

if __name__ == '__main__':
    unittest.main()


