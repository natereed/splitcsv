#!/usr/bin/python

import csv
import os
import sys
import argparse
from csvsplitter import CsvSplitter

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="splitcsv.py", description="Split a CSV file into multiple smaller CSV files with header rows intact.")
    parser.add_argument("path", help="path to input")
    parser.add_argument("size", help="size of each file", type=int)
    parser.add_argument("--output_header_row", "--o", help="output with header row (default is False)", default=False, action="store_true")
    parser.add_argument("--input_header_row", "--i", help="input contains a header row (default is False), will skip copying the first row if True", default=False, action="store_true")
    args = parser.parse_args()

    path = args.path
    size = int(args.size)

    if args.output_header_row and not args.input_header_row:
        print "Input header row must be provided if outputting header row"
        sys.exit(-1)

    header_opts = {'output_header_row' : args.output_header_row,
                   'input_header_row' : args.input_header_row}

    chunks = CsvSplitter(args.path,
                         size,
                         output_header_row=args.input_header_row,
                         input_header_row=args.input_header_row).split()
    print "Created %d chunks." % len(chunks)

