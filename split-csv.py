#!/usr/bin/python

import csv
import os
import sys
import argparse

def usage():
    print "Usage: python split-csv.py <path-to-file> <size>"

def read_fieldnames(path):
    with open(path, "rU") as path_csv:
        reader = csv.reader(path_csv)
        row = reader.next()
        return row


def name_chunk(basename, chunknum):
    return "%s_chunk%d.csv" % (basename, chunknum)


def split_csv_file(filename, size, with_header_row):
    # Get the fieldnames
    fields = read_fieldnames(filename)
    print "Fields: %s" % fields

    basename = os.path.splitext(filename)[0]

    # Open input
    with open(filename, "rU") as in_file:

        # Create reader for input
        reader = csv.DictReader(in_file)

        chunknum = 1
        row = None

        # Loop until no more rows are found, incrementing chunknum each time a new file is created:
        while True:
            print "Creating chunk #%d..." % chunknum
            out_filename = name_chunk(basename, chunknum)
            with open(out_filename, "w") as out_file:
                writer = csv.DictWriter(out_file, fields)
                if with_header_row:
                    writer.writeheader()

                for rownum in range(size):
                    try:
                        row = reader.next()
                        writer.writerow(row)
                    except StopIteration:
                        row = None
                        break

                if not row:
                    break

                chunknum += 1

parser = argparse.ArgumentParser(prog="split_csv.py", description="Split a CSV file into multiple smaller CSV files.")
parser.add_argument("path", help="path to input")
parser.add_argument("size", help="size of each file", type=int)
parser.add_argument("--with_header_row", "--w", help="output with headers (default is False)", default=False, action="store_true")
args = parser.parse_args()

path = args.path
size = int(args.size)
with_header_row = args.with_header_row

split_csv_file(path, size, with_header_row)