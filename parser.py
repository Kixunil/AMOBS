#!/usr/bin/python

import re

# Regex for finding the beginning of log
dateRegex = re.compile(r"^[1-9][0-9]{3,} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) +[1-9][0-9]? +[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.[0-9]{3}")

# Finds next beginning of the log, skipping everything else
def findDate(f):
    for line in f:
        if dateRegex.match(line):
            return True
    return False

# Parses key-value pairs (those before tables), supports : and = separators
def parseKV(l):
    separator = l.find(':')
    if separator < 0:
        separator = l.find('=')

    key = l[0:separator]
    val = l[separator+1:]

    return key.strip(), val.strip()

# Recursive function which creates list of columns
def slice_columns(header, b, e, prefix):
    last = b
    i = b
    # This loop searches for pipes inside column name and constructs new columns and their names if they are found
    while i < e:
        if header[0][i] == '|':
            if len(header) > 1:
                for f in slice_columns(header[1:], last, i, (prefix.strip() + " " + header[0][last:i].strip()).strip()):
                    yield f
            else:
                yield (prefix.strip() + " " +  header[0][last:i].strip()).strip()

            last = i + 1

        i += 1

    if last < e:
        if len(header) > 1:
            for f in slice_columns(header[1:], last, i, (prefix.strip() + " " + header[0][last:i].strip()).strip()):
                yield f
        else:
            yield (prefix.strip() + " " + header[0][last:i].strip()).strip()

# This function parses any table found inside log
def parseTable(f):
    line = f.next().strip()
    header = []

    while len(line) > 2 and line[1] != '-':
        if line[0] != '|':
            print "Error: not a table"
            return
        header.append(line)
        line = f.next().strip()

    if len(header) == 0:
        print "Warning: empty header"
    else:
        print "Header found"
        for col in slice_columns(header, 1, len(header[0]), ""):
            print col

    line = f.next().strip()

    while len(line) > 0:
        line = f.next().strip()

        row = []
        val = ""
        for char in line[1:]:
            if char == '|':
                row.append(val)
                val = ""
            else:
                val += char

        print row

# Program beginning
# Open log
f = open("final set/A.txt")

# Find first log line, skip anything else at the beginning
findDate(f)

# Prepare key-value regex
kvRegex = re.compile(".*[:=].*")

try:
    while True:
        # Iterate over lines
        line = f.next().strip()

        # Skip empty lines
        if line == "":
            pass

        # Log beginning found
        # TODO: store new log information
        elif dateRegex.match(line):
            pass
        # Check if line is beginning of a table
        elif line[0] == '-' or line[0] == '|':
            parseTable(f)

        # Check if line is key-value
        elif kvRegex.match(line):
            key, value = parseKV(line)
            print key, value

except StopIteration:
    pass
