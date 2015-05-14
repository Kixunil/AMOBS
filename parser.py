#!/usr/bin/python

import re

dateRegex = re.compile(r"^[1-9][0-9]{3,} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) +[1-9][0-9]? +[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.[0-9]{3}")

def findDate(f):
    for line in f:
        if dateRegex.match(line):
            return True
    return False

def parseKV(l):
    separator = l.find(':')
    if separator < 0:
        separator = l.find('=')

    key = l[0:separator]
    val = l[separator+1:]

    return key.strip(), val.strip()

def slice_columns(header, b, e, prefix):
    last = b
    i = b
    while i < e:
        if header[0][i] == '|':
            if len(header) > 1:
                for f in slice_columns(header[1:], last, i, prefix + header[0][last:i].strip() + " "):
                    yield f
            else:
                yield prefix + header[0][last:i].strip()

            last = i + 1

        i += 1

    if last < e:
        if len(header) > 1:
            for f in slice_columns(header[1:], last, i, prefix + header[0][last:i].strip() + " "):
                yield f
        else:
            yield prefix + header[0][last:i].strip()


def parseTable(f):
    line = f.next().strip()
    header = []

    while len(line) > 2 and line[1] != '-':
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

f = open("final set/A.txt")
findDate(f)

kvRegex = re.compile(".*[:=].*")

try:
    while True:
        line = f.next().strip()
        if line == "":
            pass
        elif dateRegex.match(line):
            pass
        elif line[0] == '-' or line[0] == '|':
            parseTable(f)
        elif kvRegex.match(line):
            key, value = parseKV(line)
            print key, value

except StopIteration:
    pass
