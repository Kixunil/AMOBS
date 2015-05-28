# -*- coding: utf8 -*-

import re
import sys
import sqlite3
import dateutil.parser as dparser

try:
    logPath = sys.argv[1]
    dataPath = sys.argv[2]
except Exception as e:
    print "NO NO NO argumenty!"
    sys.exit(1)


# Regex for finding the beginning of log
dateRegex = re.compile(r"^[1-9][0-9]{3,} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) +[1-9][0-9]? +[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.[0-9]{3}")
numberRegex = re.compile(r"^[-+]?\d*\,\d+|\d+$")

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

# Recursive function which creates list of col
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
def parseTable(f, conn, log_id, table_id):
    line = f.next().strip()
    header = []

    # Read header
    while len(line) > 2 and line[1] != '-':
        if line[0] != '|':
            print "Error: not a table"
            return False

        header.append(line)
        line = f.next().strip()

    if len(header) == 0:
        print "Warning: empty header"
    else:
        # Parse header
        cols = list(slice_columns(header, 1, len(header[0]), ""))

    line = f.next().strip()

    # Read table rows
    row_id = 1
    while len(line) > 0:
        row = line[1:-1].split('|')

        # Store row into database
        for i in range(len(row)):
            if numberRegex.match(row[i].strip()):
                row[i] = row[i].replace(",", ".").strip()
                conn.execute('''INSERT INTO tables (log_id, table_id, table_col_name, table_row_id, value_num) VALUES (?, ?, ?, ?, ?)''', [log_id, table_id, cols[i], row_id, float(row[i])])
            else:
                conn.execute('''INSERT INTO tables (log_id, table_id, table_col_name, table_row_id, value_str) VALUES (?, ?, ?, ?, ?)''', [log_id, table_id, cols[i], row_id, row[i].strip()])

        line = f.next().strip()
        row_id += 1

    return True

# Program beginning
# Open log
f = open(logPath)

# Find first log line, skip anything else at the beginning
findDate(f)

# Prepare key-value regex
kvRegex = re.compile(".*[:=].*")

# By convention IDs in SQL database count from 1, not 0
log_id = 1
table_id = 1

# Open database file
conn = sqlite3.connect(dataPath)
conn.text_factory = str

# Create tables
conn.execute('''CREATE TABLE IF NOT EXISTS logs (log_id INT, log_type INT, date TEXT)''')
conn.execute('''CREATE TABLE IF NOT EXISTS keyvals (log_id INT, key TEXT, value TEXT)''')
conn.execute('''CREATE TABLE IF NOT EXISTS tables (log_id INT, table_id TINYINT, table_col_name TEXT, table_row_id INT, value_str TEXT, value_num DOUBLE)''')

# Remove old content
conn.execute('''DELETE FROM logs''')
conn.execute('''DELETE FROM keyvals''')
conn.execute('''DELETE FROM tables''')

try:
    while True:
        # Iterate over lines
        line = f.next().strip()

        # Skip empty lines
        if line == "":
            pass

        # Log beginning found
        # TODO: store date
        elif dateRegex.match(line):
            # Log type is parsed and stored as number
            datePar = ' '.join(line.split()[:4])
            datePar = dparser.parse(datePar)
            
            log_type_b = line.find("0x")
            log_type_e = line[log_type_b:].find(" ")
            log_type = int(line[log_type_b:log_type_b+log_type_e], 0)
            conn.execute('''INSERT INTO logs (log_id, log_type, date) VALUES (?, ?, ?)''', [log_id, log_type, datePar] )
            log_id += 1
            table_id = 1

        # Check if line is beginning of a table
        elif line[0] == '-' or line[0] == '|':
            if parseTable(f, conn, log_id, table_id):
                table_id += 1

        # Check if line is key-value
        elif kvRegex.match(line):
            key, value = parseKV(line)
            conn.execute('''INSERT INTO keyvals (log_id, key, value) VALUES (?, ?, ?)''', [log_id, key, value])

# Thrown when end of file is reached
except StopIteration:
    pass

# Confirm changes to database (database is left unchanged in case of program crash)
conn.commit()
