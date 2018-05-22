import zipfile
import csv
import urllib.request
import sys
import os
from io import BytesIO, StringIO

# allow the script to import app module
script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_path + '/../../../../../')

# initialize app so that database configs are known
# noinspection PyUnresolvedReferences
import app

import mara_db.postgresql

ZIP_FILE_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
ZIP_FILE_ARCHIVE_NAME = 'eurofxref-hist.csv'
CSV_DATE_KEY = 'Date'
TARGET_TABLE = 'euro_fx.exchange_rate'

print('downloading ' + ZIP_FILE_URL)
zipfile = zipfile.ZipFile(BytesIO(urllib.request.urlopen(ZIP_FILE_URL).read()))
input_stream = StringIO(zipfile.read(ZIP_FILE_ARCHIVE_NAME).decode('utf-8'))
csv_reader = csv.DictReader(input_stream, delimiter=',')

print('writing results')
result_stream = StringIO()
csv_writer = csv.writer(result_stream, delimiter="\t")

for row in csv_reader:
    for currency in row.keys():
        # all currency codes have length 3
        if len(currency) != 3:
            continue
        if row[currency] == 'N/A':
            continue
        csv_writer.writerow([currency, row[currency], row[CSV_DATE_KEY]])


with mara_db.postgresql.postgres_cursor_context(sys.argv[1]) as cursor:
    result_stream.seek(0)
    cursor.copy_from(result_stream, TARGET_TABLE)


