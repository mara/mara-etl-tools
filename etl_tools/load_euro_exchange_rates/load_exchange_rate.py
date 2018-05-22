import csv
import sys
import urllib.request
import zipfile
from io import BytesIO, StringIO

ZIP_FILE_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
ZIP_FILE_ARCHIVE_NAME = 'eurofxref-hist.csv'
CSV_DATE_KEY = 'Date'

zipfile = zipfile.ZipFile(BytesIO(urllib.request.urlopen(ZIP_FILE_URL).read()))
input_stream = StringIO(zipfile.read(ZIP_FILE_ARCHIVE_NAME).decode('utf-8'))
csv_reader = csv.DictReader(input_stream, delimiter=',')

csv_writer = csv.writer(sys.stdout, delimiter="\t")

for row in csv_reader:
    for currency in row.keys():
        # all currency codes have length 3
        if len(currency) != 3:
            continue
        if row[currency] == 'N/A':
            continue
        csv_writer.writerow([currency, row[currency], row[CSV_DATE_KEY]])
