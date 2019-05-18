import os
import pickle
import datetime
from collections import defaultdict

import csv

def main(**args):
    db_file = args['database_file']
    with open(db_file, 'rb') as f:
        db = pickle.load(f)
    entries = db['pdfs']
    del db

    counts = {}
    # each key in entries is of the form '0704/0704.2368.pdf'
    # i.e. YYMM/YYMM.identifier.pdf
    for key, value in entries.items():
        year = int(key[0] + key[1])
        month = int(key[2] + key[3])
        monthyear = datetime.datetime(year, month, 1)
        if not monthyear in counts:
            counts[monthyear] = defaultdict(int)
        for keyword, exists in value.items():
            counts[monthyear][keyword] += int(exists)

    with open('file_by_file.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        # write header
        writer.writerow(['filename', 'year', 'month'] + list(list(counts.values())[0].keys()))
        # write rows
        for key, value in entries.items():
            year = int(key[0] + key[1])
            month = int(key[2] + key[3])
            exists = list(value.values())
            exists = [int(v) for v in exists]
            writer.writerow([str(key), str(year + 2000), str(month)] + exists)
    with open('monthly_summary.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        # write header
        writer.writerow(['year', 'month'] + list(list(counts.values())[0].keys()))
        # write rows
        for monthyear,keywords in counts.items():
            writer.writerow([str(monthyear.year + 2000), str(monthyear.month)] + list(keywords.values()))
    

if __name__ == '__main__':
  from argparse import ArgumentParser
  ap = ArgumentParser()
  ap.add_argument('--database_file', '-d', type=str, default='db.pkl', help='keeps track of already downloaded files')
  args = ap.parse_args()
  main(**vars(args))
