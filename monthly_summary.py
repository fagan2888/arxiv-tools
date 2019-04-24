import os
import pickle
import datetime
from collections import defaultdict

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

    print("Monthly Counts:")
    print(counts)
    
    total_counts = defaultdict(int)
    for month, keywords in counts.items():
        for keyword, count in keywords.items():
            total_counts[keyword] += count
    print("Total Counts:")
    print(total_counts)

if __name__ == '__main__':
  from argparse import ArgumentParser
  ap = ArgumentParser()
  ap.add_argument('--database_file', '-d', type=str, default='db.pkl', help='keeps track of already downloaded files')
  args = ap.parse_args()
  main(**vars(args))
