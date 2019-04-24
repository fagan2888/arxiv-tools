import re
import sys
import json
import subprocess
import traceback
import xml.etree.ElementTree as ET
import os
import pickle
from io import StringIO
import pdfminer.high_level
import tarfile
import re
import multiprocessing
from functools import partial
import itertools
import signal
import time

def pdf2txt(fp):
  output = StringIO()
  pdfminer.high_level.extract_text_to_fp(fp, output, laparams=pdfminer.layout.LAParams())
  return output.getvalue()


def get_file(fname, out_dir, flags='--force'):
  cmd = ['s3cmd', 'get', '--requester-pays', flags,
         's3://arxiv/%s' % fname, './%s' % out_dir]
  print(' '.join(cmd))
  subprocess.call(' '.join(cmd), shell=True)

def load_db(filename):
  if not os.path.exists(filename):
    # assume that no database exists, initialize one
    db = {'processed_tars' : set(), 'pdfs' : {}}
    save_db(db, filename)
    print('Could not find database, initialized database at ' + filename)
  with open(filename, 'rb') as handle:
    db = pickle.load(handle)
  return db

def save_db(db, filename):
  with open(filename, 'wb') as handle:
    pickle.dump(db, handle, protocol=pickle.HIGHEST_PROTOCOL)

class Timeout():
  """Timeout class using ALARM signal."""
  class Timeout(Exception):
    pass

  def __init__(self, sec):
    self.sec = sec

  def __enter__(self):
    signal.signal(signal.SIGALRM, self.raise_timeout)
    signal.alarm(self.sec)

  def __exit__(self, *args):
    signal.alarm(0)    # disable alarm

  def raise_timeout(self, *args):
    raise Timeout.Timeout()

def process_file(fname, out_dir, keywords):
  keyword_list = keywords.split(',')
  return_dict = {}
  # download file
  out_fname = '%s/%s' % (out_dir, fname)
  get_file(fname, out_fname, '--skip-existing')
  tar = tarfile.TarFile(out_fname)
  tar_members = list(tar.getmembers())
  for i, member in zip(range(len(tar_members)), tar.getmembers()):
    if re.match('[0-9]*/[0-9]{4}\.[0-9]*\.pdf', member.name):
      print("Processing file (%d / %d) in %s : %s: " % (i, len(tar_members), fname, member.name))
      return_dict[member.name] = {}
      f = tar.extractfile(member)
      # timeout of 120 seconds, some pdf2txt calls take way too long
      text = ""
      try:
        with Timeout(120):
          text = pdf2txt(f)
      except Timeout.Timeout:
        print("Failed: Timed out while processing " + member.name)
        text = ""
      except:
        print("Failed: Errored while processing " + member.name)
        traceback.print_exc()
        # print(str(e))
      for keyword in keyword_list:
        return_dict[member.name][keyword] = keyword in text.lower()
      f.close()
      
  tar.close()
  # delete file
  print("Finished processing and removing %s" % (fname,))
  os.remove(out_fname)
  return return_dict

def grouped(iterable, n):
  # https://gist.github.com/yoyonel/fb8c9d6fb06871db527492f5144b2e7b
   """
    >>> list(grouper(3, 'ABCDEFG'))
    [['A', 'B', 'C'], ['D', 'E', 'F'], ['G']]
    """
   iterable = iter(iterable)
   return iter(lambda: list(itertools.islice(iterable, n)), [])

def main(**args):
  db_file = args['database_file']
  db = load_db(db_file)
  out_dir = args['output_dir']
  keywords = args['keywords']
  os.makedirs(out_dir, exist_ok=True)
  get_file("pdf/arXiv_pdf_manifest.xml", out_dir)
  manifest_file = out_dir + '/arXiv_pdf_manifest.xml'
  log_file_path = args['log_file']

  log_file = open(log_file_path, 'a')
  try:
    file_list = []
    for event, elem in ET.iterparse(manifest_file):
      if event == 'end':
        if elem.tag == 'filename':
          fname = elem.text
          if fname not in db['processed_tars']:
            file_list += [fname]

  except:
    traceback.print_exc()

  print("Total files to process: ", len(file_list))
  pool = multiprocessing.Pool(processes=40)
  for files in grouped(file_list, 80):
    pool_arg = partial(process_file, out_dir=out_dir, keywords=keywords)
    print("Mapping tasks")
    counts = pool.map(pool_arg, files, 2)
    print("Updating database with counts")
    for filen, count in zip(files, counts):
      db['processed_tars'].add(filen)
      db['pdfs'].update(count)
      log_file.write(filen + '\n')
      log_file.write(str(count) + '\n')
      log_file.flush()
    save_db(db, db_file)
  pool.close()
  pool.join()    
    
  print('Finished')


if __name__ == '__main__':
  from argparse import ArgumentParser
  ap = ArgumentParser()
  ap.add_argument('--keywords', '-k', type=str, help="comma separated set of keyword to look for existence in the document. For example: 'apple,bat'")
  ap.add_argument('--database_file', '-d', type=str, default='db.pkl', help='keeps track of already downloaded files')
  ap.add_argument('--output_dir', '-o', type=str, default='data', help='the output directory')
  ap.add_argument('--log_file', default='processed.txt', help='a file that logs the processed txt files')
  args = ap.parse_args()
  main(**vars(args))
