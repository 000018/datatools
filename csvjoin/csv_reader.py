import csv
import gzip

class CSVOpener(object):
  def __init__(self, csv_file_name, zipped=False, mode='r'):
    self.csv_file_name_ = csv_file_name
    self.zipped_ = zipped
    self.mode_ = mode


  def open(self):
    if self.zipped_:
      return gzip.open(self.csv_file_name_, self.mode_)
    return open(self.csv_file_name_, self.mode_)


class CSVReader(CSVOpener):
  def get_header(self):
    with self.open() as fp:
      reader = csv.reader(fp)
      header = reader.next()
      return header
  

  def get_content(self):
    '''An iterator over content of csv.

       intended usage:
       from contextlib import closing
       with closing(gen_content()) as items:
         for item in items:
           # do stuff
    ''' 
    with self.open() as fp:
      reader = csv.reader(fp)
      # skip header
      reader.next()
      for row in reader:
        yield row


