# use data[key] as lookup key into lookup_data
# and combine the columns from data and lookup_data into new table.
from contextlib import closing
import csv
import csv_reader

class CSVLookup(object):
  def __init__(self, data_file_name, data_short_name,
               lookup_file_name, lookup_short_name,
               key_name, output_file_name, zipped=False):
    self.data_reader_ = csv_reader.CSVReader(data_file_name, zipped)
    self.lookup_reader_ = csv_reader.CSVReader(lookup_file_name, zipped)
    self.data_short_name_ = data_short_name
    self.lookup_short_name_ = lookup_short_name
    self.data_header_ = self.data_reader_.get_header()
    self.lookup_header_ = self.lookup_reader_.get_header()
    assert key_name in self.data_header_
    assert key_name in self.lookup_header_
    self.key_name_ = key_name
    self.output_file_name_ = output_file_name
    self.zipped_ = zipped

  def join(self):
    lookup_idx = -1
    for idx, col_name in enumerate(self.lookup_header_):
      if self.key_name_ == col_name:
        lookup_idx = idx
    assert lookup_idx >= 0
    data_idx = -1
    for idx, col_name in enumerate(self.data_header_):
      if self.key_name_ == col_name:
        data_idx = idx
    assert data_idx >= 0

    output_lookup_header = ['%s.%s' % (self.lookup_short_name_, h) for
                             h in self.lookup_header_]
    del output_lookup_header[lookup_idx]
    if self.data_short_name_:
      output_data_header = ['%s.%s' % (self.data_short_name_, h) for
                            h in self.data_header_]
    else:
      output_data_header = self.data_header_

    # do in-memory lookup for now, and assume key is unique.
    lookup_table = {}
    with closing(self.lookup_reader_.get_content()) as lookup_data:
      for lookup_row in lookup_data:
        key = lookup_row[lookup_idx]
        del lookup_row[lookup_idx]
        assert key not in lookup_table, key
        lookup_table[key] = lookup_row

    csv_opener = csv_reader.CSVOpener(self.output_file_name_, self.zipped_, 'w')
    with closing(self.data_reader_.get_content()) as data, csv_opener.open() as fp:
      writer = csv.writer(fp)
      output_header = output_data_header + output_lookup_header
      writer.writerow(output_header)
      print output_header
      for row in data:
        key = row[data_idx]
        assert key in lookup_table
        lookup_row = lookup_table[key]
        output_row = row + lookup_row
        assert len(output_row) == len(output_header), output_row
        writer.writerow(output_row)
        
