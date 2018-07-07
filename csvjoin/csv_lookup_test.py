from contextlib import closing
import csv_lookup
import csv_reader
import unittest

class CSVLookupTest(unittest.TestCase):
  def test_get_join(self):
    output_fn = '/tmp/output.csv.gz'
    lookup = csv_lookup.CSVLookup(
        data_file_name='testdata/test.csv.gz', data_short_name='',
        lookup_file_name='testdata/lookup.csv.gz', lookup_short_name='lookup',
        key_name='COL1', output_file_name=output_fn, zipped=True)
    lookup.join()

    reader = csv_reader.CSVReader(output_fn, zipped=True)
    header = reader.get_header()
    self.assertEqual(4, len(header))
    self.assertEqual(['COL1', 'COL2', 'COL3', 'lookup.COL2'], header)

    rows = []
    with closing(reader.get_content()) as content:
      for r in content:
        rows.append(r)
    self.assertEqual(2, len(rows))
    self.assertEqual(['v1.1', 'v1.2', 'v1.3', 'l1.1'], rows[0])
    self.assertEqual(['v2.1', 'v2.2', 'v2.3', 'l2.1'], rows[1])

if __name__ == '__main__':
  unittest.main()
