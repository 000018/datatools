from contextlib import closing

import csv_reader
import unittest

class CSVReaderTest(unittest.TestCase):
  def test_get_header(self):
    reader = csv_reader.CSVReader('testdata/test.csv.gz', zipped=True)
    header = reader.get_header()
    self.assertEqual(3, len(header))
    self.assertEqual(['COL1', 'COL2', 'COL3'], header)

  def test_get_content(self):
    reader = csv_reader.CSVReader('testdata/test.csv.gz', zipped=True)
    rows = []
    with closing(reader.get_content()) as content:
      for r in content:
        rows.append(r)
    self.assertEqual(2, len(rows))
    self.assertEqual(['v1.1', 'v1.2', 'v1.3'], rows[0])
    self.assertEqual(['v2.1', 'v2.2', 'v2.3'], rows[1])


if __name__ == '__main__':
  unittest.main()
