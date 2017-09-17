import unittest
import tempfile
from shutil import rmtree
import datetime
import backup

import os

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

class TestBackup(unittest.TestCase):
    def tearDown(self):
        super().tearDown()
        rmtree(self.dir)

    def setUp(self):
        super().setUp()
        self.dir = tempfile.mkdtemp()
        print("created temporary directory ", self.dir)

    def createFiles(self):
        d=datetime.date(2016,1,1)
        stop = datetime.date(2017,9,15)
        inc=datetime.timedelta(1)
        while (d < stop):
            touch(os.path.join(self.dir,"{0:04d}-{1:02d}-{2:02d}-21H10.log".format(d.year,d.month,d.day)))
            d+=inc


    def test_something(self):
        self.createFiles()
        files = os.listdir(self.dir)
        self.assertTrue(len(files)>400)
        backup.cleanUpDir(self.dir, backup.FilePattern("^([0-9]{4})-([0-9]{2})-([0-9]{2})"))
        files = os.listdir(self.dir)
        self.assertTrue(len(files) < 50)

if __name__ == '__main__':
    unittest.main()
