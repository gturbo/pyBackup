import datetime
import os
import tempfile
import unittest
from shutil import rmtree

import backup


def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

class TestBackup(unittest.TestCase):
    def tearDown(self):
        rmtree(self.dir)

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        print("created temporary directory ", self.dir)

    def createFiles(self, prefix=""):
        d=datetime.date(2016,1,1)
        stop = datetime.date(2017,9,15)
        inc=datetime.timedelta(1)
        while (d < stop):
            touch(os.path.join(self.dir, "{0}{1:04d}-{2:02d}-{3:02d}-21H10.log".format(prefix, d.year, d.month, d.day)))
            d+=inc

    def test_chronology(self):
        self.createFiles()
        files = os.listdir(self.dir)
        self.assertTrue(len(files)>400)
        backup.cleanUpDir(self.dir, backup.FilePattern("^([0-9]{4})-([0-9]{2})-([0-9]{2})"))
        files = os.listdir(self.dir)
        self.assertTrue(len(files) == 23)

    def test_multiple_series(self):
        self.createFiles("a")
        self.createFiles("b")
        files = os.listdir(self.dir)
        self.assertTrue(len(files) > 800)
        backup.cleanUpDir(self.dir, backup.FilePattern("a([0-9]{4})-([0-9]{2})-([0-9]{2})"))
        backup.cleanUpDir(self.dir, backup.FilePattern("b([0-9]{4})-([0-9]{2})-([0-9]{2})"))
        files = os.listdir(self.dir)
        self.assertTrue(len(files) == 46)

    def test_prefix(self):
        self.createFiles("a")
        self.createFiles("b")
        files = os.listdir(self.dir)
        self.assertTrue(len(files) > 800)
        backup.cleanUpDir(self.dir, backup.FilePattern("(.*?)([0-9]{4})-([0-9]{2})-([0-9]{2})", prefix=True))
        files = os.listdir(self.dir)
        self.assertTrue(len(files) == 46)

    def test_no_match(self):
        touch(os.path.join(self.dir, "a2016-01-05"))
        touch(os.path.join(self.dir, "a2016-01-06"))
        backup.cleanUpDir(self.dir, backup.FilePattern("^([0-9]{4})-([0-9]{2})-([0-9]{2})"))
        files = os.listdir(self.dir)
        self.assertTrue(len(files) == 2)

    def test_no_match_strict(self):
        touch(os.path.join(self.dir, "a2016-01-05"))
        touch(os.path.join(self.dir, "a2016-01-06"))
        try:
            backup.cleanUpDir(self.dir, backup.FilePattern("^([0-9]{4})-([0-9]{2})-([0-9]{2})", strict=True))
            self.fail("did not throw Exception")
        except:
            pass



if __name__ == '__main__':
    unittest.main()
