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
    def __init__(self, methodName='runTest'):
        super(TestBackup, self).__init__(methodName)
        # standard number of files to keep for generated chronology
        self.NBFILES = 29
        self.stop = datetime.datetime.now().date()
        # first day of current month and 15th day before are same day reduce number of files kept
        if self.stop.day == 16:
            self.NBFILES -= 1

    def tearDown(self):
        rmtree(self.dir)

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        print("created temporary directory ", self.dir)

    def createFiles(self, prefix=""):
        d = datetime.date(self.stop.year - 1, 1, 1)
        inc=datetime.timedelta(1)
        while (d < self.stop):
            touch(os.path.join(self.dir, "{0}{1:04d}-{2:02d}-{3:02d}-21H10.log".format(prefix, d.year, d.month, d.day)))
            d+=inc

    def test_chronology(self):
        self.createFiles()
        files = os.listdir(self.dir)
        self.assertTrue(len(files)>400)
        backup.cleanUpDir(
            self.dir,
            backup.FilePattern("^([0-9]{4})-([0-9]{2})-([0-9]{2})"),
            policy=backup.RetentionPolicy(now=self.stop),
            silent=True)
        files = os.listdir(self.dir)
        self.assertTrue(len(files) == self.NBFILES, "expected {} files found {}".format(self.NBFILES, len(files)))

    def test_multiple_series(self):
        self.createFiles("a")
        self.createFiles("b")
        files = os.listdir(self.dir)
        self.assertTrue(len(files) > 800)
        backup.cleanUpDir(self.dir, backup.FilePattern("a([0-9]{4})-([0-9]{2})-([0-9]{2})"),
                          policy=backup.RetentionPolicy(now=self.stop),
                          silent=True)
        backup.cleanUpDir(self.dir, backup.FilePattern("b([0-9]{4})-([0-9]{2})-([0-9]{2})"),
                          policy=backup.RetentionPolicy(now=self.stop),
                          silent=True)
        files = os.listdir(self.dir)
        self.assertTrue(len(files) == 2 * self.NBFILES)

    def test_prefix(self):
        self.createFiles("a")
        self.createFiles("b")
        files = os.listdir(self.dir)
        self.assertTrue(len(files) > 800)
        backup.cleanUpDir(self.dir, backup.FilePattern("(.*?)([0-9]{4})-([0-9]{2})-([0-9]{2})", prefix=True),
                          policy=backup.RetentionPolicy(now=self.stop),
                          silent=True)
        files = os.listdir(self.dir)
        self.assertTrue(len(files) == 2 * self.NBFILES)

    def test_no_match(self):
        touch(os.path.join(self.dir, "a2016-01-05"))
        touch(os.path.join(self.dir, "a2016-01-06"))
        backup.cleanUpDir(self.dir, backup.FilePattern("^([0-9]{4})-([0-9]{2})-([0-9]{2})"),
                          silent=True)
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

    def test_debug(self):
        touch(os.path.join(self.dir, "2016-01-05"))
        touch(os.path.join(self.dir, "2016-01-06"))
        backup.cleanUpDir(self.dir, backup.FilePattern("^([0-9]{4})-([0-9]{2})-([0-9]{2})"), test=True,
                          silent=True)
        files = os.listdir(self.dir)
        self.assertTrue(len(files) == 2)


if __name__ == '__main__':
    unittest.main()
