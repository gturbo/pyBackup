import datetime
import os
import subprocess
import tempfile
import time
import unittest
from shutil import rmtree

import backup


def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)


class TestBackupByMonth(unittest.TestCase):
    def tearDown(self):
        rmtree(self.dir)
        rmtree(self.dest)

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.dest = tempfile.mkdtemp()
        print("created temporary directory ", self.dir)

    def createFiles(self, prefix=""):
        stop = datetime.datetime(2017, 10, 10)
        d = datetime.datetime(stop.year - 1, 1, 1)
        inc = datetime.timedelta(1)
        while (d < stop):
            t = time.mktime(d.timetuple())
            touch(os.path.join(self.dir, "{0}{1:04d}-{2:02d}-{3:02d}-21H10.log".format(prefix, d.year, d.month, d.day)),
                  (t, t))
            d += inc

    def testSimple(self):
        self.createFiles()
        self.assertTrue(True)
        filesBefore = os.listdir(self.dir)
        self.assertTrue(len(filesBefore) > 600)
        backupBefore = os.listdir(self.dest)
        self.assertTrue(len(backupBefore) == 0)
        backup.backupByMonth(self.dir, self.dest, 'test', 2)
        filesAfter = os.listdir(self.dir)
        backupAfter = os.listdir(self.dest)
        self.assertTrue(len(filesAfter) == 38)
        self.assertTrue(len(backupAfter) == 23)

    def testCmdLine(self):
        self.createFiles()
        self.assertTrue(True)
        filesBefore = os.listdir(self.dir)
        self.assertTrue(len(filesBefore) > 600)
        backupBefore = os.listdir(self.dest)
        self.assertTrue(len(backupBefore) == 0)
        env = os.environ.copy()
        p = subprocess.Popen(['python', '../backup.py', 'backupByMonth', self.dir, self.dest, 'test', str(2)])
        s = p.wait()
        self.assertTrue(s == 0)
        filesAfter = os.listdir(self.dir)
        backupAfter = os.listdir(self.dest)
        self.assertTrue(len(filesAfter) == 38)
        self.assertTrue(len(backupAfter) == 23)


if __name__ == '__main__':
    unittest.main()
