import os
import subprocess
import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        print(os.getcwd())
        p = subprocess.Popen('hdfs.exe toto', shell=True)
        res = p.wait()
        self.assertEqual(0, res)


if __name__ == '__main__':
    unittest.main()
