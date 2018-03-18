import unittest
from backup import FilePattern

class TestFilePattern(unittest.TestCase):
    def test_Match(self):
        p = FilePattern("([0-9]{4})-([0-9]{2})-([0-9]{2})")
        y,m,d, prefix = p.extract("2000-01-17")
        self.assertEqual(y,2000)
        self.assertEqual(m,1)
        self.assertEqual(d,17)
        self.assertIsNone(prefix)

if __name__ == '__main__':
    unittest.main()
