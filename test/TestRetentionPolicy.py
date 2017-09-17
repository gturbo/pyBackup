import unittest
from  backup import RetentionPolicy
import datetime

class TestRetentionPolicy(unittest.TestCase):
    def test_keep(self):
        p = RetentionPolicy(1,1,1)
        now = datetime.datetime.now()

        self.assertTrue(p.keepYear(now.year))
        self.assertTrue(p.keepYear(now.year-1))
        self.assertFalse(p.keepYear(now.year-2))


        self.assertTrue(p.keepMonth((now.year, now.month)))
        prevMonth = now.replace(day=1) - datetime.timedelta(days=1)
        self.assertTrue(p.keepMonth((prevMonth.year, prevMonth.month)))
        prevMonth = prevMonth.replace(day=1) - datetime.timedelta(days=1)
        self.assertFalse(p.keepMonth((prevMonth.year, prevMonth.month)))

        day = now
        self.assertTrue(p.keepDay((day.year, day.month, day.day)))
        day = day-datetime.timedelta(1)
        self.assertTrue(p.keepDay((day.year, day.month, day.day)))
        day = day-datetime.timedelta(1)
        self.assertFalse(p.keepDay((day.year, day.month, day.day)))

if __name__ == '__main__':
    unittest.main()
