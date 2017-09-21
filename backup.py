import datetime
import os
import re


class RetentionPolicy:
    def __init__(self, keptDays=15, keptMonths=12, keptYears=-1):
        self.day = keptDays
        self.month = keptMonths
        self.year = keptYears
        d = datetime.datetime.now()
        if keptYears < 0:
            self.cutYear=-1
        else:
            self.cutYear=d.year-keptYears

        # cut month has year and month part
        self.cutMonth=(d.year - keptMonths//12, (d.month-keptMonths) % 12)
        cut = d - datetime.timedelta(keptDays)
        self.cutDay = (cut.year, cut.month, cut.day)

    def keepYear(self,year):
        return year >= self.cutYear
    def keepMonth(self, tupleYM):
        return tupleYM >= self.cutMonth
    def keepDay(self, tupleYMD):
        return tupleYMD >= self.cutDay

DEFAULT_POLICY = RetentionPolicy()

class FilePattern:
    def __init__(self, pattern, prefix=False, sortFunc=None, order=[1, 2, 3], strict=False):
        self.pattern=re.compile(pattern)
        if sortFunc != None:
            self.sort=sortFunc
        self.strict = strict
        self.order = order
        self.prefix = prefix

    def sort(self,files):
        return sorted(files)

    def extract(self, fileName):

        res=self.pattern.match(fileName)
        if self.prefix:
            if res == None or res.lastindex != 4:
                if self.strict:
                    raise Exception(
                        "unable to extract year month and day from file {} check regexp pattern {}".format(fileName,
                                                                                                           self.pattern))
                return None, None, None
            return int(res.group(self.order[0] + 1)), int(res.group(self.order[1] + 1)), int(
                res.group(self.order[2] + 1)), res.group(1)
        else:
            if res == None or res.lastindex != 3:
                if self.strict:
                    raise Exception(
                        "unable to extract year month and day from file {} check regexp pattern {}".format(fileName,
                                                                                                           self.pattern))
                return None, None, None, None
            return int(res.group(self.order[0])), int(res.group(self.order[1])), int(res.group(self.order[2])), None


def cleanUpDir(dir, pattern, policy=DEFAULT_POLICY, test=False):
    list = os.listdir(dir)
    sortedList = pattern.sort(list)

    curMonth= None
    curYear = None
    curPrefix = None
    for f in sortedList:
        fPath = os.path.join(dir, f)
        if os.path.isfile(fPath):
            #            print(f)
            year, month, day, prefix = pattern.extract(f)

            # check for prefix
            if prefix != curPrefix:
                curPrefix = prefix
                curMonth = None
                curYear = None

            # check for not a match if true skip file
            if year == None:
                continue
            # check for day limit
            if policy.keepDay((year,month, day)):
                # keep this file
                continue
            else:
                # check for year limit
                if year != curYear:
                    curYear=year
                    curMonth=month
                    if policy.keepYear(year):
                        continue
                if  month != curMonth:
                    curMonth=month
                    if  policy.keepMonth((year, month)):
                        continue
                print("deleting file: ", fPath)
                # if not protected by a keep policy delete file
                if not test:
                    os.remove(fPath)
