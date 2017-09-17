import os
import re
import datetime

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
    def __init__(self, pattern, sortFunc=None, order=[1,2,3], strict=False):
        self.pattern=re.compile(pattern)
        if sortFunc != None:
            self.sort=sortFunc
        self.strict = strict
        self.order = order

    def sort(self,files):
        return sorted(files)

    def extract(self,fileName):
        res=self.pattern.match(fileName)
        if res.lastindex != 3:
            if self.strict:
                raise Exception("unable to extract year month and day from file {}".format(fileName))
            return None, None, None
        return int(res.group(self.order[0])), int(res.group(self.order[1])),int(res.group(self.order[2]))

def cleanUpDir(dir, pattern, policy=DEFAULT_POLICY):
    list = os.listdir(dir)
    sortedList = pattern.sort(list)

    curMonth= None
    curYear = None
    for f in sortedList:
        if os.path.isfile(os.path.join(dir, f)):
            year,month, day = pattern.extract(f)
            # check for day limit
            if policy.keepDay((year,month, day)):
              # keep this and all remaining files that are inside kept days limit
              break
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
                # if not protected by a keep policy delete file
                os.remove(os.path.join(dir,f))
