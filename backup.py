import datetime
import os
import re
import subprocess
import sys
import time


class RetentionPolicy:
    def __init__(self, keptDays=15, keptMonths=12, keptYears=-1, now=datetime.datetime.now()):
        self.day = keptDays
        self.month = keptMonths
        self.year = keptYears
        self.now = now
        if keptYears < 0:
            self.cutYear=-1
        else:
            self.cutYear = now.year - keptYears

        # cut month has year and month part
        self.cutMonth = (now.year - keptMonths // 12, (now.month - keptMonths) % 12)
        cut = now - datetime.timedelta(keptDays)
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

        res = self.pattern.match(fileName)
        if self.prefix:
            if res == None or res.lastindex != 4:
                if self.strict:
                    raise Exception(
                        "unable to extract year month and day from file {0} check regexp pattern {1}".format(fileName,
                                                                                                           self.pattern))
                return None, None, None
            return int(res.group(self.order[0] + 1)), int(res.group(self.order[1] + 1)), int(
                res.group(self.order[2] + 1)), res.group(1)
        else:
            if res == None or res.lastindex != 3:
                if self.strict:
                    raise Exception(
                        "unable to extract year month and day from file {0} check regexp pattern {1}".format(fileName,
                                                                                                           self.pattern))
                return None, None, None, None
            return int(res.group(self.order[0])), int(res.group(self.order[1])), int(res.group(self.order[2])), None


def cleanUpDir(dir, pattern, policy=DEFAULT_POLICY, test=False, silent=False, verbose=False):
    if not silent:
	print("{0} starting cleanup of directory {1}".format(datetime.datetime.now(), dir)) 
    list = os.listdir(dir)
    if verbose:
        print("found {0} files".format(len(list)))
    sortedList = pattern.sort(list)

    curMonth= None
    curYear = None
    curPrefix = None
    for f in sortedList:
        fPath = os.path.join(dir, f)
        if os.path.isfile(fPath):
            if verbose:
                print("VERBOSE handling file {0}".format(f))
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
                if not silent:
                    print("deleting file: {0}".format(fPath))
                # if not protected by a keep policy delete file
                if not test:
                    os.remove(fPath)
    if not silent:
        print("{0} end cleanup of directory {1}".format(datetime.datetime.now(), dir))


def backupByMonth(src, dest, prefix='', removeOlderThan=None, debug=False):
    months = {}
    cutMonth = ''
    if removeOlderThan is not None:
        removeOlderThan2 = int(removeOlderThan) if type(removeOlderThan) is str else removeOlderThan
        curDate = datetime.datetime.now()
        year = curDate.year
        month = curDate.month
        (minusYear, minusMonth) = divmod(removeOlderThan2, 12)
        if minusMonth >= month:
            year -= 1
            month += 12
        cutMonth = '{0}-{1:02d}'.format(year - minusYear, month - minusMonth)

    for entry in os.listdir(src):
        f_entry = os.path.join(src, entry)
        # month of file modification
        mMonth = time.strftime('%Y-%m', time.gmtime(os.path.getmtime(f_entry)))
        if mMonth in months:
            months[mMonth].append(f_entry)
        else:
            months[mMonth] = [f_entry]
    # list of tuple (process, month to cleane-up) month to clean-up empty if no deletion needed
    tarProcesses = []
    for month in months:
        tarFile = os.path.join(dest, prefix + month + '.tgz')
        print('adding {0} files for month {1} to archive {2}'.format(len(months[month]), month, tarFile))
        tarCmd = ['tar', '-czf', tarFile] + months[month]
        if removeOlderThan is not None and month <= cutMonth:
            # put month to delete as second element of tuple
            tarProcesses.append((subprocess.Popen(tarCmd), month))
        else:
            tarProcesses.append((subprocess.Popen(tarCmd), None))

    rmProcess = []
    hasErrors = False
    for (p, month) in tarProcesses:
        # wait for backup completion before deletion
        s = p.wait()
        if s != 0:
            hasErrors = True
        if month is not None:
            rmCmd = ['rm', '-rf'] + months[month]
            if debug:
                print('not deleting files after backup', rmCmd)
            else:
                print('deleting files for month {0} after backup'.format(month))
                rmProcess.append(subprocess.Popen(rmCmd))

    statuses = [p.wait() for p in rmProcess]
    errors = [s for s in statuses if s != 0]

    return 0 if not hasErrors and len(errors) == 0 else 1


if __name__ == '__main__':
    # retrieve args without this file name args[0]
    args = sys.argv[1:]
    #    print(args)
    argCount = len(args)
    if argCount > 1:
        function = args[0]
        if function == 'cleanUpDir':
            if argCount > 2 and argCount <= 7:
                cleanUpDir(args[1:])
            else:
                print('wrong number of arguments for function {0}'.format(function))
        elif function == 'backupByMonth':
            if argCount > 2 and argCount <= 6:
                backupByMonth(*args[1:])
            else:
                print('wrong number of arguments for function {0}'.format(function))
        else:
            print('you must supply function name and arguments to call this module like this')
