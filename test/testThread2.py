import time

NUM_RANGE = 100000000

from multiprocessing import Process


def timefunc(f):
    t = time.time()
    f()
    return time.time() - t


def multi():
    class MultiProcess(Process):
        def __init__(self):
            Process.__init__(self)

        def run(self):
            # Alter string + test processing speed
            for i in xrange(NUM_RANGE):
                a = 20 * 20

    thread1 = MultiProcess()
    thread2 = MultiProcess()
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()


def single():
    for i in xrange(NUM_RANGE):
        a = 20 * 20

    for i in xrange(NUM_RANGE):
        a = 20 * 20


print timefunc(multi) / timefunc(single)
