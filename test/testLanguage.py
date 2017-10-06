import os
import threading
import time


def test_list():
    def getNextOrNone(it):
        try:
            return it.next()
        except StopIteration:
            return None

    l = [1, 2, 3]
    it = iter(l)
    for i in range(4):
        v = getNextOrNone(it)
        print(v)


def testThread():
    def ok(i):
        print("starting thread " + str(i))
        i += 1
        os.execvp("")

    def fail(i):
        ok(i + 100)
        raise Exception("broken")

    for j in range(2):
        # TODO give the thread a significant name in order to be able to trace errors in threaded tasks
        threading.Thread(target=ok, args=(j,), name="Thread ok " + str(j)).start()
        threading.Thread(target=fail, args=(j,), name="Thread fail " + str(j)).start()
        time.sleep(1)

    main_thread = threading.currentThread()

    while True:
        _break = True
        print("number of threads: {}".format(len(threading.enumerate())))
        for t in threading.enumerate():
            if t is main_thread:
                continue
            _break = False
            print('active:  {0}'.format(t.getName()))
        if _break:
            break
        time.sleep(1)


if True: print('must print')
if False: print('must not print')
