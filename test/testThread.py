import time
from multiprocessing import Pool

N = 1000000
P = 4


def counter(n):
    # Calculate the result
    count = 1L
    for i in range(1, n + 1):
        count += 1

    return count


def multiproc():
    pool = Pool(P)
    multiple_results = [pool.apply_async(counter, [i]) for i in range(N, N + P)]
    results = [res.get(timeout=10) for res in multiple_results]
    print(results)


def monoproc():
    multiple_results = [counter(i) for i in range(N, N + P)]
    print(multiple_results)


def timetaker(label, func):
    print("starting " + label)
    # warm-up run
    func()
    start = time.clock()
    for i in range(5):
        func()
    end = time.clock()
    duration = (end - start) / 5
    print("{0} duration {1}".format(label, duration))
    return duration


if __name__ == '__main__':
    d1 = timetaker("monoproc", monoproc)
    d2 = timetaker("multiproc", multiproc)
    print("monoproc time is {0} times multiproc time".format(d1 / d2))
