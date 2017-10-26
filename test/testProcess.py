import cStringIO
import subprocess
import time

args = ['ls', '-l', '/tmp']
NPROCESS = 100
DEV_NULL = cStringIO.StringIO().

# warmup
subprocess.call(args, stdout=DEV_NULL, bufsize=4000)

start1 = time.clock()
plist = []
for i in range(NPROCESS):
    plist.append(subprocess.Popen(args, stdout=DEV_NULL, bufsize=4000))

exit_codes = [p.wait() for p in plist]
end1 = time.clock()
print("end round 1")
for i in range(NPROCESS):
    subprocess.call(args, stdout=DEV_NULL, bufsize=4000)

end2 = time.clock()

t1 = end1 - start1
t2 = end2 - end1
print('t1 {0} t2 {1} diff {2}'.format(t1, t2, int(t1 / t2 * 100)))

DEV_NULL.close()
