import sys

print sys.stdout.encoding

print sys.exc_info()

try:
    a = 1 / 0
except:
    print sys.exc_info()

print('coucou')
t, v, s = sys.exc_info()
print(s)
