import os
import re
import subprocess
import sys

__envExecCmd = None
__isMgt = None
REG_MANAGEMENT_NODE = r'^(s[drp]iopmgt[ab][0-9]{2,4})\.(developpement|recette|production)\.local$(?i)'


def execLinuxCmd(command):
    global __isMgt, __envExecCmd
    if __isMgt is None:
        if sys.platform != 'win32':
            __isMgt = True
        else:
            __isMgt = False
            __env = os.environ.copy()
            # retrieve agent process
            environFilePath = 'U:\\.ssh\\environment'
            print(environFilePath)
            found = False
            with open(environFilePath, 'r') as envFile:
                for line in envFile:
                    pattern = re.compile(r'SSH_AUTH_SOCK=([^;]+)')
                    m = pattern.match(line)
                    if m:
                        found = True
                        __env['SSH_AUTH_SOCK'] = m.group(1)
                        break
            if not found:
                raise Exception('Unable to find ssh agent configuration file {0}'.format(environFilePath))

    if __isMgt:
        p = subprocess.Popen(['hostname'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        p = subprocess.Popen(['ssh', 'et20795@sriopmgta0101.recette.local', 'hostname'], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, env=__env)
    out, err = p.communicate()
    print('out:\n{0}\nerr:\n{1}'.format(out, err))


execLinuxCmd('hostname')
