import json
import os
import pycurl
import socket
from StringIO import StringIO


def curlGet(url, fromPosteBred=False):
    buffer = StringIO()
    c = pycurl.Curl()
    c = pycurl.Curl()
    # c.setopt(pycurl.VERBOSE,1)
    if fromPosteBred:
        # remove any proxy setting in environment
        del os.environ['HTTP_PROXY']
        del os.environ['HTTPS_PROXY']
    c.setopt(pycurl.URL, url)
    c.setopt(c.WRITEFUNCTION, buffer.write)
    c.perform()
    c.close()
    return buffer.getvalue()


fromPostBred = False
if socket.gethostname().startswith('CB'):
    fromPostBred = True


class Cluster():
    def __init__(self, namenode, port, user):
        self.namenode = namenode
        self.port = port
        self.user = user
        self.baseUrl = 'http://' + namenode + ':' + str(port) + '/webhdfs/v1'
        self.userUrl = '?user.name=' + self.user

    def lsUrl(self, dir):
        return self.baseUrl + dir + self.userUrl + '&op=LISTSTATUS'


cluster = Cluster('sriopmgta0101.recette.local', '50070', 'hdfs')
cluster.lsUrl(dir)


def getDirList(cluster, dir):
    cluster.lsUrl(dir)


dir_raw = json.loads(curlGet('http://:50070/webhdfs/v1/tmp?user.name=hdfs&op=LISTSTATUS', fromPostBred))
print(dir)
