import json
import os
import pycurl
import re
import socket
import subprocess
import urllib
from StringIO import StringIO

DEBUG = False

fromDeskTop = False
if socket.gethostname().startswith('CB'):
    fromDeskTop = True

FS_MODIFICATION_TIME = 'modificationTime'
FS_OWNER = 'owner'
FS_GROUP = 'group'
FS_PERMISSION = 'permission'
FS_LENGTH = 'length'
FS_REPLICATION = 'replication'
FS_NAME = 'pathSuffix'
FS_TYPE = 'type'
FS_NUM_CHILD = 'childrenNum'

FT_FILE = 'FILE'
FT_DIR = 'DIRECTORY'

_FSLIST = 'FileStatuses'
_FSLIST2 = 'FileStatus'

_RESP_BOOL_TRUE = '{"boolean": true}'


def curlGet(url):
    c, buffer = _curl_prepare(url)
    c.perform()
    c.close()
    return buffer.getvalue()


def _curl_prepare(url):
    buffer = StringIO()
    c = pycurl.Curl()
    if DEBUG:
        c.setopt(pycurl.VERBOSE, 9)
    if fromDeskTop:
        # remove any proxy setting in environment
        if os.environ.has_key('HTTP_PROXY'):
            del os.environ['HTTP_PROXY']
        if os.environ.has_key('HTTPS_PROXY'):
            del os.environ['HTTPS_PROXY']
    c.setopt(pycurl.URL, url)
    c.setopt(c.WRITEFUNCTION, buffer.write)
    return c, buffer


def curlDel(url):
    c, buffer = _curl_prepare(url)
    c.setopt(pycurl.CUSTOMREQUEST, 'DELETE')
    c.perform()
    c.close()
    return buffer.getvalue()


def curlPut(url):
    c, buffer = _curl_prepare(url)
    c.setopt(pycurl.CUSTOMREQUEST, 'PUT')
    c.perform()
    c.close()
    return buffer.getvalue()


def curlPutFile(url, file):
    if DEBUG:
        with open(file, 'rb') as f1:
            content = f1.read()
        print('HTTP PUT file {0} to {1} content:\n{2}'.format(file, url, content))
    c, buffer = _curl_prepare(url)
    c.setopt(pycurl.CUSTOMREQUEST, 'PUT')
    c.setopt(pycurl.POST, 1)
    fileSize = os.path.getsize(file)
    c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/octet-stream", "Content-Length: " + str(fileSize)])
    with open(file, 'rb') as f2:
        c.setopt(pycurl.READDATA, f2)
        c.perform()
    status = c.getinfo(pycurl.RESPONSE_CODE)
    c.close()
    return status, buffer.getvalue()


class DirectoryNotFound(Exception):
    def __init__(self, value):
        self.value = "Missing Directory {0}".format(value)


class MultipleFileToExistingFile(Exception):
    def __init__(self, value):
        self.value = "Trying to copy multiple file to existing file {0}".format(value)


class Cluster():
    def _encodePath(self, dir):
        return urllib.quote(dir.encode("utf-8", "replace"))

    def __init__(self, namenode, port, user):
        self.namenode = namenode
        self.port = port
        self.user = user
        self.baseUrl = 'http://' + namenode + ':' + str(port) + '/webhdfs/v1'
        self.userUrl = '?user.name=' + self.user
        self.hasHdfsClient = None
        self.baseURI = 'hdfs://' + namenode

    def _hasHdfsClient(self):
        if self.hasHdfsClient == None:
            self.hasHdfsClient = True if subprocess.call(['hdfs', 'dfs', '-help']) == 0 else False
        return self.hasHdfsClient

    def lsUrl(self, dir):
        return self.baseUrl + self._encodePath(dir) + self.userUrl + '&op=LISTSTATUS'

    def stat(self, path):
        try:
            resp = json.loads(curlGet(self.baseUrl + self._encodePath(path) + self.userUrl + '&op=GETFILESTATUS'))
            return resp[_FSLIST2]
        except (KeyError, ValueError):
            raise DirectoryNotFound(dir)

    def _ls(self, dir):
        try:
            resp = json.loads(curlGet(self.lsUrl(dir)))
            return resp[_FSLIST][_FSLIST2]
        except (KeyError, ValueError):
            raise DirectoryNotFound(dir)

    def mkDir(self, dir, octalPermission=None):
        url = self.baseUrl + self._encodePath(dir) + self.userUrl + '&op=MKDIRS'
        if octalPermission != None:
            url += '&permission=' + octalPermission
        resp = curlPut(url)
        return resp == _RESP_BOOL_TRUE

    def rm(self, dir, recursive=True):
        url = self.baseUrl + self._encodePath(dir) + self.userUrl + '&op=DELETE'
        if recursive:
            url += '&recursive=true'
        resp = curlDel(url)
        return resp == _RESP_BOOL_TRUE

    def copyFromLocal(self, src, dest, overwrite=True):
        src2 = [src] if type(src) is str else src
        try:
            destStat = self.stat(dest)
            newPath = False
        except DirectoryNotFound:
            newPath = True
            if len(src2) > 1:
                raise  # unable to copy multiple files to new dest
        destIsDir = destStat[FS_TYPE] == FT_DIR
        if len(src2) > 1:
            if not destIsDir:
                raise MultipleFileToExistingFile(dest)
            dest2 = dest if dest[-1] == '/' else dest + '/'
        else:
            dest2 = self.join(dest, os.path.basename(src2[0]))

        # if only one source create list

        if self.hasHdfsClient:
            return self._copyFromLocalCmd(src, dest2, overwrite)
        else:
            return self._copyFromLocalWeb(src2, dest2, overwrite)

    def _copyFromLocalCmd(self, src, dest, overwrite=True):
        # copy files
        destURI = '{0}/{1}'.format(self.baseURI, dest if dest.endswith('/') else dest + '/')
        cmd = "hdfs dfs -put {0} {1} {2}".format('-f' if overwrite else '', ' '.join(src), destURI)
        print(cmd)
        subprocess.call(['sudo', 'su', '-u', 'hdfs', '-c', cmd])

    def _copyFromLocalWeb(self, sources, dest, overwrite=True):
        op = '&op=CREATE&overwrite='
        if overwrite:
            op += 'true'
        else:
            op += 'false'

        # if replication != None:
        #     op += '&replication=' + str(int(replication))
        # if octalPermission != None:
        #     op += '&permission=' + octalPermission
        # if blocksize != None:
        #     op += '&blocksize=' + str(int(blocksize))
        # if buffersize != None:
        #     op += '&buffersize=' + str(int(buffersize))

        for src in sources:
            # need to create dest full name in all cases
            dest2 = dest if len(sources) == 1 else self.join(dest, os.path.basename(src))
            url = self.baseUrl + self._encodePath(dest2) + self.userUrl + op
            c, buffer = _curl_prepare(url)
            c.setopt(pycurl.CUSTOMREQUEST, 'PUT')
            headers = StringIO()
            c.setopt(pycurl.HEADER, 1)
            c.setopt(pycurl.HEADERFUNCTION, headers.write)
            c.perform()
            c.close()
            headersTxt = headers.getvalue()
            headerLines = headersTxt.split('\n')
            pattern = re.compile('([^:]+):\s*(.*?)\r')
            redirect = None
            for line in headerLines:
                res = pattern.match(line)
                if res != None and res.lastindex == 2:
                    if res.group(1) == 'Location':
                        redirect = res.group(2)
                        break
            if redirect == None:
                raise Exception("unable to create file\r\n" + headersTxt)
            status, body = curlPutFile(redirect, src)
            if status != 201:
                print('ERROR aborting Unexpected error copying file {0} among {1} with webhdfs'.format(src, sources))
                print('datanode response:')
                print(body)
                return False

    def existDir(self, dir):
        try:
            self._ls(dir)
            return True
        except DirectoryNotFound:
            return False;
            False

    def ls(self, dir):
        list = self._ls(dir)
        return [f[FS_NAME] for f in list]

    def ls_files(self, dir):
        list = self._ls(dir)
        return [f[FS_NAME] for f in list if f[FS_TYPE] == FT_FILE]

    def ls_dirs(self, dir):
        list = self._ls(dir)
        return [f[FS_NAME] for f in list if f[FS_TYPE] == FT_DIR]

    def join(self, base, *paths):
        for path in paths:
            # remove trailing / in left part
            while base[-1] == '/':
                base = base[:-1]
            # remove leading / in right part
            while path[0] == '/':
                path = path[1:]
            base = base + '/' + path
        # remove trailing / in result
        while base[-1] == '/':
            base = base[:-1]
        return base

    def mirror(self, local, hdfs, followlinks=True, verbose=False):
        if verbose: print('mirroring directory {0} to hdfs directory {1}{2}'.format(local, self.baseURI, hdfs))
        l_ls = iter(sorted(os.listdir(local)))
        h_ls = iter(sorted(self._ls(hdfs), key=lambda f: f[FS_NAME]))
        cpFiles, cpDirs = [], []

        def getNextOrNone(it):
            try:
                return it.next()
            except StopIteration:
                return None

        l_f, h_f = getNextOrNone(l_ls), getNextOrNone(h_ls)
        while l_f <> None or h_f <> None:
            try:
                h_name = h_f[FS_NAME] if h_f != None else None
                if h_f == None or l_f < h_name:
                    l_full = os.path.join(local, l_f)
                    h_full = self.join(hdfs, l_f)
                    # new local entry
                    if os.path.isdir(l_full) or (followlinks and os.path.islink(l_full)):
                        self.mkDir(h_full, oct(os.stat(l_full).st_mode)[-3:])
                        cpDirs.append((l_full, h_full))
                        if verbose: print('copying new directory: {0} to {1}'.format(l_full, h_full))
                    else:
                        cpFiles.append(l_full)
                        if verbose: print('copying new file: {0}'.format(l_full))
                    l_f = getNextOrNone(l_ls)
                elif l_f == h_name:
                    l_full = os.path.join(local, l_f)
                    h_full = self.join(hdfs, h_name)
                    if os.path.isdir(l_full) or (followlinks and os.path.islink(l_full)):
                        if h_f[FS_TYPE] == FT_DIR:
                            cpDirs.append((l_full, h_full))
                            if verbose: print(
                            'existing directory adding sync of directory {0} to {1}'.format(l_full, h_full))
                            # todo handle change of permissions ?
                        else:
                            self.rm(self.join(hdfs, l_f))
                            self.mkDir(self.join(hdfs, l_f), oct(os.stat(l_f).st_mode)[-3:])
                            cpDirs.append((l_full, h_full))
                            if verbose: print(
                            'removing existing file {1} and adding sync of directory {0} to {1}'.format(l_full, h_full))
                    else:
                        # local file
                        # if unchanged no action else delete and copy again
                        l_stat = os.stat(l_full)
                        if h_f[FS_TYPE] != FT_FILE or h_f[FS_MODIFICATION_TIME] < l_stat.st_mtime or h_f[
                            FS_LENGTH] != l_stat.st_size:
                            self.rm(h_full)
                            cpFiles.append(l_full)
                            if verbose: print('copying changed file: {0}'.format(l_full))
                    # increment both as we matched both lists
                    l_f = getNextOrNone(l_ls)
                    h_f = getNextOrNone(h_ls)
                else:
                    h_full = self.join(hdfs, h_name)
                    # file only on hdfs => delete
                    self.rm(h_full)
                    h_f = getNextOrNone(h_ls)
                    if verbose: print('deleting removed element {0}'.format(h_full))
            except:
                print('\n##########################################################################################\n')
                print('ERROR handling local file {0} {1} and hdfs file {2} {3}'.format(local, l_f, hdfs, h_f))
                print('\n##########################################################################################\n')
                raise

        # copy files
        if len(cpFiles) > 0:
            self.copyFromLocal(cpFiles, hdfs)

        # recurse
        for l, h in cpDirs:
            self.mirror(l, h, followlinks, verbose)
