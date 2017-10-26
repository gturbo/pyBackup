import os
import shutil
import tempfile
import unittest
from shutil import rmtree

import db2tools
import webhdfs

CLUSTER_PARAMS = ('sriopmgta0101.recette.local', '50070', 'et20795')
CLUSTER_PARAMS_DEST = ('sriopmgta0101.recette.local', '50070', 'et20795')
CLUSTER_SNAP_DIR = '/user/et20795/copy'
CLUSTER_MIRROR_DIR = '/user/et20795/mirror'


class TestWebHdfsFiles(unittest.TestCase):
    def tearDown(self):
        rmtree(self.tmpDir)

        with webhdfs.Cluster(*CLUSTER_PARAMS) as cluster:
            for f in cluster.ls(CLUSTER_SNAP_DIR):
                cluster.rm(cluster.join(CLUSTER_SNAP_DIR, f), True)
            for f in cluster.ls(CLUSTER_MIRROR_DIR):
            # cluster.rm(cluster.join(CLUSTER_MIRROR_DIR, f), True)
            for snap in cluster.lsSnapshot(CLUSTER_SNAP_DIR):
                cluster.deleteSnapshot(CLUSTER_SNAP_DIR, snap)

    def setUp(self):
        with webhdfs.Cluster(*CLUSTER_PARAMS) as cluster:
            self.tmpDir = tempfile.mkdtemp()
            print("created temporary directory ", self.tmpDir)
            self.hdfsTmpDir = CLUSTER_SNAP_DIR
            # cleanup directory
            for f in cluster.ls(CLUSTER_SNAP_DIR):
                cluster.rm(cluster.join(CLUSTER_SNAP_DIR, f), True)
            for f in cluster.ls(CLUSTER_MIRROR_DIR):
                cluster.rm(cluster.join(CLUSTER_MIRROR_DIR, f), True)

            self.filenum = 1
            self.dirnum = 1

    def mkFile(self, root=None, name=None, template='line {0}', lines=1, times=None):
        root2 = root if root != None else self.tmpDir
        name2 = name if name != None else 'f' + str(self.filenum)
        fname = os.path.join(root2, name2)
        with open(fname, 'a') as f:
            for i in range(lines):
                f.write(template.format(i) + '\n')
        if times != None:
            os.utime(fname, times)
        self.filenum += 1
        return fname

    def mkDir(self, root=None, name=None, files=1, dirs=1):
        root2 = root if root != None else self.tmpDir
        dirList = []
        for d in range(dirs):
            name2 = name if name != None else 'dir' + str(self.dirnum)
            if dirs > 1 and name != None:
                name2 += str(d)
            else:
                self.dirnum += 1
            fName2 = os.path.join(root2, name2)
            dirList.append(fName2)
            os.mkdir(fName2)
            for fnum in range(files):
                self.mkFile(fName2, lines=self.dirnum)
        return dirList

    def testMkDir(self):
        self.mkDir(files=2, dirs=2)
        self.listLocalDir()

    def testSnapshotDir(self):
        with webhdfs.Cluster(*CLUSTER_PARAMS) as cluster:
            self.assertTrue(cluster.existDir(CLUSTER_SNAP_DIR))
            snapName = 'test'
            snapPath = cluster.join(CLUSTER_SNAP_DIR, '.snapshot', snapName)
            try:
                cluster.deleteSnapshot(CLUSTER_SNAP_DIR, snapName)
            except webhdfs.WebHdfsApiException as w:
                if w.status != 403:
                    raise w
        self.assertFalse(cluster.existDir(snapPath))
        cluster.createSnapshot(CLUSTER_SNAP_DIR, snapName)
        self.assertTrue(cluster.existDir(snapPath))
        cluster.deleteSnapshot(CLUSTER_SNAP_DIR, snapName)
        self.assertFalse(cluster.existDir(snapPath))

    def testDistCopy(self):
        with webhdfs.Cluster(*CLUSTER_PARAMS) as cluster:
            # create source arbo
            self.mkFile()
            self.mkFile()
            [dir1Path] = self.mkDir(files=2)
            [dir2Path, dir3Path] = self.mkDir(dir1Path, files=2, dirs=2)
            self.listLocalDir(self.tmpDir)
            cluster.mirror(self.tmpDir, CLUSTER_SNAP_DIR)
            # perform first copy
            snapPath = db2tools.copyClusterPath(cluster, cluster, CLUSTER_SNAP_DIR, CLUSTER_MIRROR_DIR)
            snapName = cluster.basename(snapPath)
            print('MIRROR after init copy')
            self.listHdfsDir(cluster, CLUSTER_MIRROR_DIR)
            """ CREATING ARBO:
                + self.tmpDir
                  - f1
                  - f2
                  + dir1
                      - f3
                      - f4
                      + dir2
                          - f5
                          - f6
                      + dir3
                          - f7
                          - f8
            """
            self.assertEqual(3, len(cluster.ls(CLUSTER_MIRROR_DIR)))
            h_dir1Path = cluster.join(CLUSTER_MIRROR_DIR, 'dir1')
            subdirs = cluster.ls_dirs(h_dir1Path)
            print(subdirs)
            self.assertEqual(2, len(subdirs))
            [h_dir2Path, h_dir3Path] = [cluster.join(h_dir1Path, subdir) for subdir in subdirs]
            [dir2Path, dir3Path] = [os.path.join(dir1Path, subdir) for subdir in subdirs]
            files = cluster.ls(h_dir2Path)
            print(files)
            self.assertEqual(2, len(files))

            # make some change to check update
            self.mkFile(name='fadded')
            os.remove(os.path.join(self.tmpDir, 'f2'))
            os.remove(os.path.join(dir1Path, 'f3'))
            self.mkFile(dir1Path, 'f3')
            shutil.rmtree(dir3Path)
            self.mkDir(dir1Path, files=2)
            [f3, f4] = [f for f in cluster._ls(h_dir1Path) if f[webhdfs.FS_TYPE] == webhdfs.FT_FILE]
            cluster.mirror(self.tmpDir, CLUSTER_SNAP_DIR)

            # synchronize from previous snapshot
            diffSnapPath = db2tools.updateClusterPath(snapName, cluster, cluster, CLUSTER_SNAP_DIR, CLUSTER_MIRROR_DIR)
            print('MIRROR after update')
            self.listHdfsDir(cluster, CLUSTER_MIRROR_DIR)
            _dir1List = cluster._ls(h_dir1Path)
            [f3_after, f4_after] = [f for f in _dir1List if f[webhdfs.FS_TYPE] == webhdfs.FT_FILE]
            self.assertEqual(f4[webhdfs.FS_MODIFICATION_TIME], f4_after[webhdfs.FS_MODIFICATION_TIME],
                             'unchanged files should have same modification time')
            self.assertTrue(f3_after[webhdfs.FS_MODIFICATION_TIME] >= f3[webhdfs.FS_MODIFICATION_TIME],
                            'changed files should have different modification time')
            filesAfter = cluster.ls(CLUSTER_SNAP_DIR)
            dir1List = [f[webhdfs.FS_NAME] for f in _dir1List]
            self.assertTrue('fadded' in filesAfter)
            self.assertFalse('f2' in dir1List)
            self.assertTrue('dir4' in dir1List)
            self.assertFalse('dir3' in dir1List)
            self.listHdfsDir(cluster)

            self.assertTrue(cluster.existDir(CLUSTER_SNAP_DIR))

    def listLocalDir(self, dir=None):
        dir2 = dir if dir != None else self.tmpDir
        for dirpath, dirnames, filenames in os.walk(dir2):
            print('+ {0}'.format(dirpath))
            for f in filenames:
                print('  - {0}'.format(f))

    def listHdfsDir(self, cluster, dir=None):
        dir2 = dir if dir != None else self.tmpDir
        print('+ ' + dir2)
        try:
            files = cluster.ls_files(dir2)
        except webhdfs.DirectoryNotFound:
            return
        for f in files:
            print('  - {0}'.format(f))
        for dir in cluster.ls_dirs(dir2):
            self.listHdfsDir(cluster, cluster.join(dir2, dir))


if __name__ == '__main__':
    unittest.main()
