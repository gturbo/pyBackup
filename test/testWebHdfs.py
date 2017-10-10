import os
import shutil
import tempfile
import unittest
from shutil import rmtree

import webhdfs


class TestWebHdfsFiles(unittest.TestCase):
    def tearDown(self):
        with webhdfs.Cluster('sriopmgta0101.recette.local', '50070', 'et20795') as cluster:
            rmtree(self.tmpDir)
            cluster.rm(self.hdfsTmpDir)

    def setUp(self):
        with webhdfs.Cluster('sriopmgta0101.recette.local', '50070', 'et20795') as cluster:
            self.tmpDir = tempfile.mkdtemp()
            print("created temporary directory ", self.tmpDir)
            self.hdfsTmpDir = cluster.join('/tmp', os.path.basename(self.tmpDir))
            cluster.mkDir(self.hdfsTmpDir)
            print("created hdfs temporary directory", self.hdfsTmpDir)
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

    def testCopyFromLocal(self):
        with webhdfs.Cluster('sriopmgta0101.recette.local', '50070', 'et20795') as cluster:
            # copy with same name
            f = self.mkFile()
            hdfs_ls = cluster.ls(self.hdfsTmpDir)
            self.assertTrue(len(hdfs_ls) == 0)
            webhdfs.DEBUG = False
            cluster.copyFromLocal(f, self.hdfsTmpDir + '/')
            cluster._waitForAllProcess()
            webhdfs.DEBUG = False
            hdfs_ls = cluster._ls(self.hdfsTmpDir)
            self.assertTrue(len(hdfs_ls) == 1)
            self.assertEqual(hdfs_ls[0][webhdfs.FS_NAME], 'f1')
            self.assertEqual(hdfs_ls[0][webhdfs.FS_LENGTH], os.stat(f).st_size)

            # copy with new name
            myFile = 'my-file'
            myFilePath = cluster.join(self.hdfsTmpDir, myFile)
            cluster.copyFromLocal(f, myFilePath)
            cluster._waitForAllProcess()
            hdfs_ls = cluster.ls(self.hdfsTmpDir)
            self.assertTrue(len(hdfs_ls) == 2)
            self.assertTrue(hdfs_ls.index(myFile) >= 0)

            # test stat
            stat = cluster.stat(myFilePath)
            self.assertEqual(os.path.getsize(f), stat[webhdfs.FS_LENGTH])

            stat2 = cluster.stat(self.hdfsTmpDir)
            print(stat2)
            self.assertEqual(2, stat2[webhdfs.FS_NUM_CHILD])
            try:
                cluster.stat(cluster.join(self.hdfsTmpDir, 'does-not-exist'))
                self.assertTrue(False, 'should not be reached')
            except webhdfs.DirectoryNotFound as e:
                pass

    def testMultipleCopyFromLocal(self):
        with webhdfs.Cluster('sriopmgta0101.recette.local', '50070', 'et20795') as cluster:
            files = [self.mkFile() for i in range(3)]
            hdfs_ls = cluster.ls(self.hdfsTmpDir)
            self.assertTrue(len(hdfs_ls) == 0)
            # webhdfs.DEBUG=True
            cluster.copyFromLocal(files, self.hdfsTmpDir)
            cluster._waitForAllProcess()
            webhdfs.DEBUG = False
            hdfs_ls = cluster.ls(self.hdfsTmpDir)
            self.assertTrue(len(hdfs_ls) == 3)

    def testMirror(self):
        with webhdfs.Cluster('sriopmgta0101.recette.local', '50070', 'et20795') as cluster:
            self.mkFile()
            self.mkFile()
            [dir1Path] = self.mkDir(files=2)
            [dir2Path, dir3Path] = self.mkDir(dir1Path, files=2, dirs=2)
            self.listLocalDir(self.tmpDir)
            cluster.mirror(self.tmpDir, self.hdfsTmpDir)
            self.listHdfsDir(cluster, self.hdfsTmpDir)
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
            self.assertEqual(3, len(cluster.ls(self.hdfsTmpDir)))
            h_dir1Path = cluster.join(self.hdfsTmpDir, 'dir1')
            subdirs = cluster.ls_dirs(h_dir1Path)
            print(subdirs)
            self.assertEqual(2, len(subdirs))
            [h_dir2Path, h_dir3Path] = [cluster.join(h_dir1Path, subdir) for subdir in subdirs]
            [dir2Path, dir3Path] = [os.path.join(dir1Path, subdir) for subdir in subdirs]
            files = cluster.ls(h_dir2Path)
            print(files)
            self.assertEqual(2, len(files))
            self.mkFile(name='fadded')
            os.remove(os.path.join(self.tmpDir, 'f2'))
            os.remove(os.path.join(dir1Path, 'f3'))
            self.mkFile(dir1Path, 'f3')
            shutil.rmtree(dir3Path)
            self.mkDir(dir1Path, files=2)
            [f3, f4] = [f for f in cluster._ls(h_dir1Path) if f[webhdfs.FS_TYPE] == webhdfs.FT_FILE]
            cluster.mirror(self.tmpDir, self.hdfsTmpDir, verbose=True)
            _dir1List = cluster._ls(h_dir1Path)
            [f3_after, f4_after] = [f for f in _dir1List if f[webhdfs.FS_TYPE] == webhdfs.FT_FILE]
            self.assertEqual(f4[webhdfs.FS_MODIFICATION_TIME], f4_after[webhdfs.FS_MODIFICATION_TIME],
                             'unchanged files should have same modification time')
            self.assertTrue(f3_after[webhdfs.FS_MODIFICATION_TIME] >= f3[webhdfs.FS_MODIFICATION_TIME],
                            'changed files should have different modification time')
            filesAfter = cluster.ls(self.hdfsTmpDir)
            dir1List = [f[webhdfs.FS_NAME] for f in _dir1List]
            self.assertTrue('fadded' in filesAfter)
            self.assertFalse('f2' in dir1List)
            self.assertTrue('dir4' in dir1List)
            self.assertFalse('dir3' in dir1List)
            self.listHdfsDir(cluster)

        """ CREATING ARBO:
            + self.tmpDir
              - f1
              - f2          *** deleted
              - fadded      *** added
              + dir1
                  - f3      *** update content
                  - f4
                  + dir2
                      - f5
                      - f6
                  + dir3    *** deleted
                      - f7
                      - f8
                  + dir4    *** added
                      - f11
                      - f12
        """

    def testLocalCmd(self):
        with webhdfs.Cluster('sriopmgta0101.recette.local', '50070', 'et20795') as cluster:
            print('hasHDFSClient = {0}'.format(cluster.hasHdfsClient()))

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
