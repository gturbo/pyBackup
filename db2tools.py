import datetime
import os
import re
import subprocess
import sys

SRE_MATCH_TYPE = type(re.match("", ""))

RE_CREATE_TABLE = re.compile(
    r'^CREATE\s+(EXTERNAL\s+)?(HADOOP\s+)?TABLE\s+"?([a-zA-Z_0-9 ]+)"?\."?([a-zA-Z_0-9]+)"?\s*\(\s*(([^\(\)]*|\([^\(\)]*\))+?)\s*\)\s*(COMMENT\s*\'([^\']|\'\'|\\\\\'|\n|\r)*\')?\s*(PARTITIONED BY\s*\(\s*(([^\(\)]*|\([^\(\)]*\))+?)\s*\)\s*)?(.*?);$',
    re.IGNORECASE)
RE_CREATE_VIEW = re.compile(
    r'^CREATE\s+VIEW\s+"?([a-zA-Z_0-9 ]+)?"?\.?"?([a-zA-Z_0-9]+)"?.*',
    re.IGNORECASE)
RE_STORAGE_TEXT = re.compile(
    r'\s*ROW\s+FORMAT\s+DELIMITED\s+FIELDS\s+TERMINATED\s+BY\s+(\'[^\']+\')\s+(WITH\s+SERDE\s+PROPERTIES\s*\(.*?\'escape\.delim\'=(\'[^\']*\').*?\))?\s*.*?(LOCATION\s+\'[^\']+\')(.*?\'SERIALIZATION\.NULL\.FORMAT\'=(\'[^\']*\'))?(.*)',
    re.IGNORECASE)
RE_STORAGE_TEXT2 = re.compile(
    r'\s*ROW\s+FORMAT\s+SERDE\s+\'org\.apache\.hadoop\.hive\.serde2\.lazy\.LazySimpleSerDe\'\s+.*?(LOCATION\s+\'[^\']+\')(.*)',
    re.IGNORECASE)
RE_STORAGE_PARQUET = re.compile(
    r'\s*ROW\s+FORMAT\s+SERDE\s+\'org\.apache\.hadoop\.hive\.ql\.io\.parquet\.serde\.ParquetHiveSerDe\'\s+.*?(LOCATION\s+\'[^\']+\')(.*)',
    re.IGNORECASE)
RE_COL_COMMENT = re.compile(
    r'^COMMENT\s+ON\s+COLUMN\s*("?([a-zA-Z_0-9 ]+)"?\."?([a-zA-Z_0-9]+)"?\."?([a-zA-Z_0-9]+)"?)\s+IS+\s*(.*)',
    re.IGNORECASE
)
RE_TAB_COMMENT = re.compile(
    r'^COMMENT\s+ON\s+TABLE\s*("?([a-zA-Z_0-9 ]+)"?\."?([a-zA-Z_0-9]+)"?)\s+IS+\s*(.*)', re.IGNORECASE
)
RE_CREATE_ROLE = re.compile(
    r'^CREATE\s+ROLE\s+\"?([a-zA-Z_0-9]+)\"?.*', re.IGNORECASE
)
RE_CREATE_FUNCTION = re.compile(
    r'^CREATE\s+FUNCTION\s+"?([a-zA-Z_0-9 ]+)?"?\.?"?([a-zA-Z_0-9]+)"?.*', re.IGNORECASE
)
RE_CREATE_SCHEMA = re.compile(
    r'^CREATE\s+SCHEMA\s+\"?([a-zA-Z_0-9 ]+)\"?.*', re.IGNORECASE
)
RE_ALTER_PCT_FREE = re.compile(
    r'^ALTER\s+TABLE\s+"?([a-zA-Z_0-9 ]+)"?\."?([a-zA-Z_0-9]+)"?\s+PCTFREE 0;$', re.IGNORECASE
)
RE_CURRENT_SCHEMA = re.compile(
    r'^SET\s+CURRENT\s+SCHEMA\s*=\s*\"?([a-zA-Z_0-9]+)\"?.*', re.IGNORECASE
)
RE_SET_PATH = re.compile(
    r'^SET\s+CURRENT\s+PATH\s*=\s*(.*?);$', re.IGNORECASE
)
RE_ALTER_ADD_CONSTRAINT = re.compile(
    r'^ALTER\s+TABLE\s+"?([a-zA-Z_0-9 ]+)"?\."?([a-zA-Z_0-9]+)"?\s+ADD\s+(?:CONSTRAINT|PRIMARY\s+KEY)\s+(.*)',
    re.IGNORECASE
)
RE_SKIP = re.compile(
    r'^(?:(?:ALTER\s+TABLE|CREATE\s+INDEX)\s+"?BIGSQL\s*"?\.|CONNECT\s+|SET\s+NLS_STRING_UNITS|CREATE\s+VARIABLE\s+|COMMIT\s+|TERMINATE)',
    re.IGNORECASE
)


def execLinuxCmd(cluster, command):
    """
        Execute a system command on the cluster namenode

        Parameters
        ----------
        command : str
            The shell command
        cluster : webhdfs.Cluster.cluster
            The cluster on which to execute the command
        Returns
        -------
        int
            command return status 0 if OK

    """
    print('executing linux command:\n{0}\n'.format(command))
    if sys.platform != 'win32':
        __isMgt = True
    else:
        __isMgt = False
        __envExecCmd = os.environ.copy()
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
                    __envExecCmd['SSH_AUTH_SOCK'] = m.group(1)
                    break
        if not found:
            raise Exception(
                'Unable to find ssh agent configuration file {0}\nSorry but you need to configure an ssh agent, a connexion and perhaps a gitbash to test from windows'.format(
                    environFilePath))

    if __isMgt:
        p = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        p = subprocess.Popen(['ssh', '{0}@{1}'.format(cluster.user, cluster.namenode), command], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, env=__envExecCmd)
    out, err = p.communicate()
    print('out:\n{0}\nerr:\n{1}'.format(out, err))

def getConnectionString(database, host, port, user, password):
    return 'DATABASE={0};HOSTNAME={1};PORT={2};PROTOCOL=TCPIP;UID={3};PWD={4}'.format(database, host, str(port), user,
                                                                                      password)


def copyClusterPath(sourceCluster, destCluster, srcPath, destPath=None):
    destPath2 = destPath if destPath is not None else srcPath
    snapName = 'copyCluster-{0}'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
    snapPath = sourceCluster.createSnapshot(srcPath, snapName)
    execLinuxCmd(sourceCluster, 'hadoop distcp -overwrite -delete -pugpaxt "{0}" "{1}"'.format(
        sourceCluster.join(sourceCluster.baseURI, snapPath),
        destCluster.join(destCluster.baseURI, destPath2))
                 )
    destCluster.createSnapshot(destPath, snapName)
    return snapPath


def updateClusterPath(fromSnapshot, sourceCluster, destCluster, srcPath, destPath=None):
    """
        update mirrored path from specified snapshot
        must be used on a copy created with copyClusterPath function.

        Parameters
        ----------
        fromSnapshot : str
            The snapshot taken before precedent copy used to calculate differences
        sourceCluster : webhdfs.Cluster.cluster
            The source cluster objects
        destCluster : webhdfs.Cluster.cluster
            The destination cluster objects
        srcPath : str
            source directory for mirroring must be snapshotable and have a snapshot with name fromSnapshot
        destPath : str
            destination directory for copy defaults to same path as source

        Returns
        -------
        str
            full path of the created snapshot for source

        """
    if fromSnapshot not in sourceCluster.lsSnapshot(srcPath):
        raise Exception('Cannot find snapshot {0} for source directory {1}'.format(fromSnapshot, srcPath))
    destPath2 = destPath if destPath is not None else srcPath
    snapName = 'copyCluster-{0}'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
    snapPath = sourceCluster.createSnapshot(srcPath, snapName)
    execLinuxCmd(sourceCluster, 'hadoop distcp -delete -update -pugpaxt -diff "{0}" "{1}" "{2}" "{3}"'.format(
        fromSnapshot,
        snapName,
        sourceCluster.join(sourceCluster.baseURI, srcPath),
        destCluster.join(destCluster.baseURI, destPath2))
                 )
    destCluster.createSnapshot(destPath, snapName)
    return snapPath



class ParsingException(Exception):
    pass


class Reader:
    def __init__(self, fileSource=None):
        self.objects = []
        self.skipped = []
        self.unMatched = []
        if fileSource is not None:
            self.fileSource = fileSource
            self.linenum = 1
            self.inMultiComment = False
            self.inMultilineQuote = False
            self.currentDef = None
            self.currentLine = ''
            self.endDef()
            with open(fileSource, 'r') as f:
                self.read(f)

    def read(self, srcFile):
        self.endDef()
        for line in srcFile:
            # TODO specify encoding line = line.decode('utf-8') ?
            if self.inMultiComment:
                self.multiLineCommentReader(line)
            else:
                try:
                    self.lineReader(line)
                    # don't strip multiline quoted strings
                    if self.inMultilineQuote:
                        self.currentDef.append(self.currentLine)
                    else:
                        stripped = self.currentLine.strip()
                        if len(stripped) > 0:
                            self.currentDef.append(stripped)
                        if stripped.endswith(';'):
                            self.addObject()
                            self.endDef()
                    self.currentLine = ''
                except:
                    print(
                        sys.stderr.write(
                            'ERROR parsing source file {0} at line {1}\n'.format(self.fileSource, self.linenum)))
                    raise
            self.linenum += 1

    def multiLineCommentReader(self, line):
        i = line.find('*/')
        if i >= 0:
            rest = line[i + 3:]
            self.inMultiComment = False
            self.lineReader(rest)

    def lineReader(self, line):
        line2 = line
        if self.inMultilineQuote:
            end = line2.find("'")
            if end < 0:
                self.currentLine += line2.rstrip() + '\\n'  # replace line break
            else:
                if line2[end - 1] == '\\' and line2[end - 2] != '\\':
                    # skip escaped quote \' and not escaped backquote at the end
                    self.lineReader(line2[:end - 1] + line2[end + 1:])
                elif line2[end + 1] == "'":
                    # skip double single quote
                    self.lineReader(line2[:end] + line2[end + 2:])
                else:
                    self.currentLine += line2[:end + 1]
                    self.inMultilineQuote = False
                    self.lineReader(line2[end + 1:])
        else:
            line2 = line.lstrip()
            if len(line2) > 0 and not line2.startswith('--'):
                start = line2.find("'")
                if start >= 0:
                    self.readPartNoQuote(line2[:start])
                    while True:
                        end = line2.find("'", start + 1)
                        if end < 0:
                            # I found comments on multiple lines !!!! for column :
                            # COMMENT ON COLUMN "INT_DWH "."DWH_TIER_FPE_BILN_2033"."DT_ARRT_BILN" IS 'Date Arrete Du Bilan
                            # balance Sheet Date';
                            # swallow next line
                            self.inMultilineQuote = True
                            self.currentLine += line2[start:]
                        else:
                            if line2[end - 1] == '\\' and line2[end - 2] != '\\':
                                # skip escaped quote \'
                                line2 = line2[:end - 1] + line2[end + 1:]
                                continue
                            elif line2[end + 1] == "'":
                                # skip double single quote
                                line2 = line2[:end] + line2[end + 2:]
                                continue
                            self.currentLine += line2[start:end + 1]
                            # -2 for index start at 0 and last character is \n
                            if end < len(line2) - 2:
                                self.lineReader(line2[end + 1:])
                        break
                else:
                    self.readPartNoQuote(line2)

    def readPartNoQuote(self, part):
        if len(part) > 0 and not part.startswith('--'):
            start = part.find('"')
            if start >= 0:
                end = part.find('"', start + 1)
                if end < 0:
                    raise ParsingException('Unmatched starting and ending start in line part : {0}'.format(part))
                self.readPartNoDblQuote(part[:start])
                self.currentLine += part[start:end + 1]
                if end < len(part):
                    self.readPartNoQuote(part[end + 1:])
            else:
                self.readPartNoDblQuote(part)

    def readPartNoDblQuote(self, part):
        start = part.find('/*')
        if start >= 0:
            end = part.find('*/', start + 2)
            self.readPartNoMultilineComment(part[:start])
            if end >= 0:
                if end < len(part) - 1:
                    self.readPartNoQuote(part[end + 3:])
            else:
                self.inMultiComment = True
        else:
            self.readPartNoMultilineComment(part)

    def readPartNoMultilineComment(self, part):
        start = part.find('--')
        if start >= 0:
            self.readPartNoSingleLineComment(part[:start])
        else:
            self.readPartNoSingleLineComment(part)

    def readPartNoSingleLineComment(self, part):
        self.currentLine += part

    def endDef(self):
        self.currentDef = []

    def addObject(self):
        ddl = ' '.join(self.currentDef)
        matched = False
        for (objType, matcher) in [(DBSkip, RE_ALTER_PCT_FREE), (DBSkip, RE_SKIP),
                                   (CreateTable, RE_CREATE_TABLE),
                                   (CreateView, RE_CREATE_VIEW),
                                   (CreateFunction, RE_CREATE_FUNCTION),
                                   (AlterTable, RE_ALTER_ADD_CONSTRAINT),
                                   (CommentOnColumn, RE_COL_COMMENT),
                                   (CommentOnTable, RE_TAB_COMMENT),
                                   (Schema, RE_CREATE_SCHEMA),
                                   (Role, RE_CREATE_ROLE),
                                   (SetCurrentSchema, RE_CURRENT_SCHEMA),
                                   (SetCurrentPath, RE_SET_PATH)
                                   ]:
            m = matcher.match(ddl)
            if m is not None:
                matched = True
                if objType != DBSkip:
                    self.objects.append(objType(m))
                else:
                    self.skipped.append(ddl)
                break
        if not matched:
            self.unMatched.append(ddl)


class DBObject(Reader):
    def __init__(self, src):
        self.src = src

    def write(self, f):
        f.write(self.asString())
        f.write('\n')

    def nvl(self, s):
        return '' if s is None else s.asString() if isinstance(s, DBObject) else str(s)

    def asString(self):
        src = self.src
        if type(src) is SRE_MATCH_TYPE:
            return src.group(0)
        else:
            return str(src)


class CreateTable(DBObject):
    def __init__(self, src):
        DBObject.__init__(self, src)
        self._storageDef = None
        self._hasUnknownStorageDef = None

    def asString(self):
        return ' '.join(['CREATE', self.getExternal(), self.getHadoop(),
                         'TABLE', self.getQualifiedName(),
                         '(', self.getColumnDefinition(), ')',
                         self.nvl(self.getComment()),
                         'PARTITIONED BY (' + self.getPartitionedBy() + ')' if self.getPartitionedBy() is not None else '',
                         self.nvl(self.getStorageDef()),
                         ';'
                         ])

    def isExternal(self):
        return self.src.group(1) is not None

    def getExternal(self):
        return 'EXTERNAL' if self.isExternal() else ''

    def isHadoop(self):
        return self.src.group(2) is not None

    def hasUnknownStorageDefinition(self):
        if self._hasUnknownStorageDef is None:
            # retireve storage def to force parsing
            d = self.getStorageDef()
        return self._hasUnknownStorageDef

    def getHadoop(self):
        return 'HADOOP' if self.isHadoop() else ''

    def getSchema(self):
        return self.src.group(3)

    def getName(self):
        return self.src.group(4)

    def getQualifiedName(self):
        return '"{0}"."{1}"'.format(self.getSchema(), self.getName())

    def getColumnDefinition(self):
        return self.src.group(5)

    def getComment(self):
        return self.src.group(7)

    def getPartitionedBy(self):
        return self.src.group(10)

    def getOriginalStorageDef(self):
        return self.src.group(12)

    def getStorageDef(self):
        if self._storageDef is None:
            # match storage def
            matched = False
            ddl = self.getOriginalStorageDef()
            for (objType, matcher) in [(StorageDefTextFile, RE_STORAGE_TEXT), (StorageDefTextFile2, RE_STORAGE_TEXT2),
                                       (StorageDefParquet, RE_STORAGE_PARQUET)]:
                m = matcher.match(ddl)
                if m is not None:
                    matched = True
                    self._storageDef = objType(m)
                    self._hasUnknownStorageDef = False
                    break
            if not matched:
                self._storageDef = StorageDefUnknown(ddl)
                self._hasUnknownStorageDef = True

        return self._storageDef


# storage definitions
class StorageDef(DBObject):
    pass


class StorageDefTextFile(StorageDef):
    def getFieldSeparator(self):
        return self.src.group(1)

    def getEscapeDelimiter(self):
        v = self.src.group(3)
        return v if v is not None else "'\\\\'"

    def getLocation(self):
        return self.src.group(4)

    def getSerializationNullFormat(self):
        v = self.src.group(6)
        return v if v is not None else "''"

    def asString(self):
        return 'ROW FORMAT DELIMITED FIELDS TERMINATED BY {0} ESCAPED BY {1} STORED AS TEXTFILE TBLPROPERTIES(\'serialization.null.format\'={2})'.format(
            self.getFieldSeparator(), self.getEscapeDelimiter(), self.getSerializationNullFormat())


class StorageDefTextFile2(StorageDef):
    def getLocation(self):
        return self.src.group(1)

    def asString(self):
        return 'STORED AS TEXTFILE'


class StorageDefParquet(StorageDef):
    def getLocation(self):
        return self.src.group(4)

    def asString(self):
        return 'STORED AS PARQUET'


class StorageDefUnknown(StorageDef):
    def asString(self):
        return 'UNKNOWN STORAGE DEF {0}'.format(self.src)


class CreateView(DBObject):
    def getSchema(self):
        return self.src.group(1)

    def getName(self):
        return self.src.group(2)

    def getQualifiedName(self):
        return '"{0}"."{1}"'.format(self.getSchema(),
                                    self.getName()) if self.getSchema() is not None else '"{1}"'.format(self.getName())


class CreateFunction(DBObject):
    def getSchema(self):
        return self.src.group(1)

    def getName(self):
        return self.src.group(2)

    def getQualifiedName(self):
        return '"{0}"."{1}"'.format(self.getSchema(),
                                    self.getName()) if self.getSchema() is not None else '"{1}"'.format(self.getName())


class DBSkip(DBObject):
    pass


class AlterTable(DBObject):
    def getQualifiedName(self):
        return self.src.group(1)

    def getRest(self):
        # WARNING contains final ;
        return self.src.group(2)


class CommentOnColumn(DBObject):
    def getQualifiedName(self):
        return self.src.group(1)

    def getComment(self):
        # WARNING comment contains quotes and final ;
        return self.src.group(2)


class CommentOnTable(DBObject):
    def getQualifiedName(self):
        return self.src.group(1)

    def getComment(self):
        # WARNING comment contains quotes and final ;
        return self.src.group(2)


class Role(DBObject):
    def getName(self):
        return self.src.group(1)


class Schema(DBObject):
    def getName(self):
        return self.src.group(1)


class SetCurrentSchema(DBObject):
    def getName(self):
        return self.src.group(1)


class SetCurrentPath(DBObject):
    def getPath(self):
        return self.src.group(1)


def filterDDL(src, dest=None, err=None):
    parser = Reader(src)
    stdoutput = sys.stdout if dest is None else open(dest, 'w')
    try:
        stderror = sys.stderr if err is None else open(err, 'w')
        try:
            for dbo in parser.objects:
                output = stdoutput
                if isinstance(dbo, CreateTable):
                    if dbo.hasUnknownStorageDefinition():
                        stderror.write('-- UNKNOWN STORAGE FOR TABLE:\n')
                        output = stderror

                dbo.write(output)
        finally:
            if err is not None:
                stderror.close()
    finally:
        if dest is not None:
            stdoutput.close()


class Grant(DBObject):
    pass


if sys.version_info[0] <= 2 and sys.version_info[1] <= 6:
    raise Exception("""
    This module doesn't work with version of python before 2.7
    (due to regex performance problems)
    
    PLEASE run with a python version superior to 2.7
    
    \033[0;31mOn Hadoop management nodes, you can make python 2.7 available by sourcing :
    
    . /opt/rh/python27/enable \033[0m
      
    """)

if __name__ == '__main__':
    def printUsage():
        print("""This python file can be either imported as a module or used directly from the command line
        
Available command line options:

* python {0} filterDDL <source file path> <dest file path>
    allows filtering of BigSql DDL to allow playing them to restore a database
    dest file path default to standard output to pipe the resulting commands
        """.format(sys.argv[0]))
        # retrieve args without this file name args[0]


    args = sys.argv[1:]
    #    print(args)
    argCount = len(args)
    if argCount < 1 or args[0] in ['-h', '--help']:
        printUsage()
        exit(0)

    function = args[0]
    if function == 'filterDDL':
        if argCount > 1 and argCount <= 4:
            filterDDL(*args[1:])
        else:
            print('ERROR wrong number of arguments for function {0}'.format(function))
            printUsage()
    else:
        print('ERROR you must supply function name and arguments to call this module like this')
        printUsage()
