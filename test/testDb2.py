import sys
import tempfile
import unittest

import db2tools

# import ibm_db

BSQLREC = db2tools.getConnectionString('bigsql', 'sriopmgta0101.recette.local', 32051, 'idrelec', 'Pomme+2')
MAXPRINT = 40


class MyTestCase(unittest.TestCase):
    # def test_connection(self):
    #     conn = ibm_db.connect(BSQLREC, '', '')
    #     if conn:
    #         print "Connection succeeded."
    #         ibm_db.close(conn)
    #     else:
    #         print "Connection failed."

    def testParsingTable(self):
        parser = db2tools.Reader('../resources/sampleDDL.sql')
        self.assertEquals(5, len(parser.objects))
        for obj in parser.objects:
            #   self.assertEquals(obj.__class__.__name__,'CreateTable')
            print(obj.__class__.__name__ + ':')
            obj.write(sys.stdout)
            sys.stdout.write('\n')

    def testFilterDDL(self):
        db2tools.filterDDL('../resources/sampleDDL.sql')

    def filterDDLFileOut(self):
        dest = tempfile.mktemp('.sql', 'testFilterDDL_')
        db2tools.filterDDL('../resources/ddl_bigsql_prod_2017-10-19_19H30', dest)
        print("\nCreated filtered file: " + dest)

    def testParsingFull(self):
        parser = db2tools.Reader('../resources/ddl_bigsql_2017-10-11_21H30.sql')
        #        parser = db2tools.Reader('../resources/ddl_bigsql_prod_2017-10-19_19H30')

        nbUnknownStoDef = 0
        nbUnknownObj = 0
        notHadoopTable = []
        for obj in parser.objects:
            if isinstance(obj, db2tools.CreateTable):
                if not obj.isHadoop():
                    notHadoopTable.append(obj)
                elif isinstance(obj.getStorageDef(), db2tools.StorageDefUnknown) and nbUnknownStoDef < MAXPRINT:
                    print("UNKNOWN StorageDef in TABLE {0}:\n{1}".format(obj.getQualifiedName(),
                                                                         obj.getStorageDef().asString()))
                    # nbUnknownStoDef += 1
            elif isinstance(obj, db2tools.Role):
                pass
            elif isinstance(obj, db2tools.CommentOnColumn):
                pass
            elif isinstance(obj, db2tools.AlterTable):
                pass
            elif isinstance(obj, db2tools.Schema):
                print(obj.asString())
            elif isinstance(obj, db2tools.CreateView):
                # TODO control all views are OK ?
                pass

        for o in notHadoopTable:
            if o.getSchema().rstrip() != "CCTOOLS" and (o.getSchema().rstrip() != "BIGSQL" or not (
                    o.getName().startswith("EXPLAIN_") or o.getName().startswith("ADVISE_") or o.getName() in [
                'OBJECT_METRICS'])):
                print('NOT HADOOP TABLE {0} {1}'.format(o.getQualifiedName(), o.asString()))

        for s in parser.unMatched:
            print('UNMATCHED ENTRY:' + s)
            nbUnknownObj += 1
            if nbUnknownObj > MAXPRINT:
                break

        print('FOUND {0} Objects, {1} not Hadoop tables, {2} unmatched entries'.format(len(parser.objects),
                                                                                       len(notHadoopTable),
                                                                                       len(parser.unMatched)))

    def testRegexp(self):
        ddls = [
            """CREATE  HADOOP TABLE "INT_DRE "."TIER_RELT_TIER" ( TS_ALMT TIMESTAMP, CD_SOCT VARCHAR(2), CLE_TECH_PI_TEMPS_1 VARCHAR(50), CLE_TECH_PI_LIEU_1 SMALLINT, CLE_FONC_PI_1 VARCHAR(8), TYPE_RELT SMALLINT, CLE_TECH_PI_TEMPS_2 VARCHAR(50), CLE_TECH_PI_LIEU_2 SMALLINT, CLE_FONC_PI_2 VARCHAR(8), DT_DEB_RELT TIMESTAMP  )  PARTITIONED BY (DT_VACT TIMESTAMP ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;' ESCAPED BY '\\' STORED AS TEXTFILE TBLPROPERTIES('serialization.null.format'='');"""
            # """CREATE TABLE "DEV_DRE "."TESTJ"  ( "CHAMP1" CHAR(10) ) IN "BIGSQLCATSPACE" ORGANIZE BY ROW;""",
            # """CREATE HADOOP TABLE schema.table (c int, s varchar(10), s2 varchar(20), d decimal(10,5));""",
            # """CREATE HADOOP TABLE schema.table (c int, s varchar(10)) PARTITIONED BY ( DT_VACT TIMESTAMP(9) );""",
            # """CREATE HADOOP TABLE schema.table (c int, s varchar(10)) COMMENT '' PARTITIONED BY ( DT_VACT TIMESTAMP(9) ) STORED AS PARQUET;""",
            # """CREATE HADOOP TABLE schema.table (c int, s varchar(10)) COMMENT 'TOTO''TATA' PARTITIONED BY ( DT_VACT TIMESTAMP(9) ) STORED AS PARQUET;""",
            # """CREATE HADOOP TABLE BIGSQL  .AUDREY( ID INT, NOM VARCHAR(50) ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION 'hdfs://sriopmgta0101.recette.local:8020/apps/hive/warehouse/bigsql.db/audrey';"""
        ]
        for ddl in ddls:
            print('handling request:\n' + ddl)
            m = db2tools.RE_CREATE_TABLE.match(ddl)
            for i in range(m.lastindex):
                print('group {0}: {1}'.format(i + 1, m.group(i + 1)))

    def testRegexpStorage(self):
        # text
        ddls = [
            """ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' WITH SERDE PROPERTIES( 'escape.delim'='\\\\' ) STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION 'hdfs://sriopmgta0101.recette.local:8020/apps/int/dre/hive/tier_pers_phys_donn_finn' TBLPROPERTIES( 'SERIALIZATION.NULL.FORMAT'='', 'last_modified_by'='bigsql', 'last_modified_time'='1475726296' );""",
        ]
        for ddl in ddls:
            print('handling request:\n' + ddl)
            m = db2tools.RE_STORAGE_TEXT.match(ddl)
            self.assertTrue(m is not None,
                            'regexp:\n {0}\ndid not match:\n{1}'.format(db2tools.RE_STORAGE_TEXT.pattern, ddl))
            for i in range(m.lastindex):
                print('group {0}: {1}'.format(i + 1, m.group(i + 1)))
        ddls = [
            """ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 'hdfs://sriopmgta0101.recette.local:8020/apps/int/dwh/hive/dwh_refr_ints' TBLPROPERTIES( 'COLUMN_STATS_ACCURATE'='false', 'last_modified_by'='bigsql', 'last_modified_time'='1472225933', 'numFiles'='0', 'numRows'='-1', 'rawDataSize'='-1', 'totalSize'='0' );""",
        ]
        for ddl in ddls:
            print('handling request:\n' + ddl)
            m = db2tools.RE_STORAGE_PARQUET.match(ddl)
            self.assertTrue(m is not None,
                            'regexp:\n {0}\ndid not match:\n{1}'.format(db2tools.RE_STORAGE_TEXT.pattern, ddl))
            for i in range(m.lastindex):
                print('group {0}: {1}'.format(i + 1, m.group(i + 1)))


if __name__ == '__main__':
    unittest.main()
