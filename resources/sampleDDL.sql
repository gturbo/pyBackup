------------------------------------------------
-- DDL Statements for Table "DEV_DRE "."TESTJ"
------------------------------------------------
 

CREATE TABLE "DEV_DRE "."TESTJ"  (
		  "CHAMP1" CHAR(10 OCTETS) )   
		 IN "BIGSQLCATSPACE"  
		 ORGANIZE BY ROW; 





------------------------------------------------
-- DDL Statements for Table "BIGSQL  "."AUDREY"
------------------------------------------------
 
CREATE HADOOP TABLE BIGSQL  .AUDREY(
    ID INT,
    NOM VARCHAR(50)
)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS
    INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    'hdfs://sriopmgta0101.recette.local:8020/apps/hive/warehouse/bigsql.db/audrey';



------------------------------------------------
-- DDL Statements for Table "INT_DRE "."REFR_STRC"
------------------------------------------------
 
CREATE HADOOP TABLE INT_DRE .REFR_STRC(
    TS_ALMT TIMESTAMP,
    CLE_TEMPS_PEO VARCHAR(50),
    CLE_LIEU_PEO SMALLINT,
    NO_PEO INT, /* comment */
    CD_ROLE SMALLINT,
    TYPE_BUDG SMALLINT,
    CD_NATR SMALLINT,
    CD_NIV SMALLINT,
    FL_DELG_RISQ VARCHAR(1), /*

    dezidnezdn
    ezdezd
    cezdcezcd

    */
    FL_PEO_BCI VARCHAR(1),
    TYPE_UNIT_ORGA SMALLINT,
    FL_ATTC_COML VARCHAR(1),
    FL_PEO_POIN_VENT VARCHAR(1),
    FL_PEO_GESTR VARCHAR(1),
    TYPE_ROLE SMALLINT,
    TYPE_CLNT VARCHAR(2),
    CD_PRFX VARCHAR(2),
    LB_PREM_MIRE VARCHAR(8),
    FL_PRST VARCHAR(1),
    FL_GESTR VARCHAR(1),
    INTIT_PEO VARCHAR(64),
    NO_PEO_RATT INT,
    CD_SOCT VARCHAR(2),
    CD_ETAB SMALLINT,
    NO_PEO_AGNC_PRNC INT,
    NO_PEO_DIRC INT,
    NO_PEO_CTRE_AUTR INT,
    NO_PEO_CTRE_RESPT INT,
    NO_PEO_SOUS_SECT INT,
    MTRCL_RESP INT
)
PARTITIONED BY(
    DT_VACT TIMESTAMP
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\;'
WITH SERDE PROPERTIES(
    'escape.delim'='\\'
)
STORED AS
    INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    'hdfs://sriopmgta0101.recette.local:8020/apps/int/dre/hive/tier_relt_tier'
TBLPROPERTIES(
    'SERIALIZATION.NULL.FORMAT'='',
    'last_modified_by'='bigsql',
    'last_modified_time'='1475726298'
);
CREATE VIEW INT_ZE_CRM.SCORE_ATTRITION_IARD_AUTO AS
with stmt_iard as (
SELECT
			COALESCE(p.ID_FONCTIONEL,0) AS ID_FONCTIONEL
			, p.PREDICTION as SCORE
			, BIGINT(p.id_contrat) AS ID_CONTRAT
			, p.date_vision
			, rank() over(partition by p.ID_FONCTIONEL order by p.prediction desc) as rk
			, CASE WHEN p.prediction > 0.16 THEN 1 ELSE 0 END as flag_preco
			, CASE WHEN p.prediction > 0.11 THEN 1 ELSE 0 END as flag_min
		FROM "INT_ZE_CRM"."PREDICTION_CHURN_INT" p
) select DISTINCT
		CAST(s.ID_FONCTIONEL AS VARCHAR(8)) AS ID_CLIENT
		, s.SCORE AS NOTE_SCORE
		, (select si.ID_CONTRAT from stmt_iard si where rk=1 and si.ID_FONCTIONEL = s.ID_FONCTIONEL order by SCORE desc FETCH FIRST 1 ROWS ONLY) AS ID_CONTRAT_MAX
		, 0.16  as SEUIL_PRECO
		, 0.11  as SEUIL_MIN
		, (select count(*) from stmt_iard where flag_preco=1 and ID_FONCTIONEL = s.ID_FONCTIONEL) as NB_CONTRAT_SUP_SEUIL_PRECO
		, (select count(*) from stmt_iard where flag_min=1 and ID_FONCTIONEL = s.ID_FONCTIONEL) as NB_CONTRAT_SUP_SEUIL_MIN
	from  stmt_iard s
	where s.rk=1;

CREATE FUNCTION EXPLAIN_GET_MSGS( EXPLAIN_REQUESTER VARCHAR(128 OCTETS),
                                  EXPLAIN_TIME      TIMESTAMP,
                                  SOURCE_NAME       VARCHAR(128 OCTETS),
                                  SOURCE_SCHEMA     VARCHAR(128 OCTETS),
                                  SOURCE_VERSION    VARCHAR(64 OCTETS),
                                  EXPLAIN_LEVEL     CHAR(1 OCTETS),
                                  STMTNO            INTEGER,
                                  SECTNO            INTEGER,
                                  INLOCALE          VARCHAR(33 OCTETS) )
  RETURNS TABLE ( EXPLAIN_REQUESTER VARCHAR(128 OCTETS),
                  EXPLAIN_TIME      TIMESTAMP,
                  SOURCE_NAME       VARCHAR(128 OCTETS),
                  SOURCE_SCHEMA     VARCHAR(128 OCTETS),
                  SOURCE_VERSION    VARCHAR(64 OCTETS),
                  EXPLAIN_LEVEL     CHAR(1 OCTETS),
                  STMTNO            INTEGER,
                  SECTNO            INTEGER,
                  DIAGNOSTIC_ID     INTEGER,
                  LOCALE            VARCHAR(33 OCTETS),
                  MSG               VARCHAR(4096 OCTETS) )
  SPECIFIC EXPLAIN_GET_MSGS
  LANGUAGE SQL
  DETERMINISTIC
  NO EXTERNAL ACTION
  READS SQL DATA
  RETURN SELECT A.A_EXPLAIN_REQUESTER,
                A.A_EXPLAIN_TIME,
                A.A_SOURCE_NAME,
                A.A_SOURCE_SCHEMA,
                A.A_SOURCE_VERSION,
                A.A_EXPLAIN_LEVEL,
                A.A_STMTNO,
                A.A_SECTNO,
                A.A_DIAGNOSTIC_ID,
                F.LOCALE,
                F.MSG
         FROM EXPLAIN_DIAGNOSTIC A( A_EXPLAIN_REQUESTER,
                                    A_EXPLAIN_TIME,
                                    A_SOURCE_NAME,
                                    A_SOURCE_SCHEMA,
                                    A_SOURCE_VERSION,
                                    A_EXPLAIN_LEVEL,
                                    A_STMTNO,
                                    A_SECTNO,
                                    A_DIAGNOSTIC_ID,
                                    A_CODE ),
              TABLE( SYSPROC.EXPLAIN_GET_MSG2(
                       INLOCALE,
                       A.A_CODE,
                       ( SELECT TOKEN FROM EXPLAIN_DIAGNOSTIC_DATA B
                         WHERE A.A_EXPLAIN_REQUESTER = B.EXPLAIN_REQUESTER
                           AND A.A_EXPLAIN_TIME      = B.EXPLAIN_TIME
                           AND A.A_SOURCE_NAME       = B.SOURCE_NAME
                           AND A.A_SOURCE_SCHEMA     = B.SOURCE_SCHEMA
                           AND A.A_SOURCE_VERSION    = B.SOURCE_VERSION
                           AND A.A_EXPLAIN_LEVEL     = B.EXPLAIN_LEVEL
                           AND A.A_STMTNO            = B.STMTNO
                           AND A.A_SECTNO            = B.SECTNO
                           AND A.A_DIAGNOSTIC_ID     = B.DIAGNOSTIC_ID
                           AND B.ORDINAL=1 ),
                       ( SELECT TOKEN FROM EXPLAIN_DIAGNOSTIC_DATA B
                         WHERE A.A_EXPLAIN_REQUESTER = B.EXPLAIN_REQUESTER
                           AND A.A_EXPLAIN_TIME      = B.EXPLAIN_TIME
                           AND A.A_SOURCE_NAME       = B.SOURCE_NAME
                           AND A.A_SOURCE_SCHEMA     = B.SOURCE_SCHEMA

                           AND A.A_SOURCE_VERSION    = B.SOURCE_VERSION
                           AND A.A_EXPLAIN_LEVEL     = B.EXPLAIN_LEVEL
                           AND A.A_STMTNO            = B.STMTNO
                           AND A.A_SECTNO            = B.SECTNO
                           AND A.A_DIAGNOSTIC_ID     = B.DIAGNOSTIC_ID
                           AND B.ORDINAL=2 ),
                       ( SELECT TOKEN FROM EXPLAIN_DIAGNOSTIC_DATA B
                         WHERE A.A_EXPLAIN_REQUESTER = B.EXPLAIN_REQUESTER
                           AND A.A_EXPLAIN_TIME      = B.EXPLAIN_TIME
                           AND A.A_SOURCE_NAME       = B.SOURCE_NAME
                           AND A.A_SOURCE_SCHEMA     = B.SOURCE_SCHEMA
                           AND A.A_SOURCE_VERSION    = B.SOURCE_VERSION
                           AND A.A_EXPLAIN_LEVEL     = B.EXPLAIN_LEVEL
                           AND A.A_STMTNO            = B.STMTNO
                           AND A.A_SECTNO            = B.SECTNO
                           AND A.A_DIAGNOSTIC_ID     = B.DIAGNOSTIC_ID
                           AND B.ORDINAL=3 ) ) ) F
         WHERE ( EXPLAIN_REQUESTER IS NULL OR
                 EXPLAIN_REQUESTER = A.A_EXPLAIN_REQUESTER )
           AND ( EXPLAIN_TIME      IS NULL OR
                 EXPLAIN_TIME      = A.A_EXPLAIN_TIME      )
           AND ( SOURCE_NAME       IS NULL OR
                 SOURCE_NAME       = A.A_SOURCE_NAME       )
           AND ( SOURCE_SCHEMA     IS NULL OR
                 SOURCE_SCHEMA     = A.A_SOURCE_SCHEMA     )
           AND ( SOURCE_VERSION    IS NULL OR
                 SOURCE_VERSION    = A.A_SOURCE_VERSION    )
           AND ( EXPLAIN_LEVEL     IS NULL OR
                 EXPLAIN_LEVEL     = A.A_EXPLAIN_LEVEL     )
           AND ( STMTNO            IS NULL OR
                 STMTNO            = A.A_STMTNO            )
           AND ( SECTNO            IS NULL OR
                 SECTNO            = A.A_SECTNO            );
