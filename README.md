# HiveSQL
HiveSQL入门，了解一下？😉

```sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS db_hive;

-- 指定HDFS上存放位置
CREATE DATABASE IF NOT EXISTS db_hive2 LOCATION '/db_hive2.db';

-- 过滤查询数据库
SHOW DATABASES LIKE 'db_hive*';

-- 显示数据库信息
DESC DATABASE db_hive;
/*
+----------+----------+-------------------------------------------------------+-------------+-------------+-------------+--+
| db_name  | comment  |                       location                        | owner_name  | owner_type  | parameters  |
+----------+----------+-------------------------------------------------------+-------------+-------------+-------------+--+
| db_hive  |          | hdfs://hadoop101:9000/user/hive/warehouse/db_hive.db  | atguigu     | USER        |             |
+----------+----------+-------------------------------------------------------+-------------+-------------+-------------+--+
*/

-- 显示数据库详细信息
DESC DATABASE EXTENDED db_hive;
/*
 +----------+----------+-------------------------------------------------------+-------------+-------------+--------------------------+--+
| db_name  | comment  |                       location                        | owner_name  | owner_type  |        parameters        |
+----------+----------+-------------------------------------------------------+-------------+-------------+--------------------------+--+
| db_hive  |          | hdfs://hadoop101:9000/user/hive/warehouse/db_hive.db  | atguigu     | USER        | {createtime=2019-06-03}  |
+----------+----------+-------------------------------------------------------+-------------+-------------+--------------------------+--+

 */

--  修改数据库
ALTER DATABASE db_hive SET DBPROPERTIES ('createtime' = '20190628');
/*
 +----------+----------+-------------------------------------------------------+-------------+-------------+------------------------+--+
| db_name  | comment  |                       location                        | owner_name  | owner_type  |       parameters       |
+----------+----------+-------------------------------------------------------+-------------+-------------+------------------------+--+
| db_hive  |          | hdfs://hadoop101:9000/user/hive/warehouse/db_hive.db  | atguigu     | USER        | {createtime=20190628}  |
+----------+----------+-------------------------------------------------------+-------------+-------------+------------------------+--+
 */

// 删除数据库
-- 删除空数据库
DROP DATABASE db_hive2;

DROP DATABASE IF EXISTS db_hive2;

-- 强制删除不为空数据库
DROP DATABASE db_hive CASCADE;


-- 创建表
CREATE
[EXTERNAL] TABLE [IF NOT EXISTS] table_name
(
    clo_name col_type [COMMENT col_comment],
    col_name2 col_tyoe2 [COMMENT col_comment2]
) [COMMENT table_comment]
    [PARTITIONED BY (col_name data_type [COMMENT col_comment])]
    [CLUSTERED BY (col_name, col_name1)]
    [SORTED BY (col_name [ASC|DESC], col_name) INTO num_buckets BUCKETS]
    [ROW FORMAT row_format]
    [STORED AS file_format]
    [LOCATION hdfs_path]
    [TBLPROPERTIES (property_name=property_value, property_name1=property_value1)]
    [AS select_statement];
/*
（1）CREATE TABLE：创建一个指定名字的表。如果相同名字的表已经存在，则抛出异常；可以用IF NOT EXISTS 选项来忽略这个异常。
（2）EXTERNAL：创建一个外部表，在建表的同时可以指定一个指向实际数据的路径（LOCATION），在删除表的时候，内部表的元数据和
    数据会被一起删除，而外部表只删除元数据，不删除数据。
（3）COMMENT：为表和列添加注释。
（4）PARTITIONED BY：创建分区表
（5）CLUSTERED BY：创建分桶表
（6）SORTED BY（不常用）：对桶中的一个或多个列另外排序
（7）ROW FORMAT
        DELIMITED [FIELDS TERMINATED BY char]
                  [COLLECTION ITEMS TERMINATED BY char]
                  [MAP KEYS TERMINATED BY char]
                  [LINES TERMINATED BY char]
        | SERDE serde_name [WITH SERDEPROPERTIES (property_name=property_value, property_name=property_value, ...)]
    解释：
        SerDe（Serializer and Deserializer）
    用户在建表的时候可以自定义SerDe或者使用自带的SerDe。如果没有指定ROW FORMAT 或者ROW FORMAT DELIMITED，将会使用自带的SerDe。
    在建表的时候，用户还需要为表指定列，用户在指定表的列的同时也会指定自定义的SerDe，Hive通过SerDe确定表的具体的列的数据。
（8）STORED AS：指定存储文件类型
    常用的存储文件类型：SEQUENCEFILE（二进制序列文件）、TEXTFILE（文本）、RCFILE（列式存储格式文件）
    如果文件数据是纯文本，可以使用STORED AS TEXTFILE。如果数据需要压缩，使用STORED AS SEQUENCEFILE。
（9）LOCATION ：指定表在HDFS上的存储位置。
（10）AS：后跟查询语句，根据查询结果创建表。
（11）LIKE：允许用户复制现有的表结构，但是不复制数据。
 */

-- 创建普通表
CREATE TABLE IF NOT EXISTS student
(
    id   int,
    name string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/students';


-- 根据查询结果创建表（查询结果会添加到新创建的表中）
CREATE TABLE IF NOT EXISTS studen2 AS
SELECT id, name
FROM student;

-- 根据已存在的表结构创建表
CREATE TABLE IF NOT EXISTS student3 LIKE student;

-- 查询表的结构
DESC FORMATTED student;

/*
+-------------------------------+-------------------------------------------------------------+-----------------------+--+
|           col_name            |                          data_type                          |        comment        |
+-------------------------------+-------------------------------------------------------------+-----------------------+--+
| # col_name                    | data_type                                                   | comment               |
|                               | NULL                                                        | NULL                  |
| id                            | int                                                         |                       |
| name                          | string                                                      |                       |
|                               | NULL                                                        | NULL                  |
| # Detailed Table Information  | NULL                                                        | NULL                  |
| Database:                     | db_hive                                                     | NULL                  |
| Owner:                        | atguigu                                                     | NULL                  |
| CreateTime:                   | Mon Jun 03 20:32:50 CST 2019                                | NULL                  |
| LastAccessTime:               | UNKNOWN                                                     | NULL                  |
| Protect Mode:                 | None                                                        | NULL                  |
| Retention:                    | 0                                                           | NULL                  |
| Location:                     | hdfs://hadoop101:9000/                                      | NULL                  |
| Table Type:                   | MANAGED_TABLE                                               | NULL                  |
| Table Parameters:             | NULL                                                        | NULL                  |
|                               | COLUMN_STATS_ACCURATE                                       | true                  |
|                               | numFiles                                                    | 0                     |
|                               | numRows                                                     | 3                     |
|                               | rawDataSize                                                 | 26                    |
|                               | totalSize                                                   | 0                     |
|                               | transient_lastDdlTime                                       | 1559653841            |
|                               | NULL                                                        | NULL                  |
| # Storage Information         | NULL                                                        | NULL                  |
| SerDe Library:                | org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe          | NULL                  |
| InputFormat:                  | org.apache.hadoop.mapred.TextInputFormat                    | NULL                  |
| OutputFormat:                 | org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat  | NULL                  |
| Compressed:                   | No                                                          | NULL                  |
| Num Buckets:                  | -1                                                          | NULL                  |
| Bucket Columns:               | []                                                          | NULL                  |
| Sort Columns:                 | []                                                          | NULL                  |
| Storage Desc Params:          | NULL                                                        | NULL                  |
|                               | field.delim                                                 | \t                    |
|                               | serialization.format                                        | \t                    |
+-------------------------------+-------------------------------------------------------------+-----------------------+--+
 */

--  创建外部表
CREATE EXTERNAL TABLE stu_external
(
    id   int,
    name string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/student';

-- 查看表结构
DESC FORMATTED stu_external;
/*
+-------------------------------+-------------------------------------------------------------+-----------------------+--+
|           col_name            |                          data_type                          |        comment        |
+-------------------------------+-------------------------------------------------------------+-----------------------+--+
| # col_name                    | data_type                                                   | comment               |
|                               | NULL                                                        | NULL                  |
| id                            | int                                                         |                       |
| name                          | string                                                      |                       |
|                               | NULL                                                        | NULL                  |
| # Detailed Table Information  | NULL                                                        | NULL                  |
| Database:                     | db_hive                                                     | NULL                  |
| Owner:                        | atguigu                                                     | NULL                  |
| CreateTime:                   | Fri Jun 28 20:37:56 CST 2019                                | NULL                  |
| LastAccessTime:               | UNKNOWN                                                     | NULL                  |
| Protect Mode:                 | None                                                        | NULL                  |
| Retention:                    | 0                                                           | NULL                  |
| Location:                     | hdfs://hadoop101:9000/student                               | NULL                  |
| Table Type:                   | EXTERNAL_TABLE                                              | NULL                  |
| Table Parameters:             | NULL                                                        | NULL                  |
|                               | COLUMN_STATS_ACCURATE                                       | false                 |
|                               | EXTERNAL                                                    | TRUE                  |
|                               | numFiles                                                    | 0                     |
|                               | numRows                                                     | -1                    |
|                               | rawDataSize                                                 | -1                    |
|                               | totalSize                                                   | 0                     |
|                               | transient_lastDdlTime                                       | 1561725476            |
|                               | NULL                                                        | NULL                  |
| # Storage Information         | NULL                                                        | NULL                  |
| SerDe Library:                | org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe          | NULL                  |
| InputFormat:                  | org.apache.hadoop.mapred.TextInputFormat                    | NULL                  |
| OutputFormat:                 | org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat  | NULL                  |
| Compressed:                   | No                                                          | NULL                  |
| Num Buckets:                  | -1                                                          | NULL                  |
| Bucket Columns:               | []                                                          | NULL                  |
| Sort Columns:                 | []                                                          | NULL                  |
| Storage Desc Params:          | NULL                                                        | NULL                  |
|                               | field.delim                                                 | \t                    |
|                               | serialization.format                                        | \t                    |
+-------------------------------+-------------------------------------------------------------+-----------------------+--+
 */


-- 修改表为外部表
ALTER TABLE student
    SET TBLPROPERTIES ('EXTERNAL' = 'TRUE');
-- 注意：('EXTERNAL'='TRUE')和('EXTERNAL'='FALSE')为固定写法，区分大小写！

-- 表结构
/*
+-------------------------------+-------------------------------------------------------------+-----------------------+--+
|           col_name            |                          data_type                          |        comment        |
+-------------------------------+-------------------------------------------------------------+-----------------------+--+
| Location:                     | hdfs://hadoop101:9000/user/hive/warehouse/students          | NULL                  |
| Table Type:                   | EXTERNAL_TABLE                                              | NULL                  |
+-------------------------------+-------------------------------------------------------------+-----------------------+--+
 */


-- 创建分区表
CREATE TABLE dept_partition
(
    deptno int,
    dname  string,
    loc    string
) PARTITIONED BY (month string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 查看分区表结构
/*
+-------------------------------+----------------------------------------------------------------------+-----------------------+--+
|           col_name            |                              data_type                               |        comment        |
+-------------------------------+----------------------------------------------------------------------+-----------------------+--+
| # col_name                    | data_type                                                            | comment               |
|                               | NULL                                                                 | NULL                  |
| deptno                        | int                                                                  |                       |
| dname                         | string                                                               |                       |
| loc                           | string                                                               |                       |
|                               | NULL                                                                 | NULL                  |
| # Partition Information       | NULL                                                                 | NULL                  |
| # col_name                    | data_type                                                            | comment               |
|                               | NULL                                                                 | NULL                  |
| month                         | string                                                               |                       |
 */
-- 加载数据到分区
LOAD DATA LOCAL INPATH '/opt/module/datas/dept.txt' INTO TABLE dept_partition PARTITION (month = '201906');
LOAD DATA LOCAL INPATH '/opt/module/datas/dept.txt' INTO TABLE dept_partition PARTITION (month = '201905');
LOAD DATA LOCAL INPATH '/opt/module/datas/dept.txt' INTO TABLE dept_partition PARTITION (month = '201904');
LOAD DATA LOCAL INPATH '/opt/module/datas/dept.txt' INTO TABLE dept_partition PARTITION (month = '201903');

-- 查询分区表数据
SELECT *
FROM dept_partition
WHERE month = '201903';
/*
+------------------------+-----------------------+---------------------+-----------------------+--+
| dept_partition.deptno  | dept_partition.dname  | dept_partition.loc  | dept_partition.month  |
+------------------------+-----------------------+---------------------+-----------------------+--+
| 10                     | ACCOUNTING            | 1700                | 201903                |
| 20                     | RESEARCH              | 1800                | 201903                |
| 30                     | SALES                 | 1900                | 201903                |
| 40                     | OPERATIONS            | 1700                | 201903                |
+------------------------+-----------------------+---------------------+-----------------------+--+
 */

-- 多表联合查询
SELECT *
FROM dept_partition
WHERE month = '201904'
UNION
SELECT *
FROM dept_partition
WHERE month = '201905';
/*
+------------+-----------+---------+--_--------+
| _u2.deptno | _u2.dname | _u2.loc | _u2.month |
+------------+-----------+---------+-----------+
|10	         | ACCOUNTING| 1700	   | 201904    |
|10	         | ACCOUNTING| 1700	   | 201905    |
|20	         | RESEARCH	 | 1800	   | 201904    |
|20	         | RESEARCH	 | 1800	   | 201905    |
|30	         | SALES	 | 1900	   | 201904    |
|30	         | SALES	 | 1900	   | 201905    |
|40	         | OPERATIONS| 1700	   | 201904    |
|40	         | OPERATIONS| 1700	   | 201905    |
+------------+-----------+---------+-----------+
 */
--  增加分区
ALTER TABLE dept_partition
    ADD PARTITION (month = '201907');
-- 删除分区
ALTER TABLE dept_partition
    DROP PARTITION (month = '201906');
-- 查看分区数
SHOW PARTITIONS dept_partition;
/*
+---------------+--+
|   partition   |
+---------------+--+
| month=201709  |
| month=201903  |
| month=201904  |
| month=201905  |
| month=201906  |
+---------------+--+
 */
```



