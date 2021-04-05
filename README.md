# HiveSQL
HiveSQL入门，了解一下？😉

## [HQL小练习](https://github.com/Dang-h/movieETL)

## HQL小知识

```sql
-- 一些设置
-- 以本地模式运行
/*
 当一个job满足如下条件才能真正使用本地模式：
 1.job的输入数据大小必须小于参数：hive.exec.mode.local.auto.inputbytes.max(默认128MB)
 2.job的map数必须小于参数：hive.exec.mode.local.auto.tasks.max(默认4)
 3.job的reduce数必须为0或者1
 */
SET hive.exec.mode.local.auto=true;
-- 查看内置函数
SHOW FUNCTIONS;
-- 查看month相关的函数
SHOW FUNCTIONS LIKE '*month*'
-- 查看函数用法
DESC FUNCTION function_name;
-- 查看 add_months 函数的详细说明并举例
DESC FUNCTION EXTENDED add_months;
-- 查看分区
SHOW PARTITIONS table_name;

-- 创建数据库,数据库在HDFS上的默认存储路径是/user/hive/warehouse/*.db。
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

-- 清空指定表中的数据
TRUNCATE TABLE stu_buck;

-- 常用日期函数
// 1 date_formate
// 2 date_add
// 3 next_add
// 4 next_day
// 5 last_day
// 6 date_sub


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
    常用的存储文件类型：SEQUENCEFILE（二进制序列文件）、TEXTFILE（文本）、RCFILE（列式存储格式文件）、PARQUET（Protocolbuffer，
    thrift，json等，将这类数据存储成列式格式，以方便对其高效压缩和编码，且使用更少的IO操作取出需要的数据）
    如果文件数据是纯文本，可以使用STORED AS TEXTFILE。如果数据需要压缩，使用STORED AS SEQUENCEFILE。
（9）LOCATION ：指定表在HDFS上的存储位置。
（10）AS：后跟查询语句，根据查询结果创建表。
（11）LIKE：允许用户复制现有的表结构，但是不复制数据。
 */

-- 创建普通表
CREATE TABLE IF NOT EXISTS user_info
(
    uid  int,
    name string,
    age  int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/user_info';
CREATE TABLE IF NOT EXISTS house_info
(
    houseId string,
    uid     int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/house_info';


-- 根据查询结果创建表（查询结果会添加到新创建的表中）
CREATE TABLE IF NOT EXISTS studen2 STORED AS ORC AS
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


-- 创建分区表,Hive中的分区就是分目录,把一个大的数据集根据业务需要分割成小的数据集
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
--  分区表小例子
// 建表
CREATE TABLE t_visit_video
(
    username   string,
    video_name string
) PARTITIONED BY (day string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
// 导入数据
LOAD DATA LOCAL INPATH '/test/collect_set_test.txt' INTO TABLE t_visit_video PARTITION (day = '2019-07-10');
/*
 数据：
张三,大唐双龙传
李四,天下无贼
张三,神探狄仁杰
李四,霸王别姬
李四,霸王别姬
王五,机器人总动员
王五,放牛班的春天
王五,盗梦空间
 */
// 查看
SELECT *
FROM t_visit_video;
/*
+-------------------------+---------------------------+--------------------+--+
| t_visit_video.username  | t_visit_video.video_name  | t_visit_video.day  |
+-------------------------+---------------------------+--------------------+--+
| 张三                      | 大唐双龙传               | 2019-07-10         |
| 李四                      | 天下无贼                 | 2019-07-10         |
| 张三                      | 神探狄仁杰               | 2019-07-10         |
| 李四                      | 霸王别姬                 | 2019-07-10         |
| 李四                      | 霸王别姬                 | 2019-07-10         |
| 王五                      | 机器人总动员             | 2019-07-10         |
| 王五                      | 放牛班的春天             | 2019-07-10         |
| 王五                      | 盗梦空间                 | 2019-07-10         |
+-------------------------+---------------------------+--------------------+--+
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

--  创建二级分区表
CREATE TABLE dept_partition2
(
    deptno int,
    dname  string,
    loc    string
)
    PARTITIONED BY (month string, day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 加载数据到二级分区表
LOAD DATA LOCAL INPATH '/data/hive/dept.txt' INTO TABLE dept_partition2 PARTITION (month = '201906', day = '30');

-- 修改表
-- 重命名表
ALTER TABLE table_name
    RENAME TO new_table_name;


-- 增加/修改/替换列信息
-- 更新列
ALTER TABLE table_name
    CHANGE [COLUMN] col_old_name col_new_name column_tyoe [COMMENT col_comment];
-- 增加和替换列
ALTER TABLE table_name
    ADD | REPLACE COLUMS (col_name data_type);

-- 示例
ALTER TABLE dept_partition
    ADD COLUMNS (deptdesc string);

-- 将列deptdesc改名为desc
ALTER TABLE dept_partition
    CHANGE COLUMN deptdesc desc string;

-- 更改字段类型，change后字段名称写两遍
ALTER TABLE dept_partition2
    CHANGE deptno deptno string;


-- 数据导入
LOAD DATA [LOCAL] INPATH '/data/hive/student.txt' [OVERWRITE] INTO TABLE student [PARTITION (partCol1=cal1,...)]
/*
（1）load data:表示加载数据
（2）local:表示从本地加载数据到hive表(从本地复制上传)；否则从HDFS加载数据到hive表（在HDFS中移动）
（3）inpath:表示加载数据的路径
（4）overwrite:表示覆盖表中已有数据，否则表示追加
（5）into table:表示加载到哪张表
（6）student:表示具体的表
（7）partition:表示上传到指定分区
 */

--  通过查询语句向表中插数据
INSERT INTO TABLE student
SELECT clo1, col2, col3
FROM table_name;

-- 创建一张分区表
CREATE TABLE student
(
    id   int,
    name string
) PARTITIONED BY (month string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
INSERT INTO TABLE student PARTITION (month = '201906')
VALUES (1, 'zhangsan'),
       (2, '王五');


-- 查询小练习
-- 创建部门表
CREATE TABLE IF NOT EXISTS dept
(
    deptno int,
    dname  string,
    loc    string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 创建员工表
CREATE TABLE IF NOT EXISTS emp
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 导入数据
LOAD DATA LOCAL INPATH '/data/hive/dept.txt' INTO TABLE dept;
LOAD DATA LOCAL INPATH '/data/hive/emp.txt' INTO TABLE emp;

-- 查询名称和部门
SELECT ename AS name, deptno AS dn
FROM emp;

-- 分组函数，通常和聚合函数一起使用，按照一个或多个列结果进行分组，然后对每个组执行聚合操作,常与聚合函数一起使用，
SELECT *
FROM emp;
/*
+------------+------------+------------+----------+---------------+----------+-----------+-------------+--+
| emp.empno  | emp.ename  |  emp.job   | emp.mgr  | emp.hiredate  | emp.sal  | emp.comm  | emp.deptno  |
+------------+------------+------------+----------+---------------+----------+-----------+-------------+--+
| 7369       | SMITH      | CLERK      | 7902     | 1980-12-17    | 800.0    | NULL      | 20          |
| 7499       | ALLEN      | SALESMAN   | 7698     | 1981-2-20     | 1600.0   | 300.0     | 30          |
| 7521       | WARD       | SALESMAN   | 7698     | 1981-2-22     | 1250.0   | 500.0     | 30          |
| 7566       | JONES      | MANAGER    | 7839     | 1981-4-2      | 2975.0   | NULL      | 20          |
| 7654       | MARTIN     | SALESMAN   | 7698     | 1981-9-28     | 1250.0   | 1400.0    | 30          |
| 7698       | BLAKE      | MANAGER    | 7839     | 1981-5-1      | 2850.0   | NULL      | 30          |
| 7782       | CLARK      | MANAGER    | 7839     | 1981-6-9      | 2450.0   | NULL      | 10          |
| 7788       | SCOTT      | ANALYST    | 7566     | 1987-4-19     | 3000.0   | NULL      | 20          |
| 7839       | KING       | PRESIDENT  | NULL     | 1981-11-17    | 5000.0   | NULL      | 10          |
| 7844       | TURNER     | SALESMAN   | 7698     | 1981-9-8      | 1500.0   | 0.0       | 30          |
| 7876       | ADAMS      | CLERK      | 7788     | 1987-5-23     | 1100.0   | NULL      | 20          |
| 7900       | JAMES      | CLERK      | 7698     | 1981-12-3     | 950.0    | NULL      | 30          |
| 7902       | FORD       | ANALYST    | 7566     | 1981-12-3     | 3000.0   | NULL      | 20          |
| 7934       | MILLER     | CLERK      | 7782     | 1982-1-23     | 1300.0   | NULL      | 10          |
+------------+------------+------------+----------+---------------+----------+-----------+-------------+--+
 */

SELECT *
FROM dept;
/*
+--------------+-------------+-----------+--+
| dept.deptno  | dept.dname  | dept.loc  |
+--------------+-------------+-----------+--+
| 10           | ACCOUNTING  | 1700      |
| 20           | RESEARCH    | 1800      |
| 30           | SALES       | 1900      |
| 40           | OPERATIONS  | 1700      |
+--------------+-------------+-----------+--+
*/

-- 计算emp表每个部门的平均工资
SELECT e.deptno, avg(e.sal) AS avg_sal
FROM emp e
GROUP BY e.deptno;
/*
+-----------+---------------------+--+
| e.deptno  |       avg_sal       |
+-----------+---------------------+--+
| 10        | 2916.6666666666665  |
| 20        | 2175.0              |
| 30        | 1566.6666666666667  |
+-----------+---------------------+--+
 */

-- 计算emp每个部门中每个岗位的最高薪水
-- 1、每个部门，部门分组
-- 2、每个岗位，岗位分组
-- 2、最高薪水，选出部门中最高薪水
SELECT deptno, job
FROM emp
GROUP BY deptno, job;
/*
+---------+------------+--+
| deptno  |    job     |
+---------+------------+--+
| 10      | CLERK      |
| 10      | MANAGER    |
| 10      | PRESIDENT  |
| 20      | ANALYST    |
| 20      | CLERK      |
| 20      | MANAGER    |
| 30      | CLERK      |
| 30      | MANAGER    |
| 30      | SALESMAN   |
+---------+------------+--+
 */

SELECT e.deptno, e.job, max(e.sal) AS max_sal
FROM emp e
GROUP BY e.deptno, e.job;
/*
+-----------+------------+----------+--+
| e.deptno  |   e.job    | max_sal  |
+-----------+------------+----------+--+
| 10        | CLERK      | 1300.0   |
| 10        | MANAGER    | 2450.0   |
| 10        | PRESIDENT  | 5000.0   |
| 20        | ANALYST    | 3000.0   |
| 20        | CLERK      | 1100.0   |
| 20        | MANAGER    | 2975.0   |
| 30        | CLERK      | 950.0    |
| 30        | MANAGER    | 2850.0   |
| 30        | SALESMAN   | 1600.0   |
+-----------+------------+----------+--+
 */

-- 列转行，查看一个部门有哪些职位,及部门最高薪资
SELECT e.deptno, concat_ws('|', collect_set(e.job)) dept_job, max(e.sal) max_sal
FROM emp e
GROUP BY e.deptno;
/*
+-----------+--------------------------+----------+--+
| e.deptno  |         dept_job         | max_sal  |
+-----------+--------------------------+----------+--+
| 10        | MANAGER|PRESIDENT|CLERK  | 5000.0   |
| 20        | CLERK|MANAGER|ANALYST    | 3000.0   |
| 30        | SALESMAN|MANAGER|CLERK   | 2850.0   |
+-----------+--------------------------+----------+--+
*/

/*
 having与where不同点
（1）where后面不能写分组函数，而having后面可以使用分组函数。
（2）having只用于group by分组统计语句。
 */

-- 求每个部门的平均薪水大于2000的部门
/*
 1、求部门平均工资
 2、平均薪水>2000的部门
 */

-- 1
SELECT deptno, avg(sal) AS avg_sal
FROM emp
GROUP BY deptno;
/*
+---------+---------------------+--+
| deptno  |       avg_sal       |
+---------+---------------------+--+
| 10      | 2916.6666666666665  |
| 20      | 2175.0              |
| 30      | 1566.6666666666667  |
+---------+---------------------+--+
 */

-- 2
SELECT deptno, avg(sal) AS avg_sql
FROM emp
GROUP BY deptno
HAVING avg_sql > 2000;
/*
+---------+---------------------+--+
| deptno  |       avg_sql       |
+---------+---------------------+--+
| 10      | 2916.6666666666665  |
| 20      | 2175.0              |
+---------+---------------------+--+
 */

-- join
/*
 只支持等值连接，不支持非等值连接
 两个表m,n之间按照on条件连接，m中的一条记录和n中的一条记录组成一条新记录。
 join等值连接（内连接），只有某个值在m和n中同时存在时。
 left outer join左外连接，左边表中的值无论是否在b中存在时，都输出；右边表中的值，只有在左边表中存在时才输出，否则为null
 right outer join和left outer join相反。
 left semi join类似exists。即查找右表中的数据，是否在左表中存在，找出存在的数据。（左半连接）是 IN/EXISTS 子查询的一种更高效的实现。
 */

-- 根据员工表和部门表中的部门编号相等，查询员工编号、员工名称和部门名称
/*
 1、部门编号相等
 2、查询员工编号（empno）、员工名称（empname）、本门名称（deptname）
 */
-- 表的别名
/*
 （1）使用别名可以简化查询。
 （2）使用表名前缀可以提高执行效率。
 */
SELECT e.empno AS empno, e.ename AS ename, d.dname AS deptname
FROM emp AS e
         JOIN dept AS d
              ON e.deptno = d.deptno;
/*
+--------+---------+-------------+--+
| empno  |  ename  |  deptname   |
+--------+---------+-------------+--+
| 7369   | SMITH   | RESEARCH    |
| 7499   | ALLEN   | SALES       |
| 7521   | WARD    | SALES       |
| 7566   | JONES   | RESEARCH    |
| 7654   | MARTIN  | SALES       |
| 7698   | BLAKE   | SALES       |
| 7782   | CLARK   | ACCOUNTING  |
| 7788   | SCOTT   | RESEARCH    |
| 7839   | KING    | ACCOUNTING  |
| 7844   | TURNER  | SALES       |
| 7876   | ADAMS   | RESEARCH    |
| 7900   | JAMES   | SALES       |
| 7902   | FORD    | RESEARCH    |
| 7934   | MILLER  | ACCOUNTING  |
+--------+---------+-------------+--+
 */

--  内连接：只有进行连接的两个表中都存在与连接条件相匹配的数据才会被保留下来。
SELECT e.empno AS empno, e.ename AS ename, d.deptno AS deptno
FROM emp AS e
         JOIN dept AS d
              ON e.deptno = d.deptno;
/*
+--------+---------+---------+--+
| empno  |  ename  | deptno  |
+--------+---------+---------+--+
| 7369   | SMITH   | 20      |
| 7499   | ALLEN   | 30      |
| 7521   | WARD    | 30      |
| 7566   | JONES   | 20      |
| 7654   | MARTIN  | 30      |
| 7698   | BLAKE   | 30      |
| 7782   | CLARK   | 10      |
| 7788   | SCOTT   | 20      |
| 7839   | KING    | 10      |
| 7844   | TURNER  | 30      |
| 7876   | ADAMS   | 20      |
| 7900   | JAMES   | 30      |
| 7902   | FORD    | 20      |
| 7934   | MILLER  | 10      |
+--------+---------+---------+--+
*/

-- 左外连接
/*
 LEFT [OUTER] JOIN操作：左边表中的值无论是否在右表中存在，都输出；
                        右边表中的值，只有在左边表中存在才输出。左表中存在，右表中不存在的用null代替
 如：
    表dept
    +--------------+-------------+-----------+--+
    | dept.deptno  | dept.dname  | dept.loc  |
    +--------------+-------------+-----------+--+
    | 10           | ACCOUNTING  | 1700      |
    | 20           | RESEARCH    | 1800      |
    | 30           | SALES       | 1900      |
    | 40           | OPERATIONS  | 1700      |
    +--------------+-------------+-----------+--+

    表emp
    +------------+------------+------------+----------+---------------+----------+-----------+-------------+--+
    | emp.empno  | emp.ename  |  emp.job   | emp.mgr  | emp.hiredate  | emp.sal  | emp.comm  | emp.deptno  |
    +------------+------------+------------+----------+---------------+----------+-----------+-------------+--+
    | 7369       | SMITH      | CLERK      | 7902     | 1980-12-17    | 800.0    | NULL      | 20          |
    | 7499       | ALLEN      | SALESMAN   | 7698     | 1981-2-20     | 1600.0   | 300.0     | 30          |
    | 7521       | WARD       | SALESMAN   | 7698     | 1981-2-22     | 1250.0   | 500.0     | 30          |
    | 7566       | JONES      | MANAGER    | 7839     | 1981-4-2      | 2975.0   | NULL      | 20          |
    | 7654       | MARTIN     | SALESMAN   | 7698     | 1981-9-28     | 1250.0   | 1400.0    | 30          |
    | 7698       | BLAKE      | MANAGER    | 7839     | 1981-5-1      | 2850.0   | NULL      | 30          |
    | 7782       | CLARK      | MANAGER    | 7839     | 1981-6-9      | 2450.0   | NULL      | 10          |
    | 7788       | SCOTT      | ANALYST    | 7566     | 1987-4-19     | 3000.0   | NULL      | 20          |
    | 7839       | KING       | PRESIDENT  | NULL     | 1981-11-17    | 5000.0   | NULL      | 10          |
    | 7844       | TURNER     | SALESMAN   | 7698     | 1981-9-8      | 1500.0   | 0.0       | 30          |
    | 7876       | ADAMS      | CLERK      | 7788     | 1987-5-23     | 1100.0   | NULL      | 20          |
    | 7900       | JAMES      | CLERK      | 7698     | 1981-12-3     | 950.0    | NULL      | 30          |
    | 7902       | FORD       | ANALYST    | 7566     | 1981-12-3     | 3000.0   | NULL      | 20          |
    | 7934       | MILLER     | CLERK      | 7782     | 1982-1-23     | 1300.0   | NULL      | 10          |
    +------------+------------+------------+----------+---------------+----------+-----------+-------------+--+

 */

SELECT d.deptno, d.dname, e.empno, e.ename
FROM dept AS d
         LEFT JOIN emp AS e
                   ON e.deptno = d.deptno;
/*
+-----------+-------------+----------+----------+--+
| d.deptno  |   d.dname   | e.empno  | e.ename  |
+-----------+-------------+----------+----------+--+
| 10        | ACCOUNTING  | 7782     | CLARK    |
| 10        | ACCOUNTING  | 7839     | KING     |
| 10        | ACCOUNTING  | 7934     | MILLER   |
| 20        | RESEARCH    | 7369     | SMITH    |
| 20        | RESEARCH    | 7566     | JONES    |
| 20        | RESEARCH    | 7788     | SCOTT    |
| 20        | RESEARCH    | 7876     | ADAMS    |
| 20        | RESEARCH    | 7902     | FORD     |
| 30        | SALES       | 7499     | ALLEN    |
| 30        | SALES       | 7521     | WARD     |
| 30        | SALES       | 7654     | MARTIN   |
| 30        | SALES       | 7698     | BLAKE    |
| 30        | SALES       | 7844     | TURNER   |
| 30        | SALES       | 7900     | JAMES    |
| 40        | OPERATIONS  | NULL     | NULL     |
+-----------+-------------+----------+----------+--+
*/

-- 满外连接：返回所有表中符合WHERE语句条件的所有记录。如果任一表的指定字段没有符合条件的值的话，那么就使用NULL值替代。
SELECT e.ename, e.empno, d.deptno, d.dname
FROM emp AS e
         FULL JOIN dept AS d
                   ON e.deptno = d.deptno;
/*
+----------+----------+-----------+-------------+--+
| e.ename  | e.empno  | d.deptno  |   d.dname   |
+----------+----------+-----------+-------------+--+
| MILLER   | 7934     | 10        | ACCOUNTING  |
| KING     | 7839     | 10        | ACCOUNTING  |
| CLARK    | 7782     | 10        | ACCOUNTING  |
| ADAMS    | 7876     | 20        | RESEARCH    |
| SCOTT    | 7788     | 20        | RESEARCH    |
| SMITH    | 7369     | 20        | RESEARCH    |
| JONES    | 7566     | 20        | RESEARCH    |
| FORD     | 7902     | 20        | RESEARCH    |
| TURNER   | 7844     | 30        | SALES       |
| ALLEN    | 7499     | 30        | SALES       |
| BLAKE    | 7698     | 30        | SALES       |
| MARTIN   | 7654     | 30        | SALES       |
| WARD     | 7521     | 30        | SALES       |
| JAMES    | 7900     | 30        | SALES       |
| NULL     | NULL     | 40        | OPERATIONS  |
+----------+----------+-----------+-------------+--+
*/

-- 排序
-- order by：全局排序，只有一个Reducer参与运算，会把所有数据加载到内存中进行排序
-- Sort by：Reducern局部排序，为每个reducer产生一个排序文件。每个Reducer内部进行排序，对全局结果集来说不是排序。
SELECT *
FROM emp SORT BY deptno DESC;

-- 分区排序（Distribute By）
/*
 控制某个特定行应该到哪个reducer
 */
--  设置reduce个数
SET mapreduce.job.reduces=3;
-- 先按照部门编号分区，再按照员工编号降序排序。
INSERT OVERWRITE LOCAL DIRECTORY '/opt/module/datas/distribute-result'
SELECT *
FROM emp DISTRIBUTE BY deptno SORT BY empno DESC;



-- 分桶及抽样查询
/*
 分区提供一个隔离数据和优化查询的便利方式，
 对于一张表或者分区，可进一步组织成桶，让其数据粒度更细
 分区针对的是数据的存储路径；分桶针对的是数据文件。
 创建分桶表时，数据通过子查询的方式导入
 */
-- 创建分桶表，指定分2个桶
CREATE TABLE stu_buck
(
    id   int,
    name string
) CLUSTERED BY (id) INTO 2 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 创建普通表stu
CREATE TABLE stu
(
    id   int,
    name string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 向普通表stu导入数据
LOAD DATA LOCAL INPATH '/data/hive/student.txt' INTO TABLE stu;
-- 设置属性
SET hive.enforce.bucketing=true;
SET mapreduce.job.reduces=-1;
-- 通过子查询的方式，导入数据到分桶表
INSERT INTO TABLE stu_buck
SELECT id, name
FROM stu;
-- 查询分桶的数据
SELECT *
FROM stu_buck TABLESAMPLE (BUCKET 1 OUT OF 2 ON id);
/*
+--------------+----------------+--+
| stu_buck.id  | stu_buck.name  |
+--------------+----------------+--+
| 1016         | ss16           |
| 1010         | ss10           |
| 1002         | ss2            |
| 1012         | ss12           |
| 1006         | ss6            |
| 1014         | ss14           |
| 1004         | ss4            |
| 1008         | ss8            |
+--------------+----------------+--+
*/


-- 常用函数使用
-- NVL：给值为NULL 的数据赋值。格式：NVL( value，default_value)。default_value需要和字段类型相同
-- 如果value 为NULL，返回default_value的值，否则返回value的值，如果两个参数都为NULL，则返回NULL。
SELECT comm, nvl(comm, -1) null_comm
FROM emp;
/*
+---------+------------+--+
|  comm   | null_comm  |
+---------+------------+--+
| NULL    | -1.0       |
| 300.0   | 300.0      |
| 500.0   | 500.0      |
| NULL    | -1.0       |
| 1400.0  | 1400.0     |
| NULL    | -1.0       |
| NULL    | -1.0       |
| NULL    | -1.0       |
| NULL    | -1.0       |
| 0.0     | 0.0        |
| NULL    | -1.0       |
| NULL    | -1.0       |
| NULL    | -1.0       |
| NULL    | -1.0       |
+---------+------------+--+
*/


-- case when 的使用
-- 求出不同部门男女各多少人
/*
 结果如下：
  dept_id  | male_count  | female_count
    A           2               1
    B           1               2
 */

CREATE TABLE emp_sex
(
    name    string,
    dept_id string,
    sex     string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

SELECT dept_id,
       sum(CASE sex WHEN '男' THEN 1 ELSE 0 END) male_count,
       sum(CASE sex WHEN '女' THEN 1 ELSE 0 END) female_count
FROM emp_sex
GROUP BY dept_id;
/*
+----------+-------------+---------------+--+
| dept_id  | male_count  | female_count  |
+----------+-------------+---------------+--+
| A        | 2           | 1             |
| B        | 1           | 2             |
+----------+-------------+---------------+--+
*/

-- 行转列
/*
 相关函数：
    CONCAT(string A/col, string B/col…)：返回输入字符串连接后的结果，支持任意个输入字符
    CONCAT_WS(separator, [string | array(string)]+)：返回由分隔符分隔的字符串的连接
    COLLECT_SET(col)：返回一组消除了重复元素的对象
 */
-- 把星座和血型一样的人归类到一起
/*
 行转列
 列:
    +-------------------+----------------------------+-------------------------+--+
    | person_info.name  | person_info.constellation  | person_info.blood_type  |
    +-------------------+----------------------------+-------------------------+--+
    | 孙悟空             | 白羊座                     | A                       |
    | 大海               | 射手座                     | A                       |
    | 宋宋               | 白羊座                     | B                       |
    | 猪八戒             | 白羊座                     | A                       |
    | 凤姐               | 射手座                     | A                       |
    +-------------------+----------------------------+-------------------------+--+
行:
    射手座,A 大海|凤姐
    白羊座,A 孙悟空|猪八戒
    白羊座,B 宋宋
 */

CREATE TABLE person_info
(
    name          string,
    constellation string,
    blood_type    string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";

-- 从结果"大海|凤姐" => 需要将多个字段用指定符号连接,函数concat_ws(separator, fields)
-- 从整体结果"射手座,A 大海|凤姐" => 还需将前半段"射手座"和后半段"大海|凤姐"拼接,函数 CONCAT(string A/col, string B/col…)

-- 1. 将星座和血型拼接
SELECT name, concat(constellation, ",", blood_type) base
FROM person_info;
/*
+-------+--------+--+
| name  |  base  |
+-------+--------+--+
| 孙悟空 | 白羊座,A  |
| 大海   | 射手座,A  |
| 宋宋   | 白羊座,B  |
| 猪八戒 | 白羊座,A  |
| 凤姐   | 射手座,A  |
+-------+--------+--+
*/

-- 2. 列转行
SELECT t1.base, concat_ws('|', collect_set(t1.name)) name
FROM (SELECT name, concat(constellation, ",", blood_type) base
      FROM person_info) t1
GROUP BY t1.base;
/*
+-----------+----------+-----+
| t1.base   |   name         |
+-----------+----------+-----+
| 射手座,A   | 大海|凤姐      |
| 白羊座,A   | 孙悟空|猪八戒   |
| 白羊座,B   | 宋宋           |
+----------+----------+------+
*/
-- collect_list/set再识
/*
+-------------------------+---------------------------+--------------------+--+
| t_visit_video.username  | t_visit_video.video_name  | t_visit_video.day  |
+-------------------------+---------------------------+--------------------+--+
| 张三                    | 大唐双龙传                 | 2019-07-10         |
| 李四                    | 天下无贼                   | 2019-07-10         |
| 张三                    | 神探狄仁杰                 | 2019-07-10         |
| 李四                    | 霸王别姬                   | 2019-07-10         |
| 李四                    | 霸王别姬                   | 2019-07-10         |
| 王五                    | 机器人总动员               | 2019-07-10         |
| 王五                    | 放牛班的春天               | 2019-07-10         |
| 王五                    | 盗梦空间                   | 2019-07-10         |
+-------------------------+---------------------------+--------------------+--+
*/
-- 列转行
SELECT collect_list(video_name) movie
FROM t_visit_video;
/*
+-------------------------------------------------------------------------------------------------+--+
|                              movie                                                              |
+-------------------------------------------------------------------------------------------------+--+
| ["大唐双龙传","天下无贼","神探狄仁杰","霸王别姬","霸王别姬","机器人总动员","放牛班的春天","盗梦空间"]  |
+-------------------------------------------------------------------------------------------------+--+
*/
-- collet_list
-- 按用户分组，取出每个用户每天看过的所有视频的名字：
SELECT username, collect_list(video_name) movie
FROM t_visit_video
GROUP BY username;
/*
+-----------+-----------------------------+--+
| username  |            movie            |
+-----------+-----------------------------+--+
| 张三      | ["大唐双龙传","神探狄仁杰"]           |
| 李四      | ["天下无贼","霸王别姬","霸王别姬"]      |
| 王五      | ["机器人总动员","放牛班的春天","盗梦空间"]  |
+-----------+-----------------------------+--+
*/
-- collect_set
SELECT username, collect_set(video_name) movie
FROM t_visit_video
GROUP BY username;
/*
+-----------+-----------------------------+--+
| username  |            movie            |
+-----------+-----------------------------+--+
| 张三      | ["大唐双龙传","神探狄仁杰"]           |
| 李四      | ["天下无贼","霸王别姬"]             |
| 王五      | ["机器人总动员","放牛班的春天","盗梦空间"]  |
+-----------+-----------------------------+--+
*/
-- 突破group by限制
/*
 Hive中在group by查询的时候要求出现在select后面的列都必须是出现在group by后面的，
 即select列必须是作为分组依据的列，但是有的时候我们想根据A进行分组然后随便取出每个分组中的一个B，
 */
SELECT username /*A*/, collect_list(video_name)[0] movie/*B*/
FROM t_visit_video
GROUP BY username;
/*
+-----------+---------+--+
| username  |   movie   |
+-----------+---------+--+
| 张三      | 大唐双龙传   |
| 李四      | 天下无贼    |
| 王五      | 机器人总动员  |
+-----------+---------+--+
*/


-- 行转列
/*EXPLODE(col):将hive一列中复杂的array或者map结构拆分成多行。
explode() takes in an array (or a map) as an input and outputs the elements of the array (map) as separate rows.
UDTFs can be used in the SELECT expression list and as a part of LATERAL VIEW.
 */
-- LATERAL VIEW ；lateral view与explode等udtf就是天生好搭档，explode将复杂结构一行拆成多行，然后再用lateral view做各种聚合。
-- 创建movie表
CREATE TABLE movie_info
(
    movie    string,
    category array<string>
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
--         row format delimited fields terminated by "\t"  ==> 指定列分隔符
        COLLECTION ITEMS TERMINATED BY ",";
--          collection items terminated by ","             ==> 指定map Stract 和 array 分隔符

SELECT *
FROM movie_info;
/*
+-------------------+------------------------------------+--+
| movie_info.movie  |     movie_info.category            |
+-------------------+------------------------------------+--+
| 《疑犯追踪》       | ["悬疑","动作","科幻","剧情"]       |
| 《Lie to me》     | ["悬疑","警匪","动作","心理","剧情"] |
| 《战狼2》          | ["战争","动作","灾难"]             |
+-------------------+-----------------------------------+--+
*/

-- 行转列，炸开explode
SELECT movie, category_name
FROM movie_info LATERAL VIEW explode(category) table_tmp AS category_name;
/*
+--------------+----------------+--+
|    movie     | category_name  |
+--------------+----------------+--+
| 《疑犯追踪》  | 悬疑             |
| 《疑犯追踪》  | 动作             |
| 《疑犯追踪》  | 科幻             |
| 《疑犯追踪》  | 剧情             |
| 《Lie to me》| 悬疑             |
| 《Lie to me》| 警匪             |
| 《Lie to me》| 动作             |
| 《Lie to me》| 心理             |
| 《Lie to me》| 剧情             |
| 《战狼2》     | 战争             |
| 《战狼2》     | 动作             |
| 《战狼2》     | 灾难             |
+--------------+----------------+--+
*/
SELECT category_name, concat_ws('|', collect_list(movie)) movie
FROM movie_info LATERAL VIEW explode(category) table_tmp AS category_name
GROUP BY category_name;
/*
+----------------+-------------------------------------+--+
| category_name  |            movie                    |
+----------------+-------------------------------------+--+
| 剧情           | 《疑犯追踪》|《Lie to me》            |
| 动作           | 《疑犯追踪》|《Lie to me》|《战狼2》   |
| 心理           | 《Lie to me》                        |
| 悬疑           | 《疑犯追踪》|《Lie to me》            |
| 战争           | 《战狼2》                            |
| 灾难           | 《战狼2》                            |
| 科幻           | 《疑犯追踪》                         |
| 警匪           | 《Lie to me》                       |
+----------------+-------------------------------------+--+
*/

-- 窗口函数（给聚合函数开窗）

-- 在order by和limit 之前执行
/*
 1. OVER(partition by cli_name)：和聚合函数使用，实现分组聚合
 2. CURRENT ROW：当前行
 3. n PRECEDING：往前n行数据
 4. n FOLLOWING：往后n行数据
 5. UNBOUNDED：起点，UNBOUNDED PRECEDING 表示从前面的起点，UNBOUNDED FOLLOWING表示到后面的终点
 6. LAG(col,n,default_val)：往前第n行数据
 7. LEAD(col,n, default_val)：往后第n行数据
 8. NTILE(n)：把有序分区中的行分发到指定数据的组中，各个组有编号，编号从1开始，对于每一行，NTILE返回此行所属的组的编号。注意：n必须为int类型
 */

/*
 需求
（1）查询在2017年4月份购买过的顾客及总人数
（2）查询顾客的购买明细及月购买总额
（3）上述的场景, 将每个顾客的cost按照日期进行累加
（4）查询每个顾客上次的购买时间
（5）查询前20%时间的订单信息
 */

-- 建表
CREATE TABLE business
(
    name      string COMMENT '姓名',
    orderdate string COMMENT '购买日期',
    cost      int COMMENT '花费金额'
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
/*
+----------------+---------------------+----------------+--+
| business.name  | business.orderdate  | business.cost  |
+----------------+---------------------+----------------+--+
| jack           | 2017-01-01          | 10             |
| tony           | 2017-01-02          | 15             |
| jack           | 2017-02-03          | 23             |
| tony           | 2017-01-04          | 29             |
| jack           | 2017-01-05          | 46             |
| jack           | 2017-04-06          | 42             |
| tony           | 2017-01-07          | 50             |
| jack           | 2017-01-08          | 55             |
| mart           | 2017-04-08          | 62             |
| mart           | 2017-04-09          | 68             |
| neil           | 2017-05-10          | 12             |
| mart           | 2017-04-11          | 75             |
| neil           | 2017-06-12          | 80             |
| mart           | 2017-04-13          | 94             |
+----------------+---------------------+----------------+--+
*/
-- （1）查询在2017年4月份购买过的顾客及总人数
SELECT name, count(*) OVER ()
FROM business
WHERE substring(orderdate, 1, 7) = '2017-04'
GROUP BY name;
/*
+-------+-----------------+--+
| name  | count_window_0  |
+-------+-----------------+--+
| mart  | 2               |
| jack  | 2               |
+-------+-----------------+--+
*/

SELECT date_format(orderdate, 'yyyy-MM') order_date, name, count(*) OVER () order_count
FROM business
WHERE date_format(orderdate, 'yyyy-MM') = '2017-04'
GROUP BY name, date_format(orderdate, 'yyyy-MM');
/*
+-------------+-------+--------------+--+
| order_date  | name  | order_count  |
+-------------+-------+--------------+--+
| 2017-04     | jack  | 2            |
| 2017-04     | mart  | 2            |
+-------------+-------+--------------+--+
*/
-- （2）查询顾客的购买明细及月购买总额
SELECT name, orderdate, cost, sum(cost) OVER (PARTITION BY month(orderdate))
FROM business;
/*
+-------+-------------+-------+---------------+--+
| name  |  orderdate  | cost  | sum_window_0  |
+-------+-------------+-------+---------------+--+
| jack  | 2017-01-01  | 10    | 205           |
| jack  | 2017-01-08  | 55    | 205           |
| tony  | 2017-01-07  | 50    | 205           |
| jack  | 2017-01-05  | 46    | 205           |
| tony  | 2017-01-04  | 29    | 205           |
| tony  | 2017-01-02  | 15    | 205           |
| jack  | 2017-02-03  | 23    | 23            |
| mart  | 2017-04-13  | 94    | 341           |
| jack  | 2017-04-06  | 42    | 341           |
| mart  | 2017-04-11  | 75    | 341           |
| mart  | 2017-04-09  | 68    | 341           |
| mart  | 2017-04-08  | 62    | 341           |
| neil  | 2017-05-10  | 12    | 12            |
| neil  | 2017-06-12  | 80    | 80            |
+-------+-------------+-------+---------------+--+
*/

-- 3）上述的场景, 将每个顾客的cost按照日期进行累加
/*
 over() 的使用
 */
SELECT name,
       orderdate,
       cost,
       sum(cost) OVER () AS sample1,--所有行相加
       sum(cost) OVER (PARTITION BY name) AS sample2,--按name分组，组内数据相加
       sum(cost) OVER (PARTITION BY name ORDER BY orderdate) AS sample3,--按name分组，组内数据累加
       sum(cost)
           OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS sample4,--和sample3一样,由起点到当前行的聚合
       sum(cost)
           OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sample5, --当前行和前面一行做聚合
       sum(cost) OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) AS sample6,--当前行和前边一行及后面一行
       sum(cost)
           OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS sample7 --当前行及后面所有行
FROM business;
-- sample1 所有行相加
SELECT name,
       orderdate,
       cost,
       sum(cost) OVER () AS sample1
FROM business;
/*
+-------+-------------+-------+----------+--+
| name  |  orderdate  | cost  | sample1  |
+-------+-------------+-------+----------+--+
| jack  | 2017-01-01  | 10    | 661      |
| tony  | 2017-01-02  | 15    | 661      |
| jack  | 2017-02-03  | 23    | 661      |
| tony  | 2017-01-04  | 29    | 661      |
| jack  | 2017-01-05  | 46    | 661      |
| jack  | 2017-04-06  | 42    | 661      |
| tony  | 2017-01-07  | 50    | 661      |
| jack  | 2017-01-08  | 55    | 661      |
| mart  | 2017-04-08  | 62    | 661      |
| mart  | 2017-04-09  | 68    | 661      |
| neil  | 2017-05-10  | 12    | 661      |
| mart  | 2017-04-11  | 75    | 661      |
| neil  | 2017-06-12  | 80    | 661      |
| mart  | 2017-04-13  | 94    | 661      |
+-------+-------------+-------+----------+--+
14 rows selected (16.314 seconds)
*/
-- sample2 按照name分组，组内数据相加
SELECT name,
       orderdate,
       cost,
       sum(cost) OVER () AS sample1,
       sum(cost) OVER (PARTITION BY name) AS sample2
FROM business;
/*
+-------+-------------+-------+----------+----------+--+
| name  |  orderdate  | cost  | sample1  | sample2  |
+-------+-------------+-------+----------+----------+--+
| jack  | 2017-01-01  | 10    | 661      | 176      |
| jack  | 2017-02-03  | 23    | 661      | 176      |
| jack  | 2017-01-05  | 46    | 661      | 176      |
| jack  | 2017-04-06  | 42    | 661      | 176      |
| jack  | 2017-01-08  | 55    | 661      | 176      |
| mart  | 2017-04-13  | 94    | 661      | 299      |
| mart  | 2017-04-08  | 62    | 661      | 299      |
| mart  | 2017-04-09  | 68    | 661      | 299      |
| mart  | 2017-04-11  | 75    | 661      | 299      |
| neil  | 2017-06-12  | 80    | 661      | 92       |
| neil  | 2017-05-10  | 12    | 661      | 92       |
| tony  | 2017-01-07  | 50    | 661      | 94       |
| tony  | 2017-01-02  | 15    | 661      | 94       |
| tony  | 2017-01-04  | 29    | 661      | 94       |
+-------+-------------+-------+----------+----------+--+
14 rows selected (8.504 seconds)
*/
-- sample3 按照name分组，按照日期升序组内数据累加
SELECT name,
       orderdate,
       cost,
       sum(cost) OVER () AS sample1,
       sum(cost) OVER (PARTITION BY name) AS sample2,
       sum(cost) OVER (PARTITION BY name ORDER BY orderdate) AS sample3
FROM business;
/*
+-------+-------------+-------+----------+----------+----------+--+
| name  |  orderdate  | cost  | sample1  | sample2  | sample3  |
+-------+-------------+-------+----------+----------+----------+--+
| jack  | 2017-01-01  | 10    | 661      | 176      | 10       |
| jack  | 2017-01-05  | 46    | 661      | 176      | 56       |
| jack  | 2017-01-08  | 55    | 661      | 176      | 111      |
| jack  | 2017-02-03  | 23    | 661      | 176      | 134      |
| jack  | 2017-04-06  | 42    | 661      | 176      | 176      |
| mart  | 2017-04-08  | 62    | 661      | 299      | 62       |
| mart  | 2017-04-09  | 68    | 661      | 299      | 130      |
| mart  | 2017-04-11  | 75    | 661      | 299      | 205      |
| mart  | 2017-04-13  | 94    | 661      | 299      | 299      |
| neil  | 2017-05-10  | 12    | 661      | 92       | 12       |
| neil  | 2017-06-12  | 80    | 661      | 92       | 92       |
| tony  | 2017-01-02  | 15    | 661      | 94       | 15       |
| tony  | 2017-01-04  | 29    | 661      | 94       | 44       |
| tony  | 2017-01-07  | 50    | 661      | 94       | 94       |
+-------+-------------+-------+----------+----------+----------+--+
14 rows selected (8.773 seconds)
*/
-- sample4 按照name分组，组内数据由起点处到当前行聚合逐行累加
SELECT name,
       orderdate,
       cost,
       sum(cost) OVER () AS sample1,
       sum(cost) OVER (PARTITION BY name) AS sample2,
       sum(cost) OVER (PARTITION BY name ORDER BY orderdate) AS sample3,
       sum(cost)
           OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sample4
FROM business;
/*
+-------+-------------+-------+----------+----------+----------+----------+--+
| name  |  orderdate  | cost  | sample1  | sample2  | sample3  | sample4  |
+-------+-------------+-------+----------+----------+----------+----------+--+
| jack  | 2017-01-01  | 10    | 661      | 176      | 10       | 10       |
| jack  | 2017-01-05  | 46    | 661      | 176      | 56       | 56       |
| jack  | 2017-01-08  | 55    | 661      | 176      | 111      | 111      |
| jack  | 2017-02-03  | 23    | 661      | 176      | 134      | 134      |
| jack  | 2017-04-06  | 42    | 661      | 176      | 176      | 176      |
| mart  | 2017-04-08  | 62    | 661      | 299      | 62       | 62       |
| mart  | 2017-04-09  | 68    | 661      | 299      | 130      | 130      |
| mart  | 2017-04-11  | 75    | 661      | 299      | 205      | 205      |
| mart  | 2017-04-13  | 94    | 661      | 299      | 299      | 299      |
| neil  | 2017-05-10  | 12    | 661      | 92       | 12       | 12       |
| neil  | 2017-06-12  | 80    | 661      | 92       | 92       | 92       |
| tony  | 2017-01-02  | 15    | 661      | 94       | 15       | 15       |
| tony  | 2017-01-04  | 29    | 661      | 94       | 44       | 44       |
| tony  | 2017-01-07  | 50    | 661      | 94       | 94       | 94       |
+-------+-------------+-------+----------+----------+----------+----------+--+
*/
-- sample5 当前行和当前行前面一行累加（两行累加）
SELECT name,
       orderdate,
       cost,
       sum(cost)
           OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sample4,
       sum(cost)
           OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sample5
FROM business;
/*
+-------+-------------+-------+----------+----------+--+
| name  |  orderdate  | cost  | sample4  | sample5  |
+-------+-------------+-------+----------+----------+--+
| jack  | 2017-01-01  | 10    | 10       | 10       |
| jack  | 2017-01-05  | 46    | 56       | 56       |
| jack  | 2017-01-08  | 55    | 111      | 101      |
| jack  | 2017-02-03  | 23    | 134      | 78       |
| jack  | 2017-04-06  | 42    | 176      | 65       |
| mart  | 2017-04-08  | 62    | 62       | 62       |
| mart  | 2017-04-09  | 68    | 130      | 130      |
| mart  | 2017-04-11  | 75    | 205      | 143      |
| mart  | 2017-04-13  | 94    | 299      | 169      |
| neil  | 2017-05-10  | 12    | 12       | 12       |
| neil  | 2017-06-12  | 80    | 92       | 92       |
| tony  | 2017-01-02  | 15    | 15       | 15       |
| tony  | 2017-01-04  | 29    | 44       | 44       |
| tony  | 2017-01-07  | 50    | 94       | 79       |
+-------+-------------+-------+----------+----------+--+
*/
-- sample6 当前行和前边一行及后边一行累加（三行累加）
SELECT name,
       orderdate,
       cost,
       sum(cost)
           OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sample5,
       sum(cost) OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) AS sample6
FROM business;
/*
+-------+-------------+-------+----------+----------+--+
| name  |  orderdate  | cost  | sample5  | sample6  |
+-------+-------------+-------+----------+----------+--+
| jack  | 2017-01-01  | 10    | 10       | 56       |
| jack  | 2017-01-05  | 46    | 56       | 111      |
| jack  | 2017-01-08  | 55    | 101      | 124      |
| jack  | 2017-02-03  | 23    | 78       | 120      |
| jack  | 2017-04-06  | 42    | 65       | 65       |
| mart  | 2017-04-08  | 62    | 62       | 130      |
| mart  | 2017-04-09  | 68    | 130      | 205      |
| mart  | 2017-04-11  | 75    | 143      | 237      |
| mart  | 2017-04-13  | 94    | 169      | 169      |
| neil  | 2017-05-10  | 12    | 12       | 92       |
| neil  | 2017-06-12  | 80    | 92       | 92       |
| tony  | 2017-01-02  | 15    | 15       | 44       |
| tony  | 2017-01-04  | 29    | 44       | 94       |
| tony  | 2017-01-07  | 50    | 79       | 79       |
+-------+-------------+-------+----------+----------+--+
*/
-- sample7 当前行及后面组内所有行累加
SELECT name,
       orderdate,
       cost,
       sum(cost)
           OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sample5,
       sum(cost) OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) AS sample6,
       sum(cost)
           OVER (PARTITION BY name ORDER BY orderdate ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS sample7
FROM business;
/*
+-------+-------------+-------+----------+----------+----------+--+
| name  |  orderdate  | cost  | sample5  | sample6  | sample7  |
+-------+-------------+-------+----------+----------+----------+--+
| jack  | 2017-01-01  | 10    | 10       | 56       | 176      |
| jack  | 2017-01-05  | 46    | 56       | 111      | 166      |
| jack  | 2017-01-08  | 55    | 101      | 124      | 120      |
| jack  | 2017-02-03  | 23    | 78       | 120      | 65       |
| jack  | 2017-04-06  | 42    | 65       | 65       | 42       |
| mart  | 2017-04-08  | 62    | 62       | 130      | 299      |
| mart  | 2017-04-09  | 68    | 130      | 205      | 237      |
| mart  | 2017-04-11  | 75    | 143      | 237      | 169      |
| mart  | 2017-04-13  | 94    | 169      | 169      | 94       |
| neil  | 2017-05-10  | 12    | 12       | 92       | 92       |
| neil  | 2017-06-12  | 80    | 92       | 92       | 80       |
| tony  | 2017-01-02  | 15    | 15       | 44       | 94       |
| tony  | 2017-01-04  | 29    | 44       | 94       | 79       |
| tony  | 2017-01-07  | 50    | 79       | 79       | 50       |
+-------+-------------+-------+----------+----------+----------+--+
*/

-- （4）查询每个顾客上次的购买时间
/*
 log(col, num,default)往前num行的数据，不存在时默认为default，不设置默认值，不存在时用null补齐
 */
SELECT name,
       orderdate,
       cost,
       lag(orderdate, 1, '未购买过') OVER (PARTITION BY name ORDER BY orderdate ) AS time1,
       lag(orderdate, 2) OVER (PARTITION BY name ORDER BY orderdate) AS time2
FROM business;
/*
+-------+-------------+-------+-------------+-------------+--+
| name  |  orderdate  | cost  |    time1    |    time2    |
+-------+-------------+-------+-------------+-------------+--+
| jack  | 2017-01-01  | 10    | 1900-01-01  | NULL        |
| jack  | 2017-01-05  | 46    | 2017-01-01  | NULL        |
| jack  | 2017-01-08  | 55    | 2017-01-05  | 2017-01-01  |
| jack  | 2017-02-03  | 23    | 2017-01-08  | 2017-01-05  |
| jack  | 2017-04-06  | 42    | 2017-02-03  | 2017-01-08  |
| mart  | 2017-04-08  | 62    | 1900-01-01  | NULL        |
| mart  | 2017-04-09  | 68    | 2017-04-08  | NULL        |
| mart  | 2017-04-11  | 75    | 2017-04-09  | 2017-04-08  |
| mart  | 2017-04-13  | 94    | 2017-04-11  | 2017-04-09  |
| neil  | 2017-05-10  | 12    | 1900-01-01  | NULL        |
| neil  | 2017-06-12  | 80    | 2017-05-10  | NULL        |
| tony  | 2017-01-02  | 15    | 1900-01-01  | NULL        |
| tony  | 2017-01-04  | 29    | 2017-01-02  | NULL        |
| tony  | 2017-01-07  | 50    | 2017-01-04  | 2017-01-02  |
+-------+-------------+-------+-------------+-------------+--+
14 rows selected (17.271 seconds)
*/

-- （5）查询前20%时间的订单信息
/*
 NTILE(n)：把有序分区中的行分发到指定数据的组中，各个组有编号，编号从1开始，对于每一行，NTILE返回此行所属的组的编号。注意：n必须为int类型
 ntile(n)和where sorter = m 构成 n/m，如：ntile（2）和where sorted = 1 构成显示所有列的1/2
 20% = 1/5 ==> ntile(5),where sorted = 1
 */
SELECT *
FROM (
         SELECT name, orderdate, cost, ntile(5) OVER (ORDER BY orderdate) sorted
         FROM business
     ) t
WHERE sorted = 1;
/*
+---------+--------------+---------+-----------+--+
| t.name  | t.orderdate  | t.cost  | t.sorted  |
+---------+--------------+---------+-----------+--+
| jack    | 2017-01-01   | 10      | 1         |
| tony    | 2017-01-02   | 15      | 1         |
| tony    | 2017-01-04   | 29      | 1         |
+---------+--------------+---------+-----------+--+
3 rows selected (8.716 seconds)
*/

-- rank函数
/*
 函数说明：
 RANK() 排序相同时会重复，总数不会变
 DENSE_RANK() 排序相同时会重复，总数会减少
 ROW_NUMBER() 会根据顺序计算
 */
-- 建表，加载数据
CREATE TABLE score
(
    name    string COMMENT '姓名',
    subject string COMMENT '学科',
    score   int COMMENT '分数'
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";
LOAD DATA LOCAL INPATH '/data/hive/score.txt' INTO TABLE score;
-- 需求：计算每门学科成绩排名。
-- rank（） 排序相同时会重复，总数不变，按照学科分组，组内按照分数降序排列
SELECT name,
       subject,
       score,
       rank() OVER (PARTITION BY subject ORDER BY score DESC) rp
FROM score;
/*
+-------+----------+--------+-----+--+
| name  | subject  | score  | rp  |
+-------+----------+--------+-----+--+
| 孙悟空  | 数学    | 95     | 1   |
| 宋宋    | 数学    | 86     | 2   |
| 婷婷    | 数学    | 85     | 3   |
| 大海    | 数学    | 56     | 4   |
| 大海    | 英语    | 84     | 1   |
| 宋宋    | 英语    | 84     | 1   |
| 婷婷    | 英语    | 78     | 3   |
| 孙悟空  | 英语    | 68     | 4   |
| 大海    | 语文    | 94     | 1   |
| 孙悟空  | 语文    | 87     | 2   |
| 婷婷    | 语文    | 65     | 3   |
| 宋宋    | 语文    | 64     | 4   |
+-------+----------+--------+-----+--+
12 rows selected (15.748 seconds)
*/

-- DENSE_RANK() 排序相同时会重复，总数会减少
SELECT name,
       subject,
       score,
       rank() OVER (PARTITION BY subject ORDER BY score DESC) rp,
       dense_rank() OVER (PARTITION BY subject ORDER BY score DESC) drp
FROM score;
/*
+-------+----------+--------+-----+------+--+
| name  | subject  | score  | rp  | drp  |
+-------+----------+--------+-----+------+--+
| 孙悟空  | 数学    | 95     | 1   | 1    |
| 宋宋    | 数学    | 86     | 2   | 2    |
| 婷婷    | 数学    | 85     | 3   | 3    |
| 大海    | 数学    | 56     | 4   | 4    |
| 大海    | 英语    | 84     | 1   | 1    |
| 宋宋    | 英语    | 84     | 1   | 1    |
| 婷婷    | 英语    | 78     | 3   | 2    |
| 孙悟空  | 英语    | 68     | 4   | 3    |
| 大海    | 语文    | 94     | 1   | 1    |
| 孙悟空  | 语文    | 87     | 2   | 2    |
| 婷婷    | 语文    | 65     | 3   | 3    |
| 宋宋    | 语文    | 64     | 4   | 4    |
+-------+----------+--------+-----+------+--+
*/
-- ROW_NUMBER() 会根据顺序计算
SELECT name,
       subject,
       score,
       rank() OVER (PARTITION BY subject ORDER BY score DESC) rp,
       dense_rank() OVER (PARTITION BY subject ORDER BY score DESC) drp,
       row_number() OVER (PARTITION BY subject ORDER BY score DESC) rmp
FROM score;
/*
+-------+----------+--------+-----+------+------+--+
| name  | subject  | score  | rp  | drp  | rmp  |
+-------+----------+--------+-----+------+------+--+
| 孙悟空  | 数学    | 95     | 1   | 1    | 1    |
| 宋宋    | 数学    | 86     | 2   | 2    | 2    |
| 婷婷    | 数学    | 85     | 3   | 3    | 3    |
| 大海    | 数学    | 56     | 4   | 4    | 4    |
+---------+--------+--------+-----+------+------+--
| 大海    | 英语    | 84     | 1   | 1    | 1    |
| 宋宋    | 英语    | 84     | 1   | 1    | 2    |
| 婷婷    | 英语    | 78     | 3   | 2    | 3    |
| 孙悟空  | 英语    | 68     | 4   | 3    | 4    |
+--------+---------+--------+-----+------+------+--
| 大海    | 语文    | 94     | 1   | 1    | 1    |
| 孙悟空  | 语文    | 87     | 2   | 2    | 2    |
| 婷婷    | 语文    | 65     | 3   | 3    | 3    |
| 宋宋    | 语文    | 64     | 4   | 4    | 4    |
+-------+----------+--------+-----+------+------+--+
*/
```



