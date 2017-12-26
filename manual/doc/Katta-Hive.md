## Katta-Hive 建表

> 目前已经提供了 Katta-Hive 的支持，鉴于 HiveSQL 执行的 HQL 并非实时的，因此不能用于实时检索的场景。但是，部分报表等可以使用 HQL 的方式直接提供。

Hive-Katta 创建外部表 DDL：

```
CREATE EXTERNAL TABLE katta_userindex (
    USER_URN STRING,
    USER_ID STRING,
    CNT_FOLLOWINGS STRING,
    USER_FOLLOWINGS STRING)
STORED BY 'com.ivyft.katta.hive.KattaStorageHandler'
WITH SERDEPROPERTIES('zookeeper.servers' = 'localhost:2181', 'katta.hadoop.indexes' = 'userindex', 'katta.input.query' = 'USER_FOLLOWINGS:0', 'katta.input.key'='USER_URN', 'katta.input.limit' = '5000')
TBLPROPERTIES ('zookeeper.servers' = 'localhost:2181', 'katta.hadoop.indexes' = 'userindex', 'katta.input.query' = 'USER_FOLLOWINGS:0', 'katta.input.key'='USER_URN', 'katta.input.limit' = '5000');
```


一般的 Count SQL(TEZ 引擎):

```
hive> select count(*) as c from katta_userindex;
Query ID = zhenqin_20171225160132_4678a5bb-d2e9-4d30-97d3-b3aee2223ab5
Total jobs = 1
Launching Job 1 out of 1

Status: Running (Executing on YARN cluster with App id application_1514187820800_0003)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 1.43 s
--------------------------------------------------------------------------------
OK
762
Time taken: 1.029 seconds, Fetched: 1 row(s)
```

该表总共有 88W 的数据量，根据查询结果2s 的结果可以看到，该次查询使用了索引，2s 是因为 Hive 总是取得数据内部自行统计比较耗时。其实如果使用 katta 的原生 api 想获得一个 Query 的 count 可以做到100ms 内。如：

```
$ bin/katta search -i userindex -q '*:*'
12-25 16:07:58 [INFO] [com.ivyft.katta.client.Client(220)] indices=[userindex]
12-25 16:07:58 [INFO] [com.ivyft.katta.client.NodeProxyManager(178)] creating proxy for node: zhenqin-pro102:20000
12-25 16:07:59 [INFO] [node-interaction(161)] exec zhenqin-pro102:20000 shards [userindex#2PD95Ggl2tWnSynu8gX, userindex#OV92iJRxjPio5PX18JR] method count, cost time: 71 ms
881722 Hits found in 0.081sec.
```

也支持 Hive 使用 MR：

```
hive> set hive.execution.engine=mr;
hive> select count(*) as c from katta_userindex;
Query ID = zhenqin_20171226112241_9eea0e6e-4e78-47ff-9897-bfad62f79153
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1514252551138_0012, Tracking URL = http://localhost:8088/proxy/application_1514252551138_0012/
Kill Command = /Users/zhenqin/software/hadoop/bin/hadoop job  -kill job_1514252551138_0012
Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 1
2017-12-26 11:22:48,808 Stage-1 map = 0%,  reduce = 0%
2017-12-26 11:22:57,092 Stage-1 map = 50%,  reduce = 0%
2017-12-26 11:22:58,121 Stage-1 map = 100%,  reduce = 0%
2017-12-26 11:23:02,289 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_1514252551138_0012
MapReduce Jobs Launched:
Stage-Stage-1: Map: 2  Reduce: 1   HDFS Read: 1604 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
762
Time taken: 21.521 seconds, Fetched: 1 row(s)
```
