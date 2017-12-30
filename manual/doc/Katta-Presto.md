## Katta-Hive 建表

> 目前 Katta 已经能对Presto直接支持。毕竟使用 SQL 是大部分人都可以使用的方式，Presto 提供了标准的 JDBC 接口，有了 Presto-Katta，那部分可以使用 SQL 的分析将完全无须开发，或者少量开发。有部分的业务数据导出部分数据导出将会大大减少开发量。Katta-Presto 支持索引下推，但是目前 katta-presto 模块关于索引下推的部分目前正在完善中，支持简单的索引。

安装： 下载 [katta-presto](https://pan.baidu.com/s/1o8ooBGm) katta-presto-0.191.tar.gz, 解压后放到 presto 的 plugins 下（注意解压出来的目录名为 katta），copy 解压 katta 目录下的 katta.properties 的目录到 presto 的 presto_home/etc/catalog/ 下。


启动 presto(bin/launcher) 后：

    bin/presto --server host:8088 --catalog katta --schama default

通过 `show tables; ` 检查是否能获取到 Katta 中的索引表。

--schama 是 Presto 中指数据库的概念，Katta 中没有数据库(DB)的概念,只有索引库，并且没用 namespace。Katta-presto 因此内置了 `default`, `katta` 两个默认的数据库名称，因此这里 default 是必须的。 

Hive-Katta 创建外部表 DDL：

```
presto:katta> show tables;
   Table   
-----------
 userindex 
(1 row)

Query 20171227_010932_00002_q84vs, FINISHED, 1 node
Splits: 18 total, 18 done (100.00%)
0:00 [1 rows, 24B] [5 rows/s, 128B/s]

presto:katta> select count(*) as c from userindex;
   c    
--------
 881722 
(1 row)

Query 20171227_010954_00003_q84vs, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:22 [882K rows, 91.7MB] [40.3K rows/s, 4.19MB/s]
```


