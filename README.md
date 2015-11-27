#Katta

---

##Katta 是什么?

Katta 是一个分布式搜索引擎解决方案. 他和 Solr/ElasticSearch 工作模式不同, 它不进行创建索引, 只管理索引. 

---

##部署

###ZooKeeper

    tar -zxvf zookeeper-xx.tar.gz
    cp conf/zoo.example.cfg  conf/zoo.cfg
    vim zoo.cfg

加入（修改）如下：

    tickTime=2000
    initLimit=10
    syncLimit=5
    dataDir=/media/Study/data/zookeeper/data
    dataLogDir=/media/Study/data/zookeeper/logs
    clientPort=2181

注意 `dataDir` 和  `dataLogDir` 一定要是你机器本地能访问的路径. 启动ZooKeeper:

    bin/zkServer.sh start
    
###Katta

    tar -zxvf katta-**.tgz
    vim conf/katta.zk.properties

修改其中配置信息如下：

    zookeeper.embedded=false
    zookeeper.servers=ip:2181

配置SSH无密码访问, 这和 hadoop 的无密码访问类似：

    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    ssh-agent bash
    ssh localhost

启动Katta

    bin/start-all.sh
    
验证Katta：

    bin/katta listNodes
    正常情况下输出如:
    ----------------------------------------------------------------------
    | Name (1/1 connected) | Start time                      | State     |
    ======================================================================
    | zhenqin-pro102:20000 | 星期五, 27 十一月 2015 16:08:37 +0800 | CONNECTED |
    ----------------------------------------------------------------------
    
也可以把 Master 和 Node 分别在控制台以非守护进程启动(Windows 系统,无法打通 SSH 的, 可用该种方式启动):
    
    bin/katta master
    bin/katta node
        
增加索引分片

    #注意，luceneIndex下必须有目录，并且目录下 分别存放着Lucene的索引
    bin/katta addIndex testIndex  solrcollection  file:///data/luceneIndex
    bin/katta addIndex testIndex  solrcollection  hdfs:///data/luceneIndex
    
- testIndex: 索引名称
- solrcollection: <katta_home>/data/solr 下应该有该Solr 的配置文件. 搜索时, Query 方言用 Solr 解析.
- file:///data/luceneIndex: 本地模式, 一定要加 file://; HDFS 要加前缀 hdfs://
- 还可以有第4个参数, 该参数控制Katta 集群内该所有有几个复制(默认3个)


验证分片:

    bin/katta listIndices
    如正常安装索引如:
    -------------------------------------------------------------------------------------------------------
    | Name       | Status   | Replication State | Path               | Shards | Entries | Disk Usage |
    ====================================================================================
    | test       | DEPLOYED | UNDERREPLICATED | hdfs:///user/zhenqin/luce200 | 20  | 329033  | 337050460|
    ------------------------------------------------------------------------------------------------------
    | appinstall | DEPLOYED | UNDERREPLICATED| hdfs:///user/katta/android-app| 6 | 1373980 | 1090770344|
    -----------------------------------------------------------------------------------------------------

    
测试搜索

    #*代表搜索搜有分片。搜索指定分片可以输入分片名称
    bin/katta search '*' '*:*'

---
