#Katta

一个分布式搜索的解决方案

---

##ZooKeeper

    tar -zxvf zookeeper-xx.tar.gz
    cp conf/zoo.example.cfg conf/zoo.cfg
    vim zoo.cfg

加入（修改）如下：

    # The number of milliseconds of each tick
    tickTime=2000
    # The number of ticks that the initial 
    # synchronization phase can take
    initLimit=10
    # The number of ticks that can pass between 
    # sending a request and getting an acknowledgement
    syncLimit=5
    # the directory where the snapshot is stored.
    # do not use /tmp for storage, /tmp here is just 
    # example sakes.
    dataDir=/media/Study/data/zookeeper/data

    dataLogDir=/media/Study/data/zookeeper/logs
    # the port at which the clients will connect
    clientPort=2181
    #
    # Be sure to read the maintenance section of the 
    # administrator guide before turning on autopurge.
    #
    # http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_maintenance
    #
    # The number of snapshots to retain in dataDir
    #autopurge.snapRetainCount=3
    # Purge task interval in hours
    # Set to "0" to disable auto purge feature
    #autopurge.purgeInterval=1

启动ZooKeeper

    bin/zkServer.sh start
    
##Katta

    tar -zxvf katta-**.tgz
    vim conf/katta.zk.properties
    

修改其中配置信息如下：

    zookeeper.embedded=false
    zookeeper.servers=zhenqin-k45vm:2181

配置SSH无密码访问：

    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    ssh-agent bash
    ssh localhost

启动Katta

    bin/start-all.sh
    
验证Katta：

    bin/katta listNodes
    bin/katta listIndices
    
增加索引分片

    #注意，luceneIndex下必须有目录，并且目录下存放着Lucene的索引
    bin/katta addIndex "testIndex" file:///data/luceneIndex
    
测试搜索

    #*代表搜索搜有分片。搜索指定分片可以输入分片名称
    bin/katta search "*" "title:hello, world"
    

=======
katta
===============

Katta, 分布式搜索
>>>>>>> c95703c75f8e272926fa595aa1f7abbc38e1323a
