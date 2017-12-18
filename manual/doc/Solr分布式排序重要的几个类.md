## Solr 分布式排序重要的几个类：

1. SearchHeadler.handleRequestBody[279]
2. SearchComponent
3. QueryComponent.mergeIds[774]          重要
4. ShardDoc(ShardFieldSortedHitQueue)    涉及具体排序算法
5. ResponseBuilder
6. HttpShardHandlerFactory``
7. ShardHandler & HttpShardHandler

