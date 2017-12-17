<!DOCTYPE html>
<html>
    <head>
        <#import "common.macro.ftl" as netCommon>
	    <@netCommon.commonStyle />

        <@netCommon.commonScript />
		<script type="text/javascript">

        $(document).ready(function() {
     
        });
        </script>
    </head>
    <body>
        <@netCommon.commonHeader />
        <div class="container">
            <div class="panel panel-default">
                <div class="panel-heading">
                    <h3 class="panel-title">Index ${index.name} 的基本信息</h3>
                </div>
                <div class="panel-body">

                    <label>IndexName: <a href="${request.contextPath}/indexx?name=${index.name}">${index.name}</a></label>
                    <br />
                    <label>IndexPath: ${index.path}</label>
                    <br />
                    <label>Index Core: ${index.collectionName}</label>
                    <br />
                    <label>
                        DeployInfo:
                    <#if deployError??>
                    <span class="label label-danger">ERROR</span>
                    <#else>
                    <span class="label label-success">SUCCESS</span>
                    </#if>
                    </label>
                    <br />
                    <label>ShardSize: ${index.shardSize}</label>
                    <br />
                    <label>ReplicaInfo: ${index.replicationLevel}</label>
                    <br />

                    <h1>Shard List</h1>
                    <div class="list">
                        <table class="table table-bordered" style="width: 80%;">
                            <thead>
                            <tr>
                                <th class="sortable" >Name</th>
                                <th class="sortable" >Path</th>
                            </tr>
                            </thead>
                            <tbody>
                            <#list index.shards as shard>
                            <tr>
                                <td>${shard.name}</td>
                                <td>${shard.path}</td>
                            </tr>
                            </#list>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </body>
</html>
