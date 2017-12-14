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
        <div class="body">
            <@netCommon.commonHeader />
            <!-- Master -->
            <h1>Master List</h1>
          
            <div class="list">
                <#if master??>
                <table class="table table-bordered" style="width: 60%;">
                    <thead>
                        <tr>
                   	        <th class="sortable">Name</th>
                            <th class="sortable">ProxyPort</th>
                   	        <th class="sortable">Start Time</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr class="odd">
                            <td>${master.masterName}</td>
                            <td>${master.proxyBlckPort}</td>
                            <td>${master.startTime?string('yyyy-MM-dd HH:mm:ss')}</td>
                        </tr>
                    </tbody>
                </table>
                <#else>
                <div class="alert alert-danger alert-dismissible" style="width: 50%;" role="alert">
                    <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                        <span aria-hidden="true">&times;</span></button>
                    <strong>警告：</strong> 当前没有可用的 Katta Master。
                </div>
                </#if>
            </div>
            
            <!-- nodes -->
            <h1>Nodes List</h1>
            <div class="list">
                <#if nodes??>
                <table class="table table-bordered" style="width: 80%;">
                    <thead>
                        <tr>
                             <th class="sortable" >Name</th>
                             <th class="sortable" >State</th>
                             <th class="sortable" >Started</th>
                             <th class="sortable" >Assigned Shards</th>
                        </tr>
                    </thead>

                    <tbody>
                        <#list nodes as node> 
                        <tr class="even">
                            <td><a href="${request.contextPath}/node?name=${node.name}">${node.name}</a></td>
                            <td>OK</td>
                            <td>${node.startTime?string('yyyy-MM-dd HH:mm:ss')}</td>
                            <td>${node.queriesPerMinute}</td>
                             
                        </tr>
                        </#list>
                    </tbody>
                    
                </table>

                <#else>
                <div class="alert alert-danger alert-dismissible" style="width: 50%;" role="alert">
                    <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                        <span aria-hidden="true">&times;</span></button>
                    <strong>警告：</strong> 当前没有可用的 Katta Node。
                </div>
            </#if>
            </div>

            <!-- indexes -->
            <h1>Index List</h1>
            <div class="list">
                <table class="table table-bordered" style="width: 80%;">
                    <thead>
                        <tr>
                             <th class="sortable" >Name</th>
                             <th class="sortable" >Path</th>
                             <th class="sortable" >CoreName</th>
                             <th class="sortable" >State</th>
                             <th class="sortable" >Replication Level</th>
                        </tr>
                    </thead>
                    <tbody>
                    <#list indexes as index>
                        <tr class="odd">
                            <td><a href="${request.contextPath}/indexx?name=${index.name}">${index.name}</a></td>
                            <td>${index.path}</td>
                            <td>${index.collectionName}</td>
                            <td>${index.deployError! 'ERROR'}</td>
                            <td>${index.replicationLevel}</td>
                        </tr>
                    </#list>
                    </tbody>
                </table>
            </div>
      
        </div>
    </body>
</html>
