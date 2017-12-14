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
                <table>
                    <thead>
                        <tr>
                   	        <th class="sortable">Name</th>
                   	        <th class="sortable">Start Time</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr class="odd">
                            <td>${master.masterName}</td>
                            <td>${master.startTimeAsString?string('yyyy-MM-dd HH:mm:ss')}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
            
            <!-- nodes -->
            <h1>Nodes List</h1>
            <div class="list">
                <table style="width: 80%;">
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
                            <td>${node.startTimeStamp}</td>
                            <td>100</td>
                             
                        </tr>
                        </#list>
                    </tbody>
                    
                </table>
            </div>

            <!-- indexes -->
            <h1>Index List</h1>
            <div class="list">
                <table style="width: 80%;">
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
                            <td>${index.name}</td>
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
