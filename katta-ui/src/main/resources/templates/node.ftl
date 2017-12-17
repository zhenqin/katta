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
                    <h3 class="panel-title">Node ${name} 的基本信息</h3>
                </div>
                <div class="panel-body">
                    <h1>Payload Shard List</h1>
                    <div class="list">
                        <table class="table table-bordered">
                            <thead>
                            <tr>
                                <th class="sortable" width="30%">shard</th>
                                <th class="sortable">nodes</th>
                            </tr>
                            </thead>
                            <tbody>
                            <#list shard2Node?keys as key>
                            <tr>
                                <td>${key}</td>
                                <td>
                                    <#list shard2Node["${key}"] as node>
                                        <span class="label label-info">${node}</span>
                                    </#list>
                                </td>
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
