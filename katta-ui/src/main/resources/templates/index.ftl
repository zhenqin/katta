<!DOCTYPE html>
<html>
<head>
  	<#import "common.macro.ftl" as netCommon>
	<@netCommon.commonStyle />
</head>
<body>
    <div class="body">
        <@netCommon.commonHeader />
        <h1 style="margin-left:20px;">Welcome to Katta</h1>
        <p style="margin-left:20px;width:80%">
        	Below an overview of all registered Clusters.<br/>
        	You can <g:link controller="cluster" action="create">add a cluster</g:link> or simply click on one of the clusters to get to the Cluster dashboard.
        </p>

		<br/>
        <h2>Clusters</h2>
         <div class="list">
                <table>
                    <thead>
                        <tr>
                        <!-- TODO Frank, I dont want to use the tag g:sortableColumn, though I'm not sure what else to take so I copy pasted what the tag renders, however there might be a better way.. -->
                   	        <th class="sortable" >Name</th>
                   	        <th class="sortable" >Master uri</th>
                   	        <th class="sortable" ># Nodes</th>
                   	        <th class="sortable" ># Indexes</th>
                   	        <th class="sortable" >Action</th>
                        </tr>
                    </thead>
                    <tbody>

                        <tr class="old">
                           <td>${cluster['name']}</td>
                           <td>${cluster['uri']}</td>
                           <td>${cluster['nodeCount']}</td>
                           <td>${cluster['indexCount']}</td>
                           <td>
                             <form controller="cluster" >
			                    <input type="hidden" name="id" value="${cluster['id']}" />
			                    <span class="button"><actionSubmit class="edit" value="Edit" /></span>
			                    <span class="button"><actionSubmit class="delete" onclick="return confirm('Are you sure?');" value="Delete" /></span>
			                </form>
                           </td>
                        </tr>
                    </tbody>
                </table>
            </div>

            <br/><br/><br/>
    </div>
<@netCommon.commonScript />
</body>
</html>