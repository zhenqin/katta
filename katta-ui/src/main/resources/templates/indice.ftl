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
            ${message}

        </div>
    </body>
</html>
