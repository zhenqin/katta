<!DOCTYPE html>
<html>
<head>
    <#import "common.macro.ftl" as netCommon>
  <@netCommon.commonStyle />
</head>
<body>
<@netCommon.commonScript />
<script type="application/javascript">
    window.location = "${request.contextPath}/overview";
</script>
</body>
</html>