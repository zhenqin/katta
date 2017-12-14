<!DOCTYPE HTML>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>文件上传案例</title>
</head>
<body>
<div class="page-header"></div>
<div class="container main wrapper">
    <center>
        <h3>文件上传</h3>
        <form enctype="multipart/form-data" method="post" action="/upload">
            DataType：<input type="text" name="dataType" /><br/>
            ProjectId：<input type="text" name="projectId" /><br/>
            选择文件：<input type="file" name="file1" /><br/>
            <!--
            选择文件：<input type="file" name="file2" /><br/>
            选择文件：<input type="file" name="file3" /><br/>
            选择文件：<input type="file" name="file4" /><br/>
            选择文件：<input type="file" name="file5" /><br/>
            -->
            <input type="submit" value="上传" />
        </form>
    </center>
</div><!-- #main -->
</body>
</html>