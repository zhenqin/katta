<#macro commonStyle>
	<meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <!-- Tell the browser to be responsive to screen width -->
    <meta content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no" name="viewport">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="ZhenQin, zhzhenqin">

    <title>${title!'Katta UI'}</title>
    <link rel="shortcut icon" href="${request.contextPath}/static/images/favicon.ico" type="image/x-icon" />
    <link rel="stylesheet" href="${request.contextPath}/static/css/style.css" type="text/css" media="screen"/>
    <link rel="stylesheet" href="${request.contextPath}/static/css/main.css" />

    <script type="application/javascript">
        var base_url = '${request.contextPath}';
    </script>
</#macro>

<#macro commonScript>
	<!-- jQuery 2.1.4 -->
	<script src="${request.contextPath}/static/js/jquery.min.js"></script>
    <script src="${request.contextPath}/static/js/jquery.sparkline.min.js"></script>

	<script type="application/javascript" src="${request.contextPath}/static/js/application.js"></script>

</#macro>

<#macro commonHeader>
	<div class="nav">
        <span class="menuButton"><a class="home" href="${request.contextPath}/">Home</a></span>
        <span class="menuButton"><a class="create" href="${request.contextPath}/dashboard">Dashboard</a></span>
    </div>
    <#if flash??>
                <div class="message">${flash.message}</div>
    </#if>
</#macro>

<#macro commonLeft pageName >
	<!-- Left side column. contains the logo and sidebar -->
	<aside class="main-sidebar" id="serviceMenu">
		<!-- sidebar: style can be found in sidebar.less -->
		<section class="sidebar">
			<!-- sidebar menu: : style can be found in sidebar.less -->
			<ul class="sidebar-menu" data-widget="tree">
                <li>
                    <a href="${request.contextPath}/dashboard">
                        <i class="fa fa-dashboard"></i> <span>Dashboard</span>
                        <span class="pull-right-container">
                            <i class="label label-primary pull-right">{{serviceCount}}</i>
                        </span>
                    </a>
                </li>

                <li class="header">常用云服务</li>

                <li class="treeview">
                    <a href="#">
                        <i class="fa fa-globe text-light-blue"></i>
                        <span>弹性JavaWeb Server</span>
                        <span class="pull-right-container">
                            <i class="fa fa-angle-left pull-right"></i>
                        </span>
                    </a>

                    <ul class="treeview-menu">
                        <li class="active"><a href="${request.contextPath}/tomcat"><i class="fa fa-circle-o"></i>弹性 Tomcat</a></li>
                        <li><a href="${request.contextPath}/jetty"><i class="fa fa-circle-o"></i>弹性 Jetty</a></li>
                        <li><a href="${request.contextPath}/springboot"><i class="fa fa-circle-o"></i>弹性 SpringBoot</a></li>
                        <li><a href="${request.contextPath}/springcloud"><i class="fa fa-circle-o"></i> Spring Cloud</a></li>
                    </ul>
                </li>

                <li>
                    <a href="${request.contextPath}/redis">
                        <i class="fa fa-files-o text-red"></i>
                        <span>云服务 Redis</span>
                        <span class="pull-right-container">
                            <span class="label label-primary pull-right">{{redisCount}}</span>
                        </span>
                    </a>
                </li>
                <li>
                    <a href="${request.contextPath}/mysql">
                        <i class="fa fa-database text-light-blue"></i> <span>云服务 MySQL</span>
                        <span class="pull-right-container">
                            <small class="label pull-right bg-green">{{mysqlCount}}</small>
                        </span>
                    </a>
                </li>
                <li>
                    <a href="${request.contextPath}/mongodb">
                        <i class="fa fa-pie-chart text-green"></i>
                        <span>云服务 MongoDB</span>
                        <span class="pull-right-container">
                          <i class="label label-primary pull-right">new</i>
                        </span>
                    </a>
                </li>
                <li>
                    <a href="${request.contextPath}/hadoop">
                        <i class="fa fa-laptop text-teal"></i>
                        <span>云服务 Hadoop</span>
                        <span class="pull-right-container">
                          <i class="label label-primary pull-right">new</i>
                        </span>
                    </a>
                </li>

                <li>
                    <a href="${request.contextPath}/spark">
                        <i class="fa fa-star-o text-yellow"></i>
                        <span>云服务 Spark</span>
                        <span class="pull-right-container">
                          <i class="label label-primary pull-right">new</i>
                        </span>
                    </a>
                </li>

                <li>
                    <a href="${request.contextPath}/flume">
                        <i class="fa fa-th-list text-olive"></i>
                        <span>云服务 Flume</span>
                        <span class="pull-right-container">
                          <i class="label label-primary pull-right">new</i>
                        </span>
                    </a>
                </li>

                <li class="treeview">
                    <a href="#">
                        <i class="fa fa-gears text-purple"></i>
                        <span>自定义服务</span>
                        <span class="pull-right-container">
                            <i class="fa fa-angle-left pull-right"></i>
                        </span>
                    </a>

                    <ul class="treeview-menu">
                        <li class="active"><a href="${request.contextPath}/tomcat"><i class="fa fa-circle-o text-red"></i>MySQL + Tomcat</a></li>
                        <li><a href="${request.contextPath}/jetty"><i class="fa fa-circle-o text-yellow"></i>MySQL + Jetty</a></li>
                        <li><a href="${request.contextPath}/springboot"><i class="fa fa-circle-o text-aqua"></i>MySQL + SpringBoot</a></li>
                    </ul>
                </li>
            </ul>
		</section>
		<!-- /.sidebar -->
	</aside>
</#macro>

<#macro commonControl >
	<!-- Control Sidebar -->
	<aside class="control-sidebar control-sidebar-dark">
		<!-- Create the tabs -->
		<ul class="nav nav-tabs nav-justified control-sidebar-tabs">
			<li class="active"><a href="#control-sidebar-home-tab" data-toggle="tab"><i class="fa fa-home"></i></a></li>
			<li><a href="#control-sidebar-settings-tab" data-toggle="tab"><i class="fa fa-gears"></i></a></li>
		</ul>
		<!-- Tab panes -->
		<div class="tab-content">
			<!-- Home tab content -->
			<div class="tab-pane active" id="control-sidebar-home-tab">
				<h3 class="control-sidebar-heading">近期活动</h3>
				<ul class="control-sidebar-menu">
					<li>
						<a href="javascript::;">
							<i class="menu-icon fa fa-birthday-cake bg-red"></i>
							<div class="menu-info">
								<h4 class="control-sidebar-subheading">张三今天过生日</h4>
								<p>2015-09-10</p>
							</div>
						</a>
					</li>
					<li>
						<a href="javascript::;"> 
							<i class="menu-icon fa fa-user bg-yellow"></i>
							<div class="menu-info">
								<h4 class="control-sidebar-subheading">Frodo 更新了资料</h4>
								<p>更新手机号码 +1(800)555-1234</p>
							</div>
						</a>
					</li>
					<li>
						<a href="javascript::;"> 
							<i class="menu-icon fa fa-envelope-o bg-light-blue"></i>
							<div class="menu-info">
								<h4 class="control-sidebar-subheading">Nora 加入邮件列表</h4>
								<p>nora@example.com</p>
							</div>
						</a>
					</li>
					<li>
						<a href="javascript::;">
						<i class="menu-icon fa fa-file-code-o bg-green"></i>
						<div class="menu-info">
							<h4 class="control-sidebar-subheading">001号定时作业调度</h4>
							<p>5秒前执行</p>
						</div>
						</a>
					</li>
				</ul>
				<!-- /.control-sidebar-menu -->
			</div>
			<!-- /.tab-pane -->

			<!-- Settings tab content -->
			<div class="tab-pane" id="control-sidebar-settings-tab">
				<form method="post">
					<h3 class="control-sidebar-heading">个人设置</h3>
					<div class="form-group">
						<label class="control-sidebar-subheading"> 左侧菜单自适应
							<input type="checkbox" class="pull-right" checked>
						</label>
						<p>左侧菜单栏样式自适应</p>
					</div>
					<!-- /.form-group -->

				</form>
			</div>
			<!-- /.tab-pane -->
		</div>
	</aside>
	<!-- /.control-sidebar -->
	<!-- Add the sidebar's background. This div must be placed immediately after the control sidebar -->
	<div class="control-sidebar-bg"></div>
</#macro>

<#macro commonFooter >
	<footer class="main-footer">
        <p style="text-align: center;">
            &copy;Copyright@2017 <a href="http://www.yiidata.com" target="_blank">yiidata.com</a> 弹性私有云服务
        </p>
	</footer>
</#macro>