/*!
 * 集群展示页面操作
 */

var app = angular.module('myClusterInfo', []);
//集群监控数据获取
app.controller('myFbaseClusterInfo', function($rootScope, $scope, $http, $timeout) {
	 $http.get('/cluster/queryClusters').success(function(data){
		 if(data.code === 0){
			 $scope.clusterList = data.data;
		 }else {
			swal("获取失败", data.msg, "error");
		 }
	 });
	 //执行 打开自动扩容、自动迁移和自动failover 开关
	 $scope.toggleAutoMigrate = function(space, type){
		 //设置开关状态
		 space[type] = !space[type];
		 //提交开关设置
		 $.ajax({
         		url:'/cluster/toggleAuto?clusterId=' + space.id,
         		type:"post",
         		async: false,
                 contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                 dataType:"json",
                 data:{
                 	"autoSplitUnable":space['auto_split'],
                     "autoTransferUnable":space['auto_transfer'],
                     "autoFailoverUnable":space['auto_failover']
                 },
         		success: function(data){
         			 if(data.code != 0){
                           space[type] = !space[type];
                    	   alert("设置不成功，请稍后重试");
                     }
                 }
         	});
	 };
	 //查看集群
	 $scope.viewClusterInfo = function(space){
		 window.location.href="/page/cluster/info?clusterId=" + space.id;
	 };
	 //集群元数据管理
	 $scope.viewClusterSourceInfo = function(space){
		 window.location.href="/page/cluster/source?clusterId=" + space.id;
	 };
	 //集群master节点查看
	 $scope.viewClusterMaster = function(space){
         window.location.href="/page/master/viewNodeList?clusterId=" + space.id;
	 };
});