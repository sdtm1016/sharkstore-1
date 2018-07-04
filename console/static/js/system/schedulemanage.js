/*!
 * 集群展示页面操作
 */

var app = angular.module('sysCluster', []);
//集群监控数据获取
app.controller('myFbaseClusterInfo', function($rootScope, $scope, $http, $timeout) {
	 $http.get('/cluster/queryClusters').success(function(data){
		 if(data.code === 0){
			 $scope.clusterList = data.data;
		 }else {
			swal("获取失败", data.msg, "error");
		 }
	 });
    //集群调度
	 $scope.scheduleAdjust = function(space){
        window.location.href="/page/system/scheduleAdjust?clusterId=" + space.id;
	 };
    //集群拓扑
    $scope.topologyView = function(space){
        window.location.href="/page/cluster/topology?clusterId=" + space.id;
    };

    $scope.viewRangeTopoByRngId = function (space) {
        swal({
                title: "查看range拓扑",
                text: "请输入一个rangeId来查看",
                type: "input",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "查看",
                closeOnConfirm: false
            },
            function (inputValue) {
                if (inputValue === false) return;
                if (inputValue === "") {
                    swal.showInputError("你需要输入rangeId");
                    return
                }

                var rangeId = inputValue;
                if (rangeId == "" || rangeId == null) {
                    swal("请输入rangeId");
                    return
                }
                window.location.href = window.location.href = "/range/getRangeTopo?clusterId=" + space.id + "&rangeId=" + rangeId;
            }
        );
    };

    $scope.viewRangeOpsTopN = function (space) {
        swal({
                title: "输入查看的条数，不填默认top10",
                text: "请输入查看的条数",
                type: "input",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "查看",
                closeOnConfirm: false
            },
            function (inputValue) {
                if (inputValue === false) return;
                if (inputValue === "") {
                    inputValue = 10;
                }
                var topN = inputValue;
                window.location.href = "/page/cluster/viewRangeOpsTopN?clusterId=" + space.id + "&topN=" + topN;
            }
        );
    };
});