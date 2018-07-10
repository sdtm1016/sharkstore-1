/**
 * 数据库表列表展示
 */
var app = angular.module('myTablesInfo', []);
//集群监控数据获取
app.controller('mytables', function($rootScope, $scope, $http, $timeout) {
	//数据库id
	var dbId = $("#dbId").val();
	//数据库名字
	var dbName = $("#dbName").val();
	//集群id
	var clusterId = $('#clusterId').val();
	 $http.get('/metadata/dbTablesDataGetByDbName?dbId='+dbId +'&dbName='+dbName +"&clusterId=" + clusterId).success(function(data){
	 	if(data.code === 0){
			$scope.tableList = data.data;
		 }else {
			swal("获取失败1", data.msg, "error");
		 }
	 });
	 
	// //执行 打开自动扩容、自动迁移和自动failover 开关
	//  $scope.toggleAutoMigrate = function(space, type){
	// 	 //设置开关状态
	// 	 space[type] = !space[type];
	// 	 //上传数据
	// 	 data = {
	// 			 autoShardingUnable:space['autoShardingUnable'],
	// 			 autoTransferUnable:space['autoTransferUnable'],
	// 			 autoFailoverUnable:space['autoFailoverUnable']
	// 	 };
	// 	 //提交开关设置
	// 	$http.post('/metadata/toggleAuto?clusterId=' + clusterId + '&dbName=' + space.db_name + '&tableName=' + space.name ,data).success(function(data){
	//       if(data.code != 200){
	//         space[type] = !space[type];
	//         alert("设置不成功，请稍后重试");
	//       }
	//     }).error(function(){
	//       space[type] = !space[type];
	//       alert("系统错误，请稍后重试");
	//     });
	//  };
	 $scope.viewRangePage = function(table){
		 window.location.href="/range/viewRangeInfo?db_name="+table.db_name+"&name="+table.name+"&clusterId=" + clusterId;
	 };
     $scope.getTableMonitorInfo = function(table){
    	 window.location.href="/monitor/viewTableMonitor?dbName="+table.db_name+"&tableName="+table.name+"&tableId="+table.id+"&clusterId=" + clusterId;
     };
     $scope.getColumnsView = function(table){
    	 window.location.href="/page/metadata/viewTableColumns?db_name="+table.db_name+"&name="+table.name+"&clusterId=" + clusterId;
     };
     $scope.getTableEditView = function(table){
		window.location.href="/page/metadata/edittable?dbName="+table.db_name+"&tableName="+table.name+"&clusterId=" + clusterId;
     };
     $scope.checkTopology = function(table){
        $.ajax({
            url:"/topology/check",
            type:"post",
            contentType:"application/x-www-form-urlencoded; charset=UTF-8",
            dataType:"json",
            data:{
                "clusterId":clusterId,
                "dbName":table.db_name,
                "tableName":table.name
            },
            success: function(data){
                if(data.code === 0){
                    swal("拓扑正确!", data.msg, "success");
                }else {
                    swal("拓扑检查失败", data.msg, "error");
                }
            },
            error: function(res){
                swal("拓扑检查失败", res, "error");
            }
        });
     };
    $scope.getRangeDuplicateView = function(table){
        window.location.href="/page/table/duplicateRange?dbName="+table.db_name+"&tableName="+table.name+"&clusterId=" + clusterId;
    };
    $scope.getTopologyMissingView = function(table){
        window.location.href="/page/table/topology?dbName="+table.db_name+"&tableName="+table.name+"&clusterId=" + clusterId;
    };
    $scope.getAbnormalRangeView = function (table) {
        swal({
                title: "异常range or 查询单个range",
                text: "请输入一个rangeId来查询",
                type: "input",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "查询",
                closeOnConfirm: false
            },
            function (inputValue) {
                if (inputValue === false) return;
                var rangeId = inputValue;
                window.location.href = "/page/range/unhealthy?dbName=" + table.db_name + "&tableName=" + table.name + "&clusterId=" + clusterId + "&rangeId=" + rangeId;
            }
        );
    };

    $scope.getUnstableRangeView = function (table) {
        window.location.href = "/page/range/unstable?dbName=" + table.db_name + "&tableName=" + table.name + "&clusterId=" + clusterId;
    };

	$scope.deleteRow = function(table) {
	 swal({
		  title: "删除表?",
		  type: "warning",
		  showCancelButton: true,
		  confirmButtonColor: "#DD6B55",
		  confirmButtonText: "删除",
		  closeOnConfirm: false
		},
		function(){
		//执行ajax提交
			$.ajax({
				url:"/metadata/delTable",
				type:"post",
				contentType:"application/x-www-form-urlencoded; charset=UTF-8",
				dataType:"json",
				data:{
					"clusterId":clusterId,
					"dbName":table.db_name,
					"tableName":table.name,
					"flag": false
				},
				success: function(data){
					if(data.code === 0){
						swal("删除成功!", data.msg, "success");
						window.location.reload();
					}else {
						swal("删除失败", data.msg, "error");
					}
				},
				error: function(res){
					swal("删除失败", res, "error");
				}
			});
		});
	};
	$scope.fastDeleteRow = function(table) {
	 swal({
		  title: "快速删除表?【一个周期清表】",
		  type: "warning",
		  showCancelButton: true,
		  confirmButtonColor: "#DD6B55",
		  confirmButtonText: "删除",
		  closeOnConfirm: false
		},
		function(){
		//执行ajax提交
			$.ajax({
				url:"/metadata/delTable",
				type:"post",
				contentType:"application/x-www-form-urlencoded; charset=UTF-8",
				dataType:"json",
				data:{
					"clusterId":clusterId,
					"dbName":table.db_name,
					"tableName":table.name,
					"flag": true
				},
				success: function(data){
					if(data.code === 0){
						swal("删除成功!", data.msg, "success");
						window.location.reload();
					}else {
						swal("删除失败", data.msg, "error");
					}
				},
				error: function(res){
					swal("删除失败", res, "error");
				}
			});
		});
	};
    //设置表读写策略
    $scope.rwPolicys = function(table) {
    	var policyObject = {};
    	policyObject.policy = parseInt(table.RwPolicy.policy);
        swal({
                title: "确定修改Table读取策略?",
                type: "warning",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "修改",
                closeOnConfirm: false
            },
			function() {
                $.ajax({
                    url:"/metadata/tableRwPolicy",
                    type:"post",
                    contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                    dataType:"json",
                    data:{
                        "option":"set",
                        "clusterId":clusterId,
                        "dbName":table.db_name,
                        "tableName":table.name,
                        "policy":JSON.stringify(policyObject)
                    },
                    success: function(data){
                        if(data.attach.code === 0){
                            swal("设置成功!", data.attach.message, "success");
                        }else {
                            swal("设置失败", data.attach.message, "error");
                        }
                    },
                    error: function(res){
                        swal("设置失败", res, "error");
                    }
                });
            });
    };
});

//时间格式化
function formatDate(formatDate){
	  function padding(str){
	    if(str > 0 && str < 10){
	      return '0' + str;
	    }else{
	      return str;
	    }
	  }
	  var year = formatDate.getFullYear(), 
	      month = padding(formatDate.getMonth() + 1), 
	      date = padding(formatDate.getDate()),
	      hour = padding(formatDate.getHours()),
	      minute = padding(formatDate.getMinutes());
	  return year + '-' + month + '-' + date + ' ' + hour + ':' + minute; 
	}