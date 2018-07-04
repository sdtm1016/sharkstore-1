$(function(){
    //设置时间
	datePairs($('#startMonitor'), $('#endMonitor'));
	datePairs($('#startTask'), $('#endTask'));
	datePairs($('#startGateWay'), $('#endGateWay'));
	datePairs($('#startEvent'), $('#endEvent'));
});
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
function datePairs(date1, date2, start, maxDate){
	  var nowdate = new Date(),
	      startDate = new Date(nowdate.getTime() - (maxDate || 1000 * 60 * 60 *24 * 30)),
	      defaultEnd = formatDate(nowdate),
	      startVal = start || 4 * 1000 * 60 * 60,
	      defaultStart = formatDate(new Date(nowdate.getTime() - startVal));
	  date1.datetimepicker({
	    format: 'yyyy-mm-dd hh:ii',
	    autoclose: true,
	    todayBtn: true,
	    startDate: startDate,
	    endDate: nowdate
	  })
	  .on('changeDate', function(ev){
	    if (date1.val() > date2.val()){
	        alert("开始时间不能大于结束时间");
	        date1.val(date2.val());
	        date1.datetimepicker('update');
	    }
	  });
	  date2.datetimepicker({
	    format: 'yyyy-mm-dd hh:ii',
	    autoclose: true,
	    todayBtn: true,
	    startDate: startDate,
	    endDate: nowdate
	  }).on('changeDate', function(ev){
	    if (date1.val() > date2.val()){
	        alert("束时间不能小于开始时间");
	        date2.val(date1.val());
	        date2.datetimepicker('update');
	    }
	  });
	  date1.val(defaultStart).datetimepicker('update');
	  date2.val(defaultEnd).datetimepicker('update');
	}
var app = angular.module('myCluster', []);
//gateway监控
app.controller('clusterGateWay', function($rootScope, $scope, $http, $timeout) {

});
//集群事件监控
app.controller('clusterEvent', function($rootScope, $scope, $http, $timeout) {
	var clusterId = $('#clusterId').val();
	var startTime = $('#startEvent').val();
	var endTime = $('#endEvent').val();
	var options = {
    	data: [],
    	columns: [
              	  {
		          	  title: '数据库名',
		              field: 'dbName',
		              align: 'left',
		              valign: 'left',
		          },{
                	  title: '表名',
                      field: 'tableName',
                      align: 'left',
                      valign: 'left',
                  }, {
                      title: 'rangeId',
                      field: 'rangeId',
                      align: 'center',
                      valign: 'middle'
                  },{
                      title: 'nodeId',
                      field: 'nodeId',
                      align: 'center',
                      valign: 'middle'
                  },{
                      title: '类型',
                      field: 'statisticsType',
                      align: 'center',
                      valign: 'middle'
                  },{
                      title: '开始时间',
                      field: 'startTime',
                      align: 'center',
                      valign: 'middle'
                  },{
                      title: '结束时间',
                      field: 'endTime',
                      align: 'center',
                      valign: 'middle'
                  },{
                      title: '持续时间（纳秒）',
                      field: 'continuedTime',
                      align: 'center',
                      valign: 'middle'
                  }
              ],
      pagination: true,
      iconSize: 'outline'
    };
	//请求数据
	$.ajax({
		url:"/cluster/getClusterEvent",
		type:"post",
		async: false,
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
        	"clusterId":clusterId,
        	"startTime":startTime,
        	"endTime":endTime
        },
		success: function(data){
			if(data.code === 0){
				options.data = data.attach;
			}
        }
	});
	//渲染表单
	$('#viewclustereventlist').bootstrapTable(options);
});
//集群监控数据获取
app.controller('myClusterMonitor', function($rootScope, $scope, $http, $timeout) {
    //默认不查
    //load_monitor_data("defaultOptions")
    function load_monitor_data(type){//获取集群id
        var clusterId = $('#clusterId').val();
        var startTime ;
        var endTime ;
        if(type != "defaultOptions"){
            startTime = Date.parse(new Date($("#startMonitor").val()));
            endTime = Date.parse(new Date($("#endMonitor").val()));
        }

        $("#clusterSummary").attr("src", getMonitorUrl("ji-qun-gai-kuang", "1", clusterId,  startTime, endTime, ""))
        $("#clusterSummary").load();

        $("#clientNum").attr("src", getMonitorUrl("ji-qun-gai-kuang", "5", clusterId,  startTime, endTime, ""))
        $("#clientNum").load();

        $("#capacity").attr("src", getMonitorUrl("ji-qun-gai-kuang", "9", clusterId,  startTime, endTime, ""))
        $("#capacity").load();

        $("#tps").attr("src", getMonitorUrl("ji-qun-gai-kuang", "3", clusterId,  startTime, endTime, ""))
        $("#tps").load();

        $("#tp999").attr("src", getMonitorUrl("ji-qun-gai-kuang", "8", clusterId,  startTime, endTime, ""))
        $("#tp999").load();

        $("#tpStd").attr("src", getMonitorUrl("ji-qun-gai-kuang", "4", clusterId,  startTime, endTime, ""))
        $("#tpStd").load();
    }
    //根据时间段去查询数据
    $scope.queryFilter = function(){
        load_monitor_data("setOptions");
    }
});
//集群DB列表信息展示
app.controller('myClusterDbList', function($rootScope, $scope, $http, $timeout) {
	var clusterId = $('#clusterId').val();
	var bt_data = [];
//	console.log("getalldb of cluster")
	$.ajax({
		url:"/metadata/dbDataGetAll",
		type:"post",
		async: false,
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
            "clusterId":clusterId
        },
		success: function(data){
		    if(data.code == 0){
		        if(data.data.length > 0){
		         bt_data = data.data;
		        }
		    }
        }
	});
    $('#viewdbList').bootstrapTable({
    	data: bt_data,
    	columns: [
                  {
                	  title: 'ID',
                      field: 'id',
                      align: 'center',
                      valign: 'middle',
                  }, {
                      title: '数据库名',
                      field: 'name',
                      align: 'center',
                      valign: 'middle'
                  },{
                	  field: '操作',
                      title: '操作',
                      align: 'center',
                      formatter: function(value,row,index){
                    	  return [
"<button id=\"dbconsole\" class=\"btn btn-primary btn-rounded\" type=\"button\" value=\"控制台\" onclick=\"viewConsole('"+row.id+"','"+row.name+"');\">控制台</button>&nbsp;&nbsp;",
"<button id=\"updatenode\" class=\"btn btn-primary btn-rounded\" type=\"button\" value=\"查看表\" onclick=\"getTableInfo('"+row.id+"','"+row.name+"');\">查看表</button>&nbsp;&nbsp;",
"<button id=\"viewdbmonitor\" class=\"btn btn-primary btn-rounded\" type=\"button\" value=\"监控\" onclick=\"dbmonitor('"+row.id+"','"+row.name+"');\">监控</button>"
						     ].join('');
                      }
                  }
              ],
      pagination: true,
      iconSize: 'outline'
    });
});
//展示数据库控制台
function viewConsole(dbId,dbName) {
	var clusterId = $('#clusterId').val();
	window.location.href="/page/db/viewConsole?dbName="+dbName+"&dbId="+dbId+"&clusterId="+clusterId;
}
/**
 * 根据数据库dbName 、 dbid 查询tables
 * 跳转到tables页面
 * @param id
 */
function getTableInfo(dbId,dbName) {
	var clusterId = $('#clusterId').val();
	window.location.href="/page/tables/tablelist?dbName="+dbName+"&dbId="+dbId+"&clusterId="+clusterId;
}
//请求db监控页面
function dbmonitor(dbId,dbName) {
	var clusterId = $('#clusterId').val();
	window.location.href="/monitor/dbmonitor?dbId="+dbId+"&dbName="+dbName+"&clusterId="+clusterId;
}
//集群中的服务器实例展示列表
app.controller('myClusterNodeInfo', function($rootScope, $scope, $http, $timeout) {
	var bt_data;
	var clusterId = $('#clusterId').val();
	$.ajax({
		url:"/node/nodeInfoGetAll",
		type:"post",
		async: false,
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
            "clusterId":clusterId
        },
        success: function(data){
            if(data.code === 0){
            	bt_data = data.data;
            }
        }
	});
    $('#viewnodeList').bootstrapTable({
    	data: bt_data,
    	columns:[
                  {
                     field: '',checkbox : true, align : 'center'
                  }, {
                	  title: 'ID',
                      field: 'id',
                      align: 'center',
                      valign: 'middle',
                  }, {
                      title: 'address',
                      field: 'server_addr',
                      align: 'center',
                      valign: 'middle'
                  }, {
                      title: 'heartbeat_addr',
                      field: 'http_addr',
                      align: 'center',
                      valign: 'middle'
                  }, {
                      title: 'replicate_addr',
                      field: 'raft_addr',
                      align: 'center',
                      valign: 'middle'
                  }, {
    	              title: 'opsStat_max',
                      field: 'version',
                      align: 'center',
                      valign: 'middle'
                  }, {
                      title: 'isolation_label',
                      field: 'isolation_label',
                      align: 'center',
                      valign: 'middle'
                  }, {
                      title: '状态',
                      field: 'state',
                      align: 'center',
                      valign: 'middle',
                      formatter: function(value, row, index){
                        if(value == 0){
                            return "N_Invalid";
                        }else if(value == 1){
                            return "N_Login";
                        }else if(value == 2){
                            return "N_Logout";
                        }else if(value == 3){
                            return "N_Offline";
                        }else if(value == 4){
                            return "N_Tombstone";
                        }else if(value == 5){
                            return "N_Upgrade";
                        }else if(value == 6){
                            return "N_Initial";
                        }else{
                            return value;
                        }
                      }
                  }, {
                	  field: '操作',
                      title: '操作',
                      align: 'center',
                      formatter: function(value,row,index){
                    	  return [
"<button id=\"upgradeNode\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"升级\" onclick=\"upgradeNode('"+row.id+"');\">升级</button>&nbsp;&nbsp;",
"<button id=\"viewNodeMonitor\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"监控\" onclick=\"viewNodeMonitor('"+row.id+"','"+row.server_addr+"');\">监控</button>&nbsp;&nbsp;",
"<button id=\"updateNodeStatus\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"修改状态\" onclick=\"updateNodeStatus('"+row.id+"');\">状态修改</button>&nbsp;&nbsp;",
"<button id=\"deleteNode\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"删除\" onclick=\"deleteNode("+row.id+");\">删除</button>&nbsp;&nbsp;",
"<button id=\"updateNodeLogLevel\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"修改日志级别\" onclick=\"updateNodeLogLevel('"+row.id+"');\">设置日志级别</button>&nbsp;&nbsp;",
"<button id=\"getRangeTopoOfNode\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"查看range\" onclick=\"getRangeTopoOfNode('"+row.id+"');\">查看range</button>&nbsp;&nbsp;",
"<button id=\"updateIsolationLabel\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"修改隔离标识\" onclick=\"updateIsolationLabel('"+row.id+"');\">修改隔离标识/button>&nbsp;&nbsp;"
						     ].join('');
                      }
                  }
              ],
      pagination: true,
      pageNumber: 1,
      pageSize: 10,
      pageList: [10, 100, 1000],
      iconSize: 'outline'
    });
});
//node监控数据展示
function viewNodeMonitor(nodeId,host){
	var clusterId = $('#clusterId').val();
	window.location.href="/monitor/nodeMonitor?nodeId="+nodeId+"&host="+host + "&clusterId=" + clusterId;
}
//修改node状态， force上线(initail)、下线logout//todo 是否需要支持其他状态 ：login、tombstone、offline
function updateNodeStatus(nodeId){
	swal({
  	  title: "实例状态操作",
  	  text: "<select class='form-control' id='selectNodeStatus' style='height: 33px'><option value='1'>force上线</option><option value='2'>下线</option></select>",
  	  html: true,
  	  showCancelButton: true,
  	  confirmButtonColor: "#DD6B55",
  	  confirmButtonText: "执行",
  	  closeOnConfirm: false
  	},
    function () {
         var status = $('#selectNodeStatus').val();
         if( !hasText(status)){
            swal("请选择要修改的节点状态");
            return
         }
	   	 //获取集群id
	   	 var clusterId = $('#clusterId').val();
	   	 $.ajax({
		        url:"/node/nodeStatusUpdate",
		        type:"post",
		        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
		        dataType:"json",
		        data:{
		        	"nodeId":nodeId,
		        	"clusterId":clusterId,
		        	"status": status
		        },
		        success: function(data){
		        	if(data.code === 0){
		        		swal("节点状态修改成功！", "节点状态修改成功!", "success");//todo 刷状态
		        	}else{
		        		swal("节点状态修改失败！", data.message, "error");
		        	}
		        },
		        error: function(res){
		        	swal("状态修改失败！", "请联系管理员!", "error");
		        }
		    });
    });
}

//node批量下线操作
function batchDeleteNode(){
    var selectedNodeRows = $('#viewnodeList').bootstrapTable('getSelections');
    if(selectedNodeRows.length <= 0){
        swal("批量删除实例", "请选择要删除的实例", "error")
        return
    }
    var nodeIds = [];
    for (var i = 0; i < selectedNodeRows.length; i++) {
        nodeIds.push(selectedNodeRows[i].id);
    }
    swal({
            title: "批量删除实例, 只有logout的节点才能被删除",
            type: "warning",
            showCancelButton: true,
            confirmButtonColor: "#DD6B55",
            confirmButtonText: "执行",
            closeOnConfirm: false
        },
        function () {
            //获取集群id
            var clusterId = $('#clusterId').val();
            $.ajax({
                url:"/node/deleteNode",
                type:"post",
                contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                dataType:"json",
                data:{
                    "clusterId":clusterId,
                    "nodeIds": JSON.stringify(nodeIds)
                },
                success: function(data){
                    if(data.code === 0){
                        swal("删除成功！", "删除成功!", "success");
                        for (var i = 0; i < nodeIds.length; i++) {
                            $('#viewnodeList').bootstrapTable('remove',{
                                field:'id',
                                values:nodeIds[i]
                            });
                        }
                    }else{
                        swal("删除失败！", data.message, "error");
                    }
                },
                error: function(res){
                    swal("删除失败！", "请联系管理员!", "error");
                }
            });
        });
}

//node下线操作
function deleteNode(nodeId){
    var nodeIds = [];
    nodeIds.push(nodeId);
	swal({
  	  title: "删除实例操作, 只有logout的节点才能被删除",
  	  type: "warning",
  	  showCancelButton: true,
  	  confirmButtonColor: "#DD6B55",
  	  confirmButtonText: "执行",
  	  closeOnConfirm: false
  	},
    function () {
	   	 //获取集群id
	   	 var clusterId = $('#clusterId').val();
	   	 $.ajax({
		        url:"/node/deleteNode",
		        type:"post",
		        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
		        dataType:"json",
		        data:{
		        	"nodeIds": JSON.stringify(nodeIds),
		        	"clusterId":clusterId
		        },
		        success: function(data){
		        	if(data.code === 0){
		        		swal("删除成功！", "删除成功!", "success");
		        		$('#viewnodeList').bootstrapTable('remove',{
		        			field:'id',
		        			values:nodeId
		        		});
		        	}else{
		        		swal("删除失败！", data.message, "error");
		        	}
		        },
		        error: function(res){
		        	swal("删除失败！", "请联系管理员!", "error");
		        }
		    });
    });
}

//调用后台升级操作
function upgradeNode(nodeId) {
    swal({
  	  title: "您确定要升级吗?",
  	  type: "warning",
  	  showCancelButton: true,
  	  confirmButtonColor: "#DD6B55",
  	  confirmButtonText: "执行",
  	  closeOnConfirm: false
  	},
    function () {
	   	 //获取集群id
         var clusterId = $('#clusterId').val();
         $.ajax({
                url:"/node/nodeStatusUpdate",
                type:"post",
                contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                dataType:"json",
                data:{
                    "nodeId":nodeId,
                    "clusterId":clusterId,
                    "status": 3
                },
                success: function(data){
                    if(data.code === 0){
                        swal("节点升级成功！", "节点升级成功!", "success");
                    }else{
                        swal("节点升级失败！", data.message, "error");
                    }
                },
                error: function(res){
                    swal("状态升级失败！", "请联系管理员!", "error");
                }
            });
    });
//	 swal({
//	         title: "您确定要升级吗?",
//	         text: "升级后将无法恢复，请谨慎操作！",
//	         type: "input",
//	         inputPlaceholder: "请输入版本号",
//	         showCancelButton: true,
//	         confirmButtonColor: "#DD6B55",
//	         confirmButtonText: "升级！",
//	         cancelButtonText: "再考虑一下…",
//	         closeOnConfirm: false
//	     },
//	     function (inputValue) {
//	    	 //获取集群id
//	    	 var clusterId = $('#clusterId').val();
//	    	 if (inputValue === false){
//	    		 return false;
//	    	 }
//	    	 if (inputValue === "") {
//    		    swal.showInputError("请输入版本号!");
//    		    return false;
//    		  }
//	        	 $.ajax({
//	 		        url:"/node/upGrade",
//	 		        type:"post",
//	 		        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
//	 		        dataType:"json",
//	 		        data:{
//	 		        	"nodeId":nodeId,
//	 		        	"serverVersion":inputValue,
//	 		        	"clusterId":clusterId
//	 		        },
//	 		        success: function(data){
//	 		        	if(data.code === 200){
//	 		        		swal("升级成功！", "您已经成功升级到最新版本!", "success");
//	 			         	//执行升级后删除升级按钮
//	 			         	$("#updatenode").remove();
//	 		        	}
//	 		        },
//	 		        error: function(res){
//	 		        	swal("升级失败！", "请联系管理员或重新升级!", "error");
//	 		        }
//	 		    });
//	     });
}

function updateNodeLogLevel(nodeId){
    swal({
      title: "日志级别设置",
      text: "<select class='form-control' id='selectLogLevel' style='height: 33px'><option value='debug'>debug</option><option value='info'>info</option><option value='warn'>warn</option><option value='error'>error</option></select>",
      html: true,
      showCancelButton: true,
      confirmButtonColor: "#DD6B55",
      confirmButtonText: "执行",
      closeOnConfirm: false
    },
    function () {
         var logLevel = $('#selectLogLevel').val();
         if( !hasText(logLevel)){
            swal("请选择要设置的日志级别");
            return
         }
         //获取集群id
         var clusterId = $('#clusterId').val();
         $.ajax({
                url:"/node/nodeLogLevelUpdate",
                type:"post",
                contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                dataType:"json",
                data:{
                    "nodeId":nodeId,
                    "clusterId":clusterId,
                    "logLevel": logLevel
                },
                success: function(data){
                    if(data.code === 0){
                        swal("节点日志级别修改成功！", "节点日志级别修改成功!", "success");
                    }else{
                        swal("节点日志级别修改失败！", data.message, "error");
                    }
                },
                error: function(res){
                    swal("节点日志级别修改失败！", "请联系管理员!", "error");
                }
            });
    });
}

function getRangeTopoOfNode(nodeId) {
    swal({
            title: "查看range",
            text: "若range较多，页面加载耗时可能需要较长时间。",
            showCancelButton: true,
            confirmButtonColor: "#DD6B55",
            confirmButtonText: "继续查看",
            closeOnConfirm: false
        },
        function () {
            //获取集群id
            var clusterId = $('#clusterId').val();
            window.location.href = "/node/getRangeTopo?clusterId=" + clusterId + "&nodeId=" + nodeId;
        });
}

function updateIsolationLabel(nodeId) {
    swal({
            title: "修改Node隔离标志",
            text: "输入新的IsolationLabel",
            type: "input",
            showCancelButton: true,
            confirmButtonColor: "#DD6B55",
            confirmButtonText: "确认",
            closeOnConfirm: false
        },
        function (inputValue) {
            if (inputValue === false) return;
            if (inputValue === "") {
                swal.showInputError("不能为空字符");
                return
            }

            var clusterId = $('#clusterId').val();
            var isolationLabel = inputValue;
            if (isolationLabel == "" || isolationLabel == null) {
                swal("请输入IsolationLabel");
                return
            }
            window.location.href = "/node/updateIsolationLabel?clusterId=" + clusterId + "&nodeId=" + nodeId + "&isolationLabel=" + isolationLabel;
            window.location.reload();
        }
    );
}

//获取历史任务列表
app.controller('historyClusterTask', function($rootScope, $scope, $http, $timeout) {
     //获取任务类型
     $http.get('/task/getTypeAll?clusterId=' + $('#clusterId').val()).success(function(data){
     		 if(data.code === 0){
     			 $scope.taskTypes = data.data;
     			 $scope.item = "所有"
     		 }else {
     			swal("获取失败", data.msg, "error");
     		 }
     });
//    load_history_task("defaultOptions")
    function load_history_task(type){//获取集群id
        var clusterId = $('#clusterId').val();
        var startTime ;
        var endTime ;
        if(type != "defaultOptions"){
            startTime = Date.parse(new Date($("#startTask").val()));
            endTime = Date.parse(new Date($("#endTask").val()));
        }
        var taskType = $('#taskType').val();
        if(!hasText(taskType)){
                $("#historyTask").attr("src", getMonitorUrl("li-shi-ren-wu-lie-biao-cluster", "1", clusterId,  startTime, endTime, ""))
        }else{
                $("#historyTask").attr("src", getMonitorUrl("li-shi-ren-wu-lie-biao-cluster-type", "1", clusterId,  startTime, endTime, $('#taskType').find("option:selected").text()))
        }
        $("#historyTask").load();
    }
    //根据时间段查询任务列表
    $scope.queryFilter = function() {
    	load_history_task("setOptions");
	};
});
//加载当前任务
app.controller('presentClusterTask', function($rootScope, $scope, $http, $timeout) {
	var clusterId = $('#clusterId').val();
	var selectTask=$("#selectTask").val();
	var options = {
    	data: [],
    	columns: [
              	  {
		          	  title: '任务ID',
		              field: 'id',
		              align: 'left',
		              valign: 'left',
		          },
                  {
                      title: 'rangeID',
                      field: 'range_id',
                      align: 'left',
                      valign: 'left',
                  },{
                      title: '类型',
                      field: 'type',
                      align: 'center',
                      valign: 'middle'
                  },{
                	  title: '任务描述',
                      field: 'describe',
                      align: 'left',
                      valign: 'left',
                  // }, {
                  //     title: '创建时间',
                  //     field: 'createtime',
                  //     align: 'center',
                  //     valign: 'middle',
                  //     formatter : function(cellValue) {
					// 		// 调用下面方法进行时间戳格式化
					// 		return formatDate((new Date(cellValue * 1000)),"yyyy-MM-dd hh:mm:ss");
					// 	}
                  },{
                      title: '状态',
                      field: 'state',
                      align: 'center',
                      valign: 'middle'
                  },{
                field: '操作',
                align: 'center',
                checkbox:true
            }
              ],
      pagination: true,
      iconSize: 'outline',
    };
    //请求数据
	$.ajax({
		url:"/task/getPresentTaskById",
		type:"post",
		async: false,
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
        	"type":"allTask",
        	"clusterId":clusterId
        },
		success: function(data){
			if(data.code === 0){
			    if(data.data.length > 0){
			        options.data = data.data;
			    }
			}
        }
	});
	//渲染表单
	$('#viewpresentlist').bootstrapTable(options);
	//执行集群任务操作，删除、暂停、重试
	$('#queryPut').click(function () {
	    var selectedTaskRows = $('#viewpresentlist').bootstrapTable('getSelections');
		if (selectedTaskRows.length == 0) {
			alert("请选择任务!");
			return;
		}
		var ids = [];
	    $.each(selectedTaskRows, function (index, row) {
			ids.push(row.id);
	    });
	    var selectTask = $('#selectTask').val();
	    swal({
	    	  title: "任务操作",
	    	  type: "warning",
	    	  showCancelButton: true,
	    	  confirmButtonColor: "#DD6B55",
	    	  confirmButtonText: "执行",
	    	  closeOnConfirm: false
	    	},
	    function(){
	    	//执行ajax提交
    		$.ajax({
    			url:"/task/taskOperationById",
    	        type:"post",
    	        async: false,
    	        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
    	        dataType:"json",
    	        data:{
    	            "type":selectTask,
    	            "clusterId":clusterId,
    	            "taskId":JSON.stringify(ids)
    	        },
    			success: function(data){
    				if(data.code === 0){
    	            	if(data.data.length > 0){
    	            		swal("结果", data.msg+"\n 失败任务ID："+data.data, "info");
    	            		for(var i = 0;i < data.data.length; i++){
    	            			for(var j = 0;j < ids.length; j++){
    	            				if(data.data[i] == ids[j]){
    	            					ids.splice(j, 1);
    	            				}
    		            		}
    	            		}
    	            	}else{
    	            		swal("成功", data.msg, "info");
    	            	}
    	            	//如果是删除操作，执行删除删除的行
    	            	if(selectTask == "delete"){
    	            		$('#viewpresentlist').bootstrapTable('remove', {field: 'id', values: ids});
    	            	}
    	            }
    	        },
    	        error: function(res){
    	        	swal("操作失败", res, "error");
    	        }
	    	});
	    });
	});
});

//格式化日期,
function formatDate(date, format) {
	var paddNum = function(num) {
		num += "";
		return num.replace(/^(\d)$/, "0$1");
	}
	// 指定格式字符
	var cfg = {
		yyyy : date.getFullYear() // 年 : 4位
		,
		yy : date.getFullYear().toString().substring(2)// 年 : 2位
		,
		M : date.getMonth() + 1 // 月 : 如果1位的时候不补0
		,
		MM : paddNum(date.getMonth() + 1) // 月 : 如果1位的时候补0
		,
		d : date.getDate() // 日 : 如果1位的时候不补0
		,
		dd : paddNum(date.getDate())// 日 : 如果1位的时候补0
		,
		hh : date.getHours() // 时
		,
		mm : date.getMinutes() // 分
		,
		ss : date.getSeconds()
	// 秒
	}
	format || (format = "yyyy-MM-dd hh:mm:ss");
	return format.replace(/([a-z])(\1)*/ig, function(m) {
		return cfg[m];
	});
}

function tabsHandler(event) {
     var data = event.data;
     showTabs(data.id, data.url);
     return false; //阻止默认a标签响应
}

function showTabs(tabsId, url) {
     $("a[href='#"+tabsId+"']").tab('show');
//     $('#'+tabsId).load();
 }

function getMonitorUrl(dashboardName, panelId, clusterId, startTime, endTime, type){
    return "/page/monitor/cluster?clusterId="+clusterId+"&dashboardName="+dashboardName+"&panelId=" + panelId + "&startTime="+ startTime + "&endTime=" + endTime + "&type=" + type;
}







