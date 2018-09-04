/*!
 * 集群展示页面操作
 */

var app = angular.module('schedulerInfo', []);
//集群监控数据获取
app.controller('mySchedulerInfo', function($rootScope, $scope, $http, $timeout) {
	 $http.get('/scheduler/getAll?clusterId=' + $('#clusterId').val()).success(function(data){
		 if(data.code === 0){
			 $scope.schedulerList = data.data;
		 }else {
			swal("获取失败", data.msg, "error");
		 }
	 });

	 //集群添加调度
     $scope.addScheduler = function(schedulerName){
         swal({
          	  title: "添加scheduler",
          	  type: "warning",
          	  showCancelButton: true,
          	  confirmButtonColor: "#DD6B55",
          	  confirmButtonText: "执行",
          	  closeOnConfirm: false
          	},
            function () {
                 if( !hasText(schedulerName)){
                    swal("增加的调度类型未知！");
                    return
                 }
        	   	 //获取集群id
        	   	 var clusterId = $('#clusterId').val();
        	   	 $.ajax({
                      url:"/scheduler/adjust",
                      type:"post",
                      contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                      dataType:"json",
                      data:{
                          "clusterId":clusterId,
                          "optType": 1,
                          "scheduler": schedulerName
                      },
                      success: function(data){
                          if(data.code === 0){
                              swal("调度任务增加成功！", "调度任务增加成功!", "success");
                              window.location.reload();
                          }else{
                              swal("调度任务失败！", data.msg, "error");
                          }
                      },
                      error: function(res){
                          swal("调度任务失败！", "请联系管理员!", "error");
                      }
                  });
         });

     };
    //集群移除调度
	 $scope.removeScheduler = function(schedulerName){
	    swal({
          	  title: "移除调度任务！",
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
                     url:"/scheduler/adjust",
                     type:"post",
                     contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                     dataType:"json",
                     data:{
                         "clusterId":clusterId,
                         "optType": 2,
                         "scheduler": schedulerName
                     },
                     success: function(data){
                         if(data.code === 0){
                             swal("移除调度任务成功！", "移除调度任务成功!", "success");
                             window.location.reload();
                         }else{
                             swal("移除调度任务失败！", data.msg, "error");
                         }
                     },
                     error: function(res){
                         swal("移除调度任务失败！", "请联系管理员!", "error");
                     }
                 });
        });
	 };
    $scope.detailScheduler = function(schedulerName){
        var clusterId = $('#clusterId').val();
        $http.get('/scheduler/getDetail?clusterId=' + clusterId +"&name=" + schedulerName).success(function(data){
            if(data.code === 0){
                swal("获取worker信息成功！", data.data, "success");
            }else {
                swal("获取失败", data.msg, "error");
            }
        });
    };
});