/*!
 * 集群展示页面操作
 */

var app = angular.module('metricCluster', []);
//集群监控数据获取
app.controller('myFbaseClusterInfo', function($rootScope, $scope, $http, $timeout) {
	 $http.get('/cluster/queryClusters').success(function(data){
		 if(data.code === 0){
			 $scope.clusterList = data.data;
		 }else {
			swal("获取失败", data.msg, "error");
		 }
	 });
    $http.get('/metric/server/getAll').success(function(data){
        if(data.code === 0){
            $('#msAddr').empty();
            for(var i =0; i< data.data.length; i++){
                $('#msAddr').append("<option value='"+data.data[i].addr+"'>"+data.data[i].addr+"</option>");
            }
        }else {
            swal("获取监控列表失败", data.msg, "error");
        }
    });

    //集群监控调整
    $scope.adjustConfig = function (space) {
        //先获取配置
        $.ajax({
            url:"/metric/config/get",
            type:"post",
            contentType:"application/x-www-form-urlencoded; charset=UTF-8",
            dataType:"json",
            data:{
                "clusterId": space.id
            },
            success: function(data){
                if(data.code === 0){
                    $("#clusterId").val(space.id);
                    $("#msAddr").val(data.data["ms"].address);
                    $("#msInterval").val(data.data["ms"].interval);
                    $("#gsAddr").val(data.data["gs"].address);
                    // $("#gsInterval").val(data.data["gs"].interval);

                    $('#metricConfigDetail').modal('show');
                }else{
                    swal("获取集群监控配置异常！", data.msg, "error");
                }
            },
            error: function(res){
                swal("获取集群监控配置异常！", "请联系管理员!", "error");
            }
        });
    };
});

//添加服务监控地址
function update() {
    var clusterId = $('#clusterId').val();
    var msAddr = $('#msAddr').val();
    var msInterval = $('#msInterval').val();
    var gsAddr = $('#gsAddr').val();
    if (msAddr != gsAddr) {
        swal("ms的监控地址必须跟gw的一致", "请重新设置", "error");
        return
    }
    //执行ajax提交
    $.ajax({
        url:"/metric/config/set",
        type:"post",
        async: false,
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
            "clusterId": clusterId,
            "metricAddr": msAddr,
            "metricInterval": msInterval
        },
        success: function(data){
            if(data.code === 0){
                swal("设置", "设置成功", "success");
                //关闭模态框
                $('#metricConfigDetail').modal('hide');
            }else {
                swal("设置失败", data.msg, "error");
            }
        },
        error: function(res){
            swal("设置失败", res, "error");
        }
    });
}

//网关监控地址跟ms的监控地址一致
$('#msAddr').change(function(){
    var msAddr = $("#msAddr").val();
    $('#gsAddr').val(msAddr);
});