//集群master节点信息
var app = angular.module('masterInfo', []);
//添加erp用户的逻辑
app.controller('masterNodeCtrl', ['$scope', '$element', '$compile', function($scope, $element, $compile){

    var bt_data;
    var clusterId = $('#clusterId').val();
    $.ajax({
        url:"/master/queryAll",
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
            }else {
                swal("查询集群master的节点列表", data.msg, "error");
            }
        },
        error: function(res){
            swal("查询集群master的节点列表", res, "error");
        }
    });
    $('#viewnodeList').bootstrapTable({
        data: bt_data.node,
        columns: [
            {
                title: 'ID',
                field: 'id',
                align: 'center',
                valign: 'middle',
            }, {
                title: 'web_address',
                field: 'web_addr',
                align: 'center',
                valign: 'middle'
            }, {
                title: 'rpc_address',
                field: 'rpc_addr',
                align: 'center',
                valign: 'middle'
            }, {
                title: 'raft_hb_addr',
                field: 'raft_hb_addr',
                align: 'center',
                valign: 'middle'
            }, {
                title: 'raft_replicate_addr',
                field: 'raft_rp_addr',
                align: 'center',
                valign: 'middle'
            }, {
                field: 'id',
                title: '操作',
                align: 'center',
                formatter: function(value,row,index){
                    if(bt_data.leader_id == value){
                        return [
                             "<button id=\"updateLogLevel\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"修改日志级别\" onclick=\"updateLogLevel('"+row.id+"');\">设置日志级别</button>&nbsp;&nbsp;"
                        ].join('');
                    }else {
                        return ""
                    }
                }
            }
        ],
        pagination: true,
        iconSize: 'outline'
    });
}]);
function updateLogLevel(){
    swal({
            title: "日志级别设置",
            text: "<select class='form-control' id='selectLogLevel' style='height: 33px'><option value='trace'>trace</option><option value='debug'>debug</option><option value='info'>info</option><option value='warn'>warn</option><option value='error'>error</option></select>",
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
                url:"/master/logLevelUpdate",
                type:"post",
                contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                dataType:"json",
                data:{
                    "clusterId":clusterId,
                    "logLevel": logLevel
                },
                success: function(data){
                    if(data.code === 0){
                        swal("master日志级别修改成功！", "master日志级别修改成功!", "success");
                    }else{
                        swal("master日志级别修改失败！", data.msg, "error");
                    }
                },
                error: function(res){
                    swal("master日志级别修改失败！", "请联系管理员!", "error");
                }
            });
        });
}



