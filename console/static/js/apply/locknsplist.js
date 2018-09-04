(function () {
    /**
     * lock Namespace申请列表展示
     */
    //lock申请数据获取
    $('#nspApplyLists').bootstrapTable({
        url: "/lock/namespace/queryList",
        striped: true,      //是否显示行间隔色
        cache: false,       //是否使用缓存，默认为true（*）
        // search: true,       //是否显示表格搜索
        pagination: true,   //是否显示分页（*）
        pageNumber: 1,      //初始化加载第一页，默认第一页,并记录
        pageSize: 10,       //每页的记录行数（*）
        pageList: [10, 25, 50, 100],//可供选择的每页的行数（*）
        sidePagination: "server",//分页方式：client客户端分页，server服务端分页（*）
        clickToSelect: true,
        // showColumns: true,  //是否显示所有的列（选择显示的列）
        // showRefresh: true,  //是否显示刷新按钮
        iconSize: 'outline',
        toolbar: '#nspApplyListsToolbar',
        height: 500,
        icons: {
            refresh: 'glyphicon-repeat'
        },
        // 得到查询的参数
        queryParams: function (params) {
            //这里的键的名字和控制器的变量名必须一直，这边改动，控制器也需要改成一样的
            var temp = {
                rows: params.limit,                         //页面大小
                page: (params.offset / params.limit) + 1,   //页码
                sort: params.sort,      //排序列名
                sortOrder: params.order //排位命令（desc，asc）
            };
            return temp;
        },
        columns: [
            {field: '', checkbox: true, align: 'center'},
            {field: 'db_name', title: '库名', align: 'center'},
            {field: 'table_name', title: '表名', align: 'center'},
            {field: 'cluster_id', title: '集群Id', align: 'center'},
            {field: 'applyer', title: '申请人', align: 'center'},
            {
                field: 'create_time', title: '申请时间', align: 'center',
                formatter: function (value, row, index) {
                    //调用下面方法进行时间戳格式化
                    return formatDate((new Date(value * 1000)), "yyyy-MM-dd hh:mm:ss");
                }
            },
            {
                field: 'status', title: '状态', align: 'center',
                formatter: function (value, row, index) {
                    if (value == 1) {
                        return "待审核";
                    }
                    if (value == 2) {
                        return "通过并创建";
                    }
                    if (value == 3) {
                        return "驳回";
                    }
                }
            },
            {
                field: '操作', title: '操作', align: 'center',
                formatter: function (value, row, index) {
                    var buttonS = "<button id=\"updateOwner\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"修改负责人\" onclick=\"updateOwner('" + row.id + "');\">修改负责人</button>&nbsp;&nbsp;" +
                        "<button id=\"viewCluster\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"集群详情\" onclick=\"viewCluster('" + row.cluster_id + "');\">集群详情</button>&nbsp;&nbsp;";
                    if (row.status == 2) {
                        buttonS = buttonS +
                            "<button id=\"viewLock\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"lock详情\" onclick=\"viewLock('" + row.db_name + "','" + row.table_name + "','" + row.cluster_id + "');\">lock详情</button>&nbsp;&nbsp;" +
                            "<button id=\"viewToken\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"查看token\" onclick=\"viewToken('" + row.db_id + "','" + row.table_id + "');\">token详情</button>";
                    }
                    return buttonS;
                }
            }
        ],
        responseHandler: function (res) {
            if (res.code === 0) {
                return {
                    "total": res.data.total,//总页数
                    "rows": res.data.data   //数据
                };
            } else {
                swal("失败", res.msg, "error");
                return {
                    "total": 0,//总页数
                    "rows": res.data   //数据
                };
            }
        }
    });
})(document, window, jQuery);

//能看到记录，就能修改
function updateOwner(applyId) {
    swal({
            title: "编辑",
            text: "修改所属人",
            type: "input",
            showCancelButton: true,
            confirmButtonColor: "#DD6B55",
            confirmButtonText: "执行",
            closeOnConfirm: false
        },
        function (inputValue) {
            if (!inputValue) {
                swal("请先录入该namespace的所属人！");
                return
            }
            $.ajax({
                url: "/lock/namespace/update",
                type: "post",
                contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                dataType: "json",
                data: {
                    id: applyId,
                    applyer: inputValue
                },
                success: function (data) {
                    if (data.code === 0) {
                        swal("更新成功!", data.msg, "success");
                        $('#nspApplyLists').bootstrapTable('refresh', {url: '/lock/namespace/queryList'});

                    } else {
                        swal("更新失败", data.msg, "error");
                    }
                },
                error: function (res) {
                    swal("更新namespace的所属人失败", res, "error");
                }
            });
        });
}

function viewCluster(clusterId) {
    $.ajax({
        url: "/lock/cluster/getInfo",
        type: "post",
        contentType: "application/x-www-form-urlencoded; charset=UTF-8",
        dataType: "json",
        data: {
            clusterId: clusterId
        },
        success: function (data) {
            if (data.code === 0) {
                var innerhtml = "<table><tr><td>集群id：</td><td>" + data.data.id + "</td></tr>" +
                    "<tr><td>集群名：</td><td>" + data.data.name + "</td></tr>" +
                    "<tr><td>访问地址：</td><td>" + data.data.master_url + "</td></tr>" +
                    "</table>"
                swal({
                    title: "集群详情",
                    text: innerhtml,
                    html: true,
                    showCancelButton: false
                });
            } else {
                swal("查询失败", data.msg, "error");
            }
        },
        error: function (res) {
            swal("查询失败", res, "error");
        }
    });
}

function viewLock(dbName, tableName, clusterId) {
    window.location.href = "/page/lock/viewLock?clusterId=" + clusterId + "&dbName=" + dbName + "&tableName=" + tableName;
}


function viewToken(dbId, tableId) {
    $.ajax({
        url: "/lock/client/getToken",
        type: "post",
        contentType: "application/x-www-form-urlencoded; charset=UTF-8",
        dataType: "json",
        data: {
            "dbId": dbId,
            "tableId": tableId
        },
        success: function (data) {
            if (data.code === 0) {
                swal("客户端token", data.data, "success");
            } else {
                swal("获取token失败！", data.msg, "warning");
            }
        },
        error: function (res) {
            swal("获取token失败！", "请联系管理员!", "error");
        }
    });
}

//审批
function auditNsp() {
    var selectedApplyRows = $('#nspApplyLists').bootstrapTable('getSelections');
    if (selectedApplyRows.length == 0) {
        swal("审批申请", "请选择要审批的申请记录", "error");
        return;
    }
    var ids = [];
    for (var i = 0; i < selectedApplyRows.length; i++) {
        if (selectedApplyRows[i].status != 1) {
            swal("审批申请", "请选择待审核状态的申请记录", "error");
            return;
        }
        ids.push(selectedApplyRows[i].id);
    }
    if (ids.length == 0) {
        return;
    }
    swal({
            title: "审批操作",
            type: "warning",
            showCancelButton: true,
            confirmButtonColor: "#DD6B55",
            confirmButtonText: "通过",
            closeOnConfirm: false
        },
        function () {
            $.ajax({
                url: "/lock/namespace/audit",
                type: "post",
                contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                dataType: "json",
                data: {
                    "ids": JSON.stringify(ids),
                    "status": 2
                },
                success: function (data) {
                    if (data.code === 0) {
                        swal("审批成功！", "审批成功!", "success");
                        $('#nspApplyLists').bootstrapTable('refresh', {url: '/lock/namespace/queryList'});
                    } else {
                        swal("审批失败！", data.msg, "error");
                    }
                },
                error: function (res) {
                    swal("审批失败！", "请联系管理员!", "error");
                }
            });
        });
}

//驳回，支持批量
function rejectNsp() {
    var selectedApplyRows = $('#nspApplyLists').bootstrapTable('getSelections');
    if (selectedApplyRows.length == 0) {
        swal("驳回申请", "请选择要驳回的申请记录", "error");
        return;
    }
    var ids = [];
    for (var i = 0; i < selectedApplyRows.length; i++) {
        if (selectedApplyRows[i].status > 1) {
            swal("驳回申请", "请选择待审核状态的申请记录", "error");
            return;
        }
        ids.push(selectedApplyRows[i].id);
    }
    if (ids.length == 0) {
        return
    }
    swal({
            title: "驳回操作",
            type: "warning",
            showCancelButton: true,
            confirmButtonColor: "#DD6B55",
            confirmButtonText: "驳回",
            closeOnConfirm: false
        },
        function () {
            $.ajax({
                url: "/lock/namespace/audit",
                type: "post",
                contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                dataType: "json",
                data: {
                    "ids": JSON.stringify(ids),
                    "status": 3
                },
                success: function (data) {
                    if (data.code === 0) {
                        swal("驳回成功！", "驳回成功!", "success");
                        $('#nspApplyLists').bootstrapTable('refresh', {url: '/lock/namespace/queryList'});
                    } else {
                        swal("驳回失败！", data.msg, "error");
                    }
                },
                error: function (res) {
                    swal("驳回失败！", "请联系管理员!", "error");
                }
            });
        });
}

//lock namespace apply
function applyNsp() {
    window.location.href = "/page/lock/applyNamespace";
}

//能看到，就能删除，支持批量【只能删除未审批的】
function deleteNsp() {
    var selectedApplyRows = $('#nspApplyLists').bootstrapTable('getSelections');
    if (selectedApplyRows.length == 0) {
        swal("驳回申请", "请选择要驳回的申请记录", "error");
        return;
    }
    var ids = [];
    for (var i = 0; i < selectedApplyRows.length; i++) {
        if (selectedApplyRows[i].status == 2) {
            swal("删除", "请选择待审核、驳回状态的记录操作", "error");
            return;
        }
        ids.push(selectedApplyRows[i].id);
    }
    if (ids.length == 0) {
        return;
    }
    swal({
            title: "删除操作",
            type: "warning",
            showCancelButton: true,
            confirmButtonColor: "#DD6B55",
            confirmButtonText: "确认",
            closeOnConfirm: false
        },
        function () {
            $.ajax({
                url: "/lock/namespace/delete",
                type: "post",
                contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                dataType: "json",
                data: {
                    "ids": JSON.stringify(ids)
                },
                success: function (data) {
                    if (data.code === 0) {
                        swal("删除成功！", data.msg, "success");
                        $('#nspApplyLists').bootstrapTable('refresh', {url: '/lock/namespace/queryList'});
                    } else {
                        swal("删除失败！", data.msg, "error");
                    }
                },
                error: function (res) {
                    swal("删除失败！", "请联系管理员!", "error");
                }
            });
        });
}