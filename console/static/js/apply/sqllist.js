(function () {
    $('#sqlApplyLists').bootstrapTable({
        url: "/sql/queryApplyList",
        striped: true,
        cache: false,
        // search: true,
        pagination: true,
        pageNumber: 1,
        pageSize: 10,
        pageList: [10, 25, 50, 100],
        sidePagination: "server",
        clickToSelect: true,
        // showColumns: true,
        // showRefresh: true,
        iconSize: 'outline',
        toolbar: '#sqlApplyListsToolbar',
        height: 600,
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
            {field: 'applyer', title: '申请人', align: 'center'},
            {
                field: 'create_time', title: '申请时间', align: 'center',
                formatter: function (value, row, index) {
                    //调用下面方法进行时间戳格式化
                    return formatDate((new Date(value * 1000)), "yyyy-MM-dd hh:mm:ss");
                }
            },
            {field: 'remark', title: '备注', align: 'center'},
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
                    return "<button id=\"showDetail\" class=\"btn btn-primary btn-rounded\" type=\"button\" value==\"详情\" onclick=\"showDetail('" + row.id + "');\">详情</button>&nbsp;&nbsp;";
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

//展示模态框
function applySql() {
    $("#dbName").val("");
    $("#tableName").val("");
    $("#sentence").val("");
    $("#remark").val("");
    $("#saveButton").attr("style", "");
    $('#sqlApplyModal').modal('show');
}

//添加申请
function saveSqlApply() {
    var dbName = $('#dbName').val();
    var tableName = $('#tableName').val();
    var sentence = $("#sentence").val();
    if (!hasText(dbName) || !hasText(tableName) || !hasText(sentence)) {
        swal("申请", "请先填写库名、表名、sql语句", "error");
        return
    }
    var remark = $("#remark").val();
    //执行ajax提交
    $.ajax({
        url: "/sql/apply",
        type: "post",
        async: false,
        contentType: "application/x-www-form-urlencoded; charset=UTF-8",
        dataType: "json",
        data: {
            "dbName": dbName,
            "tableName": tableName,
            "sentence": sentence,
            "remark": remark
        },
        success: function (data) {
            if (data.code === 0) {
                swal("设置", "设置成功", "success");
                //关闭模态框
                $('#sqlApplyModal').modal('hide');
                //更新页面
                $('#sqlApplyLists').bootstrapTable('refresh', {url: '/sql/queryApplyList'});
            }
        },
        error: function (res) {
            swal("设置失败", res, "error");
        }
    });
}

function showDetail(id) {
    $.ajax({
        url: "/sql/apply/detail?id=" + id,
        type: "get",
        async: false,
        success: function (data) {
            if (data.code === 0) {
                $("#dbName").val(data.data.db_name);
                $("#tableName").val(data.data.table_name);
                $("#sentence").val(data.data.sentence);
                $("#remark").val(data.data.remark);
                $("#saveButton").attr("style", "display:none");
                $('#sqlApplyModal').modal('show');
            }
        },
        error: function (res) {
            swal("设置失败", res, "error");
        }
    });

}

//审批
function auditSql() {
    var selectedApplyRows = $('#sqlApplyLists').bootstrapTable('getSelections');
    if (selectedApplyRows.length == 0) {
        swal("审批申请", "请选择要审批的申请记录", "error")
        return
    }
    var ids = [];
    for (var i = 0; i < selectedApplyRows.length; i++) {
        if (selectedApplyRows[i].status > 1) {
            swal("审批申请", "请选择待审核状态的申请记录", "error")
            return
        }
        ids.push(selectedApplyRows[i].id);
    }
    if (ids.length == 0) {
        return
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
                url: "/sql/audit",
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
                        $('#sqlApplyLists').bootstrapTable('refresh', {url: '/sql/queryApplyList'});
                    } else {
                        swal("审批失败！", data.message, "error");
                    }
                },
                error: function (res) {
                    swal("审批失败！", "请联系管理员!", "error");
                }
            });
        });
}

//驳回，支持批量
function rejectSql() {
    var selectedApplyRows = $('#sqlApplyLists').bootstrapTable('getSelections');
    if (selectedApplyRows.length == 0) {
        swal("驳回申请", "请选择要驳回的申请记录", "error")
        return
    }
    var ids = [];
    for (var i = 0; i < selectedApplyRows.length; i++) {
        if (selectedApplyRows[i].status > 1) {
            swal("驳回申请", "请选择待审核状态的申请记录", "error")
            return
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
                url: "/sql/audit",
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
                        $('#sqlApplyLists').bootstrapTable('refresh', {url: '/sql/queryApplyList'});
                    } else {
                        swal("驳回失败！", data.message, "error");
                    }
                },
                error: function (res) {
                    swal("驳回失败！", "请联系管理员!", "error");
                }
            });
        });
}