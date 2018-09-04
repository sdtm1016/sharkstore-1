var pageIndex = 1;
var pageSize = 10;
(function () {
    /**
     * lock 详情列表展示
     */
    $('#lockLists').bootstrapTable({
        url: getUrl(),
        striped: true,
        cache: false,
        sidePagination: "server",
        clickToSelect: true,
        iconSize: 'outline',
        toolbar: '#lockListsToolbar',
        height: 500,
        icons: {
            refresh: 'glyphicon-repeat'
        },
        columns: [
            {field: '', radio: true, align: 'center'},
            {field: 'k', title: 'key', align: 'center'},
            {field: 'v', title: '配置', align: 'center'},
            {field: 'version', title: '版本', align: 'center'},
            {field: 'extend', title: '扩展', align: 'center'},
            {field: 'lock_id', title: '锁id', align: 'center'},
            {
                field: 'expired_time', title: '过期时间', align: 'center',
                formatter: function (value, row, index) {
                    if(!hasText(value) || value == 0){
                        return "";
                    }
                    return value + "ms";
                }
            },

            {
                field: 'upd_time', title: '更新时间', align: 'center',
                formatter: function (value, row, index) {
                    //调用下面方法进行时间戳格式化
                    if(!hasText(value) || value == 0){
                        return "";
                    }
                    return formatDate((new Date(value)), "yyyy-MM-dd hh:mm:ss");
                }
            },
            {field: 'creator', title: '客户端ip', align: 'center'}
        ],
        responseHandler: function (res) {
            if (res.code === 0) {
                pageIndex = res.data.pageIndex;
                pageSize = res.data.pageSize;
                setPageDisable();
                if (res.data.total < 10) {
                    $("li.page-next").addClass("disabled");
                    $("li.page-next").find("a").removeAttr("onclick");
                }
                return {
                    "total": res.data.total,//当前页面数据
                    "rows": res.data.data   //数据
                };
            } else {
                pageIndex = 1;
                pageSize = 10;
                swal("失败", res.msg, "error");
                setPageDisable();
                return {
                    "total": 0,//总页数
                    "rows": res.data   //数据
                };
            }
        }
    });
})(document, window, jQuery);

function getUrl() {
    return "/lock/lock/queryList?clusterId=" + $('#clusterId').val()
        + "&dbName=" + $('#dbName').val()
        + "&tableName=" + $('#tableName').val()
        + "&page=" + pageIndex
        + "&rows=" + pageSize
}

//能看到记录，就能修改
function forceUnLock() {
    var selectedApplyRows = $('#lockLists').bootstrapTable('getSelections');
    if (selectedApplyRows.length != 1) {
        swal("强制解锁", "请选择且只选单个要强制解锁的记录", "error");
        return;
    }
    var key = selectedApplyRows[0].k;
    var clusterId = $("#clusterId").val();
    var dbName = $("#dbName").val();
    var tableName = $("#tableName").val();
    if (!hasText(key) || !hasText(clusterId) || !hasText(dbName) || !hasText(tableName)) {
        swal("强制解锁", "请选择要解锁的lock", "error");
        return;
    }
    swal({
            title: "强制解锁",
            type: "warning",
            showCancelButton: true,
            confirmButtonColor: "#DD6B55",
            confirmButtonText: "确认",
            closeOnConfirm: false
        },
        function () {
            $.ajax({
                url: "/lock/lock/forceUnLock",
                type: "post",
                contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                dataType: "json",
                data: {
                    clusterId: clusterId,
                    dbName: dbName,
                    tableName: tableName,
                    key: key
                },
                success: function (data) {
                    if (data.code === 0) {
                        swal("强制解锁成功!", data.msg, "success");
                        window.location.href = "/page/lock/viewLock?clusterId=" + clusterId + "&dbName=" + dbName + "&tableName=" + tableName;
                    } else {
                        swal("强制解锁失败", data.msg, "error");
                    }
                },
                error: function (res) {
                    swal("强制解锁失败", res, "error");
                }
            });
        });
}

function refreshLockList() {
    var opt = {
        url: getUrl(),
    };
    $("#lockLists").bootstrapTable('refresh', opt);
}

function refreshPageFirst() {
    pageIndex = 1;
    refreshLockList();
}

function refreshPagePre() {
    if (pageIndex > 1) {
        pageIndex = pageIndex - 1;
    }
    refreshLockList();
}

function refreshPageNext() {
    pageIndex = pageIndex + 1;
    refreshLockList();
}

function setPageDisable() {
    $("#currentPage").text(pageIndex);
    var first = $("li.page-first");
    var pre = $("li.page-pre");
    var next = $("li.page-next");
    if (pageIndex == 1) {
        first.addClass("disabled");
        pre.addClass("disabled");
        first.find("a").removeAttr("onclick");
        pre.find("a").removeAttr("onclick");
        next.find("a").attr("onclick", "refreshPageNext();");
    } else if (pageIndex > 1) {
        first.removeClass("disabled");
        pre.removeClass("disabled");
        next.removeClass("disabled");
        first.find("a").attr("onclick", "refreshPageFirst();");
        pre.find("a").attr("onclick", "refreshPagePre();");
        next.find("a").attr("onclick", "refreshPageNext();");
    }
}