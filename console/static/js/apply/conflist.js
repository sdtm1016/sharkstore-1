var pageIndex = 1;
var pageSize = 10;
(function () {
    /**
     * configure 详情列表展示
     */
    $('#confLists').bootstrapTable({
        url: getUrl(),
        striped: true,
        cache: false,
        sidePagination: "server",
        clickToSelect: true,
        iconSize: 'outline',
        toolbar: '#confListsToolbar',
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
            {
                field: 'create_time', title: '过期时间', align: 'center',
                formatter: function (value, row, index) {
                    //调用下面方法进行时间戳格式化
                    if(!hasText(value) || value == 0){
                        return "";
                    }
                    return formatDate((new Date(value)), "yyyy-MM-dd hh:mm:ss");
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
    return "/configure/queryList?clusterId=" + $('#clusterId').val()
        + "&dbName=" + $('#dbName').val()
        + "&tableName=" + $('#tableName').val()
        + "&page=" + pageIndex
        + "&rows=" + pageSize
}

function refreshLockList() {
    var opt = {
        url: getUrl(),
    };
    $("#confLists").bootstrapTable('refresh', opt);
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