$(function () {
    //监控日期选择初始化功能
    datePairs($('#start'), $('#end'));
    //按钮切换功能
    $(".panel-tab-form").children("div").click(function () {
        var i = $(this).index();
        $(this).addClass('active').siblings().removeClass('active');
        $(this).parents('.panel-tab').children('.panel-body-tab').eq(i).show().siblings('.panel-body-tab').hide();
        if ($('.topology').css('display') == 'none') {
            $('svg').hide();
        } else {
            $('svg').show();
        }
    });
});

function formatDate(formatDate) {
    function padding(str) {
        if (str > 0 && str < 10) {
            return '0' + str;
        } else {
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

function datePairs(date1, date2, start, maxDate) {
    var nowdate = new Date(),
        startDate = new Date(nowdate.getTime() - (maxDate || 1000 * 60 * 60 * 24 * 30)),
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
        .on('changeDate', function (ev) {
            if (date1.val() > date2.val()) {
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
    }).on('changeDate', function (ev) {
        if (date1.val() > date2.val()) {
            alert("束时间不能小于开始时间");
            date2.val(date1.val());
            date2.datetimepicker('update');
        }
    });
    date1.val(defaultStart).datetimepicker('update');
    date2.val(defaultEnd).datetimepicker('update');
}

app = angular.module('rangeTopo', []);
app.controller('ParentCtrl', ['$rootScope', '$scope', '$http', '$timeout', function ($rootScope, $scope, $http, $timeout) {
    $scope.$on('subChange', function (event, ipport, rangeId, peerId) {
        $scope.ipport = ipport;
        $scope.rangeId = rangeId;
        $scope.peerId = peerId;
        $scope.$broadcast('subChangeP', ipport, rangeId, peerId);
    });
}]);
app.controller('rangeTopoCtrl', ['$rootScope', '$scope', '$http', '$timeout', function ($rootScope, $scope, $http, $timeout) {
    var myJsPlumb = jsPlumb.getInstance();
    myJsPlumb.importDefaults({
        Anchors: ["TopCenter", "BottomCenter"],
        Endpoint: "Blank",
        Connector: "Straight",
        PaintStyle: {lineWidth: 2, strokeStyle: "#6f6f6f"},
        HoverClass: "connector-hover",
        HoverPaintStyle: {strokeStyle: "#ff0"},
        ConnectionOverlays: [
            ["PlainArrow", {
                location: 1,
                width: 10,
                length: 10,
                visible: true,
                id: "PlainArrow"
            }],
            ["Label", {
                cssClass: "l1 component label",
                label: function () {
                    return ""
                },
                location: 0.7,
                id: "label",
                events: {
                    "click": function (label, evt) {
                        console.log("click label");
                    }
                }
            }]
        ]
    });

    //做连接线
    function drawLine(nodes) {
        var lis = nodes.children('ul').children('li');
        if (lis.length) {
            lis.each(function (i, item) {
                myJsPlumb.connect({
                    source: $(item),
                    target: nodes.children('.topology-ip')
                });
                drawLine($(item));
            });
        }
    }

    var dbName = $("#dbName").val();
    var tableName = $("#tableName").val();
    var clusterId = $("#clusterId").val();
    var rangeId = $("#rangeId").val();
    var source = $("#source").val();
    var getRangeAllUrl = "/range/getRangeByBbTable?dbName=" + dbName + "&tableName=" + tableName + "&clusterId=" + clusterId;
    if (source == "range") {
        getRangeAllUrl = "/range/getRangeTopoByRange?rangeId=" + rangeId + "&clusterId=" + clusterId;
    } else if (source == "node") {
        getRangeAllUrl = "/node/getRangeTopoByNode?nodeId=" + rangeId + "&clusterId=" + clusterId;
    }
    $scope.init = function () {
        $http.get(getRangeAllUrl).success(function (data) {
            if (data.code === 0) {
                rangeTree = data.data;
                //给拓扑图view使用
                $rootScope.rangeTreeArr = [];
                if (Array.isArray(rangeTree)) {
                    for (var i = 0; i < rangeTree.length; i++) {
                        $rootScope.rangeTreeArr.push(rangeTree[i]);
                    }
                }else {
                    $rootScope.rangeTreeArr.push(rangeTree);
                }
            }
        });
    };
    $scope.init();
    $scope.$on('ngRepeatFinished', function (event, elem) {
        $scope.$emit('topoFinish');
        $('#topology').children('li').each(function (i, item) {
            drawLine($(item));
        });
        $(window).on('resize', function () {
            $(elem).children('li').each(function (i, item) {
                drawLine($(item));
            });
            if ($('#topology').parent('.topology').css('display') == 'none') {
                $('svg').hide();
            }
        });
//	    $(elem).find('strong').click(function(){
//			alert("");
//		});
    });
    $scope.showRangeInfo = function (rangeLeaderRoot, type) {
        $('#subDetail').modal('show', {backdrop: 'static'});
        if (type == 'leader') {
            var ipport = rangeLeaderRoot.leader.node.server_addr;
            var peerId = rangeLeaderRoot.leader.node.id;
            var rangeId = rangeLeaderRoot.leader.id;
            //把IP和端口发送的父controller，接受参数的 名字是subChange
            $scope.$emit('subChange', ipport, rangeId, peerId);
        } else {
            var ipport = rangeLeaderRoot.node.server_addr;
            var peerId = rangeLeaderRoot.node.id;
            var rangeId = rangeLeaderRoot.id;
            //把IP和端口发送的父controller，接受参数的 名字是subChange
            $scope.$emit('subChange', ipport, rangeId, peerId);
        }
    };
    //展示拓扑图下的按钮
    $scope.checkShard = function (spaceroot, elem) {
        $timeout.cancel(spaceroot.timer);
        spaceroot.statusBtn = true;
    };

    $scope.checkShardHide = function (spaceroot) {
        spaceroot.timer = $timeout(function () {
            spaceroot.statusBtn = false;
        }, 1000);
    };
    //leader分片分裂
    $scope.splitInstance = function (rangeLeaderRoot) {
        var clusterId = $('#clusterId').val();
        var dbName = $("#dbName").val();
        var tableName = $("#tableName").val();
        swal({
                title: "分裂?",
                type: "warning",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "分裂",
                closeOnConfirm: false
            },
            function () {
                //执行ajax提交
                $.ajax({
                    url: "/range/split",
                    type: "post",
                    contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                    dataType: "json",
                    data: {
                        "clusterId": clusterId,
                        "rangeId": rangeLeaderRoot.leader.id,
                        "dbName": dbName,
                        "tableName": tableName,
                    },
                    success: function (data) {
                        if (data.attach.code === 0) {
                            swal("分裂成功!", data.attach.message, "success");
                        } else {
                            swal("分裂失败", data.attach.message, "error");
                        }

                    },
                    error: function (res) {
                        swal("分裂失败", res, "error");
                    }
                });
            });
    };
    //leader删除分片实例
    $scope.moveInstance = function (rangeLeaderRoot) {
        var clusterId = $('#clusterId').val();
        swal({
                title: "删除分片?",
                type: "warning",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "删除分片",
                closeOnConfirm: false
            },
            function () {
                //执行ajax提交
                $.ajax({
                    url: "/range/peerDel",
                    type: "post",
                    contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                    dataType: "json",
                    data: {
                        "clusterId": clusterId,
                        "rangeId": rangeLeaderRoot.range.id,
                        "peerId": rangeLeaderRoot.leader.id
                    },
                    success: function (data) {
                        if (data.code === 0) {
                            swal("删除成功!", data.message, "success");
                        } else {
                            swal("删除失败", data.message, "error");
                        }

                    },
                    error: function (res) {
                        swal("删除失败", res, "error");
                    }
                });
            });
    };
    //添加副本
    $scope.addInstance = function (rangeLeaderRoot) {
        var clusterId = $('#clusterId').val();
        swal({
                title: "添加副本?",
                type: "warning",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "添加副本",
                closeOnConfirm: false
            },
            function () {
                //执行ajax提交
                $.ajax({
                    url: "/range/peerAdd",
                    type: "post",
                    contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                    dataType: "json",
                    data: {
                        "clusterId": clusterId,
                        "rangeId": rangeLeaderRoot.range.id,
                    },
                    success: function (data) {
                        if (data.code === 0) {
                            swal("添加成功!", "添加成功", "success");
                            window.location.reload();
                        } else {
                            swal("添加失败", data.message, "error");
                        }
                    },
                    error: function (res) {
                        swal("添加失败", res, "error");
                    }
                });
            });
    };
    //offline range
    $scope.offlineRange = function (rangeLeaderRoot) {
        var clusterId = $('#clusterId').val();
        var dbName = $("#dbName").val();
        var tableName = $("#tableName").val();
        swal({
                title: "下线range?",
                type: "warning",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "下线range",
                closeOnConfirm: false
            },
            function () {
                //执行ajax提交
                $.ajax({
                    url: "/range/offlineRange",
                    type: "post",
                    contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                    dataType: "json",
                    data: {
                        "clusterId": clusterId,
                        "dbName": dbName,
                        "tableName": tableName,
                        "rangeId": rangeLeaderRoot.range.id,
                        "peerId": rangeLeaderRoot.leader.id
                    },
                    success: function (data) {
                        if (data.code === 0) {
                            swal("下线成功!", "下线成功!", "success");
                            window.location.reload();
                        } else {
                            swal("下线失败", data.message, "error");
                        }
                    },
                    error: function (res) {
                        swal("下线失败", "请联系管理员!", "error");
                    }
                });
            });
    };
    //改变副本为主 changeLeaderInstance(rangeReplicate)
    $scope.changeLeaderInstance = function (rangeLeaderRoot, rangeId) {
        var clusterId = $('#clusterId').val();
        swal({
                title: "切换为主?",
                type: "warning",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "切换为主",
                closeOnConfirm: false
            },
            function () {
                //执行ajax提交
                $.ajax({
                    url: "/range/changeLeader",
                    type: "post",
                    contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                    dataType: "json",
                    data: {
                        "clusterId": clusterId,
                        "rangeId": rangeId,
                        "peerId": rangeLeaderRoot.id
                    },
                    success: function (data) {
                        if (data.code === 0) {
                            swal("切换成功!", "切换成功", "success");
                        } else {
                            swal("切换失败", data.message, "error");
                        }
                    },
                    error: function (res) {
                        swal("切换失败", res, "error");
                    }
                });
            });
    };
    //删除副本 moveReplicateInstance(rangeReplicate)
    $scope.moveReplicateInstance = function (rangeLeaderRoot, rangeId) {
        var clusterId = $('#clusterId').val();
        swal({
                title: "删除副本?",
                type: "warning",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "删除副本",
                closeOnConfirm: false
            },
            function () {
                //执行ajax提交
                $.ajax({
                    url: "/range/peerDel",
                    type: "post",
                    contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                    dataType: "json",
                    data: {
                        "clusterId": clusterId,
                        "rangeId": rangeId,
                        "peerId": rangeLeaderRoot.id
                    },
                    success: function (data) {
                        if (data.code === 0) {
                            swal("删除成功!", data.message, "success");
                        } else {
                            swal("删除失败", data.message, "error");
                        }

                    },
                    error: function (res) {
                        swal("删除失败", res, "error");
                    }
                });
            });
    };
    //迁移副本 transferInstance(rangeReplicate)
    $scope.transferInstance = function (rangeLeaderRoot, rangeId) {
        var clusterId = $('#clusterId').val();
        swal({
                title: "迁移?",
                type: "warning",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "迁移",
                closeOnConfirm: false
            },
            function () {
                //执行ajax提交
                $.ajax({
                    url: "/range/transfer",
                    type: "post",
                    contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                    dataType: "json",
                    data: {
                        "clusterId": clusterId,
                        "rangeId": rangeId,
                        "peerId": rangeLeaderRoot.id
                    },
                    success: function (data) {
                        if (data.code === 0) {
                            swal("迁移成功!", "迁移成功", "success");
                        } else {
                            swal("迁移失败", data.message, "error");
                        }
                    },
                    error: function (res) {
                        swal("迁移失败", res, "error");
                    }
                });
            });
    };
}]);
//执行 画图完成后的连线和事件动作
app.directive('onFinishRender', ['$timeout', function ($timeout) {
    return {
        restrict: 'A',
        link: function (scope, elem, attr) {
            if (scope.$last === true) {
                $timeout(function () {
                    scope.$emit('ngRepeatFinished', $(elem).parent('.repeatParent'));
                });
            }
        }
    };
}]);
//range详细信息展示操作 js
app.controller('jsplumbmoreCtrl', ['$rootScope', '$scope', '$http', '$timeout', function ($rootScope, $scope, $http, $timeout) {
    $scope.$on('subChangeP', function (event, ipport, rangeId, peerId) {
        $scope.ipport = ipport;
        $scope.rangeId = rangeId;
    });
}]);
//分片详细信息
app.controller('subdetailCtrl', ['$rootScope', '$scope', '$http', '$timeout', function ($rootScope, $scope, $http, $timeout) {
    $scope.$on('subChangeP', function (event, ipport, rangeId, peerId) {
        var dbName = $('#dbName').val();
        var tableName = $('#tableName').val();
        var clusterId = $('#clusterId').val();
        $http.get('/range/getRangeInfoById?rangeId=' + $scope.rangeId + '&peerId=' + peerId + '&dbName=' + dbName + '&tableName=' + tableName + '&clusterId=' + clusterId).success(function (data) {
            if (data.code === 200) {
                $scope.detail = data.attach;
                $scope.detailkeys = Object.keys($scope.detail);
                $scope.detailkeys.forEach(function (item, index) {
                    $scope.detail[item + 'subkeys'] = Object.keys($scope.detail[item]);
                });
            } //if end
        })//$http end
    });//subchangep end
}]);
app.controller('monitorCtrl', ['$rootScope', '$scope', '$http', '$timeout', function ($rootScope, $scope, $http, $timeout) {
    $scope.$on('subChangeP', function (event, ipport, rangeId) {
        $scope.rangeId = rangeId;
        //ops
        var options_ops = {
            chart: {
                renderTo: 'container_ops',
                type: 'spline'
            },
            title: {
                text: '分片 访问量监控'
            },
            xAxis: {
                type: 'datetime',
                dateTimeLabelFormats: {
                    millisecond: '%Y-%m-%d, %H:%M:%S',
                    day: '%Y-%m-%d'
                }
            },
            yAxis: {
                title: {
                    text: 'ops(times)'
                },
                labels: {//刻度
                    formatter: function () {
                        return this.value;
                    },
                    style: {
                        color: '#89A54E'
                    }
                },
                min: 0
            },
            tooltip: {
                headerFormat: '<b>{series.name}</b><br>',
                pointFormat: '{point.x:%e. %b}: {point.y:.2f} 次'
            },
            plotOptions: {
                series: {
                    turboThreshold: 0
                },
                spline: {
                    marker: {
                        enabled: true
                    }
                }
            },
            series: []
        };
        //size
        var options_size = {
            chart: {
                renderTo: 'container_size',
                type: 'areaspline'
            },
            title: {
                text: '分片占用空间统计'
            },
            legend: {
                layout: 'vertical',
                align: 'left',
                verticalAlign: 'top',
                x: 150,
                y: 100,
                floating: true,
                borderWidth: 1,
                backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
            },
            xAxis: {
                type: 'datetime',
                dateTimeLabelFormats: {
                    millisecond: '%Y-%m-%d, %H:%M:%S',
                    day: '%Y-%m-%d'
                }
            },
            yAxis: {
                title: {
                    text: '空间 单位'
                }
            },
            tooltip: {
                shared: true,
                valueSuffix: ' 单位'
            },
            credits: {
                enabled: false
            },
            plotOptions: {
                areaspline: {
                    fillOpacity: 0.5
                }
            },
            series: []
        };
        var options_outper_inper = {
            chart: {
                renderTo: 'container_outper_inper',
                type: 'spline'
            },
            title: {
                text: '分片出入流量监控'
            },
            xAxis: {
                type: 'datetime',
                dateTimeLabelFormats: {
                    millisecond: '%Y-%m-%d, %H:%M:%S',
                    day: '%Y-%m-%d'
                }
            },
            yAxis: {
                title: {
                    text: '出入流量 '
                },
                min: 0
            },
            tooltip: {
                headerFormat: '<b>{series.name}</b><br>',
                pointFormat: '{point.x:%e. %b}: {point.y:.2f} 流量'
            },
            plotOptions: {
                series: {
                    turboThreshold: 0
                },
                spline: {
                    marker: {
                        enabled: true
                    }
                }
            },
            series: []
        };
        //
        $scope.queryFilter = function () {
            var type = "setOptions";
            load_monitor_data($scope.rangeId, options_ops, options_outper_inper, options_size, type);
        };
        //加载数据
        var type = "defaultOptions";
        load_monitor_data($scope.rangeId, options_ops, options_outper_inper, options_size, type);

        function load_monitor_data(rangeId, options_ops, options_outper_inper, options_size, type) {
            Highcharts.setOptions({
                global: {
                    useUTC: false
                }
            });
            //初始化
            var chart_ops = new Highcharts.Chart(options_ops);
            var chart_outper_inper = new Highcharts.Chart(options_outper_inper);
            var chart_size = new Highcharts.Chart(options_size);
            //获取集群id
            var clusterId = $('#clusterId').val();
            var startTime = "";
            var endTime = "";
            if (type != "defaultOptions") {
                startTime = $("#start").val();
                endTime = $("#end").val();
            }
            chart_ops.showLoading("Loading....");
            chart_outper_inper.showLoading("Loading....");
            chart_size.showLoading("Loading....");
            $.ajax({
                url: "/monitor/getRangeData",
                type: "post",
                dataType: 'json',
                contentType: "application/x-www-form-urlencoded; charset=UTF-8",
                data: {
                    "clusterId": clusterId,
                    "rangeId": rangeId,
                    "startTime": startTime,
                    "endTime": endTime
                },
                success: function (data) {
                    data = data.attach;
                    if (data == null) {
                        chart_ops.showLoading("没有数据!");
                        chart_outper_inper.showLoading("没有数据!");
                        chart_size.showLoading("没有数据!");
                    } else {
                        //db ops
                        data.ops.name = "分片ID:" + data.ops.name;
                        chart_ops.addSeries(data.ops);
                        //出流量
                        data.bytes_out_per_sec.name = "分片ID:" + data.bytes_out_per_sec.name + " 出流量";
                        //入流量
                        data.bytes_in_per_sec.name = "分片ID:" + data.bytes_in_per_sec.name + " 入流量";
                        chart_outper_inper.addSeries(data.bytes_out_per_sec);
                        chart_outper_inper.addSeries(data.bytes_in_per_sec);
                        //db 占用磁盘大小
                        data.size.name = "分片ID:" + data.size.name;
                        chart_size.addSeries(data.size);
                        //关闭loading
                        chart_ops.hideLoading();
                        chart_outper_inper.hideLoading();
                        chart_size.hideLoading();
                    }
                },
                error: function (res) {

                }
            });
        }
    });//subchangep end
}]);


