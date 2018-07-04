(function() {
	$('#metricTableEvents').bootstrapTable({
	      url: "/metric/server/getAll",
	      search: true,
	      pagination: true,
	      showRefresh: true,
	      pageNumber: 1,
	      pageSize: 10,
          pageList: [10, 25, 50, 100],
	      showColumns: true,
	      iconSize: 'outline',
	      toolbar: '#metricTableEventsToolbar',
//	      height: 500,
	      icons: {
	        refresh: 'glyphicon-repeat'
	      },
          columns:[
            {field: '',checkbox : true, align : 'center'},
            {field: 'addr',title: '地址', align : 'center'}
          ]
	    });
})(document, window, jQuery);
//用户添加展示模态框
function showServerModal(type){
	if(type == 'add'){
	    $("#addr").val("");
		$('#serverEditDetail').modal('show');
	}
};

//添加服务监控地址
function save() {
    var addr = $('#addr').val();
    //执行ajax提交
    $.ajax({
        url:"/metric/server/add",
        type:"post",
        async: false,
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
            "addr": addr
        },
        success: function(data){
            if(data.code === 0){
                swal("设置", "设置成功", "success");
                //关闭模态框
                $('#serverEditDetail').modal('hide');
                //更新页面
                $('#metricTableEvents').bootstrapTable('refresh', {url: '/metric/server/getAll'});
            }
        },
        error: function(res){
            swal("设置失败", res, "error");
        }
    });
}

//删除监控服务实例
function delMetricServer() {
	//获取用户ids
	var selectedServerRows = $('#metricTableEvents').bootstrapTable('getSelections');
    if(selectedServerRows.length <= 0){
        swal("批量删除实例", "请选择要删除的实例", "error")
        return
    }
	var serverAddrs = new Array();
    for (var i = 0; i < selectedServerRows.length; i++) {
        serverAddrs.push(selectedServerRows[i].addr);
   	}
    swal({
  	  title: "删除监控服务实例",
	  type: "warning",
	  showCancelButton: true,
	  confirmButtonColor: "#DD6B55",
	  confirmButtonText: "执行",
	  closeOnConfirm: false
  	},
	  function(){
	  	//执行ajax提交
		$.ajax({
			url:"/metric/server/del",
	        type:"post",
	        async: false,
	        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
	        dataType:"json",
	        data:{
	            "addrs":JSON.stringify(serverAddrs)
	        },
			success: function(data){
				if(data.code === 0){
					swal("删除!", "删除成功", "success");
                    window.location.reload();
	            }	    				
	        },
	        error: function(res){
	        	swal("操作失败", res, "error");
	        }
	  	});
  	});
}