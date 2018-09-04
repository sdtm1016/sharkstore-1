// 格式化日期,
function showFormatDate(cellValue){
	return formatDate(new Date(cellValue),"yyyy-MM-dd hh:mm:ss");  
}

(function() {
	$('#exampleTableEvents').bootstrapTable({
	      url: "/userInfo/getUserList",
	      search: true,
	      pagination: true,
	      showRefresh: true,
	      pageNumber: 1,
	      pageSize: 10,  
          pageList: [10, 25, 50, 100],
          sidePagination: "server",
	      showColumns: true,
	      iconSize: 'outline',
	      toolbar: '#exampleTableEventsToolbar',
	      height: 500,
	      icons: {
	        refresh: 'glyphicon-repeat'
	      },
	      columns:[
	        {field: 'state',checkbox : true, align : 'center'},
	        {field: 'id',title: 'ID', align : 'center; width:5%;'},
	        {field: 'userName',title: '用户名', align : 'center; width:8%;'},
	        {field: 'erp',title: 'ERP', align : 'center; width:8%;'},
	        {field: 'mail',title: '邮箱', align : 'center; width:10%;'},
	        {field: 'createTime',
	        	title: '创建时间', 
	        	align : 'center; width:10%;',
	        	formatter : function(cellValue) {
				// 调用下面方法进行时间戳格式化
				return showFormatDate(cellValue);
			}},
	        {field: 'userAuthority.rid',title: '角色', align : 'center',
	        	formatter : function(cellValue) {
	        		if(cellValue == 1){
	        			return "系统管理员";
	        		}
	        		if(cellValue == 2){
	        			return "集群管理员";
	        		}
	        		if(cellValue == 3){
	        			return "普通用户";
	        		}
				}},
	        {field: 'userAuthority.cid',title: '集群ID', align : 'center'}
	        ]
	    });
})(document, window, jQuery);
//用户添加展示模态框
function showUserModal(type){
	if(type == 'add'){
		$('#userAddDetail').modal('show');
	}
	if(type == 'edit'){
		var selectedTaskRows = $('#exampleTableEvents').bootstrapTable('getSelections')[0];
    	if(typeof(selectedTaskRows) != 'undefined'){
			$('#userEditDetail').modal('show');
			//展示当前选择的用户erp
			$('#erp').val(selectedTaskRows.erp);
			//展示当前用户使用的集群
			$('#clusterName').val('1,2,3');
			//用户权限
			$("#roleName").get(0).options[0].selected = true;  
		}else{
			alert("请选择一个用户");
		}
	}
};
//根据选择指定集群值
$('#roleName').change(function(){ 
	var roleId = $("#roleName").val();
	if(roleId == 1){
		$('#clusterName').val('all');
	}
});
//添加用户权限和集群
function saveUserAuthority() {
	var selectedTaskRows = $('#exampleTableEvents').bootstrapTable('getSelections')[0];
	if(typeof(selectedTaskRows) != 'undefined'){
		var clusterIds = $('#clusterName').val();
		var roleId = $("#roleName").val();
		//执行ajax提交
		$.ajax({
			url:"/userInfo/addUserAuthority",
	        type:"post",
	        async: false,
	        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
	        dataType:"json",
	        data:{
	            "userId":selectedTaskRows.id,
	            "roleId":roleId,
	            "clusterIds":clusterIds
	        },
			success: function(data){
				if(data.code === 200){
					swal("设置", "设置成功", "success");
					//关闭模态框
					$('#userEditDetail').modal('hide');
					//更新页面
					$('#exampleTableEvents').bootstrapTable('refresh', {url: '/userInfo/getUserList'});
	            }	    				
	        },
	        error: function(res){
	        	swal("设置失败", res, "error");
	        }
	  	});
	}
}

//保存用户erp信息到fbase表中
function saveUserErps() {
	//获取用户输入的erp列表
	var erps = $('#userErpList').val();
	//提交数据库
	$.ajax({
		url:"/userInfo/saveUserErp",
		type:"post",
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
        	"erps":erps
        },
		success: function(data){
			if(data.code === 200){
				//关闭模态框
				$('#userAddDetail').modal('hide');
				//更新页面
				$('#exampleTableEvents').bootstrapTable('refresh', {url: '/userInfo/getUserList'});  
			}else {
				alert(JSON.stringify(data.attach));
			}
        }
	});
}
//根据用户id删除用户
function delUserErps() {
	//获取用户ids
	var selectedTaskRows = $('#exampleTableEvents').bootstrapTable('getSelections');
	var ids = [];
    for (var i = 0; i < selectedTaskRows.length; i++) {
		ids.push(selectedTaskRows[i].id);
	}
    swal({
  	  title: "删除用户",
	  type: "warning",
	  showCancelButton: true,
	  confirmButtonColor: "#DD6B55",
	  confirmButtonText: "执行",
	  closeOnConfirm: false
  	},
	  function(){
	  	//执行ajax提交
		$.ajax({
			url:"/userInfo/delUserInfoById",
	        type:"post",
	        async: false,
	        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
	        dataType:"json",
	        data:{
	            "userIds":JSON.stringify(ids)
	        },
			success: function(data){
				if(data.code === 200){
					swal("删除!", "删除成功", "success");
					$('#exampleTableEvents').bootstrapTable('remove', {field: 'id', values: ids});
	            }	    				
	        },
	        error: function(res){
	        	swal("操作失败", res, "error");
	        }
	  	});
  	});
}