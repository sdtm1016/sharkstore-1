// // 格式化日期,
// function showFormatDate(cellValue){
// 	return formatDate(new Date(cellValue),"yyyy-MM-dd hh:mm:ss");
// }

(function() {
	$('#exampleTableEvents').bootstrapTable({
	      url: "/userInfo/getPrivilegeList",
	      search: true,
	      pagination: true,
	      showRefresh: true,
	      pageNumber: 1,
	      pageSize: 10,
          pageList: [10, 25, 50, 100],
//          sidePagination: "server",
	      showColumns: true,
	      iconSize: 'outline',
	      toolbar: '#exampleTableEventsToolbar',
//	      height: 500,
	      icons: {
	        refresh: 'glyphicon-repeat'
	      },
          columns:[
            {field: 'state',checkbox : true, align : 'center'},
            {field: 'user_name',title: '用户名', align : 'center; width:8%;'},
            {field: 'cluster_id',title: '集群ID', align : 'center'},
            {field: 'privilege',title: '角色', align : 'center',
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
                }}
          ]
	    });
})(document, window, jQuery);
//用户添加展示模态框
function showUserModal(type){
	if(type == 'add'){
	    $("#erp").val("");
	    $("#clusterName").val("");
	    $("#erp").attr("readonly", false);
		$('#userEditDetail').modal('show');
	}else if(type == 'edit'){
	    $("#erp").attr("readonly", true);
		var selectedTaskRows = $('#exampleTableEvents').bootstrapTable('getSelections')[0];
    	if(typeof(selectedTaskRows) != 'undefined'){
			$('#userEditDetail').modal('show');
			//展示当前选择的用户erp
			$('#erp').val(selectedTaskRows.user_name);
			//展示当前用户使用的集群
			$('#clusterId').val(selectedTaskRows.cluster_id);
			//用户权限
			$("#roleName").val(selectedTaskRows.privilege);
		}else{
			alert("请选择一条记录");
		}
	}
};
//根据选择指定集群值
$('#roleName').change(function(){ 
	var roleId = $("#roleName").val();
	if(roleId == 1){
		$('#clusterId').val('0');
	}
});
//添加用户权限
function saveUserPrivilege() {
    var erp = $('#erp').val();
	var clusterId = $('#clusterId').val();
    var roleId = $("#roleName").val();
    //执行ajax提交
    $.ajax({
        url:"/userInfo/updatePrivilege",
        type:"post",
        async: false,
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
            "userName": erp,
            "roleId": roleId,
            "clusterId": clusterId
        },
        success: function(data){
            if(data.code === 0){
                swal("设置", "设置成功", "success");
                //关闭模态框
                $('#userEditDetail').modal('hide');
                //更新页面
                $('#exampleTableEvents').bootstrapTable('refresh', {url: '/userInfo/getPrivilegeList'});
            }
        },
        error: function(res){
            swal("设置失败", res, "error");
        }
    });
}

////保存用户erp信息到fbase表中
//function saveUserErps() {
//	//获取用户输入的erp列表
//	var erps = $('#userErpList').val();
//	//提交数据库
//	$.ajax({
//		url:"/userInfo/saveUserErp",
//		type:"post",
//        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
//        dataType:"json",
//        data:{
//        	"erps":erps
//        },
//		success: function(data){
//			if(data.code === 200){
//				//关闭模态框
//				$('#userAddDetail').modal('hide');
//				//更新页面
//				$('#exampleTableEvents').bootstrapTable('refresh', {url: '/userInfo/getUserList'});
//			}else {
//				alert(JSON.stringify(data.attach));
//			}
//        }
//	});
//}
//删除用户权限
function delPrivileges() {
	//获取用户ids
	var selectedTaskRows = $('#exampleTableEvents').bootstrapTable('getSelections');
	var privileges = new Array();
    for (var i = 0; i < selectedTaskRows.length; i++) {
        var privilege = {};
        privilege.user_name = selectedTaskRows[i].user_name;
        privilege.cluster_id = selectedTaskRows[i].cluster_id;
        privileges.push(privilege);
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
			url:"/userInfo/delPrivileges",
	        type:"post",
	        async: false,
	        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
	        dataType:"json",
	        data:{
	            "privileges":JSON.stringify(privileges)
	        },
			success: function(data){
				if(data.code === 0){
					swal("删除!", "删除成功", "success");
					$('#exampleTableEvents').bootstrapTable('refresh', {url: '/userInfo/getPrivilegeList'});
	            }	    				
	        },
	        error: function(res){
	        	swal("操作失败", res, "error");
	        }
	  	});
  	});
}