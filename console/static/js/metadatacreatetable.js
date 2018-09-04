/**
 * 数据库列表展示
 */
$(document).ready(function() {
	$.jgrid.defaults.styleUI = 'Bootstrap';
	// Configuration for jqGrid Example 1
	var clusterId = $('#clusterId').val();
	$("#db_table_list").jqGrid({
		url:'/metadata/dbDataGetAllViewPage?clusterId=' + clusterId,
		datatype: "json",
		height : "50%",
		autowidth : true,
		shrinkToFit : true,
		rowNum : 10,
		rowList : [ 10, 20, 30 ],
		colNames : [ '序号', '数据库名','操作'],
		colModel : [ 
		    {name : 'id',index:'id',align : "center",width : 20, sorttype : "int"}, 
			{name : 'name',index:'name',width : 90,align : "center"}, 
			{name:'act',index:'act', width:35,sortable:false ,align : "center"}
			],
		pager : "#db_pager_list",
		viewrecords : true,
		caption : "数据库列表",
		sortname : "id",
		sortorder: "desc",
		gridComplete: function(){
			var ids = jQuery("#db_table_list").jqGrid('getDataIDs');
			for(var i=0;i < ids.length;i++){
				var rowId = ids[i];
				var rowData = jQuery("#db_table_list").getRowData(rowId);
				var dbId = rowData.id;
				var dbName = rowData.name;
				createTable = "<button class=\"btn btn-primary\" type=\"button\" title=\"创建表\" value=\"创建表\" onclick=\"viewcreateTable("+dbId+",'"+dbName+"');\">创建表</button> &nbsp;&nbsp;&nbsp;&nbsp;";
				sqlCreateTable = "<button class=\"btn btn-primary\" type=\"button\" title=\"SQL\" value=\"SQL\" onclick=\"viewsqlCreateTable("+dbId+");\">SQL</button> &nbsp;&nbsp;&nbsp;&nbsp;";
				viewTable = "<button class=\"btn btn-primary\" type=\"button\" title=\"查看表\" value=\"查看表\" onclick=\"getTableInfo("+dbId+",'"+dbName+"');\">查看表</button>"; 
				jQuery("#db_table_list").jqGrid('setRowData',ids[i],{act:createTable+sqlCreateTable+viewTable});
			}	
		},
		hidegrid : false
	});

	// Add responsive to jqGrid
	$(window).bind('resize', function() {
		var width = $('.jqGrid_wrapper').width();
		$('#db_table_list').setGridWidth(width);
	});
});
//显示sql创建表窗口
function viewsqlCreateTable(dbId){
	//设置选中行
	$("#db_table_list").jqGrid('setSelection',dbId);
	$('#sqlCreateTableDetail').modal('show');
}
function saveSqlCreateTable() {
	var clusterId = $('#clusterId').val();
	var sql = $('#sql').val();
	//获取选中行id
	var rowId = $('#db_table_list').jqGrid('getGridParam','selrow');
	//获取选中行的数据
	var rowData = $('#db_table_list').jqGrid('getRowData',rowId);
	swal({
  	  title: "创建表?",
  	  type: "warning",
  	  showCancelButton: true,
  	  confirmButtonColor: "#DD6B55",
  	  confirmButtonText: "创建",
  	  closeOnConfirm: false
  	},
  	function(){
  	//执行ajax提交
  		$.ajax({
  			url:"/metadata/sqlCreateTable",
  			type:"post",
  	        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
  	        dataType:"json",
  	        data:{
  	        	"dbName":rowData.name,
  	        	"sql":sql,
  	        	"clusterId":clusterId
  	        },
  			success: function(data){
  				if(data.code === 0){
  					swal("创建成功!", "表创建成功", "success");
  				}else {
  					swal("创建表失败", data.msg, "error");
  				}
  				
  	        },
  	        error: function(res){
  	        	swal("创建表失败", res, "error");
  	        }
  		});
  	});
}
/**
 * 根据数据库dbName 、 dbid 创建tables
 * 跳转到tables页面
 * @param id
 */
function viewcreateTable(dbId,dbName) {
	var clusterId = $('#clusterId').val();
	window.location.href="/page/metadata/createtable?dbName="+dbName+"&dbId="+dbId+"&clusterId="+clusterId;
}

function getTableInfo(dbId,dbName) {
	var clusterId = $('#clusterId').val();
	window.location.href="/page/tables/tablelist?dbName="+dbName+"&dbId="+dbId+"&clusterId="+clusterId;
}