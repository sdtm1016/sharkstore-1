$(document).ready(function() {
	//var data = [{"name":"size","data_type":4},{"name":"ops","id":1,"data_type":4,"primary_key": 1},{"name":"bytes_in_per_sec","id":2,"data_type":4},{"name":"bytes_out_per_sec","id":3,"data_type":4},{"name":"rangeid","id":4,"data_type":4,"primary_key":1},{"name":"time","id":5,"data_type":4,"primary_key":1}];
	//库名
	var dbName = $("#dbName").val();
	//表名
	var name = $("#name").val();
	
	var clusterId = $('#clusterId').val();
	//执行ajax提交
	var columnsArray = new Array();
	var regxsArray = new Array();
	$.ajax({
		url:"/metadata/getTableColumns",
		type:"post",
		async: false,
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
        	"dbName":dbName,
        	"name":name,
        	"clusterId":clusterId
        },
		success: function(data){
		    console.log("--------table detail "+ data);
			if(data.code === 0){
				for(var i=0;i<data.data.columns.length;i++){
					columnsArray.push(data.data.columns[i]);
				}
				if(data.data.regxs != null){
					for(var i=0;i<data.data.regxs.length;i++){
						regxsArray.push(data.data.regxs[i]);
					}
				}
			}else {
				swal("获取失败", data.msg, "error");
			}
			
        },
        error: function(res){
        	swal("获取失败", res, "error");
        }
	});
        var dataTypes = [{
        	name: "Tinyint",
            value: 1
        }, {
            name: "Smallint",
            value: 2
        }, {
            name: "Int",
            value: 3
        }, {
            name: "BigInt",
            value: 4
        }, {
            name: "Float",
            value: 5
        }, {
            name: "Double",
            value: 6
        }, {
            name: "Varchar",
            value: 7
        }, {
            name: "Binary",
            value: 8
        }, {
            name: "Date",
            value: 9
        }, {
            name: "TimeStamp",
            value: 10
        }];
        $("#jsGrid").jsGrid({
            width: "100%",
            data: columnsArray, // 数据
            editing: true,
            inserting: true,
            fields: [{
                title: '列名',
                name: "name",
                type: "text",
                validate: "required",
                width: 100, align: "center"
            }, {
            	title: '类型',
            	name: "data_type",
                type: "select",
                items: dataTypes,
                valueField: "value",
                textField: "name",
                editing: false,
                width: 80, align: "center"            
            // }, {
            //     title: '默认值',
            //     name: "default_value",
            //     type: "text",
            //     width: 100,
            //     editing: false,
            //     align: "center"
            }, {
            	title: "Unsigned",
                name: "unsigned",
                type: "checkbox",
                editing: false,
                sorting: false
            },{
            	title: "是否为主键",
                name: "primary_key",
                type: "checkbox",
                width: 80,
                editing: false,
                align: "center"
            },{
                title: "自增id",
                name: "auto_increment",
                type: "checkbox",
                width: 80,
                editing: false,
                align: "center"
            },{
            	title: "动态列",
                name: "regxs",
                type: "checkbox",
                editing: false,
                sorting: false
            },{
                type: "control", 
                editButton: false, 
                modeSwitchButton: false,
                headerTemplate: function() {
                    return $("<button class=\"btn btn-primary\">").attr("type", "button").text("提交")
                    .on("click", function () {
                    	createDataBaseTable(columnsArray);
                    });
            }
            }]
        });
    });
//正则去掉空格
function myTrim(x) {
    return x.replace(/^\s+|\s+$/gm,'');
}
function createDataBaseTable(data) {
	if(data.length == 0){
		swal("至少添加一列");
		return false;
	}
    //验证，主键 至少一个主键
    var pkFlg = false;
    //验证，自增只能是主键，且只能是int类型能有一个字段
    var incFlg = true;
    //验证，自增且只能存在一列
    var incCount = 0;

    for(var i =0;i<data.length;i++){
        if(data[i].primary_key && !pkFlg){
            pkFlg = true;
        }
        if(data[i].auto_increment && (!isIntType(data[i].data_type || !data[i].primary_key ))){
            incFlg = false;
            break
        }
        if(data[i].primary_key && data[i].auto_increment) {
            incCount++
        }

    }
    if(!pkFlg){
        swal("至少有一个主键");
        return false;
    }
    if (!incFlg) {
        swal("只能设置int类型的主键为自增列");
        return false;
    }
    if (incCount > 1){
        swal("只能有一个int类型的列为自增列");
        return false;
    }
	//提取宽表
	var regxsList = [];
	for(var i =0;i<data.length;i++){
		//列名不能有中横线“-”
		var columnName = data[i].name;
		if(columnName.indexOf("-") >= 0){
			swal("列名不能有“-”");
			return false;
		}
		data[i].index=true;
		//主键
		if(data[i].primary_key){
			data[i].primary_key=1;
		}else{
			data[i].primary_key=0;
		}
		if(data[i].regxs == 1){
			regxsList.push(data[i]);
			data.splice(i,1);
		}
	}
	//预分裂key中的内容是以“,”分隔的
	var rangeKeys = $("#rangeKeys").val();
	rangeKeys = myTrim(rangeKeys);
	//库名
	var dbName = $("#dbName").val();
	//表名
	var name = $("#name").val();
	//集群id
	var clusterId = $('#clusterId').val();
	//验证表名
    if(name == undefined || name == null || name == "") {
    	swal("表名必须填写");
        return false;
    }
    swal({
    	  title: "修改表?",
    	  type: "warning",
    	  showCancelButton: true,
    	  confirmButtonColor: "#DD6B55",
    	  confirmButtonText: "修改",
    	  closeOnConfirm: false
    	},
    	function(){
    	//执行ajax提交
    		$.ajax({
    			url:"/metadata/editTable",
    			type:"post",
    	        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
    	        dataType:"json",
    	        data:{
    	        	"dbName":dbName,
    	        	"name":name,
    	        	"columns":JSON.stringify(data),
    	        	"regxs":JSON.stringify(regxsList),
    	        	"rangeKeys":rangeKeys,
    	        	"clusterId":clusterId
    	        },
    			success: function(data){
    				if(data.code === 0){
    					swal("修改成功!", "表修改成功", "success");
    				}else {
    					swal("修改表失败", data.msg, "error");
    				}
    				
    	        },
    	        error: function(res){
    	        	swal("修改表失败", res, "error");
    	        }
    		});
    	});
}