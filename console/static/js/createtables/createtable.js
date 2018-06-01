$(document).ready(function() {
        var data = [];
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
        var isprimarykey = [{
            name: "是",
            value: 1
        }, {
            name: "否",
            value: 0
        }];
        $("#jsGrid").jsGrid({
            width: "100%",
            data: data, // 数据
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
                width: 80, align: "center"            
            }, {
                title: '默认值',
                name: "default_value",
                type: "text",
                width: 100, align: "center"
            }, {
            	title: "Unsigned",
                name: "unsigned",
                type: "checkbox",
                sorting: false
            }, {
            	title: "是否为主键",
                name: "primary_key",
                type: "checkbox",
                items: isprimarykey,
                valueField: "value",
                textField: "name",
                sorting: false
            }, {
            	title: "动态列",
                name: "regxs",
                type: "checkbox",
                sorting: false
            },{
                type: "control", 
                editButton: false, 
                modeSwitchButton: false,
                headerTemplate: function() {
                    return $("<button class=\"btn btn-primary\">").attr("type", "button").text("提交")
                    .on("click", function () {
                    	createDataBaseTable(data);
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
	var flg = false;
	for(var i =0;i<data.length;i++){
		if(data[i].primary_key){
			flg = data[i].primary_key;
		}
		if(flg){
			break;
		}
	}
	if(!flg){
		swal("至少有一个主键");
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
	// 隔离标识
    var isolationLabel = $("#isolationLabel").val();
	//集群id
	var clusterId = $('#clusterId').val();
	//验证表名
    if(name == undefined || name == null || name == "") {
    	swal("表名必须填写");
        return false;
    }
    var policy = $('#policy').val();
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
    			url:"/metadata/createTable",
    			type:"post",
    	        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
    	        dataType:"json",
    	        data:{
    	        	"dbName":dbName,
    	        	"name":name,
                    "isolationLabel": isolationLabel,
    	        	"columns":JSON.stringify(data),
    	        	"regxs":JSON.stringify(regxsList),
    	        	"rangeKeys":rangeKeys,
    	        	"clusterId":clusterId,
    	        	"policy":policy
    	        },
    			success: function(data){
    				if(data.code === 0){
    					swal("创建成功!", "表创建成功", "success");
                        window.history.back(-1);
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