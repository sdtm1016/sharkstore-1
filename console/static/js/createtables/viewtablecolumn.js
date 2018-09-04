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
			if(data.code === 0){
				for(var i=0;i<data.data.columns.length;i++){
					columnsArray.push(data.data.columns[i]);
				}
				for(var i=0;i<data.data.regxs.length;i++){
					regxsArray.push(data.data.regxs[i]);
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
        
        var isprimarykey = [{
            name: "是",
            value: 1
        }, {
            name: "否",
            value: 0
        }];
        var unsignedTypes = [{
                name: "是",
                value: true
            }, {
                name: "否",
                value: false
        }];
        var autoIncrTypes = [{
            name: "是",
            value: true
        }, {
            name: "否",
            value: false
        }];
        $("#jsGrid").jsGrid({
            width: "100%",
            data: columnsArray, // 数据
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
            // }, {
            //     title: '默认值',
            //     name: "default_value",
            //     type: "text",
            //     width: 100, align: "center"
            }, {
            	title: "Unsigned",
                name: "unsigned",
                type: "select",
                items: unsignedTypes,
                valueField: "value",
                textField: "name",
                width: 80, align: "center"
            }, {
            	title: "是否为主键",
                name: "primary_key",
                type: "select",
                items: isprimarykey,
                valueField: "value",
                textField: "name",
                width: 80, align: "center"
            }, {
                title: "自增id",
                name: "auto_increment",
                type: "select",
                items: autoIncrTypes,
                valueField: "value",
                textField: "name",
                width: 80, align: "center"
            }]
        });
        $("#jsGridRegxs").jsGrid({
            width: "100%",
            data: regxsArray, // 数据
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
            }]
        });
    });