$(function (){
    clearModal();
})
function clearModal(){
    $('#clusterId').val();
    $('#masterUrl').val();
    $("#resultMsg").text();
}
function initCluster(){
    var clusterId = $('#clusterId').val();
    var masterUrl = $('#masterUrl').val();
    var isolationLabel = $('#tableIsolationLabel').val();
    if (!hasText(clusterId)){
        alert("请输入要初始的集群Id");
        return
    }
    if (!hasText(masterUrl)){
        alert("请输入要初始的机器master地址，格式为`协议://IP:Port`");
        return
    }
    $.ajax({
                url:"/cluster/initCluster",
                type:"post",
                async: false,
                contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                dataType:"json",
                data:{
                    "masterUrl":masterUrl,
                    "clusterId":clusterId,
                    "isolationLabel":isolationLabel
                },
                success: function(data){
                    $("#resultMsg").text(data.msg);
                },
                error: function(res){
                    alert(res.msg);
                }
        });
}