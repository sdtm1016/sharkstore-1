var app = angular.module('unstableRanges', []);

app.controller('unsRange', function($rootScope, $scope, $http, $timeout) {
     var clusterId = $('#clusterId').val();
	 var dbName = $("#dbName").val();
     var tableName = $("#tableName").val();
	 $http.get('/range/getUnstableRanges?tableName='+tableName +'&dbName='+dbName +'&clusterId=' + clusterId).success(function(data){
	 	if(data.code === 0){
			$scope.unstableRanges = data.data;
            $scope.size = data.data.length;
		 }else {
			swal("获取失败", data.msg, "error");
		 }
	 });

	  $scope.viewPeerInfo = function(rangeId) {
	        window.location.href="/page/range/peerinfo?rangeId="+ rangeId +"&tableName="+tableName+"&dbName="+dbName+"&clusterId="+clusterId + "&flag="+false;
       };
 });
