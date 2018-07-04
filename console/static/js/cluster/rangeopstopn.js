var app = angular.module('rangeOpsTopN', []);
app.controller('rangeTopNCtrl', ['$rootScope', '$scope', '$http', '$timeout', function ($rootScope, $scope, $http, $timeout) {
    var clusterId = $("#clusterId").val();
    var topN = $("#topN").val();

    var getRangeAllUrl = "/range/getOpsTopN?topN=" + topN + "&clusterId=" + clusterId;
    $http.get(getRangeAllUrl).success(function (data) {
        if (data.code === 0) {
            $scope.statList = data.data;
        }
    });
}]);