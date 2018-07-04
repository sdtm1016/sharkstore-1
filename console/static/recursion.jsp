<li ng-repeat="rangeReplicate in rangeTreeArr.range.peers">
    <p class="topology-ip" ng-class="{'off': rangeReplicate.node.state != 1 }"
       ng-mouseout="checkShardHide(rangeReplicate)" ng-mouseover="checkShard(rangeReplicate)">
        <strong ng-click="showRangeInfo(rangeReplicate,'peer')">{{rangeReplicate.node.server_addr}}</strong>
        <span class="topology-btn" ng-show="rangeReplicate.statusBtn">
			<span class="btn btn-black" ng-click="changeLeaderInstance(rangeReplicate,rangeTreeArr.range.id)">切换主</span>
            <span class="btn btn-black" ng-click="moveReplicateInstance(rangeReplicate,rangeTreeArr.range.id)">删除</span>
            <span class="btn btn-black" ng-click="transferInstance(rangeReplicate,rangeTreeArr.range.id)">迁移</span>
		</span>
    </p>
</li>
