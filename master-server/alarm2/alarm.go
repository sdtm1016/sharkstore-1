package alarm2

import (
	"fmt"
	"time"
	"context"

	"util/log"
	"util/deepcopy"
	"model/pkg/alarmpb2"
)

type cacheKey struct {
	RuleName 	string 	`json:"rule_name"`
	AppName 	string 	`json:"app_name"`
	ClusterId 	int64 	`json:"cluster_id"`
	AppAddr 	string 	`json:"app_addr"`
}
type cacheValue struct {
	TriggerTime int64 	`json:"trigger_time"`
	Count 		int64 	`json:"count"`
}

func (s *Server) timingDbPulling() {
	ctx, cancel := context.WithCancel(s.context)
	defer cancel()
	duration := s.conf.MysqlPullingDurationSec * time.Second
	t := time.NewTimer(duration)
	for {
		select {
		case <-t.C:
			s.tableAppPulling()
			s.tableGlobalRulePulling()
			s.tableClusterRulePulling()
			s.tableReceiverPulling()

			t.Reset(duration)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) tableAppPulling() {
	ret, err := s.dbOpImpl.getTableAppData()
	if err != nil {
		log.Error("pull table app failed: %v", err)
		return
	}

	s.updateMapApp(ret)
}

func (s *Server) tableGlobalRulePulling() {
	ret, err := s.dbOpImpl.getTableGlobalRuleData()
	if err != nil {
		log.Error("pull table global rule failed: %v", err)
		return
	}

	s.updateMapGlobalRule(ret)
}

func (s *Server) tableClusterRulePulling() {
	ret, err := s.dbOpImpl.getTableClusterRuleData()
	if err != nil {
		log.Error("pull table cluster rule failed: %v", err)
		return
	}

	s.updateMapClusterRule(ret)
}

func (s *Server) tableReceiverPulling() {
	ret, err := s.dbOpImpl.getTableReceiveData()
	if err != nil {
		log.Error("pull table receiver failed: %v", err)
		return
	}

	s.updateMapReceiver(ret)
}

func (s *Server) updateMapApp(data []TableApp) {
	if len(data) == 0 {
		log.Warn("no data in table app")
		return
	}

	newMap := make(appClusterMap)
	for _, d := range data {
		if _, ok := newMap[d.ClusterId]; !ok {
			newMap[d.ClusterId] = make(appMap)
		}
		dd := newMap[d.ClusterId]
		dd[d.IpAddr] = d
	}

	s.appLock.Lock()
	defer s.appLock.Unlock()
	s.clusterApp = newMap
}

func (s *Server) getMapApp() (ret appClusterMap) {
	s.appLock.RLock()
	defer s.appLock.RUnlock()

	ret = deepcopy.Iface(s.clusterApp).(appClusterMap)
	return
}

func (s *Server) updateMapGlobalRule(data []TableGlobalRule) {
	if len(data) == 0 {
		log.Warn("no data in table global rule")
		return
	}

	newMap := make(globalRuleMap)
	for _, d := range data {
		newMap[d.Name] = d
	}

	s.globalRuleLock.Lock()
	defer s.globalRuleLock.Unlock()
	s.globalRule = newMap
}

func (s *Server) getMapGlobalRule() (ret globalRuleMap) {
	s.globalRuleLock.RLock()
	defer s.globalRuleLock.RUnlock()

	ret = deepcopy.Iface(s.globalRule).(globalRuleMap)
	return
}

func (s *Server) updateMapClusterRule(data []TableClusterRule) {
	if len(data) == 0 {
		log.Warn("no data in table cluster rule")
		return
	}

	newMap := make(ruleClusterMap)
	for _, d := range data {
		if _, ok := newMap[d.ClusterId]; !ok {
			newMap[d.ClusterId] = make(ruleClusterNameMap)
		}
		dd := newMap[d.ClusterId]
		dd[d.Name] = d
	}

	s.globalRuleLock.Lock()
	defer s.globalRuleLock.Unlock()
	s.clusterRule = newMap
}

func (s *Server) getMapClusterRule() (ret ruleClusterMap) {
	s.clusterRuleLock.RLock()
	defer s.clusterRuleLock.RUnlock()

	ret = deepcopy.Iface(s.clusterRule).(ruleClusterMap)
	return
}

func (s *Server) updateMapReceiver(data []TableReceiver) {
	if len(data) == 0 {
		log.Warn("no data in table receiver")
		return
	}

	newMap := make(receiverClusterMap)
	for _, d := range data {
		if _, ok := newMap[d.ClusterId]; !ok {
			newMap[d.ClusterId] = make(receiverMap)
		}
		dd := newMap[d.ClusterId]
		dd[d.Erp] = d
	}

	s.globalRuleLock.Lock()
	defer s.globalRuleLock.Unlock()
	s.clusterReceiver = newMap
}

func (s *Server) getMapReceiver() (ret receiverClusterMap) {
	s.receiverLock.RLock()
	defer s.receiverLock.RUnlock()

	ret = deepcopy.Iface(s.clusterReceiver).(receiverClusterMap)
	return
}

func (s *Server) getMapClusterReceiver(clusterId int64) (ret receiverMap) {
	m := s.getMapReceiver()
	if _, ok := m[clusterId]; !ok {
		return nil
	}
	return m[clusterId]
}

func (s *Server) aliveChecking() {
	ctx, cancel := context.WithCancel(s.context)
	defer cancel()
	duration := s.conf.AppAliveCheckingDurationSec * time.Second
	t := time.NewTimer(duration)
	for {
		select {
		case <-t.C:
			var apps []TableApp
			for clusterId, clusterApps := range s.getMapApp() {
				for ipAddr, app := range clusterApps {
					key, err := encodeCacheKey(cacheKey{
						ALARMRULE_APP_NOTALIVE,
						app.ProcessName,
						app.ClusterId,
						app.IpAddr})
					if err != nil {
						log.Error("new alive key failed: process name[%v] cluster id[%v] ip addr[%v]: %v",
							app.ProcessName, app.ClusterId, app.IpAddr, err)
						continue
					}

					if err := s.cacheOpImpl.exists(key); err != nil {
						log.Error("jim exists command error: %v", err)

						// treat as key not exists
						log.Warn("app not alive: cluster id[%v] ip addr[%v] app name[%v]", clusterId, ipAddr, app.ProcessName)
						apps = append(apps, app)
					}
				}
			}

			if len(apps) != 0 {
				for _, app := range apps {
					resp, err := s.handleRuleAlarm(&alarmpb2.RequestHeader{
						ClusterId: app.ClusterId,
						IpAddr: app.IpAddr,
						AppName: app.ProcessName,
					}, &alarmpb2.RuleAlarmRequest{
						RuleName: ALARMRULE_APP_NOTALIVE,
						AlarmValue: 1,
						CmpType: alarmpb2.AlarmValueCompareType_GREATER_THAN,
						Remark: []string{fmt.Sprintf("app name: %v", app.ProcessName)},
					})
					if err != nil {
						log.Error("handle rule alarm error: %v", err)
					} else {
						if resp.GetHeader().GetCode() != alarmpb2.AlarmResponseCode_OK ||
							len(resp.GetHeader().GetError()) != 0 {
								log.Error("handle rule alarm do failed: %v", resp.GetHeader().GetError())
						}
					}
				}
			}

			t.Reset(duration)
		case <-ctx.Done():
			return
		}
	}
}

