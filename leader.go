package main

import (
	"sort"
	"strconv"
	"strings"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type reconcileAction uint8

const (
	bothAction      reconcileAction = 0
	blacklistAction reconcileAction = 1
	whitelistAction reconcileAction = 2
)

var performAction = blacklistAction

func leaderWork(config *Config, cluster *serf.Serf) {
	_, _, leader := theOneAndOnlyNumber.getValue()
	if leader == cluster.LocalMember().Name {
		logger.Debug("I AM LEADER", zap.String("ME", cluster.LocalMember().Name), zap.Bool("AM Leader", amLeader))
		logger.Info("Beginning Reconcile Work", zap.String("Leader", cluster.LocalMember().Name))
		for name, cluster := range config.Clusters {
			logger.Info("Reconciling Cluster", zap.String("Cluster", name))
			reconcileTopics(cluster, name, performAction, config.Monitor.Execute)
		}
	}
}

func leaderCheck(cluster *serf.Serf) {
	logger.Debug("Checking Leader Status")
	var highestVal int
	var highestNode string
	for _, m := range cluster.Members() {
		var val int
		switch m.Status.String() {
		case `alive`:
			for _, oct := range strings.Split(m.Addr.String(), `.`) {
				o, _ := strconv.Atoi(oct)
				val += o
			}
		default:
		}
		if val > highestVal {
			highestVal = val
			highestNode = m.Name
		}
	}
	logger.Debug("Leader Candidate Results", zap.String("Leader Choice", highestNode), zap.Int("Score", highestVal))
	curVal, _, _ := theOneAndOnlyNumber.getValue()
	if curVal != highestVal {
		logger.Info("Changing Leader", zap.String("New Leader", highestNode))
		if highestNode == cluster.LocalMember().Name {
			theOneAndOnlyNumber.setValue(highestVal, highestNode)
			amLeader = true
			logger.Info("I am the new leader", zap.String("Node", cluster.LocalMember().Name), zap.Bool("Leader", amLeader))
		} else {
			amLeader = false
			logger.Info("I am not the new leader", zap.String("Node", cluster.LocalMember().Name), zap.Bool("Leader", amLeader))
		}
	}
}

func reconcileTopics(C Cluster, replName string, action reconcileAction, execute bool, args ...string) {
	if (C == Cluster{}) {
		logger.Error("Could not reconcile cluster!", zap.String("reason", "Invalid Configuration"), zap.String("Cluster", replName))
	}
	ok := validateCluster(C)
	if ok {
		var errCount int
		targetTopics := make(map[reconcileAction][]string)
		kafErr := launchKafka(C.SourceBroker, C.BrokerAddress)
		defer func() {
			if kafErr == nil {
				srcKafkaClient.Close()
				dstKafkaClient.Close()
			}
		}()
		err := launchZKClient(C.ZKAddress)
		if err != nil {
			logger.Error("Error connecting to ZooKeeper", zap.String("Address", C.ZKAddress), zap.Error(err))
			errCount++
		}
		err = getZKTarget(C.ZKRoot, replName)
		if err != nil {
			logger.Error("Error validating ZooKeeper", zap.String("ZKRoot", C.ZKRoot), zap.String("Cluster", replName), zap.Error(err))
			errCount++
		}
		switch {
		case errCount > 0:
			logger.Error("Could not reconcile topics, too many errors")
		default:
			switch action {
			case bothAction:
				targetTopics[blacklistAction] = filterDeletedTopics(C.ZKRoot)
				targetTopics[whitelistAction] = filterReAddedTopics(C.ZKRoot)
				sort.SliceStable(targetTopics[blacklistAction], func(i, j int) bool { return targetTopics[blacklistAction][i] < targetTopics[blacklistAction][j] })
				sort.SliceStable(targetTopics[whitelistAction], func(i, j int) bool { return targetTopics[whitelistAction][i] < targetTopics[whitelistAction][j] })
			case whitelistAction:
				targetTopics[whitelistAction] = filterReAddedTopics(C.ZKRoot)
				sort.SliceStable(targetTopics[whitelistAction], func(i, j int) bool { return targetTopics[whitelistAction][i] < targetTopics[whitelistAction][j] })
			default:
				targetTopics[blacklistAction] = filterDeletedTopics(C.ZKRoot)
				sort.SliceStable(targetTopics[blacklistAction], func(i, j int) bool { return targetTopics[blacklistAction][i] < targetTopics[blacklistAction][j] })
			}
			if len(args) > 0 {
				for k := range targetTopics {
					targetTopics[k] = filterArgsTopics(args, targetTopics[k])
				}
			}
			for action, topics := range targetTopics {
				var finalStr string
				L := logger.With(zap.String("Cluster", replName),
					zap.Uint8("ReconcileAction", uint8(performAction)),
					zap.Bool("Perform Execution", execute))
				switch action {
				case blacklistAction:
					switch {
					case len(topics) < 1:
						L.Info("No topics need blacklist reconciliation")
						continue
					case execute:
						L.Info("Blacklisting Topics ...")
						blacklistTopics(C.ReplAPI, topics...)
						continue
					default:
						finalStr = "Topics replicated but not available"
					}
				case whitelistAction:
					switch {
					case len(topics) < 1:
						L.Info("No topics need whitelist reconciliation.")
						continue
					case execute:
						L.Info("Whitelisting Topics ...")
						whitelistTopics(C.ReplAPI, topics...)
						continue
					default:
						finalStr = "Topics available but not replicated"
					}
				}
				L.Info(finalStr, zap.Strings("Topics", topics))
			}
		}
	} else {
		logger.Error("Could not reconcile cluster!", zap.String("reason", "Validation Checks Failed"), zap.String("Cluster", replName))
	}
}
