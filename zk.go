package main

import (
	"fmt"
	"time"

	"github.com/jbvmio/zk"
	"go.uber.org/zap"
)

const (
	topicsPath    = `/CONFIGS/RESOURCE`
	blacklistPath = `/BLACKLISTED_TOPICS`
)

var (
	zkClient *zk.ZooKeeper
)

func launchZKClient(zkAddress ...string) error {
	zkClient = zk.NewZooKeeper()
	zkClient.EnableLogger(false)
	zkClient.SetServers(zkAddress)
	ok, err := zkClient.Exists("/")
	if !ok || err != nil {
		return fmt.Errorf("Error Validating Zookeeper Configuration: %v", zkAddress)
	}
	return nil
}

func zkCheckExists(path string) (bool, error) {
	return zkClient.Exists(path)
}

func zkLS(basePath string) []string {
	sp, err := zkClient.Children(basePath)
	for i := 0; i < 3; i++ {
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 333)
		sp, err = zkClient.Children(basePath)
	}
	if err != nil {
		logger.Error("Error retrieving path, retrying", zap.String("path", basePath))
	}
	return sp
}

func zkListTopics(basePath string) []string {
	path := basePath + topicsPath
	sp, err := zkClient.Children(path)
	for i := 0; i < 3; i++ {
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 333)
		sp, err = zkClient.Children(basePath)
	}
	if err != nil {
		logger.Error("Error retrieving path, retrying", zap.String("path", basePath))
	}
	return sp
}

func zkListBlacklist(basePath string) []string {
	path := basePath + blacklistPath
	sp, err := zkClient.Children(path)
	for i := 0; i < 3; i++ {
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 333)
		sp, err = zkClient.Children(path)
	}
	if err != nil {
		logger.Error("Error retrieving path, retrying", zap.String("path", path))
	}
	return sp
}
