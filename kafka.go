package main

import (
	"fmt"

	"github.com/jbvmio/kafka"
)

var (
	dstKafkaClient *kafka.KClient
	srcKafkaClient *kafka.KClient
)

func launchKafka(srcBroker, dstBroker string) error {
	var err error
	conf := kafka.GetConf()
	conf.Version = kafka.RecKafkaVersion
	conf.ClientID = `skrr`
	srcKafkaClient, err = kafka.NewCustomClient(conf, srcBroker)
	if err != nil {
		return fmt.Errorf("Error connecting to source kafka cluster: %v", err)
	}
	dstKafkaClient, err = kafka.NewCustomClient(conf, dstBroker)
	if err != nil {
		return fmt.Errorf("Error connecting to destination kafka cluster: %v", err)
	}
	return nil
}

func getKafkaTopics(client *kafka.KClient) ([]string, error) {
	return client.ListTopics()
}
