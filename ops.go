package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"

	"go.uber.org/zap"
)

const (
	apiTopicPath = `/topics`
)

var replicationTarget string

func getZKTopics(zkRoot string) []string {
	path := zkRoot + `/` + replicationTarget
	return zkListTopics(path)
}

func getZKBlacklist(zkRoot string) []string {
	path := zkRoot + `/` + replicationTarget
	return zkListBlacklist(path)
}

func getZKTarget(zkRoot, cluster string) error {
	dcList := zkLS(zkRoot)
	switch {
	case len(dcList) < 1:
		return fmt.Errorf("No DC Replications Found")
	case cluster == `adhoc`:
		switch {
		case len(dcList) == 1:
			replicationTarget = dcList[0]
		default:
			return fmt.Errorf("Multiple Replications Found: %v", dcList)
		}
	default:
		for _, dc := range dcList {
			if dc == cluster {
				replicationTarget = dc
				break
			}
		}
	}
	if replicationTarget == "" {
		return fmt.Errorf("No Replications found for %v", cluster)
	}
	return nil
}

func filterDeletedTopics(zkRoot string) []string {
	var (
		removeTopics []string
		dstTopics    []string
		srcTopics    []string
		dstErr       error
		srcErr       error
		zkTopics     = getZKTopics(zkRoot)
		blTopics     = getZKBlacklist(zkRoot)
		blRegex      = makeRegex(blTopics...)
		wg           sync.WaitGroup
	)
	if len(zkTopics) < 1 || len(blTopics) < 1 {
		return removeTopics
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dstTopics, dstErr = getKafkaTopics(dstKafkaClient)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		srcTopics, srcErr = getKafkaTopics(srcKafkaClient)
	}()
	wg.Wait()
	switch {
	case dstErr != nil && srcErr != nil:
		logger.Error("Error connecting to both source and destination kafka clusters", zap.Errors("errstack", []error{srcErr, dstErr}))
	case dstErr != nil:
		logger.Error("Error connecting to destination kafka cluster", zap.Error(dstErr))
	case srcErr != nil:
		logger.Error("Error connecting to source kafka cluster", zap.Error(srcErr))
	default:
		dstRegex := makeRegex(dstTopics...)
		srcRegex := makeRegex(srcTopics...)
		for _, topic := range zkTopics {
			if !blRegex.MatchString(topic) {
				if !dstRegex.MatchString(topic) && !srcRegex.MatchString(topic) {
					removeTopics = append(removeTopics, topic)
				}
			}
		}
	}
	return removeTopics
}

func filterReAddedTopics(zkRoot string) []string {
	var (
		addedTopics []string
		dstTopics   []string
		srcTopics   []string
		dstErr      error
		srcErr      error
		blTopics    = getZKBlacklist(zkRoot)
		dstRegex    = makeRegex(dstTopics...)
		srcRegex    = makeRegex(srcTopics...)
		wg          sync.WaitGroup
	)
	if len(blTopics) < 1 {
		return addedTopics
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dstTopics, dstErr = getKafkaTopics(dstKafkaClient)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		srcTopics, srcErr = getKafkaTopics(srcKafkaClient)
	}()
	wg.Wait()
	switch {
	case dstErr != nil && srcErr != nil:
		logger.Error("Error connecting to both source and destination kafka clusters", zap.Errors("errstack", []error{srcErr, dstErr}))
	case dstErr != nil:
		logger.Error("Error connecting to destination kafka cluster", zap.Error(dstErr))
	case srcErr != nil:
		logger.Error("Error connecting to source kafka cluster", zap.Error(srcErr))
	default:
		for _, topic := range blTopics {
			if dstRegex.MatchString(topic) && srcRegex.MatchString(topic) {
				addedTopics = append(addedTopics, topic)
			}
		}
	}
	return addedTopics
}

func filterArgsTopics(args, topics []string) []string {
	var filtered []string
	regex := makeRegex(args...)
	for _, topic := range topics {
		if regex.MatchString(topic) {
			filtered = append(filtered, topic)
		}
	}
	return filtered
}

func blacklistTopics(apiURL string, topics ...string) {
	client := &http.Client{}
	for _, topic := range topics {
		url := apiURL + apiTopicPath + `/` + topic
		deleteRequest(client, url)
	}
}

func deleteRequest(client *http.Client, urlTarget string) {
	L := logger.With(zap.String("Request", "Blacklist"))
	req, err := http.NewRequest("DELETE", urlTarget, nil)
	if err != nil {
		L.Error("received error", zap.String("URL", urlTarget), zap.Error(err))
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		L.Error("received error", zap.String("URL", urlTarget), zap.Error(err))
		return
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		L.Error("received error", zap.String("URL", urlTarget), zap.Error(err))
		return
	}
	L.Info("Result", zap.String("Response", resp.Status), zap.String("Message", fmt.Sprintf("%s", respBody)))
}

func whitelistTopics(apiURL string, topics ...string) {
	regex := makeRegex(topics...)
	topicMeta, err := srcKafkaClient.GetTopicMeta()
	if err != nil {
		logger.Error("Unable to retrieve source topics from kafka", zap.Error(err))
	} else {
		topicParts := make(map[string]int)
		for _, meta := range topicMeta {
			if regex.MatchString(meta.Topic) {
				topicParts[meta.Topic]++
			}
		}
		for topic, parts := range topicParts {
			url := apiURL + apiTopicPath
			reAddRequest(url, topic, parts)
		}
	}
}

func reAddRequest(urlTarget, topic string, parts int) {
	L := logger.With(zap.String("Request", "Whitelist"))
	j, err := json.Marshal(PostRequest{
		Topic:         topic,
		NumPartitions: strconv.Itoa(parts),
	})
	if err != nil {
		L.Error("received marshalling error", zap.String("topic", topic), zap.Error(err))
		return
	}
	resp, err := http.Post(urlTarget, `application/json`, bytes.NewBuffer(j))
	if err != nil {
		L.Error("received POST error", zap.String("topic", topic), zap.Error(err))
		return
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		L.Error("received error", zap.String("URL", urlTarget), zap.Error(err))
		return
	}
	L.Info("Whitelist Result", zap.String("Topic", topic), zap.String("Response", resp.Status), zap.String("Message", fmt.Sprintf("%s", respBody)))
}

// PostRequest .
type PostRequest struct {
	Topic         string `json:"topic"`
	NumPartitions string `json:"numPartitions"`
}
