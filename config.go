package main

import (
	"log"
	"os"
	"regexp"
	"sort"
	"time"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var performSilently bool

// Config .
type Config struct {
	LogLevel string
	Monitor  Monitoring
	Clusters map[string]Cluster
}

// Cluster .
type Cluster struct {
	ReplAPI       string
	BrokerAddress string
	SourceBroker  string
	ZKAddress     string
	ZKRoot        string
}

// Monitoring .
type Monitoring struct {
	BindAddress string
	BindPort    int
	APIPort     string
	LeaderCheck time.Duration
	PeerCheck   time.Duration
	Reconcile   time.Duration
	Execute     bool
	Whitelist   bool
	Peers       []string
}

// GetConfig reads in the config file.
func GetConfig(filePath string) *Config {
	viper.SetConfigFile(filePath)
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Unable to Read Config: %v\n", err)
	}
	var C Config
	var list []string
	for k := range viper.GetStringMap(`details`) {
		list = append(list, k)
	}
	sort.SliceStable(list, func(i, j int) bool { return list[i] < list[j] })
	viper.SetDefault(`loglevl`, `info`)
	viper.SetDefault(`monitor.leadercheck`, `1m`)
	viper.SetDefault(`monitor.peercheck`, `2m`)
	viper.SetDefault(`monitor.reconcile`, `5m`)
	monitor := Monitoring{
		BindAddress: viper.GetString(`monitor.bindaddress`),
		BindPort:    viper.GetInt(`monitor.bindport`),
		APIPort:     viper.GetString(`monitor.apiport`),
		LeaderCheck: viper.GetDuration(`monitor.leadercheck`),
		PeerCheck:   viper.GetDuration(`monitor.peercheck`),
		Reconcile:   viper.GetDuration(`monitor.reconcile`),
		Execute:     viper.GetBool(`monitor.execute`),
		Whitelist:   viper.GetBool(`monitor.whitelist`),
		Peers:       viper.GetStringSlice(`monitor.peers`),
	}
	C.LogLevel = viper.GetString(`loglevel`)
	C.Monitor = monitor
	C.Clusters = make(map[string]Cluster, len(list))
	for _, l := range list {
		path := `details.` + l
		c := viper.GetStringMapString(path)
		cluster := Cluster{
			ReplAPI:       c[`replicationapi`],
			BrokerAddress: c[`brokeraddress`],
			SourceBroker:  c[`sourcebroker`],
			ZKAddress:     c[`zkaddress`],
			ZKRoot:        c[`zkroot`],
		}
		C.Clusters[l] = cluster
	}
	return &C
}

func validateCluster(cluster Cluster) (good bool) {
	switch {
	case cluster.ReplAPI == "":
		return
	case cluster.BrokerAddress == "":
		return
	case cluster.ZKAddress == "":
		return
	case cluster.ZKRoot == "":
		return
	}
	return true
}

func makeRegex(args ...string) *regexp.Regexp {
	var regexString string
	regexString += `^(`
	switch {
	case len(args) > 1:
		first := args[0]
		rest := args[1:]
		regexString += first
		for _, r := range rest {
			regexString += `|` + r
		}
	case len(args) == 1:
		regexString += args[0]
	}
	regexString += `)$`
	return regexp.MustCompile(regexString)
}

func fileExists(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}

func homeDir() string {
	home, err := homedir.Dir()
	if err != nil {
		log.Printf("WARN: unable to determine $HOME directory\n")
	}
	return home
}
