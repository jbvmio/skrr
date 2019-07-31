package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/hashicorp/serf/serf"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Constants:

type notifierType string

const (
	defaultAppName              = `skrr`
	notifierKey    notifierType = `notifier`
	recipientKey   notifierType = `recipient`
	timeoutSecs                 = 3
)

var (
	cfg           string
	apiAddr       string
	apiPort       string
	bindAddr      string
	bindPort      int
	advertiseAddr string
	advertisePort int
	peerList      []string
	amLeader      bool

	logger              *zap.Logger
	pf                  *pflag.FlagSet
	theOneAndOnlyNumber *OneAndOnlyNumber
)

func init() {
	pf = pflag.NewFlagSet(defaultAppName, pflag.ExitOnError)
	pf.StringVar(&cfg, "config", "./config.yaml", "Config location")
}

func main() {
	pf.Parse(os.Args[1:])
	config := GetConfig(cfg)
	logger = configureLogger(config.LogLevel)
	defer logger.Sync()

	advertisePort = config.Monitor.BindPort
	bindAddr = config.Monitor.BindAddress
	bindPort = config.Monitor.BindPort
	apiPort = config.Monitor.APIPort
	peerList = config.Monitor.Peers

	if config.Monitor.Whitelist {
		performAction = bothAction
	}

	apiAddr = bindAddr + `:` + apiPort
	cluster, err := setupCluster(bindAddr, advertiseAddr, bindPort, advertisePort, peerList...)
	if err != nil {
		logger.Fatal("Error Building Cluster", zap.Error(err))
	}
	defer cluster.Leave()

	theOneAndOnlyNumber = InitTheNumber(-1)
	launchHTTPAPI(theOneAndOnlyNumber)

	ctx := context.Background()
	if name, err := os.Hostname(); err == nil {
		ctx = context.WithValue(ctx, notifierKey, name)
	}

	debugDataPrinterTicker := time.Tick(time.Second * 5)
	numberBroadcastTicker := time.Tick(config.Monitor.PeerCheck)
	leaderBroadcastTicker := time.Tick(config.Monitor.LeaderCheck)
	leaderWorkTicker := time.Tick(config.Monitor.Reconcile)
	for {
		select {
		case <-debugDataPrinterTicker:
			if config.LogLevel == `debug` {
				var membs []string
				for _, m := range cluster.Members() {
					membs = append(membs, m.Name)
				}
				logger.Debug("Cluster Members", zap.Int("Count", cluster.NumNodes()), zap.Strings("Members", membs))
				curVal, curGen, curMeta := theOneAndOnlyNumber.getValue()
				logger.Debug("Cluster Status", zap.Int("Current Value", curVal), zap.Int("Current Generation", curGen), zap.String("Current Leader", curMeta))
			}
		case <-numberBroadcastTicker:
			members := getOtherMembers(cluster)
			go notifyOthers(ctx, members, theOneAndOnlyNumber)
		case <-leaderBroadcastTicker:
			leaderCheck(cluster)
		case <-leaderWorkTicker:
			logger.Info("Check topics for any reconciliation", zap.Bool("Leader", amLeader))
			if amLeader {
				logger.Info("Perform reconciliation, I am the leader", zap.Bool("Leader", amLeader))
				leaderWork(config, cluster)
			} else {
				logger.Info("Skipping reconciliation, I am not the leader", zap.Bool("Leader", amLeader))
			}
		}
	}

}

func getOtherMembers(cluster *serf.Serf) []serf.Member {
	members := cluster.Members()
	for i := 0; i < len(members); {
		if members[i].Name == cluster.LocalMember().Name || members[i].Status != serf.StatusAlive {
			if i < len(members)-1 {
				members = append(members[:i], members[i+1:]...)
			} else {
				members = members[:i]
			}
		} else {
			i++
		}
	}
	return members
}

func notifyOthers(ctx context.Context, otherMembers []serf.Member, db *OneAndOnlyNumber) {
	g, ctx := errgroup.WithContext(ctx)
	notifier := fmt.Sprintf("%s", ctx.Value(notifierKey))
	if len(otherMembers) <= MembersToNotify {
		for _, member := range otherMembers {
			cx, cancel := context.WithTimeout(ctx, time.Second*timeoutSecs)
			curMember := member
			g.Go(func() error {
				return notifyMember(cx, curMember.Addr.String(), apiPort, curMember.Name, db)
			})
			go timeoutCancel(cx, cancel, notifier, curMember.Name, time.Second*timeoutSecs)
			logger.Debug("Sent Member Notification", zap.String("Notifier", notifier), zap.String("Recipient", curMember.Name))
		}
	} else {
		randIndex := rand.Int() % len(otherMembers)
		for i := 0; i < MembersToNotify; i++ {
			cx, cancel := context.WithTimeout(ctx, time.Second*timeoutSecs)
			g.Go(func() error {
				return notifyMember(
					cx,
					otherMembers[(randIndex+i)%len(otherMembers)].Addr.String(),
					apiPort,
					otherMembers[(randIndex+i)%len(otherMembers)].Name,
					db)
			})
			go timeoutCancel(cx, cancel, notifier, otherMembers[(randIndex+i)%len(otherMembers)].Name, time.Second*timeoutSecs)
			logger.Debug("Sent Member Notification", zap.String("Notifier", notifier), zap.String("Recipient", otherMembers[(randIndex+i)%len(otherMembers)].Name))
		}
	}

	err := g.Wait()
	if err != nil {
		logger.Error("Error notifying other members", zap.Error(err))
	}
}

func notifyMember(ctx context.Context, addr, port, recipient string, db *OneAndOnlyNumber) error {
	notifier := fmt.Sprintf("%s", ctx.Value(notifierKey))
	logger.Debug("Notifying Member", zap.String("Notifier", notifier), zap.String("Recipient", recipient))
	val, gen, _ := db.getValue()
	URL := fmt.Sprintf("http://%v:%v/notify/%v/%v?notifier=%v", addr, port, val, gen, notifier)
	req, err := http.NewRequest("POST", URL, nil)
	if err != nil {
		return fmt.Errorf("unable to create POST request: %v", err)
	}
	req = req.WithContext(ctx)
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		err = fmt.Errorf("unable to complete request: %v", err)
	} else {
		logger.Debug("Member notification successful")
	}
	ctx.Done()
	return nil
}

func timeoutCancel(ctx context.Context, cancel context.CancelFunc, notifier, recipient string, timeout time.Duration) {
timerLoop:
	select {
	case <-time.After(timeout):
		logger.Warn("context deadline exceeded", zap.Error(ctx.Err()), zap.String("notifier", notifier), zap.String("recipient", recipient))
		cancel()
		break timerLoop
	case <-ctx.Done():
		logger.Debug("notification completed within context window", zap.String("notifier", notifier), zap.String("recipient", recipient))
		break timerLoop
	}
}

func launchHTTPAPI(db *OneAndOnlyNumber) {
	go func() {
		m := mux.NewRouter()
		m.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
			val, _, _ := db.getValue()
			fmt.Fprintf(w, "%v", val)
		})

		m.HandleFunc("/set/{newVal}/{metadata}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			newVal, err := strconv.Atoi(vars["newVal"])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "%v", err)
				return
			}
			db.setValue(newVal, vars["metadata"])
			fmt.Fprintf(w, "%v", newVal)
		})

		m.HandleFunc("/notify/{curVal}/{curGeneration}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			curVal, err := strconv.Atoi(vars["curVal"])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "%v", err)
				return
			}
			curGeneration, err := strconv.Atoi(vars["curGeneration"])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "%v", err)
				return
			}

			notifier := r.URL.Query().Get("notifier")
			if changed := db.notifyValue(curVal, curGeneration, notifier); changed {
				logger.Info("New Value Notification", zap.Int("NewValue", curVal), zap.Int("Generation", curGeneration), zap.String("Notifier", notifier))
				w.WriteHeader(http.StatusOK)
			}
		})
		logger.Info(`Started API`, zap.String("address", apiAddr))
		err := http.ListenAndServe(apiAddr, m)
		if err != nil {
			logger.Fatal("API Failure", zap.Error(err))
		}
	}()
}

func setupCluster(bindAddr, advertiseAddr string, bindPort, advertisePort int, peers ...string) (*serf.Serf, error) {
	conf := serf.DefaultConfig()
	conf.Init()
	conf.Logger = zap.NewStdLog(logger)
	conf.MemberlistConfig.BindAddr = bindAddr
	conf.MemberlistConfig.BindPort = bindPort
	conf.MemberlistConfig.AdvertiseAddr = advertiseAddr
	conf.MemberlistConfig.AdvertisePort = advertisePort
	conf.MemberlistConfig.Logger = zap.NewStdLog(logger)
	conf.MemberlistConfig.Logger.SetOutput(&logFilter)

	cluster, err := serf.Create(conf)
	if err != nil {
		return nil, fmt.Errorf("unable to create cluster: %v", err)
	}

	var filtered []string
	for _, p := range peers {
		if cluster.LocalMember().Name != p && cluster.LocalMember().Addr.String() != p {
			filtered = append(filtered, p)
		}
	}

	_, err = cluster.Join(filtered, true)
	if err != nil {
		logger.Warn("Couldn't join cluster, starting alone", zap.Error(err))
	}

	return cluster, nil
}
