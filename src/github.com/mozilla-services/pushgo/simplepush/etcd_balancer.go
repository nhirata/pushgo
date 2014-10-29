/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package simplepush

import (
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type EtcdBalancerConf struct {
	// Dir is the etcd directory containing the connected client counts.
	// Defaults to "push_conns".
	Dir string

	// Servers is a list of etcd servers.
	Servers []string

	// TTL is the maximum amount of time that published client counts will be
	// considered valid. Defaults to "5m".
	TTL string

	// Threshold is the connected client threshold. Once this threshold is
	// exceeded, the balancer will redirect connecting clients to other hosts.
	Threshold float64

	// UpdateInterval is the interval for publishing client counts to etcd.
	// Defaults to "1m".
	UpdateInterval string `toml:"update_interval" env:"update_interval"`
}

// EtcdBalancer stores client counts in etcd. Clients connecting to an
// overloaded host are redirected to hosts with the fewest connections.
type EtcdBalancer struct {
	client         *etcd.Client
	pinger         *EtcdPinger
	updater        *PeriodicUpdater
	maxWorkers     int
	threshold      float64
	dir            string
	host           string
	key            string
	workerCount    func() int
	log            *SimpleLogger
	metrics        *Metrics
	updateInterval time.Duration
	ttl            time.Duration
	closeLock      sync.Mutex
	isClosed       bool
	lastErr        error
}

func (b *EtcdBalancer) ConfigStruct() interface{} {
	return &EtcdBalancerConf{
		Dir:            "push_conns",
		Servers:        []string{"http://localhost:4001"},
		TTL:            "5m",
		Threshold:      0.75,
		UpdateInterval: "1m",
	}
}

func (b *EtcdBalancer) Init(app *Application, config interface{}) (err error) {
	conf := config.(*EtcdBalancerConf)
	b.log = app.Logger()
	b.metrics = app.Metrics()

	b.workerCount = app.ClientCount
	b.maxWorkers = app.Server().MaxClientConns()

	b.threshold = conf.Threshold
	b.dir = path.Clean(conf.Dir)
	if b.host = app.Server().ClientHost(); len(b.host) > 0 {
		b.key = path.Join(b.dir, b.host)
	}

	if b.updateInterval, err = time.ParseDuration(conf.UpdateInterval); err != nil {
		b.log.Alert("balancer", "Error parsing 'updateInterval'", LogFields{
			"error": err.Error(), "updateInterval": conf.UpdateInterval})
		return err
	}
	if b.ttl, err = time.ParseDuration(conf.TTL); err != nil {
		b.log.Alert("balancer", "Error parsing 'ttl'", LogFields{
			"error": err.Error(), "ttl": conf.TTL})
		return err
	}

	b.client = etcd.NewClient(conf.Servers)
	if _, err = b.client.CreateDir(b.dir, 0); err != nil {
		if !IsKeyExist(err) {
			b.log.Alert("balancer", "Error creating etcd directory",
				LogFields{"error": err.Error()})
			return err
		}
	}

	publishInterval := time.Duration(0.75*b.ttl.Seconds()) * time.Second
	b.updater = NewPeriodicUpdater(b, publishInterval, b.updateInterval)
	b.pinger = &EtcdPinger{b.client, b.log}

	return nil
}

// NextHost returns the host with the fewest connected clients. Implements
// Balancer.NextHost().
func (b *EtcdBalancer) NextHost() (host string, ok bool) {
	workerCount := int64(b.workerCount())
	if float64(workerCount)/float64(b.maxWorkers) < b.threshold {
		return "", false
	}
	reply, err := b.updater.Fetch()
	if err != nil {
		if b.log.ShouldLog(WARNING) {
			b.log.Warn("balancer", "Failed to retrieve WebSocket count",
				LogFields{"error": err.Error()})
		}
		return "", false
	}
	response := reply.(*etcd.Response)
	minCount := int64(1<<63 - 1)
	for _, node := range response.Node.Nodes {
		if len(node.Value) == 0 {
			continue
		}
		nodeCount, err := strconv.ParseInt(node.Value, 10, 64)
		if err != nil {
			if b.log.ShouldLog(WARNING) {
				b.log.Warn("balancer", "Failed to parse WebSocket count", LogFields{
					"error": err.Error(), "host": node.Key, "count": node.Value})
			}
			continue
		}
		if nodeCount < minCount {
			host = node.Key
			minCount = nodeCount
		}
	}
	if host == b.host || minCount >= workerCount {
		return "", false
	}
	return host, true
}

// Status determines whether etcd is available. Implements Balancer.Status().
func (b *EtcdBalancer) Status() (bool, error) {
	return b.pinger.Healthy()
}

// Close stops the balancer and closes the connection to etcd. Implements
// Balancer.Close().
func (b *EtcdBalancer) Close() (err error) {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()
	err = b.lastErr
	if b.isClosed {
		return err
	}
	b.isClosed = true
	b.lastErr = b.updater.Close()
	if len(b.key) > 0 {
		_, b.lastErr = b.client.Delete(b.key, false)
	}
	return err
}

// Fetch retrieves the client counts for all nodes from etcd. Implements
// Updater.Fetch().
func (b *EtcdBalancer) Fetch() (reply interface{}, canRetry bool, err error) {
	if reply, err = b.client.Get(b.dir, false, false); err != nil {
		return nil, true, err
	}
	return reply, false, nil
}

// Publish stores the client count for the current node in etcd. Implements
// Updater.Publish().
func (b *EtcdBalancer) Publish() (canRetry bool, err error) {
	workerCount := strconv.Itoa(b.workerCount())
	if b.log.ShouldLog(INFO) {
		b.log.Info("balancer", "Publishing WebSocket count",
			LogFields{"host": b.host, "workers": workerCount})
	}
	if _, err = b.client.Set(b.key, workerCount, uint64(b.ttl/time.Second)); err != nil {
		if b.log.ShouldLog(ERROR) {
			b.log.Error("balancer", "Error publishing WebSocket count",
				LogFields{"host": b.host, "workers": workerCount, "error": err.Error()})
		}
		return true, err
	}
	return false, nil
}

func init() {
	AvailableBalancers["etcd"] = func() HasConfigStruct { return new(EtcdBalancer) }
}
