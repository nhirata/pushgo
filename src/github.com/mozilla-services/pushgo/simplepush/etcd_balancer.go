/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package simplepush

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

// ErrNoPeers is returned if the cluster is full.
var ErrNoPeers = errors.New("No peers available")

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
	// Defaults to 0.75.
	Threshold float64

	// UpdateInterval is the interval for publishing client counts to etcd.
	// Defaults to "1m".
	UpdateInterval string `toml:"update_interval" env:"update_interval"`
}

// EtcdBalancer stores client counts in etcd. Clients connecting to an
// overloaded host are redirected to hosts with the fewest connections.
type EtcdBalancer struct {
	client         *etcd.Client
	updater        *PeriodicUpdater
	maxWorkers     int
	threshold      float64
	dir            string
	url            *url.URL
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

	workerURL := app.Server().ClientURL()
	if b.url, err = url.ParseRequestURI(workerURL); err != nil {
		b.log.Alert("balancer", "Error parsing client endpoint", LogFields{
			"error": err.Error(), "url": workerURL})
		return err
	}
	if len(b.url.Host) > 0 {
		b.key = path.Join(b.dir, b.url.Host)
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

	return nil
}

// RedirectURL returns the absolute URL of the peer with the fewest connected
// clients. Implements Balancer.NextURL().
func (b *EtcdBalancer) RedirectURL() (origin string, ok bool, err error) {
	currentWorkers := int64(b.workerCount())
	if float64(currentWorkers)/float64(b.maxWorkers) < b.threshold {
		return "", false, nil
	}
	reply, err := b.updater.Fetch()
	if err != nil {
		if b.log.ShouldLog(WARNING) {
			b.log.Warn("balancer", "Failed to retrieve client counts from etcd",
				LogFields{"error": err.Error()})
		}
		return "", false, err
	}
	response := reply.(*etcd.Response)
	minWorkers := int64(1<<63 - 1)
	var scheme, host string
	for _, node := range response.Node.Nodes {
		if len(node.Value) == 0 {
			continue
		}
		nextHost := path.Base(node.Key)
		if nextHost == b.url.Host {
			continue
		}
		query, err := url.ParseQuery(node.Value)
		if err != nil {
			if b.log.ShouldLog(WARNING) {
				b.log.Warn("balancer", "Failed to decode etcd peer info", LogFields{
					"error": err.Error(), "host": nextHost, "info": node.Value})
			}
			continue
		}
		workerCount := query.Get("w")
		workers, err := strconv.ParseInt(workerCount, 10, 64)
		if err != nil {
			if b.log.ShouldLog(WARNING) {
				b.log.Warn("balancer", "Failed to parse client count", LogFields{
					"error": err.Error(), "host": nextHost, "count": workerCount})
			}
			continue
		}
		if workers < minWorkers {
			if scheme = query.Get("s"); len(scheme) == 0 {
				if b.log.ShouldLog(WARNING) {
					b.log.Warn("balancer", "etcd peer info missing scheme", LogFields{
						"host": nextHost, "info": node.Value})
				}
				continue
			}
			host = nextHost
			minWorkers = workers
		}
	}
	if len(host) == 0 || minWorkers >= currentWorkers {
		return "", false, ErrNoPeers
	}
	return fmt.Sprintf("%s://%s", scheme, host), true, nil
}

// Status determines whether etcd is available. Implements Balancer.Status().
func (b *EtcdBalancer) Status() (bool, error) {
	return IsEtcdHealthy(b.client)
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
	currentWorkers := strconv.Itoa(b.workerCount())
	if b.log.ShouldLog(INFO) {
		b.log.Info("balancer", "Publishing client count to etcd",
			LogFields{"host": b.url.Host, "workers": currentWorkers})
	}
	query := make(url.Values)
	query.Set("s", b.url.Scheme)
	query.Set("w", currentWorkers)
	if _, err = b.client.Set(b.key, query.Encode(),
		uint64(b.ttl/time.Second)); err != nil {

		if b.log.ShouldLog(ERROR) {
			b.log.Error("balancer", "Error publishing client count to etcd", LogFields{
				"error":   err.Error(),
				"workers": currentWorkers,
				"host":    b.url.Host})
		}
		return true, err
	}
	return false, nil
}

func init() {
	AvailableBalancers["etcd"] = func() HasConfigStruct { return new(EtcdBalancer) }
}
