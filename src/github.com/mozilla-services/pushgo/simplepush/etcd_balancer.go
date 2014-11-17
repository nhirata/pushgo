/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package simplepush

import (
	"container/list"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

var (
	// ErrNoPeers is returned if the cluster is full.
	ErrNoPeers = errors.New("No peers available")

	// ErrNoDir is returned if an etcd key path for a peer node does not start
	// with the directory name.
	ErrNoDir = errors.New("Key missing directory name")

	// ErrNoScheme is returned if an etcd key path does not contain the scheme of
	// a peer server.
	ErrNoScheme = errors.New("Key missing scheme")

	// ErrNoHost is returned if an etcd key path does not contain the peer's host
	// and port.
	ErrNoHost = errors.New("Key missing host")
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
	// Defaults to 0.75.
	Threshold float64

	// UpdateInterval is the interval for publishing client counts to etcd.
	// Defaults to "1m".
	UpdateInterval string `toml:"update_interval" env:"update_interval"`
}

// EtcdBalancer stores client counts in etcd. Clients connecting to an
// overloaded host are redirected to hosts with the fewest connections.
type EtcdBalancer struct {
	client    *etcd.Client
	maxConns  int
	threshold float64
	dir       string
	url       *url.URL
	key       string
	connCount func() int

	fetchLock sync.RWMutex // Protects the following fields.
	peerURL   string
	peerConns int64
	fetchErr  error
	lastFetch time.Time

	log            *SimpleLogger
	metrics        *Metrics
	updateInterval time.Duration
	ttl            time.Duration

	closeLock sync.Mutex // Protects isClosed.
	isClosed  bool

	closeWait   sync.WaitGroup
	closeSignal chan bool
}

func NewEtcdBalancer() *EtcdBalancer {
	return &EtcdBalancer{
		closeSignal: make(chan bool),
	}
}

func (b *EtcdBalancer) ConfigStruct() interface{} {
	return &EtcdBalancerConf{
		Dir:            "push_conns",
		Servers:        []string{"http://localhost:4001"},
		TTL:            "10s",
		Threshold:      0.75,
		UpdateInterval: "10s",
	}
}

func (b *EtcdBalancer) Init(app *Application, config interface{}) (err error) {
	conf := config.(*EtcdBalancerConf)
	b.log = app.Logger()
	b.metrics = app.Metrics()

	b.connCount = app.ClientCount
	b.maxConns = app.Server().MaxClientConns()

	b.threshold = conf.Threshold
	b.dir = path.Clean(conf.Dir)

	clientURL := app.Server().ClientURL()
	if b.url, err = url.ParseRequestURI(clientURL); err != nil {
		b.log.Alert("balancer", "Error parsing client endpoint", LogFields{
			"error": err.Error(), "url": clientURL})
		return err
	}
	if len(b.url.Host) > 0 {
		b.key = path.Join(b.dir, b.url.Scheme, b.url.Host)
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
		if !IsEtcdKeyExist(err) {
			b.log.Alert("balancer", "Error creating etcd directory",
				LogFields{"error": err.Error()})
			return err
		}
	}

	b.closeWait.Add(2)
	go b.fetchLoop()
	go b.publishLoop()

	return nil
}

// RedirectURL returns the absolute URL of the peer with the fewest connected
// clients. Implements Balancer.RedirectURL().
func (b *EtcdBalancer) RedirectURL() (url string, ok bool, err error) {
	currentConns := int64(b.connCount())
	if float64(currentConns)/float64(b.maxConns) < b.threshold {
		return "", false, nil
	}
	b.fetchLock.RLock()
	url = b.peerURL
	peerConns := b.peerConns
	if b.fetchErr != nil && time.Since(b.lastFetch) > b.ttl {
		err = b.fetchErr
	}
	b.fetchLock.RUnlock()
	if len(url) == 0 || peerConns >= currentConns {
		return "", false, ErrNoPeers
	}
	return url, true, nil
}

func (b *EtcdBalancer) fetchLoop() {
	defer b.closeWait.Done()
	ticker := time.NewTicker(b.updateInterval)
	for ok := true; ok; {
		select {
		case ok = <-b.closeSignal:
		case t := <-ticker.C:
			peerURL, peerConns, err := b.Fetch()
			b.fetchLock.Lock()
			if err != nil {
				b.fetchErr = err
			} else {
				b.lastFetch = t
				b.peerURL = peerURL
				b.peerConns = peerConns
			}
			b.fetchLock.Unlock()
		}
	}
	ticker.Stop()
}

func (b *EtcdBalancer) publishLoop() {
	defer b.closeWait.Done()
	publishInterval := time.Duration(0.75*b.ttl.Seconds()) * time.Second
	ticker := time.NewTicker(publishInterval)
	for ok := true; ok; {
		select {
		case ok = <-b.closeSignal:
		case <-ticker.C:
			b.Publish()
		}
	}
	ticker.Stop()
}

// Status determines whether etcd is available. Implements Balancer.Status().
func (b *EtcdBalancer) Status() (bool, error) {
	return IsEtcdHealthy(b.client)
}

// Close stops the balancer and closes the connection to etcd. Implements
// Balancer.Close().
func (b *EtcdBalancer) Close() (err error) {
	b.closeLock.Lock()
	isClosed := b.isClosed
	if !isClosed {
		b.isClosed = true
	}
	b.closeLock.Unlock()
	if isClosed {
		return nil
	}
	close(b.closeSignal)
	b.closeWait.Wait()
	if len(b.key) > 0 {
		_, err = b.client.Delete(b.key, false)
	}
	return err
}

// parseKey extracts the scheme and host from an etcd key.
func (b *EtcdBalancer) parseKey(key string) (scheme, host string, err error) {
	if len(key) == 0 || key[0] != '/' {
		return "", "", ErrNoDir
	}
	path := key[1:]
	if len(path) <= len(b.dir) || path[:len(b.dir)] != b.dir {
		return "", "", ErrNoDir
	}
	startScheme := strings.IndexByte(path[len(b.dir):], '/')
	if startScheme < 0 {
		return "", "", ErrNoScheme
	}
	startScheme += len(b.dir) + 1
	startHost := strings.IndexByte(path[startScheme:], '/')
	if startHost < 0 {
		return "", "", ErrNoHost
	}
	startHost += startScheme
	return path[startScheme:startHost], path[startHost+1:], nil
}

// minPeer returns the scheme, host, and connection count of the peer node
// with the smallest number of connections.
func (b *EtcdBalancer) minPeer(root *etcd.Node) (scheme, host string, conns int64) {
	logWarning := b.log.ShouldLog(WARNING)
	conns = 1<<63 - 1
	nodes := list.New()
	nodes.PushBack(root)
	for e := nodes.Front(); e != nil; e = e.Next() {
		node := e.Value.(*etcd.Node)
		if len(node.Nodes) > 0 {
			for _, n := range node.Nodes {
				nodes.PushBack(n)
			}
			continue
		}
		if len(node.Value) == 0 {
			continue
		}
		peerScheme, peerHost, err := b.parseKey(node.Key)
		if err != nil {
			if logWarning {
				b.log.Warn("balancer", "Failed to parse host key", LogFields{
					"error": err.Error(), "key": node.Key})
			}
			continue
		}
		if peerScheme == b.url.Scheme && peerHost == b.url.Host {
			continue
		}
		peerConns, err := strconv.ParseInt(node.Value, 10, 64)
		if err != nil {
			if logWarning {
				b.log.Warn("balancer", "Failed to parse client count", LogFields{
					"error": err.Error(), "host": peerHost, "count": node.Value})
			}
			continue
		}
		if peerConns < conns {
			scheme = peerScheme
			host = peerHost
			conns = peerConns
		}
	}
	return
}

// Fetch retrieves the client counts for all nodes from etcd.
func (b *EtcdBalancer) Fetch() (url string, conns int64, err error) {
	response, err := b.client.Get(b.dir, false, true)
	if err != nil {
		if b.log.ShouldLog(ERROR) {
			b.log.Error("balancer", "Failed to retrieve client counts from etcd",
				LogFields{"error": err.Error()})
		}
		b.metrics.Increment("balancer.fetch.error")
		return "", 0, err
	}
	b.metrics.Increment("balancer.fetch.success")
	scheme, host, conns := b.minPeer(response.Node)
	if len(scheme) == 0 || len(host) == 0 {
		return "", 0, ErrNoPeers
	}
	return fmt.Sprintf("%s://%s", scheme, host), conns, nil
}

// Publish stores the client count for the current node in etcd.
func (b *EtcdBalancer) Publish() (err error) {
	currentConns := strconv.Itoa(b.connCount())
	if b.log.ShouldLog(INFO) {
		b.log.Info("balancer", "Publishing client count to etcd",
			LogFields{"host": b.url.Host, "conns": currentConns})
	}
	if _, err = b.client.Set(b.key, currentConns,
		uint64(b.ttl/time.Second)); err != nil {

		if b.log.ShouldLog(ERROR) {
			b.log.Error("balancer", "Error publishing client count to etcd", LogFields{
				"error": err.Error(),
				"conns": currentConns,
				"host":  b.url.Host})
		}
		b.metrics.Increment("balancer.publish.error")
		return err
	}
	b.metrics.Increment("balancer.publish.success")
	return nil
}

func init() {
	AvailableBalancers["etcd"] = func() HasConfigStruct { return NewEtcdBalancer() }
}
