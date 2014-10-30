/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package simplepush

import (
	"fmt"
	"math/rand"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

const (
	minTTL = 2 * time.Second
)

var (
	ErrMinTTL = fmt.Errorf("Default TTL too short; want at least %s", minTTL)
)

type EtcdLocatorConf struct {
	// Dir is the etcd key prefix for storing contacts. Defaults to
	// "push_hosts".
	Dir string

	// Servers is a list of etcd servers.
	Servers []string

	// DefaultTTL is the maximum amount of time that registered contacts will be
	// considered valid. Defaults to "24h".
	DefaultTTL string `env:"ttl"`

	// RefreshInterval is the maximum amount of time that a cached contact list
	// will be considered valid. Defaults to "5m".
	RefreshInterval string `toml:"refresh_interval" env:"refresh_interval"`
}

// EtcdLocator stores routing endpoints in etcd and polls for new contacts.
type EtcdLocator struct {
	logger          *SimpleLogger
	metrics         *Metrics
	refreshInterval time.Duration
	defaultTTL      time.Duration
	serverList      []string
	dir             string
	url             *url.URL
	key             string
	client          *etcd.Client
	updater         *PeriodicUpdater
	closeLock       sync.Mutex
	isClosed        bool
	lastErr         error
}

func (*EtcdLocator) ConfigStruct() interface{} {
	return &EtcdLocatorConf{
		Dir:             "push_hosts",
		Servers:         []string{"http://localhost:4001"},
		DefaultTTL:      "24h",
		RefreshInterval: "5m",
	}
}

func (l *EtcdLocator) Init(app *Application, config interface{}) (err error) {
	conf := config.(*EtcdLocatorConf)
	l.logger = app.Logger()
	l.metrics = app.Metrics()

	if l.refreshInterval, err = time.ParseDuration(conf.RefreshInterval); err != nil {
		l.logger.Alert("locator", "Could not parse refreshInterval",
			LogFields{"error": err.Error(),
				"refreshInterval": conf.RefreshInterval})
		return err
	}
	// default time for the server to be "live"
	if l.defaultTTL, err = time.ParseDuration(conf.DefaultTTL); err != nil {
		l.logger.Alert("locator",
			"Could not parse etcd default TTL",
			LogFields{"value": conf.DefaultTTL, "error": err.Error()})
		return err
	}
	if l.defaultTTL < minTTL {
		l.logger.Alert("locator",
			"default TTL too short",
			LogFields{"value": conf.DefaultTTL})
		return ErrMinTTL
	}

	l.serverList = conf.Servers
	l.dir = path.Clean(conf.Dir)

	// Use the hostname and port of the current server as the etcd key.
	routerURL := app.Router().URL()
	if l.url, err = url.ParseRequestURI(routerURL); err != nil {
		l.logger.Alert("locator", "Error parsing router URL", LogFields{
			"error": err.Error(), "url": routerURL})
		return err
	}
	if len(l.url.Host) > 0 {
		l.key = path.Join(l.dir, l.url.Host)
	}

	if l.logger.ShouldLog(INFO) {
		l.logger.Info("locator", "connecting to etcd servers",
			LogFields{"list": strings.Join(l.serverList, ";")})
	}
	l.client = etcd.NewClient(l.serverList)

	// create the push hosts directory (if not already there)
	if _, err = l.client.CreateDir(l.dir, 0); err != nil {
		if !IsKeyExist(err) {
			l.logger.Alert("locator", "etcd createDir error", LogFields{
				"error": err.Error()})
			return err
		}
	}

	publishInterval := time.Duration(0.75*l.defaultTTL.Seconds()) * time.Second
	l.updater = NewPeriodicUpdater(l, publishInterval, l.refreshInterval)

	return nil
}

// Close stops the locator and closes the etcd client connection. Implements
// Locator.Close().
func (l *EtcdLocator) Close() (err error) {
	l.closeLock.Lock()
	defer l.closeLock.Unlock()
	err = l.lastErr
	if l.isClosed {
		return err
	}
	l.isClosed = true
	l.lastErr = l.updater.Close()
	if l.key != "" {
		_, l.lastErr = l.client.Delete(l.key, false)
	}
	return err
}

// Contacts returns a shuffled list of all nodes in the Simple Push cluster.
// Implements Locator.Contacts().
func (l *EtcdLocator) Contacts(string) (contacts []string, err error) {
	reply, err := l.updater.Fetch()
	if err != nil {
		if l.logger.ShouldLog(ERROR) {
			l.logger.Error("locator", "Could not get server list from etcd",
				LogFields{"error": err.Error()})
		}
		return nil, err
	}
	contacts = reply.([]string)
	for length := len(contacts); length > 0; {
		index := rand.Intn(length)
		length--
		contacts[index], contacts[length] = contacts[length], contacts[index]
	}
	return contacts, nil
}

// Status determines whether etcd can respond to requests. Implements
// Locator.Status().
func (l *EtcdLocator) Status() (bool, error) {
	return IsEtcdHealthy(l.client)
}

// Publish registers the server to the etcd cluster. Implements
// Updater.Publish().
func (l *EtcdLocator) Publish() (canRetry bool, err error) {
	if l.logger.ShouldLog(INFO) {
		l.logger.Info("locator", "Publishing host to etcd", LogFields{
			"key": l.key, "host": l.url.Host})
	}
	query := make(url.Values)
	query.Set("s", l.url.Scheme)
	if _, err = l.client.Set(l.key, query.Encode(),
		uint64(l.defaultTTL/time.Second)); err != nil {

		if l.logger.ShouldLog(ERROR) {
			l.logger.Error("locator", "Failed to publish host to etcd", LogFields{
				"error": err.Error(), "key": l.key, "host": l.url.Host})
		}
		return true, err
	}
	return false, nil
}

// Fetch gets the current contact list from etcd. Implements Updater.Fetch().
func (l *EtcdLocator) Fetch() (servers interface{}, canRetry bool, err error) {
	nodeList, err := l.client.Get(l.dir, false, false)
	if err != nil {
		if l.logger.ShouldLog(ERROR) {
			l.logger.Error("locator", "Could not fetch contacts from etcd",
				LogFields{"error": err.Error(), "dir": l.dir})
		}
		return nil, true, err
	}
	reply := make([]string, 0, len(nodeList.Node.Nodes))
	for _, node := range nodeList.Node.Nodes {
		if node.Value == "" {
			continue
		}
		host := path.Base(node.Key)
		if host == l.url.Host {
			continue
		}
		query, err := url.ParseQuery(node.Value)
		if err != nil {
			if l.logger.ShouldLog(WARNING) {
				l.logger.Warn("locator", "Failed to parse etcd contact info", LogFields{
					"error": err.Error(), "host": host, "info": node.Value})
			}
			continue
		}
		scheme := query.Get("s")
		if scheme == "" {
			if l.logger.ShouldLog(WARNING) {
				l.logger.Warn("locator", "etcd contact info missing scheme", LogFields{
					"host": host, "info": node.Value})
			}
			continue
		}
		reply = append(reply, fmt.Sprintf("%s://%s", scheme, host))
	}
	return reply, false, nil
}

func init() {
	AvailableLocators["etcd"] = func() HasConfigStruct { return new(EtcdLocator) }
}
