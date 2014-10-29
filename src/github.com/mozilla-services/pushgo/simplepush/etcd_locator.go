/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package simplepush

import (
	"fmt"
	"math/rand"
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
	host            string
	key             string
	client          *etcd.Client
	pinger          *EtcdPinger
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
		l.logger.Alert("etcd", "Could not parse refreshInterval",
			LogFields{"error": err.Error(),
				"refreshInterval": conf.RefreshInterval})
		return err
	}
	// default time for the server to be "live"
	if l.defaultTTL, err = time.ParseDuration(conf.DefaultTTL); err != nil {
		l.logger.Alert("etcd",
			"Could not parse etcd default TTL",
			LogFields{"value": conf.DefaultTTL, "error": err.Error()})
		return err
	}
	if l.defaultTTL < minTTL {
		l.logger.Alert("etcd",
			"default TTL too short",
			LogFields{"value": conf.DefaultTTL})
		return ErrMinTTL
	}

	l.serverList = conf.Servers
	l.dir = path.Clean(conf.Dir)

	// Use the hostname and port of the current server as the etcd key.
	if l.host = app.Router().Host(); l.host != "" {
		l.key = path.Join(l.dir, l.host)
	}

	if l.logger.ShouldLog(INFO) {
		l.logger.Info("etcd", "connecting to etcd servers",
			LogFields{"list": strings.Join(l.serverList, ";")})
	}
	l.client = etcd.NewClient(l.serverList)

	// create the push hosts directory (if not already there)
	if _, err = l.client.CreateDir(l.dir, 0); err != nil {
		if !IsKeyExist(err) {
			l.logger.Alert("etcd", "etcd createDir error", LogFields{
				"error": err.Error()})
			return err
		}
	}

	publishInterval := time.Duration(0.75*l.defaultTTL.Seconds()) * time.Second
	l.updater = NewPeriodicUpdater(l, publishInterval, l.refreshInterval)
	l.pinger = &EtcdPinger{l.client, l.logger}

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
			l.logger.Error("etcd", "Could not get server list",
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
	return l.pinger.Healthy()
}

// Publish registers the server to the etcd cluster. Implements
// Updater.Publish().
func (l *EtcdLocator) Publish() (canRetry bool, err error) {
	if l.logger.ShouldLog(INFO) {
		l.logger.Info("etcd", "Registering host", LogFields{"key": l.key,
			"host": l.host})
	}
	if _, err = l.client.Set(l.key, l.host, uint64(l.defaultTTL/time.Second)); err != nil {
		if l.logger.ShouldLog(ERROR) {
			l.logger.Error("etcd", "Failed to register",
				LogFields{"error": err.Error(),
					"key":  l.key,
					"host": l.host})
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
			l.logger.Error("etcd", "Could not register with etcd",
				LogFields{"error": err.Error(), "dir": l.dir})
		}
		return nil, true, err
	}
	reply := make([]string, 0, len(nodeList.Node.Nodes))
	for _, node := range nodeList.Node.Nodes {
		if node.Value == l.host || node.Value == "" {
			continue
		}
		reply = append(reply, node.Value)
	}
	return reply, false, nil
}

func init() {
	AvailableLocators["etcd"] = func() HasConfigStruct { return new(EtcdLocator) }
}