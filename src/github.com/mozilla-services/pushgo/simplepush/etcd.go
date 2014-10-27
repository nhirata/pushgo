/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package simplepush

import (
	"fmt"
	"time"

	"github.com/coreos/go-etcd/etcd"

	"github.com/mozilla-services/pushgo/id"
)

// ErrEtcdUnhealthy is returned if etcd is unavailable.
var ErrEtcdUnhealthy = fmt.Errorf("etcd returned unexpected health check result")

// IsKeyExist indicates whether the given error reports that an etcd key
// already exists.
func IsKeyExist(err error) bool {
	clientErr, ok := err.(*etcd.EtcdError)
	return ok && clientErr.ErrorCode == 105
}

// EtcdPinger verifies if etcd is available.
type EtcdPinger struct {
	Client *etcd.Client
	Log    *SimpleLogger
}

// Healthy indicates whether etcd can respond to requests.
func (p *EtcdPinger) Healthy() (ok bool, err error) {
	fakeID, err := id.Generate()
	if err != nil {
		return false, err
	}
	key, expected := "status_"+fakeID, "test"
	if _, err = p.Client.Set(key, expected, uint64(6*time.Second)); err != nil {
		return false, err
	}
	resp, err := p.Client.Get(key, false, false)
	if err != nil {
		return false, err
	}
	if resp.Node.Value != expected {
		if p.Log.ShouldLog(ERROR) {
			p.Log.Error("etcd", "Unexpected health check result",
				LogFields{"expected": expected, "actual": resp.Node.Value})
		}
		return false, ErrEtcdUnhealthy
	}
	p.Client.Delete(key, false)
	return true, nil
}
