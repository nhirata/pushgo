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

// IsEtcdKeyExist indicates whether the given error reports that an etcd key
// already exists.
func IsEtcdKeyExist(err error) bool {
	clientErr, ok := err.(*etcd.EtcdError)
	return ok && clientErr.ErrorCode == 105
}

// IsEtcdTemporary indicates whether the given error is a temporary
// etcd error.
func IsEtcdTemporary(err error) bool {
	clientErr, ok := err.(*etcd.EtcdError)
	// Raft (300-class) and internal (400-class) errors are temporary.
	return !ok || clientErr.ErrorCode >= 300 && clientErr.ErrorCode < 500
}

// IsEtcdHealthy indicates whether etcd can respond to requests.
func IsEtcdHealthy(client *etcd.Client) (ok bool, err error) {
	fakeID, err := id.Generate()
	if err != nil {
		return false, err
	}
	key, expected := "status_"+fakeID, "test"
	if _, err = client.Set(key, expected, uint64(6*time.Second)); err != nil {
		return false, err
	}
	resp, err := client.Get(key, false, false)
	if err != nil {
		return false, err
	}
	if resp.Node.Value != expected {
		return false, fmt.Errorf("Unexpected health check result: got %s; want %s",
			resp.Node.Value, expected)
	}
	client.Delete(key, false)
	return true, nil
}
