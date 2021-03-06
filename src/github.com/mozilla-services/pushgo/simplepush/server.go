/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package simplepush

import (
	"bytes"
	"errors"
	"runtime"
	"strconv"
	"text/template"
	"time"
)

// -- SERVER this handles REST requests and coordinates between connected
// clients (e.g. wakes client when data is ready, potentially issues remote
// wake command to client, etc.)

type Client struct {
	// client descriptor info.
	Worker Worker
	PushWS *PushWS `json:"-"`
	UAID   string  `json:"uaid"`
}

// Basic global server options
type ServerConfig struct {
	PushEndpoint string `toml:"push_endpoint_template" env:"push_url_template"`
}

type Server interface {
	RequestFlush(client *Client, channel string, version int64,
		data string) (err error)
	UpdateClient(client *Client, chid, uid string, vers int64,
		time time.Time, data string) (err error)
	HandleCommand(cmd PushCommand, sock *PushWS) (
		result int, args JsMap)
	Close() error
}

func NewServer() *Serv {
	return new(Serv)
}

type Serv struct {
	app      *Application
	logger   *SimpleLogger
	metrics  Statistician
	store    Store
	router   Router
	key      []byte
	template *template.Template
	prop     PropPinger
}

func (self *Serv) ConfigStruct() interface{} {
	return &ServerConfig{
		PushEndpoint: "{{.CurrentHost}}/update/{{.Token}}",
	}
}

func (self *Serv) Init(app *Application, config interface{}) (err error) {
	conf := config.(*ServerConfig)

	self.app = app
	self.logger = app.Logger()
	self.metrics = app.Metrics()
	self.store = app.Store()

	self.prop = app.PropPinger()
	self.key = app.TokenKey()
	self.router = app.Router()

	if self.template, err = template.New("Push").Parse(conf.PushEndpoint); err != nil {
		self.logger.Panic("server", "Could not parse push endpoint template",
			LogFields{"error": err.Error()})
		return err
	}

	return nil
}

// A client connects!
func (self *Serv) Hello(worker Worker, cmd PushCommand, sock *PushWS) (result int, arguments JsMap) {

	args := cmd.Arguments
	uaid := args["uaid"].(string)

	if self.logger.ShouldLog(INFO) {
		self.logger.Info("server", "handling 'hello'",
			LogFields{"uaid": uaid})
	}

	if connect, _ := args["connect"].([]byte); len(connect) > 0 && self.prop != nil {
		if err := self.prop.Register(uaid, connect); err != nil {
			if self.logger.ShouldLog(WARNING) {
				self.logger.Warn("server", "Could not set proprietary info",
					LogFields{"error": err.Error(),
						"connect": string(connect)})
			}
		}
	}

	// Create a new, live client entry for this record.
	// See Bye for discussion of potential longer term storage of this info
	client := &Client{
		Worker: worker,
		PushWS: sock,
		UAID:   uaid,
	}
	self.app.AddClient(uaid, client)
	self.router.Register(uaid)
	self.logger.Info("dash", "Client registered", nil)

	// We don't register the list of known ChannelIDs since we echo
	// back any ChannelIDs sent on behalf of this UAID.
	arguments = args
	result = 200
	return result, arguments
}

func (self *Serv) Bye(sock *PushWS) {
	// Remove the UAID as a registered listener.
	// NOTE: in instances where proprietary wake-ups are issued, you may
	// wish not to delete the record from Clients, since this is the only
	// way to note a record needs waking.
	//
	// For that matter, you may wish to store the Proprietary wake data to
	// something commonly shared (like memcache) so that the device can be
	// woken when not connected.
	now := time.Now()
	uaid := sock.UAID()
	if self.logger.ShouldLog(DEBUG) {
		self.logger.Debug("server", "Cleaning up socket",
			LogFields{"uaid": uaid})
	}
	if self.logger.ShouldLog(INFO) {
		self.logger.Info("dash", "Socket connection terminated",
			LogFields{
				"uaid":     uaid,
				"duration": strconv.FormatInt(int64(now.Sub(sock.Born)), 10)})
	}
	if !sock.IsClosed() {
		self.router.Unregister(uaid)
		self.app.RemoveClient(uaid)
	}
	sock.Close()
}

func (self *Serv) Regis(cmd PushCommand, sock *PushWS) (result int, arguments JsMap) {
	// A semi-no-op, since we don't care about the appid, but we do want
	// to create a valid endpoint.
	var err error
	args := cmd.Arguments
	args["status"] = 200
	// Generate the call back URL
	uaid := sock.UAID()
	chid, _ := args["channelID"].(string)
	token, ok := self.store.IDsToKey(uaid, chid)
	if !ok {
		return 500, nil
	}
	if token, err = self.encodePK(token); err != nil {
		if self.logger.ShouldLog(ERROR) {
			self.logger.Error("server", "Token Encoding error",
				LogFields{"uaid": uaid,
					"channelID": chid})
		}
		return 500, nil
	}

	if args["push.endpoint"], err = self.genEndpoint(token); err != nil {
		if self.logger.ShouldLog(ERROR) {
			self.logger.Error("server",
				"Could not generate Push Endpoint",
				LogFields{"error": err.Error()})
		}
		return 500, nil
	}
	if self.logger.ShouldLog(INFO) {
		self.logger.Info("server",
			"Generated Push Endpoint",
			LogFields{"uaid": uaid,
				"channelID": chid,
				"token":     token,
				"endpoint":  args["push.endpoint"].(string)})
	}
	return 200, args
}

func (self *Serv) encodePK(key string) (token string, err error) {
	if len(self.key) == 0 {
		return key, nil
	}
	// if there is a key, encrypt the token
	btoken := []byte(key)
	return Encode(self.key, btoken)
}

func (self *Serv) genEndpoint(token string) (string, error) {
	// cheezy variable replacement.
	endpoint := new(bytes.Buffer)
	if err := self.template.Execute(endpoint, struct {
		Token       string
		CurrentHost string
	}{
		token,
		self.app.EndpointHandler().URL(),
	}); err != nil {
		return "", err
	}
	return endpoint.String(), nil
}

func (self *Serv) RequestFlush(client *Client, channel string, version int64, data string) (err error) {
	defer func(client *Client, version int64) {
		if r := recover(); r != nil {
			var uaid string
			if client != nil {
				uaid = client.UAID
			}
			if flushErr, ok := r.(error); ok {
				err = flushErr
			} else {
				err = errors.New("Error requesting flush")
			}
			if self.logger.ShouldLog(ERROR) {
				stack := make([]byte, 1<<16)
				n := runtime.Stack(stack, false)
				self.logger.Error("server",
					"requestFlush failed",
					LogFields{"error": err.Error(),
						"uaid":  uaid,
						"stack": string(stack[:n])})
			}
			if len(uaid) > 0 && self.prop != nil {
				self.prop.Send(uaid, version, data)
			}
		}
		return
	}(client, version)

	if client != nil {
		if self.logger.ShouldLog(INFO) {
			self.logger.Info("server",
				"Requesting flush",
				LogFields{"uaid": client.UAID,
					"chid":    channel,
					"version": strconv.FormatInt(version, 10),
					"data":    data,
				})
		}

		// Attempt to send the command
		return client.Worker.Flush(client.PushWS, 0, channel, version, data)
	}
	return nil
}

func (self *Serv) UpdateClient(client *Client, chid, uid string, vers int64,
	time time.Time, data string) (err error) {

	var reason string
	if err = self.store.Update(uid, chid, vers); err != nil {
		reason = "Failed to update channel"
		goto updateError
	}

	if err = self.RequestFlush(client, chid, vers, data); err != nil {
		reason = "Failed to flush"
		goto updateError
	}
	return nil

updateError:
	if self.logger.ShouldLog(ERROR) {
		self.logger.Error("server", reason,
			LogFields{"error": err.Error(),
				"uaid": uid,
				"chid": chid})
	}
	return err
}

func (self *Serv) HandleCommand(cmd PushCommand, sock *PushWS) (result int, args JsMap) {
	var ret JsMap
	if cmd.Arguments != nil {
		args = cmd.Arguments
	} else {
		args = make(JsMap)
	}

	switch cmd.Command {
	case HELLO:
		if self.logger.ShouldLog(DEBUG) {
			self.logger.Debug("server", "Handling HELLO event", nil)
		}
		worker := args["worker"].(Worker)
		result, ret = self.Hello(worker, cmd, sock)
	case REGIS:
		if self.logger.ShouldLog(DEBUG) {
			self.logger.Debug("server", "Handling REGIS event", nil)
		}
		result, ret = self.Regis(cmd, sock)
	case DIE:
		if self.logger.ShouldLog(DEBUG) {
			self.logger.Debug("server", "Cleanup", nil)
		}
		self.Bye(sock)
		return 0, nil
	}

	args["uaid"] = ret["uaid"]
	return result, args
}

func (self *Serv) Close() error {
	return nil
}

// o4fs
// vim: set tabstab=4 softtabstop=4 shiftwidth=4 noexpandtab
