/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package simplepush

import (
	"errors"
	"sync"
	"time"
)

// ErrUpdaterClosed is the error used for fetches from a closed periodic
// updater.
var ErrUpdaterClosed = errors.New("updater: fetch from closed periodic updater")

// An Updater describes a type that publishes and fetches data.
type Updater interface {
	Publish() (err error)
	Fetch() (reply interface{}, err error)
}

// RetryHandler is an optional interface implemented by Updaters for
// customizing the retry behavior.
type RetryHandler interface {
	HandleRetry(attempt int, err error) bool
}

// periodicFuture represents a response to a fetch.
type periodicFuture struct {
	signal  chan bool
	replies chan interface{}
	errors  chan error
}

// NewPeriodicUpdater returns a periodic updater with default retry
// settings.
func NewPeriodicUpdater(u Updater, publishInterval,
	fetchInterval time.Duration) (p *PeriodicUpdater) {

	p = &PeriodicUpdater{
		Updater:         u,
		PublishInterval: publishInterval,
		FetchInterval:   fetchInterval,
		RetryDelay:      20 * time.Millisecond,
		MaxDelay:        5 * time.Second,
		MaxRetries:      5,
		futures:         make(chan periodicFuture),
		closeSignal:     make(chan bool),
	}
	p.closeWait.Add(2)
	go p.publishLoop()
	go p.fetchLoop()
	return p
}

// A PeriodicUpdater publishes and fetches data on an interval, caches
// fetches, and retries failed operations with exponential backoff.
type PeriodicUpdater struct {
	Updater
	PublishInterval time.Duration
	FetchInterval   time.Duration
	RetryDelay      time.Duration
	MaxDelay        time.Duration
	MaxRetries      int
	futures         chan periodicFuture
	closeWait       sync.WaitGroup
	closeLock       sync.Mutex
	closeSignal     chan bool
	isClosing       bool
	isClosed        bool
	lastErr         error
	lastReply       interface{}
	lastFetch       time.Time
}

// canRetry determines whether a request should be retried.
func (p *PeriodicUpdater) canRetry(attempt int, err error) bool {
	if attempt > p.MaxRetries {
		return false
	}
	if handler, ok := p.Updater.(RetryHandler); ok {
		return handler.HandleRetry(attempt, err)
	}
	return true
}

// Publish submits new data using the underlying updater, retrying with
// exponential backoff if necessary.
func (p *PeriodicUpdater) Publish() (err error) {
	var (
		attempt    int
		retryDelay time.Duration
	)
	for ok := true; ok; {
		if err = p.Updater.Publish(); err != nil {
			attempt++
			if !p.canRetry(attempt, err) {
				return err
			}
			if retryDelay == 0 {
				retryDelay = p.RetryDelay
			} else if retryDelay *= 2; retryDelay > p.MaxDelay {
				retryDelay = p.MaxDelay
			}
			select {
			case ok = <-p.closeSignal:
			case <-time.After(retryDelay):
			}
			continue
		}
		break
	}
	return nil
}

// Fetch retrieves new data from the underlying updater. Returns
// ErrUpdaterClosed
func (p *PeriodicUpdater) Fetch() (reply interface{}, err error) {
	replies, errors := make(chan interface{}), make(chan error)
	signal := make(chan bool)
	defer close(signal)
	select {
	case <-p.closeSignal:
		return nil, ErrUpdaterClosed

	case p.futures <- periodicFuture{signal, replies, errors}:
	}
	select {
	case reply = <-replies:
	case err = <-errors:
	}
	return
}

// fetch retries failed fetches and records the last reply and fetch time.
func (p *PeriodicUpdater) fetch() (reply interface{}, err error) {
	var (
		attempt    int
		retryDelay time.Duration
	)
	for ok := true; ok; {
		if reply, err = p.Updater.Fetch(); err != nil {
			attempt++
			if !p.canRetry(attempt, err) {
				return nil, err
			}
			if retryDelay == 0 {
				retryDelay = p.RetryDelay
			} else if retryDelay *= 2; retryDelay > p.MaxDelay {
				retryDelay = p.MaxDelay
			}
			select {
			case ok = <-p.closeSignal:
			case <-time.After(retryDelay):
			}
			continue
		}
		p.lastReply = reply
		p.lastFetch = time.Now()
		break
	}
	return
}

// Close stops the periodic updater and waits for the publish and fetch loops
// to exit. Any pending Fetch() calls will be unblocked with errors.
func (p *PeriodicUpdater) Close() (err error) {
	p.closeLock.Lock()
	if p.isClosed {
		p.closeLock.Unlock()
		return p.lastErr
	}
	p.isClosed = true
	p.signalClose()
	p.closeLock.Unlock()
	p.closeWait.Wait()
	return nil
}

// fatal acquires p.closeLock, stops the publish and fetch loops, and releases
// the lock, recording the error in p.lastErr.
func (p *PeriodicUpdater) fatal(err error) {
	p.closeLock.Lock()
	p.signalClose()
	p.lastErr = err
	p.closeLock.Unlock()
}

// signalClose sends a stop signal. The caller must hold p.closeLock.
func (p *PeriodicUpdater) signalClose() {
	if p.isClosing {
		return
	}
	close(p.closeSignal)
	p.isClosing = true
}

// CloseNotify returns a receive-only channel that blocks until the underlying
// updater is closed.
func (p *PeriodicUpdater) CloseNotify() <-chan bool {
	return p.closeSignal
}

// publishLoop periodically publishes updates.
func (p *PeriodicUpdater) publishLoop() {
	defer p.closeWait.Done()
	if err := p.Publish(); err != nil {
		p.fatal(err)
		return
	}
	ticker := time.NewTicker(p.PublishInterval)
	for ok := true; ok; {
		select {
		case ok = <-p.closeSignal:
		case <-ticker.C:
			if err := p.Publish(); err != nil {
				p.fatal(err)
				break
			}
		}
	}
	ticker.Stop()
}

// fetchLoop periodically fetches new data and returns cached results.
func (p *PeriodicUpdater) fetchLoop() {
	defer p.closeWait.Done()
	for ok := true; ok; {
		var err error
		select {
		case ok = <-p.closeSignal:
		case <-time.After(p.FetchInterval):
			if _, err = p.fetch(); err != nil {
				p.fatal(err)
				break
			}

		case future := <-p.futures:
			var (
				reply   interface{}
				replies chan interface{}
				errors  chan error
			)
			if !p.lastFetch.IsZero() && time.Now().Sub(p.lastFetch) < p.FetchInterval {
				reply = p.lastReply
			} else {
				reply, err = p.fetch()
			}
			if err != nil {
				errors = future.errors
			} else {
				replies = future.replies
			}
			select {
			case <-future.signal:
			case replies <- reply:
			case errors <- err:
			}
		}
	}
	p.lastReply = nil
}
