// Copyright (c) 2014-2017 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package peer

import (
	"github.com/bitmark-inc/bitmarkd/background"
	"github.com/bitmark-inc/bitmarkd/fault"
	"github.com/bitmark-inc/bitmarkd/zmqutil"
	"github.com/bitmark-inc/logger"
	"sync"
)

// hardwired connections
// this is read from a lua configuration file
type Connection struct {
	PublicKey string `gluamapper:"public_key" json:"public_key"`
	Subscribe string `gluamapper:"subscribe" json:"subscribe"`
	Connect   string `gluamapper:"connect" json:"connect"`
}

// a block of configuration data
// this is read from a lua configuration file
type Configuration struct {
	PrivateKey string       `gluamapper:"private_key" json:"private_key"`
	PublicKey  string       `gluamapper:"public_key" json:"public_key"`
	Node       []Connection `gluamapper:"node" json:"node"`
}

// globals for background proccess
type peerData struct {
	sync.RWMutex // to allow locking

	// logger
	log *logger.L

	conn connector  // for RPC requests
	sbsc subscriber // for subscriptions

	// for background
	background *background.T

	// set once during initialise
	initialised bool
}

// global data
var globalData peerData

// initialise peer backgrouds processes
func Initialise(configuration *Configuration) error {

	globalData.Lock()
	defer globalData.Unlock()

	// no need to start if already started
	if globalData.initialised {
		return fault.ErrAlreadyInitialised
	}

	globalData.log = logger.New("peer")
	globalData.log.Info("starting…")

	// read the keys
	privateKey, err := zmqutil.ReadPrivateKey(configuration.PrivateKey)
	if nil != err {
		globalData.log.Errorf("read private key file: %q  error: %s", configuration.PrivateKey, err)
		return err
	}
	publicKey, err := zmqutil.ReadPublicKey(configuration.PublicKey)
	if nil != err {
		globalData.log.Errorf("read public key file: %q  error: %s", configuration.PublicKey, err)
		return err
	}
	globalData.log.Tracef("peer private key: %q", privateKey)
	globalData.log.Tracef("peer public key:  %q", publicKey)

	if err := globalData.conn.initialise(privateKey, publicKey, configuration.Node); nil != err {
		return err
	}
	if err := globalData.sbsc.initialise(privateKey, publicKey, configuration.Node); nil != err {
		return err
	}

	// all data initialised
	globalData.initialised = true

	// start background processes
	globalData.log.Info("start background…")

	var processes = background.Processes{
		&globalData.conn,
		&globalData.sbsc,
	}

	globalData.background = background.Start(processes, globalData.log)

	return nil
}

// finialise - stop all background tasks
func Finalise() error {
	globalData.Lock()
	defer globalData.Unlock()

	if !globalData.initialised {
		return fault.ErrNotInitialised
	}

	globalData.log.Info("shutting down…")
	globalData.log.Flush()

	// stop background
	globalData.background.Stop()

	// finally...
	globalData.initialised = false

	return nil
}
