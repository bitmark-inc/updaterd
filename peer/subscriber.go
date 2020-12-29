// Copyright (c) 2014-2017 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package peer

import (
	"bytes"
	"encoding/hex"
	"time"

	"github.com/bitmark-inc/bitmarkd/fault"
	"github.com/bitmark-inc/bitmarkd/mode"
	"github.com/bitmark-inc/bitmarkd/util"
	"github.com/bitmark-inc/go-programs/updaterd/storage"
	"github.com/bitmark-inc/go-programs/updaterd/zmqutil"
	"github.com/bitmark-inc/logger"
	zmq "github.com/pebbe/zmq4"
)

const (
	subscriberSignal = "inproc://bitmark-subscriber-signal"

	// must be the same as bitmarkd: peer/broadcaster.go
	heartbeatInterval = 60 * time.Second
	heartbeatTimeout  = 2 * heartbeatInterval
)

type subscriber struct {
	log     *logger.L
	push    *zmq.Socket
	pull    *zmq.Socket
	clients []*zmqutil.Client
}

// initialise the subscriber
func (sbsc *subscriber) initialise(privateKey []byte, publicKey []byte, connections []Connection) error {

	log := logger.New("subscriber")
	sbsc.log = log

	log.Info("initialising…")

	// validate connection count
	connectionCount := len(connections)
	if 0 == connectionCount {
		log.Error("zero connections are available")
		return fault.ErrNoConnectionsAvailable
	}

	// signalling channel
	err := error(nil)
	sbsc.push, sbsc.pull, err = zmqutil.NewSignalPair(subscriberSignal)
	if nil != err {
		return err
	}

	// all sockets
	sbsc.clients = make([]*zmqutil.Client, connectionCount)

	// error for goto fail
	errX := error(nil)

	// connect all static sockets
	for i, c := range connections {
		address, err := util.NewConnection(c.Subscribe)
		if nil != err {
			log.Errorf("client[%d]=address: %q  error: %s", i, c.Subscribe, err)
			errX = err
			goto fail
		}
		serverPublicKey, err := hex.DecodeString(c.PublicKey)
		if nil != err {
			log.Errorf("client[%d]=public: %q  error: %s", i, c.PublicKey, err)
			errX = err
			goto fail
		}

		// prevent connection to self
		if bytes.Equal(publicKey, serverPublicKey) {
			errX = fault.ErrConnectingToSelfForbidden
			log.Errorf("client[%d]=public: %q  error: %s", i, c.PublicKey, errX)
			goto fail
		}

		client, err := zmqutil.NewClient(zmq.SUB, privateKey, publicKey, 0)
		if nil != err {
			log.Errorf("client[%d]=%q  error: %s", i, c.Subscribe, err)
			errX = err
			goto fail
		}

		sbsc.clients[i] = client

		err = client.Connect(address, serverPublicKey, mode.ChainName())
		if nil != err {
			log.Errorf("connect[%d]=%q  error: %s", i, c.Subscribe, err)
			errX = err
			goto fail
		}
		log.Infof("public key: %x  at: %q", serverPublicKey, c.Subscribe)
	}

	return nil

	// error handling
fail:
	zmqutil.CloseClients(sbsc.clients)
	return errX
}

// subscriber main loop
func (sbsc *subscriber) Run(args interface{}, shutdown <-chan struct{}) {

	log := sbsc.log

	log.Info("starting…")

	go func() {

		expiryRegister := make(map[*zmq.Socket]time.Time)
		checkAt := time.Now().Add(heartbeatTimeout)
		poller := zmqutil.NewPoller()

		for _, client := range sbsc.clients {
			socket := client.BeginPolling(poller, zmq.POLLIN)
			if nil != socket {
				expiryRegister[socket] = checkAt
			}
		}
		poller.Add(sbsc.pull, zmq.POLLIN)

	loop:
		for {
			log.Info("waiting…")

			//polled, _ := poller.Poll(-1)
			polled, _ := poller.Poll(heartbeatTimeout)
			if 0 == len(polled) {
				log.Infof("timeout exceed at %s", heartbeatTimeout.String())
			}

			now := time.Now()
			expiresAt := now.Add(heartbeatTimeout)
			if now.After(checkAt) {
				log.Debugf("current time %s after check time %s",
					now.String(),
					checkAt.String(),
				)
				checkAt = expiresAt
				for s, expires := range expiryRegister {
					if now.After(expires) {
						client := zmqutil.ClientFromSocket(s)
						log.Infof("socket %s expired", s.String())
						if nil == client { // this socket has been closed
							log.Errorf("cannto find client from list, delete from expiry list",
								s.String(),
							)
							log.Info("expiry list:")
							for k, v := range expiryRegister {
								log.Infof("socket %s expired at %s",
									k.String(),
									v.String(),
								)
							}
							delete(expiryRegister, s)
						} else if client.IsConnected() {
							log.Infof("client %s reconnect to remote", client.BasicInfo())
							s, err := client.ReconnectReturningSocket()
							if nil != err {
								log.Errorf("client %s reconnect with error: %s",
									client.BasicInfo(),
									err,
								)
							} else {
								delete(expiryRegister, s)
								// note this new entry may or may not be rescanned by range in this loop
								// since it will have future time it will not be immediately deleted
								log.Infof("client %s reconnects, extend expiry time",
									client.BasicInfo(),
								)
								expiryRegister[s] = expiresAt
							}
						} else {
							log.Debugf("client %s not connected, extend expiry time",
								client.BasicInfo(),
							)
							expiryRegister[s] = expiresAt
						}
					} else if expires.Before(checkAt) {
						log.Debugf("shorten check time from %s to %s",
							checkAt.String(),
							expires.String(),
						)
						checkAt = expires
					}
				}
			}

			for _, p := range polled {
				switch s := p.Socket; s {
				case sbsc.pull:
					_, err := s.RecvMessageBytes(0)
					if nil != err {
						log.Errorf("pull receive error: %s", err)
					}
					break loop

				default:
					data, err := s.RecvMessageBytes(0)
					if nil != err {
						log.Errorf("receive error: %s", err)
					} else {
						theChain := string(data[0])
						if theChain != mode.ChainName() {
							log.Errorf("invalid chain: actual: %q  expect: %s", theChain, mode.ChainName())
							continue loop
						}
						client := zmqutil.ClientFromSocket(s)
						sbsc.process(data[1:], client)
					}
					expiryRegister[s] = expiresAt
				}
			}
		}
		sbsc.pull.Close()
		zmqutil.CloseClients(sbsc.clients)
	}()

loop:
	for {
		log.Info("select…")

		select {
		// wait for shutdown
		case <-shutdown:
			break loop
			// wait for message
		}
	}

	sbsc.push.SendMessage("stop")
	sbsc.push.Close()
}

// process the received subscription
func (sbsc *subscriber) process(data [][]byte, client *zmqutil.Client) {

	log := sbsc.log
	log.Info("incoming message")

	switch string(data[0]) {
	case "block":
		log.Infof("received block: %x", data[1])
		if mode.Is(mode.Normal) {
			err := storage.StoreBlock(data[1])
			if nil != err {
				if err == fault.ErrPreviousBlockDigestDoesNotMatch {
					mode.Set(mode.Resynchronise)
					globalData.conn.state = cStateHighestBlock
				}
				log.Errorf("failed to store block: error: %s", err)
			}
		} else {
			err := fault.ErrNotAvailableDuringSynchronise
			log.Warnf("failed block: error: %s", err)
		}

	case "assets":
		log.Infof("received assets: %x", data[1])
		err := storage.StoreTransactions(data[1])
		if nil != err {
			log.Errorf("failed assets: error: %s", err)
		}

	case "issues":
		log.Infof("received issues: %x", data[1])
		err := storage.StoreTransactions(data[1])
		if nil != err {
			log.Errorf("failed issues: error: %s", err)
		}

	case "transfer":
		log.Infof("received transfer: %x", data[1])
		err := storage.StoreTransactions(data[1])
		if nil != err {
			log.Errorf("failed transfer: error: %s", err)
		}

	case "heart":
		log.Infof("received heart: %x from client: %s", data[1], client.BasicInfo())

	}
}
