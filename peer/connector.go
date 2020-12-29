// Copyright (c) 2014-2017 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package peer

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"github.com/bitmark-inc/bitmarkd/blockdigest"
	"github.com/bitmark-inc/bitmarkd/fault"
	"github.com/bitmark-inc/bitmarkd/genesis"
	"github.com/bitmark-inc/bitmarkd/mode"
	"github.com/bitmark-inc/bitmarkd/util"
	"github.com/bitmark-inc/go-programs/updaterd/storage"
	"github.com/bitmark-inc/go-programs/updaterd/zmqutil"
	"github.com/bitmark-inc/logger"
	zmq "github.com/pebbe/zmq4"
	"time"
)

// various timeouts
const (
	cycleInterval       = 10 * time.Second // pause to limit bandwidth
	connectorTimeout    = 30 * time.Second // time out for connections
	samplelingLimit     = 10               // number of cycles to be 1 block out of sync before resync
	fetchBlocksPerCycle = 500              // number of blocks to fetch in one set
)

// a state type for the thread
type connectorState int

// state of the connector process
const (
	cStateConnecting   connectorState = iota // register to nodes and make outgoing connections
	cStateHighestBlock connectorState = iota // locate node(s) with highest block number
	cStateForkDetect   connectorState = iota // read block hashes to check for possible fork
	cStateFetchBlocks  connectorState = iota // fetch blocks from current or fork point
	cStateRebuild      connectorState = iota // rebuild database from fork point (config setting to force total rebuild)
	cStateSampling     connectorState = iota // signal resync complete and sample nodes to see if out of sync occurs
)

// type to hold server info (see bitmarkd/peer/listener.go for full record)
type serverInfo struct {
	Version string `json:"version"`
	Chain   string `json:"chain"`
	Normal  bool   `json:"normal"`
	Height  uint64 `json:"height"`
}

// data for the connector
type connector struct {
	log     *logger.L
	clients []*zmqutil.Client
	state   connectorState

	theClient          *zmqutil.Client // client to fetch blocak data from
	startBlockNumber   uint64          // block number wher local chain forks
	highestBlockNumber uint64          // block number on best node
	samples            int             // counter to detect missed block broadcast
}

// initialise the connector
func (conn *connector) initialise(privateKey []byte, publicKey []byte, connections []Connection) error {

	log := logger.New("connector")
	conn.log = log

	log.Info("initialising…")

	// allocate all sockets
	connectionCount := len(connections)
	if 0 == connectionCount {
		log.Error("zero connection connections are available")
		return fault.ErrNoConnectionsAvailable
	}
	conn.clients = make([]*zmqutil.Client, connectionCount)

	// error code for goto fail
	errX := error(nil)

	// initially connect all static sockets
	for i, c := range connections {
		address, err := util.NewConnection(c.Connect)
		if nil != err {
			log.Errorf("client[%d]=address: %q  error: %s", i, c.Connect, err)
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

		client, err := zmqutil.NewClient(zmq.REQ, privateKey, publicKey, connectorTimeout)
		if nil != err {
			log.Errorf("client[%d]=%q  error: %s", i, address, err)
			errX = err
			goto fail
		}

		conn.clients[i] = client

		err = client.Connect(address, serverPublicKey, mode.ChainName())
		if nil != err {
			log.Errorf("connect[%d]=%q  error: %s", i, address, err)
			errX = err
			goto fail
		}
		log.Infof("public key: %x  at: %q", serverPublicKey, c.Connect)
	}

	// start state machine
	conn.state = cStateConnecting

	return nil

	// error handling
fail:
	zmqutil.CloseClients(conn.clients)
	return errX
}

// various RPC calls to upstream connections
func (conn *connector) Run(args interface{}, shutdown <-chan struct{}) {

	log := conn.log

	log.Info("starting…")

loop:
	for {
		// wait for shutdown
		log.Info("waiting…")

		select {
		case <-shutdown:
			break loop

		case <-time.After(cycleInterval):
			conn.process()
		}
	}
	zmqutil.CloseClients(conn.clients)
}

// process the connect and return response
func (conn *connector) process() {
	log := conn.log

	log.Infof("current state: %s", conn.state)

	switch conn.state {
	case cStateConnecting:
		mode.Set(mode.Resynchronise)
		err := checkNodes(log, conn.clients)
		if nil != err {
			log.Criticalf("connection to node failed: error: %s", err)
			logger.Panicf("connection to node failed: error: %s", err)
		}
		conn.state += 1

	case cStateHighestBlock:
		conn.highestBlockNumber, conn.theClient = highestBlock(log, conn.clients)
		if conn.highestBlockNumber > 0 && nil != conn.theClient {
			conn.state += 1
		} else {
			if conn.theClient == nil {
				log.Critical("no alived connections in pool, stay in state HighestBlock")
			}
		}
		log.Infof("highest block number: %d", conn.highestBlockNumber)

	case cStateForkDetect:
		h, err := storage.GetBlockHeight()
		if nil != err {
			log.Criticalf("GetBlockHeight failed: error: %s", err)
			logger.Panicf("GetBlockHeight failed: error: %s", err)
		}

		log.Infof("local block number: %d", h)
		log.Infof("highest block number: %d", conn.highestBlockNumber)
		if conn.highestBlockNumber <= h {
			conn.state = cStateRebuild
		} else {
			// first block number
			conn.startBlockNumber = genesis.BlockNumber + 1
			conn.state += 1 // assume success

			// check digests of descending blocks (to detect a fork)
		check_digests:
			for ; h > genesis.BlockNumber; h -= 1 {
				log.Infof("examine block number: %d", h)
				digest, err := storage.DigestForBlock(h)
				if nil != err {
					log.Errorf("block number: %d  local digest error: %s", h, err)
					conn.state = cStateHighestBlock // retry
					break check_digests
				}
				remoteDigest, err := blockDigest(conn.theClient, h)
				if nil != err {
					log.Errorf("block number: %d  fetch digest error: %s", h, err)
					conn.state = cStateHighestBlock // retry
					break check_digests
				} else if remoteDigest == *digest {
					conn.startBlockNumber = h + 1
					log.Infof("fork from block number: %d  digest: %v", conn.startBlockNumber, remoteDigest)

					// remove old blocks
					err := storage.DeleteDownToBlock(conn.startBlockNumber)
					if nil != err {
						log.Errorf("delete down to block number: %d  error: %s", conn.startBlockNumber, err)
						conn.state = cStateHighestBlock // retry
					}
					break check_digests
				} else {
					log.Infof("mismatched digests local: %v  remote: %v", digest, remoteDigest, remoteDigest)
				}
			}
		}

	case cStateFetchBlocks:
		log.Infof("start   block number: %d", conn.startBlockNumber)
		log.Infof("highest block number: %d", conn.highestBlockNumber)

	fetch_some_blocks:
		for n := 0; n < fetchBlocksPerCycle; n += 1 {

			if conn.startBlockNumber > conn.highestBlockNumber {
				conn.state = cStateHighestBlock // just in case block height has changed
				break fetch_some_blocks
			}

			log.Infof("fetch block number: %d", conn.startBlockNumber)
			packedBlock, err := blockData(conn.theClient, conn.startBlockNumber)
			if nil != err {
				log.Errorf("fetch block number: %d  error: %s", conn.startBlockNumber, err)
				conn.state = cStateHighestBlock // retry
				break fetch_some_blocks
			}
			log.Debugf("store block number: %d", conn.startBlockNumber)
			err = storage.StoreBlock(packedBlock)
			if nil != err {
				log.Errorf("store block number: %d  error: %s", conn.startBlockNumber, err)
				conn.state = cStateHighestBlock // retry
				break fetch_some_blocks
			}

			// next block
			conn.startBlockNumber += 1

		}

	case cStateRebuild:
		// return to normal operations
		conn.state += 1  // next state
		conn.samples = 0 // zero out the counter
		mode.Set(mode.Normal)

	case cStateSampling:
		// check peers
		conn.highestBlockNumber, conn.theClient = highestBlock(log, conn.clients)
		if conn.theClient == nil {
			conn.state = cStateHighestBlock
			log.Critical("no alived connections in pool, move state back to HighestBlock")
			return
		}
		height, err := storage.GetBlockHeight()
		if nil != err {
			log.Criticalf("GetBlockHeight failed: error: %s", err)
			logger.Panicf("GetBlockHeight failed: error: %s", err)
		}

		log.Infof("height: remote: %d  local: %d", conn.highestBlockNumber, height)

		if conn.highestBlockNumber > height {
			if conn.highestBlockNumber-height >= 2 {
				conn.state = cStateForkDetect
			} else {
				conn.samples += 1
				if conn.samples > samplelingLimit {
					conn.state = cStateForkDetect
				}
			}
		}

	}
	log.Debugf("next state: %s", conn.state)
}

// check all nodes are on the same chain as this program
func checkNodes(log *logger.L, clients []*zmqutil.Client) error {

	clientCount := 0

scan_clients:
	for _, client := range clients {
		if !client.IsConnected() {
			// ***** FIX THIS: should there be code to disable client?
			continue scan_clients
		}

		err := client.Send("I")
		if nil != err {
			log.Errorf("checkNodes: send error: %s, node: %s", err, client.String())
			client.Reconnect()
			continue scan_clients
		}
		data, err := client.Receive(0)
		if nil != err {
			log.Errorf("checkNodes: receive error: %s, node: %s", err, client.String())
			client.Reconnect()
			continue scan_clients
		}

		switch string(data[0]) {
		case "E":
			log.Errorf("checkNodes: rpc error response: %q", data[1])
			continue scan_clients
		case "I":
			var info serverInfo
			err = json.Unmarshal(data[1], &info)
			if nil != err {
				log.Errorf("checkNodes: fail to parse server info: %s.", string(data[1]))
				continue scan_clients
			}

			if info.Chain != mode.ChainName() {
				log.Errorf("checkNodes: expected chain: %q but received: %q", mode.ChainName(), info.Chain)
				continue scan_clients
			}
			clientCount += 1
		default:
			log.Errorf("checkNodes: invalid peer response: %s", string(data[1]))
			continue scan_clients
		}
	}
	if 0 == clientCount {
		return fault.ErrNoConnectionsAvailable
	}
	return nil
}

// determine client with highest block
func highestBlock(log *logger.L, clients []*zmqutil.Client) (uint64, *zmqutil.Client) {

	h := uint64(0)
	c := (*zmqutil.Client)(nil)

scan_clients:
	for _, client := range clients {
		if !client.IsConnected() {
			continue scan_clients
		}

		err := client.Send("N")
		if nil != err {
			log.Errorf("highestBlock: send error: %s", err)
			client.Reconnect()
			continue scan_clients
		}

		data, err := client.Receive(0)
		if nil != err {
			log.Errorf("highestBlock: receive error: %s", err)
			client.Reconnect()
			continue scan_clients
		}
		if 2 != len(data) {
			log.Errorf("highestBlock: received: %d  expected: 2", len(data))
			continue scan_clients
		}
		switch string(data[0]) {
		case "E":
			log.Errorf("highestBlock: rpc error response: %q", data[1])
			continue scan_clients
		case "N":
			if 8 != len(data[1]) {
				continue scan_clients
			}
			n := binary.BigEndian.Uint64(data[1])

			if n > h {
				h = n
				c = client
			}
		default:
		}
	}
	return h, c
}

// fetch block digest
func blockDigest(client *zmqutil.Client, blockNumber uint64) (blockdigest.Digest, error) {
	parameter := make([]byte, 8)
	binary.BigEndian.PutUint64(parameter, blockNumber)
	err := client.Send("H", parameter)
	if nil != err {
		client.Reconnect()
		return blockdigest.Digest{}, err
	}

	data, err := client.Receive(0)
	if nil != err {
		client.Reconnect()
		return blockdigest.Digest{}, err
	}

	if 2 != len(data) {
		return blockdigest.Digest{}, fault.ErrInvalidPeerResponse
	}

	switch string(data[0]) {
	case "E":
		return blockdigest.Digest{}, fault.ErrorFromRunes(data[1])
	case "H":
		d := blockdigest.Digest{}
		if blockdigest.Length == len(data[1]) {
			err := blockdigest.DigestFromBytes(&d, data[1])
			return d, err
		}
	default:
	}
	return blockdigest.Digest{}, fault.ErrInvalidPeerResponse
}

// fetch block data
func blockData(client *zmqutil.Client, blockNumber uint64) ([]byte, error) {
	parameter := make([]byte, 8)
	binary.BigEndian.PutUint64(parameter, blockNumber)
	err := client.Send("B", parameter)
	if nil != err {
		client.Reconnect()
		return nil, err
	}

	data, err := client.Receive(0)
	if nil != err {
		client.Reconnect()
		return nil, err
	}

	if 2 != len(data) {
		return nil, fault.ErrInvalidPeerResponse
	}

	switch string(data[0]) {
	case "E":
		return nil, fault.ErrorFromRunes(data[1])
	case "B":
		return data[1], nil
	default:
	}
	return nil, fault.ErrInvalidPeerResponse
}

func (state connectorState) String() string {
	switch state {
	case cStateConnecting:
		return "Connecting"
	case cStateHighestBlock:
		return "HighestBlock"
	case cStateForkDetect:
		return "ForkDetect"
	case cStateFetchBlocks:
		return "FetchBlocks"
	case cStateRebuild:
		return "Rebuild"
	case cStateSampling:
		return "Sampling"
	default:
		return "*Unknown*"
	}
}
