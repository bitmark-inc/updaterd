// Copyright (c) 2014-2017 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package storage

import (
	"database/sql"
	"github.com/bitmark-inc/logger"
	"time"
)

// various timeouts
const (
	expiryInterval = 60 * time.Minute // pause to limit expiry load
)

// data for the expiry
type expiry struct {
	log      *logger.L
	database *sql.DB
}

// initialise the expiry
func (exp *expiry) initialise(database *sql.DB) error {

	log := logger.New("expiry")
	exp.log = log

	log.Info("initialising…")

	exp.database = database

	return nil
}

// background for expiry process
func (exp *expiry) Run(args interface{}, shutdown <-chan struct{}) {

	log := exp.log

	log.Info("starting…")

loop:
	for {
		// wait for shutdown
		log.Info("waiting…")

		select {
		case <-shutdown:
			break loop

		case <-time.After(expiryInterval):
			log.Info("removing any expired records")
			err := deleteExpiredRecords(exp.database)
			if nil != err {
				log.Errorf("delete error: %s", err)
			}
		}
	}
}
