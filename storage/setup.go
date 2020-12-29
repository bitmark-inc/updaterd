// Copyright (c) 2014-2017 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package storage

import (
	"database/sql"
	"github.com/bitmark-inc/bitmarkd/background"
	"github.com/bitmark-inc/bitmarkd/fault"
	"github.com/bitmark-inc/logger"
	"strings"
	"sync"
)

// holds the database handle
var globalData struct {
	sync.Mutex
	log        *logger.L
	database   *sql.DB
	exp        expiry
	background *background.T
}

// Configuration of database server
type Configuration struct {
	Database    string `gluamapper:"database" json:"database"`       // The name of the database to connect to.
	User        string `gluamapper:"user" json:"user"`               // The user to sign in as.
	Password    string `gluamapper:"password" json:"password"`       // The user's password.
	Host        string `gluamapper:"host" json:"host"`               // The host to connect to. Values that start with / are for unix domain sockets. (default is localhost)
	Port        string `gluamapper:"port" json:"port"`               // The port to bind to. (default is 5432)
	SslMode     string `gluamapper:"sslmode" json:"sslmode"`         // Whether or not to use SSL (default is require, this is not the default for libpq)
	Fallback    string `gluamapper:"fallback" json:"fallback"`       // An application_name to fall back to if one isn't provided.
	Timeout     string `gluamapper:"timeout" json:"timeout"`         // Maximum wait for connection, in seconds. Zero or not specified means wait indefinitely.
	SslCert     string `gluamapper:"sslcert" json:"sslcert"`         // Cert file location. The file must contain PEM encoded data.
	SslKey      string `gluamapper:"sslkey" json:"sslkey"`           // Key file location. The file must contain PEM encoded data.
	SslRootCert string `gluamapper:"sslrootcert" json:"sslrootcert"` // The location of the root certificate file. The file must contain PEM encoded data.
}

// open up the database connection
func Initialise(database Configuration) error {
	globalData.Lock()
	defer globalData.Unlock()

	if nil != globalData.database {
		return fault.ErrAlreadyInitialised
	}

	log := logger.New("storage")
	globalData.log = log
	log.Info("starting…")

	connectionString := quote("dbname", database.Database) +
		quote("host", database.Host) +
		quote("port", database.Port) +
		quote("user", database.User) +
		quote("password", database.Password) +
		quote("sslmode", database.SslMode) +
		quote("fallback_application_name", database.Fallback) +
		quote("connect_timeout", database.Timeout) +
		quote("sslcert", database.SslCert) +
		quote("sslkey", database.SslKey) +
		quote("sslrootcert", database.SslRootCert)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Criticalf("failed to connect to database %s  error: %s", database.Database, err)
		return err
	}
	globalData.database = db

	// // ensure that the database is compatible
	// versionValue, err := globalData.database.Get(versionKey, nil)
	// if leveldb.ErrNotFound == err {
	// 	err = globalData.database.Put(versionKey, currentVersion, nil)
	// 	if nil != err {
	// 		return err
	// 	}
	// } else if nil != err {
	// 	return err
	// } else if !bytes.Equal(versionValue, currentVersion) {
	// 	return fmt.Errorf("incompatible database version: expected: 0x%x  actual: 0x%x", currentVersion, versionValue)
	// }

	if err := globalData.exp.initialise(db); nil != err {
		return err
	}

	// start background processes
	globalData.log.Info("start background…")

	var processes = background.Processes{
		&globalData.exp,
	}

	globalData.background = background.Start(processes, globalData.log)

	return nil
}

// close the database connection
func Finalise() {
	globalData.Lock()
	defer globalData.Unlock()

	// no need to stop if already stopped
	if nil == globalData.database {
		return
	}

	globalData.log.Info("shutting down…")
	globalData.log.Flush()

	// stop background
	globalData.background.Stop()

	globalData.database.Close()
	globalData.database = nil
}

// produce "name='value'
func quote(name string, value string) string {
	if "" == name || "" == value {
		return ""
	}
	v := strings.Replace(value, "'", "\\'", -1)
	return " " + name + "='" + v + "'"
}
