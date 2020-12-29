// Copyright (c) 2014-2017 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"github.com/bitmark-inc/bitmarkd/blockrecord"
	"github.com/bitmark-inc/bitmarkd/mode"
	"github.com/bitmark-inc/bitmarkd/zmqutil"
	"github.com/bitmark-inc/exitwithstatus"
	"github.com/bitmark-inc/getoptions"
	"github.com/bitmark-inc/go-programs/updaterd/peer"
	"github.com/bitmark-inc/go-programs/updaterd/storage"
	"github.com/bitmark-inc/logger"
	"os"
	"os/signal"
	//"runtime/pprof"
	"strings"
	"syscall"
)

// set by the linker: go build -ldflags "-X main.version=M.N" ./...
var version string = "zero" // do not change this value

// updaterd main program
func main() {
	// ensure exit handler is first
	defer exitwithstatus.Handler()

	flags := []getoptions.Option{
		{Long: "help", HasArg: getoptions.NO_ARGUMENT, Short: 'h'},
		{Long: "verbose", HasArg: getoptions.NO_ARGUMENT, Short: 'v'},
		{Long: "quiet", HasArg: getoptions.NO_ARGUMENT, Short: 'q'},
		{Long: "version", HasArg: getoptions.NO_ARGUMENT, Short: 'V'},
		{Long: "config-file", HasArg: getoptions.REQUIRED_ARGUMENT, Short: 'c'},
		{Long: "set", HasArg: getoptions.REQUIRED_ARGUMENT, Short: 's'},
	}

	program, options, arguments, err := getoptions.GetOS(flags)
	if nil != err {
		exitwithstatus.Message("%s: getoptions error: %s", program, err)
	}

	if len(options["version"]) > 0 {
		exitwithstatus.Message("%s: version: %s", program, version)
	}

	if len(options["help"]) > 0 {
		exitwithstatus.Message("usage: %s [--help] [--verbose] [--quiet] --config-file=FILE [[command|help] arguments...]", program)
	}

	if 1 != len(options["config-file"]) {
		exitwithstatus.Message("%s: only one config-file option is required, %d were detected", program, len(options["config-file"]))
	}

	// extract command-line variables
	variables := make(map[string]string)
	for _, v := range options["set"] {
		s := strings.SplitN(v, "=", 2)
		if 2 == len(s) {
			variables[s[0]] = s[1]
		}
	}

	// read options and parse the configuration file
	configurationFile := options["config-file"][0]
	masterConfiguration, err := getConfiguration(configurationFile, variables)
	if nil != err {
		exitwithstatus.Message("%s: failed to read configuration from: %q  error: %s", program, configurationFile, err)
	}

	// start logging
	if err = logger.Initialise(masterConfiguration.Logging); nil != err {
		exitwithstatus.Message("%s: logger setup failed with error: %s", program, err)
	}
	defer logger.Finalise()

	// create a logger channel for the main program
	log := logger.New("main")
	defer log.Info("shutting down…")
	log.Info("starting…")
	log.Infof("version: %s", version)
	log.Tracef("masterConfiguration: %v", masterConfiguration)

	blockrecord.Initialise()

	// ------------------
	// start of real main
	// ------------------

	// optional PID file
	// use if not running under a supervisor program like daemon(8)
	if "" != masterConfiguration.PidFile {
		lockFile, err := os.OpenFile(masterConfiguration.PidFile, os.O_WRONLY|os.O_EXCL|os.O_CREATE, os.ModeExclusive|0600)
		if err != nil {
			if os.IsExist(err) {
				exitwithstatus.Message("%s: another instance is already running", program)
			}
			exitwithstatus.Message("%s: PID file: %q creation failed, error: %s", program, masterConfiguration.PidFile, err)
		}
		fmt.Fprintf(lockFile, "%d\n", os.Getpid())
		lockFile.Close()
		defer os.Remove(masterConfiguration.PidFile)
	}

	// command processing - need lock so do not affect an already running process
	// these commands process data needed for initial setup
	if len(arguments) > 0 {
		processSetupCommand(log, arguments, masterConfiguration)
		return
	}

	// // if requested start profiling
	// if "" != masterConfiguration.ProfileFile {
	// 	f, err := os.Create(masterConfiguration.ProfileFile)
	// 	if nil != err {
	// 		log.Criticalf("cannot open profile output file: '%s'  error: %s", masterConfiguration.ProfileFile, err)
	// 		exitwithstatus.Exit(1)
	// 	}
	// 	defer f.Close()
	// 	pprof.StartCPUProfile(f)
	// 	defer pprof.StopCPUProfile()
	// }

	// set the initial system mode - before any background tasks are started
	mode.Initialise(masterConfiguration.Chain)
	defer mode.Finalise()

	// general info
	log.Infof("test mode: %v", mode.IsTesting())
	log.Debugf("database: %q", masterConfiguration.Database)

	// connection info
	log.Debugf("%s = %#v", "Peering", masterConfiguration.Peering)

	// start the data storage
	log.Info("initialise storage")
	err = storage.Initialise(masterConfiguration.Database)
	if nil != err {
		log.Criticalf("storage initialise error: %s", err)
		exitwithstatus.Message("storage initialise error: %s", err)
	}
	defer storage.Finalise()

	// initialise encryption
	err = zmqutil.StartAuthentication()
	if nil != err {
		log.Criticalf("zmq.AuthStart(): error: %s", err)
		exitwithstatus.Message("%s: zmq.AuthStart() error: %s", program, err)
	}

	// start up the peering background processes
	err = peer.Initialise(&masterConfiguration.Peering)
	if nil != err {
		log.Criticalf("peer initialise error: %s", err)
		exitwithstatus.Message("peer initialise error: %s", err)
	}
	defer peer.Finalise()

	// wait for CTRL-C before shutting down to allow manual testing
	if 0 == len(options["quiet"]) {
		fmt.Printf("\n\nWaiting for CTRL-C (SIGINT) or 'kill <pid>' (SIGTERM)…")
	}

	// turn Signals into channel messages
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	sig := <-ch
	log.Infof("received signal: %v", sig)
	if 0 == len(options["quiet"]) {
		fmt.Printf("\nreceived signal: %v\n", sig)
		fmt.Printf("\nshutting down...\n")
	}
}
