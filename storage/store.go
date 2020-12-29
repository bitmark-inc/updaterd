// Copyright (c) 2014-2017 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package storage

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"golang.org/x/crypto/sha3"

	"github.com/bitmark-inc/bitmarkd/blockdigest"
	"github.com/bitmark-inc/bitmarkd/blockrecord"
	"github.com/bitmark-inc/bitmarkd/currency"
	"github.com/bitmark-inc/bitmarkd/fault"
	"github.com/bitmark-inc/bitmarkd/genesis"
	"github.com/bitmark-inc/bitmarkd/merkle"
	"github.com/bitmark-inc/bitmarkd/mode"
	"github.com/bitmark-inc/bitmarkd/transactionrecord"
	"github.com/bitmark-inc/logger"
)

const (
	notifyBlockSQL     = `SELECT notify_new_block($1);`
	notifyAssetSQL     = `SELECT notify_new_assets($1);`
	notifyIssueSQL     = `SELECT notify_new_issues($1);`
	notifyTransferSQL  = `SELECT notify_new_transfers($1);`
	notifyPendingTxSQL = `SELECT notify_pending_transaction($1);`

	// insertBlock:
	//   1:  block_number   INT8
	//   2:  hash           TEXT
	//   3:  created_on     TIMESTAMP WITH TIME ZONE
	insertBlockSQL = `SELECT blockchain.insert_block($1, $2, $3);`

	// insertAsset:
	//   1:  asset_id       TEXT
	//   2:  name           TEXT
	//   3:  fingerprint    TEXT
	//   4:  metadata       JSONB
	//   5:  registrant     TEXT
	//   6:  signature      TEXT
	//   7:  status         status_type
	//   8:  block_number   INT8
	//   9:  block_offset   INT8
	insertAssetSQL = `SELECT blockchain.insert_asset($1, $2, $3, $4::jsonb, $5, $6, $7::blockchain.status_type, $8, $9);`

	// insertBitMark:
	//   1:  bitmark_txid     TEXT
	//   2:  owner            TEXT
	//   3:  signature        TEXT
	//   4:  countersignature TEXT
	//   5:  asset_id         TEXT     -- NULL for non-issue
	//   6:  previous_txid    TEXT     -- NULL for an issue
	//   7:  status           status_type
	//   8:  payments         JSONB    -- only for foundation / block ownership
	//   9:  pay_id           TEXT
	//   10: block_number     INT8
	//   11: block_offset     INT8
	insertBitmarkSQL = `SELECT blockchain.insert_transaction($1, $2, $3, $4, $5, $6, $7::blockchain.status_type, $8::jsonb, $9, $10, $11);`

	// insertShare:
	//   1:  bitmark_txid     TEXT
	//   2:  quantity         INTEGER
	//   3:  signature        TEXT
	//   4:  previous_txid    TEXT     -- NULL for an issue
	//   5:  pay_id           TEXT
	//   6:  status           status_type
	//   7:  block_number     INT8
	//   8:  block_offset     INT8
	insertShareSQL = `SELECT blockchain.insert_share_transaction($1, $2, $3, $4, $5, $6::blockchain.status_type, $7, $8);`

	// insertGrant:
	//   1:  bitmark_txid     TEXT
	//   2:  share_txid       TEXT
	//   3:  quantity         INTEGER
	//   4:  owner			  TEXT
	//   5:  recipient 		  TEXT
	//   6:  signature        TEXT
	//   7:  countersignature TEXT
	//   8:  pay_id           TEXT
	//   9:  shares           JSONB
	//  10:  status           status_type
	//  11:  block_number     INT8
	//  12:  block_offset     INT8
	insertGrantSQL = `SELECT blockchain.insert_grant_transaction($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::blockchain.status_type, $11, $12);`

	// insertSwap:
	//   1:  bitmark_txid     TEXT
	//   2:  share_one        TEXT
	//   3:  quantity_one     INTEGER
	//   4:  owner_one		  TEXT
	//   5:  share_two        TEXT
	//   6:  quantity_two     INTEGER
	//   7:  owner_two		  TEXT
	//   8:  signature        TEXT
	//   9:  countersignature TEXT
	//  10:  pay_id           TEXT
	//  11:  shares           JSONB
	//  12:  status           status_type
	//  13:  block_number     INT8
	//  14:  block_offset     INT8
	insertSwapSQL = `SELECT blockchain.insert_swap_transaction($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::blockchain.status_type, $13, $14);`

	// updateEditions:
	//   1: block_number
	updateEditionsSQL = `SELECT blockchain.update_editions($1);`

	// getBlockHeight returns:
	//   1:  digest         TEXT
	//   2:  block_number   INT8
	getBlockHeightSQL = `SELECT blockchain.get_block_height();`

	// getBlockDigest:
	//   1:  block_number   INT8
	// returns:
	//   1:  digest         TEXT
	getBlockDigestSQL = `SELECT blockchain.get_block_digest($1);`

	// deleteDownToBlock:
	//   1:  block_number   INT8
	deleteDownToBlockSQL = `SELECT blockchain.delete_down_to_block($1);`

	// deleteExpiredRecords:
	deleteExpiredRecordsSQL = `SELECT blockchain.expire_records();`
)

// PostgreSQL error codes
const (
	not_null_violation = "not_null_violation"
)

// type for indicating recoru status
type statusType int

const blockRevertLimit = 5

// all possible value for status
const (
	statusPending statusType = iota
	statusVerified
	statusConfirmed
)

// store an incoming block checking to make sure it is valid first
func StoreBlock(packedBlock []byte) error {

	testnet := mode.IsTesting()

	newAssets := []string{}
	newIssues := []string{}
	newTransfers := []string{}

	log := globalData.log
	if nil == globalData.database {
		return fault.ErrNotInitialised
	}

	h, err := GetBlockHeight()
	if nil != err {
		log.Criticalf("GetBlockHeight failed: error: %s", err)
		logger.Panicf("GetBlockHeight failed: error: %s", err)
	}

	header, digest, data, err := blockrecord.ExtractHeader(packedBlock, h+1)
	if nil != err {
		return err
	}

	// Check if the previous block existed and consistent.
	previousBlock, err := DigestForBlock(header.Number - 1)
	if nil != err {
		return err
	}
	if *previousBlock != header.PreviousBlock {

		log.Debugf("previous block hashes differ: local: %s  remote: %s",
			previousBlock.String(), header.PreviousBlock.String())

		// revert local block
		if err := DeleteDownToBlock(h - blockRevertLimit); err != nil {
			log.Criticalf("fail to revert block: error: %s", err)
		}
		return fault.ErrPreviousBlockDigestDoesNotMatch
	}

	type txn struct {
		unpacked interface{}
	}

	txs := make([]txn, header.TransactionCount)
	txIds := make([]merkle.Digest, header.TransactionCount)

	// check all transactions are valid
	for i := uint16(0); i < header.TransactionCount; i += 1 {
		transaction, n, err := transactionrecord.Packed(data).Unpack(testnet)
		if nil != err {
			return err
		}

		txs[i].unpacked = transaction
		txIds[i] = merkle.NewDigest(data[:n])

		data = data[n:]
	}

	// build the tree of transaction IDs
	fullMerkleTree := merkle.FullMerkleTree(txIds)
	merkleRoot := fullMerkleTree[len(fullMerkleTree)-1]

	if merkleRoot != header.MerkleRoot {
		return fault.ErrMerkleRootDoesNotMatch
	}

	blockNumber := header.Number

	if header.Timestamp > 9224318015999 {
		header.Timestamp = 9224318015999
	}

	createdOn := time.Unix(int64(header.Timestamp), 0).UTC()

	assetStatus := statusPending
	transferStatus := statusPending
	if blockNumber != 0 {
		assetStatus = statusConfirmed
		transferStatus = statusConfirmed
	}

	// start the database transaction
	db, err := globalData.database.Begin()
	if nil != err {
		log.Errorf("transaction begin error: %s", err)
		return err
	}

	// Note: after here, do not: return err
	//       instead, do:        errX=err; goto rollback
	errX := error(nil)

	// create the foundation record
	foundationTxId := blockrecord.FoundationTxId(header, digest)

	// store the block
	err = insertBlock(blockNumber, digest, createdOn, db, log)
	if nil != err {
		errX = err
		goto rollback
	}

	// extract data from old base records (old headers records 0..1)
	{
		var f *transactionrecord.BlockFoundation

	scan_oldbase:
		for _, item := range txs {
			switch tx := item.unpacked.(type) {
			case *transactionrecord.OldBaseData:
				if nil == f {
					f = &transactionrecord.BlockFoundation{
						Version:   0,
						Payments:  make(currency.Map),
						Owner:     tx.Owner,
						Nonce:     tx.Nonce,
						Signature: tx.Signature,
					}
				}
				f.Payments[tx.Currency] = tx.PaymentAddress
			default:
				break scan_oldbase
			}
		}

		if nil != f {
			_, err := insertFoundation(foundationTxId, f, transferStatus, blockNumber, 0, "", db, log)
			if nil != err {
				errX = err
				goto rollback
			}
		}
	}

	// store transactions
	for i, item := range txs {
		blockOffset := uint64(i)
		txId := txIds[i]
		switch tx := item.unpacked.(type) {

		case *transactionrecord.OldBaseData:
			// no action here

		case *transactionrecord.AssetData:
			id, err := insertAsset(tx, assetStatus, blockNumber, blockOffset, db, log)
			if nil != err {
				errX = err
				goto rollback
			}
			newAssets = append(newAssets, id)

		case *transactionrecord.BitmarkIssue:
			id, err := insertIssue(txId, tx, transferStatus, blockNumber, blockOffset, "", db, log)
			if nil != err {
				errX = err
				goto rollback
			}
			newIssues = append(newIssues, id)

		case *transactionrecord.BitmarkTransferUnratified, *transactionrecord.BitmarkTransferCountersigned, *transactionrecord.BlockOwnerTransfer:
			id, err := insertTransfer(txId, tx.(transactionrecord.BitmarkTransfer), transferStatus, blockNumber, blockOffset, "", db, log)
			if nil != err {
				errX = err
				goto rollback
			}
			newTransfers = append(newTransfers, id)

		case *transactionrecord.BitmarkShare:
			_, err := insertShare(txId, tx, transferStatus, blockNumber, blockOffset, "", db, log)
			if nil != err {
				errX = err
				goto rollback
			}

		case *transactionrecord.ShareGrant:
			_, err := insertShareGrant(txId, tx, transferStatus, blockNumber, blockOffset, "", db, log)
			if nil != err {
				errX = err
				goto rollback
			}

		case *transactionrecord.ShareSwap:
			_, err := insertSwapTransaction(txId, tx, transferStatus, blockNumber, blockOffset, "", db, log)
			if nil != err {
				errX = err
				goto rollback
			}

		case *transactionrecord.BlockFoundation:
			id, err := insertFoundation(foundationTxId, tx, transferStatus, blockNumber, 0, "", db, log)
			if nil != err {
				errX = err
				goto rollback
			}
			newIssues = append(newIssues, id)

		default:
			globalData.log.Criticalf("unhandled transaction: %v", tx)
			logger.Panicf("unhandled transaction: %v", tx)
		}
	}

	if err := updateEditions(blockNumber, db, log); err != nil {
		errX = err
		goto rollback
	}

	err = db.Commit()
	if nil != err {
		log.Errorf("transaction commit error: %s", err)
		errX = err
		goto rollback
	}

	// Ignore the block which is created 72 hours before
	if time.Now().UTC().Sub(createdOn) < 72*time.Hour {
		if len(newAssets) != 0 {
			err = notifyAsset(newAssets, globalData.database, log)
			if err != nil {
				log.Errorf("assets notify error: %s", err)
			}
		}

		if len(newIssues) != 0 {
			err = notifyIssue(newIssues, globalData.database, log)
			if err != nil {
				log.Errorf("assets notify error: %s", err)
			}
		}

		if len(newTransfers) != 0 {
			err = notifyTransfer(newTransfers, globalData.database, log)
			if err != nil {
				log.Errorf("transfers notify error: %s", err)
			}
		}

		err = notifyBlock(blockNumber, globalData.database, log)
		if err != nil {
			log.Errorf("block notify error: %s", err)
		}
	}

	return nil

rollback:
	db.Rollback()
	return errX
}

// store transactions
func StoreTransactions(packedTransactions []byte) error {
	log := globalData.log
	if nil == globalData.database {
		return fault.ErrNotInitialised
	}

	// start the database transaction
	db, err := globalData.database.Begin()
	if nil != err {
		log.Errorf("transaction begin error: %s", err)
		return err
	}

	testnet := mode.IsTesting()

	// Note: after here, do not: return err
	//       instead, do:        errX=err; goto rollback
	errX := error(nil)

	blockNumber := uint64(0)
	blockOffset := uint64(0)
	assetStatus := statusPending
	transferStatus := statusPending

	// payment identifier for whole block as a hex string
	d := sha3.Sum384(packedTransactions)
	payId := hex.EncodeToString(d[:])

	for 0 != len(packedTransactions) {
		transaction, n, err := transactionrecord.Packed(packedTransactions).Unpack(testnet)
		if nil != err {
			errX = err
			goto rollback
		}
		txId := transactionrecord.Packed(packedTransactions[:n]).MakeLink()

		switch tx := transaction.(type) {

		case *transactionrecord.OldBaseData:
			// no action needed here

		case *transactionrecord.AssetData:
			_, err := insertAsset(tx, assetStatus, blockNumber, blockOffset, db, log)
			if nil != err {
				errX = err
				goto rollback
			}

		case *transactionrecord.BitmarkIssue:
			_, err := insertIssue(txId, tx, transferStatus, blockNumber, blockOffset, "", db, log)
			if nil != err {
				errX = err
				goto rollback
			}

			if id, err := txId.MarshalText(); err == nil {
				if err := notifyPendingTx(string(id), globalData.database, log); err != nil {
					log.Errorf("pending tx notify error: %s", err)
				}
			} else {
				log.Errorf("pending tx notify error: %s", err)
			}

		case *transactionrecord.BitmarkTransferUnratified, *transactionrecord.BitmarkTransferCountersigned, *transactionrecord.BlockOwnerTransfer:
			_, err := insertTransfer(txId, tx.(transactionrecord.BitmarkTransfer), transferStatus, blockNumber, blockOffset, payId, db, log)
			if nil != err {
				errX = err
				goto rollback
			}

			if id, err := txId.MarshalText(); err == nil {
				if err := notifyPendingTx(string(id), globalData.database, log); err != nil {
					log.Errorf("pending tx notify error: %s", err)
				}
			} else {
				log.Errorf("pending tx notify error: %s", err)
			}

		case *transactionrecord.BitmarkShare:
			_, err := insertShare(txId, tx, transferStatus, blockNumber, blockOffset, payId, db, log)
			if nil != err {
				errX = err
				goto rollback
			}

		case *transactionrecord.ShareGrant:
			_, err := insertShareGrant(txId, tx, transferStatus, blockNumber, blockOffset, payId, db, log)
			if nil != err {
				errX = err
				goto rollback
			}

		case *transactionrecord.ShareSwap:
			_, err := insertSwapTransaction(txId, tx, transferStatus, blockNumber, blockOffset, payId, db, log)
			if nil != err {
				errX = err
				goto rollback
			}

		case *transactionrecord.BlockFoundation:
			// no action needed here

		default:
			globalData.log.Criticalf("unhandled transaction: %v", tx)
			logger.Panicf("unhandled transaction: %v", tx)
		}
		packedTransactions = packedTransactions[n:]
	}

	err = db.Commit()
	if nil != err {
		log.Errorf("transaction commit error: %s", err)
		errX = err
		goto rollback
	}

	return nil

rollback:
	db.Rollback()
	return errX
}

// notify a block record
func notifyBlock(blockNumber uint64, db *sql.DB, log *logger.L) error {
	_, err := db.Exec(notifyBlockSQL, fmt.Sprintf("%d", blockNumber))
	if nil != err {
		log.Errorf("notifyBlockSQL: number: %d  error: %s", blockNumber, err)
		return err
	}
	log.Debugf("notifyBlockSQL: number: %d", blockNumber)

	return nil
}

// notify asset records
func notifyAsset(assetIds []string, db *sql.DB, log *logger.L) error {
	assets := strings.Join(assetIds, ",")

	_, err := db.Exec(notifyAssetSQL, assets)
	if nil != err {
		log.Errorf("notifyAssetSQL: id: %s error: %s", assets, err)
		return err
	}
	log.Debugf("notifyAssetSQL: id: %s", assets)

	return nil
}

// notify issue records
func notifyIssue(issueIds []string, db *sql.DB, log *logger.L) error {
	log.Debugf("start notifing issues: %+v", issueIds)

	isssueLen := len(issueIds)
	for i := 0; i < isssueLen; i += 20 {
		j := i + 20
		if j > isssueLen {
			j = isssueLen
		}
		issues := strings.Join(issueIds[i:j], ",")
		_, err := db.Exec(notifyIssueSQL, issues)
		if nil != err {
			log.Errorf("notifyIssueSQL: id: %s error: %s", issueIds, err)
			return err
		}
		log.Debugf("notifyIssueSQL: id: %s (%d/%d)", issues, j, isssueLen)
	}
	return nil
}

// notify transfer records
func notifyTransfer(transferIds []string, db *sql.DB, log *logger.L) error {
	transfers := strings.Join(transferIds, ",")

	_, err := db.Exec(notifyTransferSQL, transfers)
	if nil != err {
		log.Errorf("notifyTransferSQL: id: %s error: %s", transfers, err)
		return err
	}
	log.Debugf("notifyTransferSQL: id: %s", transfers)

	return nil
}

// notifyPendingTx notifies every incoming pending transactions
func notifyPendingTx(txId string, db *sql.DB, log *logger.L) error {
	_, err := db.Exec(notifyPendingTxSQL, txId)
	if nil != err {
		log.Errorf("notifyPendingTxSQL: id: %s error: %s", txId, err)
		return err
	}
	log.Debugf("notifyPendingTxSQL: id: %s", txId)

	return nil
}

// store a block record
func insertBlock(blockNumber uint64, digest blockdigest.Digest, createdOn time.Time, db *sql.Tx, log *logger.L) error {
	hash := digest.String() // big endian

	_, err := db.Exec(insertBlockSQL, blockNumber, hash, createdOn)
	if nil != err {
		log.Errorf("insertBlock: "+
			"number: %d, hash: %q, created_on: %v  error: %s",
			blockNumber, hash, createdOn, err)
		return err
	}
	log.Debugf("insertBlock: "+
		"number: %d, hash: %q, created_on: %v",
		blockNumber, hash, createdOn)

	return nil
}

func insertAsset(asset *transactionrecord.AssetData, status statusType, blockNumber uint64, blockOffset uint64, db *sql.Tx, log *logger.L) (string, error) {
	assetId := asset.AssetId()
	id, err := assetId.MarshalText()
	if nil != err {
		return "", err
	}
	name := asset.Name
	fingerprint := asset.Fingerprint
	packedMetadata := asset.Metadata
	registrant := asset.Registrant.String()
	signature, err := asset.Signature.MarshalText()
	if nil != err {
		return "", err
	}
	m := strings.Split(packedMetadata, "\u0000")
	metaMap := make(map[string]string)
	if 1 == len(m)%2 {
		m = m[:len(m)-1]
	}
	if len(m) != 0 {
		for i := 0; i < len(m); i += 2 {
			metaMap[m[i]] = m[i+1]
		}
	}
	metadata, err := json.Marshal(metaMap)
	if nil != err {
		return "", err
	}

	_, err = db.Exec(insertAssetSQL, id, name, fingerprint, metadata, registrant, signature, status, blockNumber, blockOffset)
	if nil != err {
		log.Errorf("insertAsset: "+
			"assetId: %q, name: %q, fingerprint: %q, metadata: %q, "+
			"registrant: %q, signature: %q, status: %q, block: %d  error: %s",
			id, name, fingerprint, metadata,
			registrant, signature, status, blockNumber, err)
		return "", err
	}
	log.Debugf("insertAsset: "+
		"assetId: %q, name: %q, fingerprint: %q, metadata: %q, "+
		"registrant: %q, signature: %q, status: %q, block: %d",
		id, name, fingerprint, metadata,
		registrant, signature, status, blockNumber)

	return assetId.String(), nil
}

func insertIssue(txId merkle.Digest, issue *transactionrecord.BitmarkIssue, status statusType, blockNumber uint64, blockOffset uint64, payId string, db *sql.Tx, log *logger.L) (string, error) {
	id, err := txId.MarshalText()
	if nil != err {
		return "", err
	}
	owner := issue.Owner.String()
	signature, err := issue.Signature.MarshalText()
	if nil != err {
		return "", err
	}
	asset_id, err := issue.AssetId.MarshalText()
	if nil != err {
		return "", err
	}

	_, err = db.Exec(insertBitmarkSQL, id, owner, signature, "", asset_id, nil, status, nil, payId, blockNumber, blockOffset)
	if nil != err {
		log.Errorf("insertIssue: "+
			"id: %q, owner: %q, signature: %q, asset_id: %q, "+
			"status: %q, block: %d  error: %s",
			id, owner, signature, asset_id,
			status, blockNumber, err)
		return "", err
	}
	log.Debugf("insertIssue: "+
		"id: %q, owner: %q, signature: %q, asset_id: %q, "+
		"status: %q, block: %d to DB",
		id, owner, signature, asset_id,
		status, blockNumber)

	return string(id), nil
}

func insertFoundation(txId merkle.Digest, issue *transactionrecord.BlockFoundation, status statusType, blockNumber uint64, blockOffset uint64, payId string, db *sql.Tx, log *logger.L) (string, error) {
	id, err := txId.MarshalText()
	if nil != err {
		return "", err
	}
	owner := issue.Owner.String()
	signature, err := issue.Signature.MarshalText()
	if nil != err {
		return "", err
	}

	currencies, err := json.Marshal(issue.Payments)
	if nil != err {
		return "", err
	}

	_, err = db.Exec(insertBitmarkSQL, id, owner, signature, "", nil, nil, status, currencies, payId, blockNumber, blockOffset)
	if nil != err {
		log.Errorf("insertFoundation: "+
			"id: %q, owner: %q, signature: %q, currencies: %q, "+
			"status: %q, block: %d  error: %s",
			id, owner, signature, currencies,
			status, blockNumber, err)
		return "", err
	}
	log.Debugf("insertFoundation: "+
		"id: %q, owner: %q, signature: %q, currencies: %q, "+
		"status: %q, block: %d to DB",
		id, owner, signature, currencies,
		status, blockNumber)

	return string(id), nil
}

func insertTransfer(txId merkle.Digest, transfer transactionrecord.BitmarkTransfer, status statusType, blockNumber uint64, blockOffset uint64, payId string, db *sql.Tx, log *logger.L) (string, error) {
	id, err := txId.MarshalText()
	if nil != err {
		return "", err
	}
	owner := transfer.GetOwner().String()
	countersignature, err := transfer.GetCountersignature().MarshalText()
	if nil != err {
		return "", err
	}
	signature, err := transfer.GetSignature().MarshalText()
	if nil != err {
		return "", err
	}
	previous_id, err := transfer.GetLink().MarshalText()
	if nil != err {
		return "", err
	}

	var currencies *string
	if payments := transfer.GetCurrencies(); nil != payments {
		c, err := json.Marshal(payments)
		if nil != err {
			return "", err
		}
		c1 := string(c)
		if "null" == c1 || "{}" == c1 {
			log.Criticalf("currencies has unxpected value: %q", c1)
			logger.Panicf("currencies has unxpected value: %q", c1)
		}
		currencies = &c1
	}

	_, err = db.Exec(insertBitmarkSQL, id, owner, signature, countersignature, nil, previous_id, status, currencies, payId, blockNumber, blockOffset)
	if err, ok := err.(*pq.Error); ok {
		log.Errorf("insertTransfer: "+
			"id: %q, owner: %q, signature: %q, countersignature: %q, previous_id: %q, "+
			"status: %q, block: %d  error: %s",
			id, owner, signature, countersignature, previous_id,
			status, blockNumber, err)

		if err.Code.Name() == not_null_violation { // pre_id transfer is not in DB
			log.Criticalf("Database is corrupt: block: %d insert transfer: %q  previous transfer: %q does not exist (%v)",
				blockNumber, txId, previous_id, err.Code.Name())
			logger.Panicf("Database is corrupt: block: %d insert transfer: %q  previous transfer: %q does not exist (%v)",
				blockNumber, txId, previous_id, err.Code.Name())
		}
		return "", err
	}

	log.Debugf("insertTransfer: "+
		"id: %q, owner: %q, signature: %q, previous_id: %q, "+
		"status: %q, block: %d",
		id, owner, signature, previous_id,
		status, blockNumber)

	return string(id), nil
}

func insertShare(txId merkle.Digest, share *transactionrecord.BitmarkShare, status statusType, blockNumber uint64, blockOffset uint64, payId string, db *sql.Tx, log *logger.L) (string, error) {
	id, err := txId.MarshalText()
	if nil != err {
		return "", err
	}

	signature, err := share.GetSignature().MarshalText()
	if nil != err {
		return "", err
	}

	previous_id, err := share.GetLink().MarshalText()
	if nil != err {
		return "", err
	}

	_, err = db.Exec(insertShareSQL, id, share.Quantity, signature, previous_id, payId, status, blockNumber, blockOffset)
	if err, ok := err.(*pq.Error); ok {
		log.Errorf("insertShare: "+
			"id: %q, signature: %q, previous_id: %q, status: %q, block: %d  error: %s",
			id, signature, previous_id, status, blockNumber, err)
	}

	return string(id), err
}

func insertShareGrant(txId merkle.Digest, grant *transactionrecord.ShareGrant, status statusType, blockNumber uint64, blockOffset uint64, payId string, db *sql.Tx, log *logger.L) (string, error) {
	id, err := txId.MarshalText()
	if nil != err {
		return "", err
	}

	shareId, err := grant.ShareId.MarshalText()
	if nil != err {
		return "", err
	}

	signature, err := grant.Signature.MarshalText()
	if nil != err {
		return "", err
	}

	countersignature, err := grant.Countersignature.MarshalText()
	if nil != err {
		return "", err
	}

	shareInfo, err := json.Marshal(map[string]interface{}{
		"share_id": string(shareId),
		"from":     grant.Owner,
		"to":       grant.Recipient,
		"quantity": grant.Quantity,
	})
	if nil != err {
		return "", err
	}

	_, err = db.Exec(insertGrantSQL, id, shareId, grant.Quantity, grant.Owner.String(), grant.Recipient.String(),
		signature, countersignature, payId, shareInfo, status, blockNumber, blockOffset)
	if err, ok := err.(*pq.Error); ok {
		log.Errorf("insertShareGrant: "+
			"id: %q, shareId: %q, quantity: %q, owner: %q, recipient: %q, signature: %q, countersignature: %q, status: %q, block: %d  error: %s",
			id, shareId, grant.Quantity, grant.Owner.String(), grant.Recipient.String(), signature, countersignature, status, blockNumber, err)
	}

	return string(id), err
}

func insertSwapTransaction(txId merkle.Digest, swap *transactionrecord.ShareSwap, status statusType, blockNumber uint64, blockOffset uint64, payId string, db *sql.Tx, log *logger.L) (string, error) {
	id, err := txId.MarshalText()
	if nil != err {
		return "", err
	}

	shareOne, err := swap.ShareIdOne.MarshalText()
	if nil != err {
		return "", err
	}

	shareTwo, err := swap.ShareIdTwo.MarshalText()
	if nil != err {
		return "", err
	}

	signature, err := swap.Signature.MarshalText()
	if nil != err {
		return "", err
	}

	countersignature, err := swap.Countersignature.MarshalText()
	if nil != err {
		return "", err
	}

	swapInfo, err := json.Marshal(map[string]interface{}{
		"share_id_one": swap.ShareIdOne,
		"quantity_one": swap.QuantityOne,
		"owner_one":    swap.OwnerOne,
		"share_id_two": swap.ShareIdTwo,
		"quantity_two": swap.QuantityTwo,
		"owner_two":    swap.OwnerTwo,
	})
	if nil != err {
		return "", err
	}

	_, err = db.Exec(insertSwapSQL, id,
		shareOne, swap.QuantityOne, swap.OwnerOne.String(),
		shareTwo, swap.QuantityTwo, swap.OwnerTwo.String(),
		signature, countersignature, payId, swapInfo, status, blockNumber, blockOffset)
	if err, ok := err.(*pq.Error); ok {
		log.Errorf("insertSwap: "+
			"id: %q, shareOne: %q, quantityOne: %q, ownerOne: %q, shareOne: %q, quantityOne: %q, ownerOne: %q,"+
			"signature: %q, countersignature: %q, status: %q, block: %d  error: %s",
			id, shareOne, swap.QuantityOne, swap.OwnerOne.String(),
			shareTwo, swap.QuantityTwo, swap.OwnerTwo.String(),
			signature, countersignature, status, blockNumber, err)
	}

	return string(id), err
}

func updateEditions(blockNumber uint64, db *sql.Tx, log *logger.L) error {
	_, err := db.Exec(updateEditionsSQL, blockNumber)
	if err, ok := err.(*pq.Error); ok {
		log.Errorf("updateEditions: block: %d  error: %s", blockNumber, err)
	}

	return err
}

// return values from the highest block
func GetBlockHeight() (uint64, error) {
	var blockNumber uint64
	row := globalData.database.QueryRow(getBlockHeightSQL)
	err := row.Scan(&blockNumber)
	if nil != err {
		return 0, err
	}
	if blockNumber <= genesis.BlockNumber {
		if mode.IsTesting() {
			return genesis.BlockNumber, nil
		} else {
			return genesis.BlockNumber, nil
		}
	}

	return blockNumber, nil
}

// delete all blocks up from and including the start value
func DeleteDownToBlock(startBlockNumber uint64) error {
	_, err := globalData.database.Exec(deleteDownToBlockSQL, startBlockNumber)
	return err
}

// get the digest for a specific block
func DigestForBlock(blockNumber uint64) (*blockdigest.Digest, error) {

	if blockNumber <= genesis.BlockNumber {
		if mode.IsTesting() {
			return &genesis.TestGenesisDigest, nil
		} else {
			return &genesis.LiveGenesisDigest, nil
		}
	}

	var stringDigest string
	row := globalData.database.QueryRow(getBlockDigestSQL, blockNumber)
	err := row.Scan(&stringDigest)
	if nil != err {
		return nil, err
	}

	digest := &blockdigest.Digest{}
	n, err := fmt.Sscan(stringDigest, digest)
	if nil != err {
		return nil, err
	}

	if 1 != n {
		return nil, fault.ErrBlockNotFound
	}

	return digest, nil
}

// to clean out any expired records
func deleteExpiredRecords(db *sql.DB) error {
	_, err := db.Exec(deleteExpiredRecordsSQL)
	return err
}

// convert status to string
func (s statusType) String() string {
	switch s {
	case statusPending:
		return "pending"
	case statusVerified:
		return "verified"
	case statusConfirmed:
		return "confirmed"
	default:
		return "*unknown*"
	}
}

// for the SQL Exec and query functions
func (s statusType) Value() (driver.Value, error) {
	return s.String(), nil
}
