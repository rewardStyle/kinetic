package listener

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

// ShardIterator represents the settings used to retrieve a shard iterator from
// the GetShardIterator API.
type ShardIterator struct {
	shardIteratorType string
	sequenceNumber    string
	timestamp         time.Time
}

// NewShardIterator creates a new ShardIterator.  The default shard iterator
// type is TRIM_HORIZON.
func NewShardIterator() *ShardIterator {
	return &ShardIterator{
		shardIteratorType: "TRIM_HORIZON",
	}
}

// TrimHorizon sets the shard iterator to TRIM_HORIZON.
func (it *ShardIterator) TrimHorizon() *ShardIterator {
	it.shardIteratorType = "TRIM_HORIZON"
	return it
}

// Latest sets the shard iterator to LATEST.
func (it *ShardIterator) Latest() *ShardIterator {
	it.shardIteratorType = "LATEST"
	return it
}

// AtSequenceNumber sets the shard iterator to AT_SEQUENCE_NUMBER.
func (it *ShardIterator) AtSequenceNumber(sequenceNumber string) *ShardIterator {
	it.shardIteratorType = "AT_SEQUENCE_NUMBER"
	it.sequenceNumber = sequenceNumber
	return it
}

// AfterSequenceNumber sets the shard iterator to AFTER_SEQUENCE_NUMBER.
func (it *ShardIterator) AfterSequenceNumber(sequenceNumber string) *ShardIterator {
	it.shardIteratorType = "AFTER_SEQUENCE_NUMBER"
	it.sequenceNumber = sequenceNumber
	return it
}

// AtTimestamp sets the shard iterator to AT_TIMESTAMP.
func (it *ShardIterator) AtTimestamp(timestamp time.Time) *ShardIterator {
	it.shardIteratorType = "AT_TIMESTAMP"
	it.timestamp = timestamp
	return it
}

func (it *ShardIterator) getStartingSequenceNumber() *string {
	if it.sequenceNumber == "" {
		return nil
	}
	return aws.String(it.sequenceNumber)
}

func (it *ShardIterator) getTimestamp() *time.Time {
	if it.timestamp.IsZero() {
		return nil
	}
	return aws.Time(it.timestamp)
}
