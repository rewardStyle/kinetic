package message

import (
	"time"

	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Message represents an item on the Kinesis stream
type Message struct {
	// For kinesis.Record
	ApproximateArrivalTimestamp *time.Time
	Data                        []byte
	PartitionKey                *string
	SequenceNumber              *string

	// For kinesis.PutRecordRequestEntry
	ExplicitHashKey *string

	// For kinesis.PutRecordResultEntry
	ErrorCode    *string
	ErrorMessage *string
	ShardID      *string

	// For firehose.PutRecordBatchResponseEntry
	RecordID *string

	FailCount int
}

// FromRecord creates a message from the kinesis.Record returned from GetRecords
func FromRecord(record *kinesis.Record) *Message {
	return &Message{
		ApproximateArrivalTimestamp: record.ApproximateArrivalTimestamp,
		Data:           record.Data,
		PartitionKey:   record.PartitionKey,
		SequenceNumber: record.SequenceNumber,
	}
}

// MakeRequestEntry creates a kinesis.PutRecordsRequestEntry to be used in the
// kinesis.PutRecords API call.
func (m *Message) MakeRequestEntry() *kinesis.PutRecordsRequestEntry {
	return &kinesis.PutRecordsRequestEntry{
		Data:            m.Data,
		ExplicitHashKey: m.ExplicitHashKey,
		PartitionKey:    m.PartitionKey,
	}
}

// MakeFirehoseRecord creates a firehose.Record to be used in the
// firehose.PutRecordBatch API call.
func (m *Message) MakeFirehoseRecord() *firehose.Record {
	return &firehose.Record{
		Data: m.Data,
	}
}
