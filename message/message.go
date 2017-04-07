package message

import (
	"time"

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
	ShardId      *string

	FailCount int
}

func FromRecord(record *kinesis.Record) *Message {
	return &Message{
		ApproximateArrivalTimestamp: record.ApproximateArrivalTimestamp,
		Data:           record.Data,
		PartitionKey:   record.PartitionKey,
		SequenceNumber: record.SequenceNumber,
	}
}

func (m *Message) MakeRequestEntry() *kinesis.PutRecordsRequestEntry {
	return &kinesis.PutRecordsRequestEntry{
		Data:            m.Data,
		ExplicitHashKey: m.ExplicitHashKey,
		PartitionKey:    m.PartitionKey,
	}
}
