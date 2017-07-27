package kinetic

import (
	"encoding/json"
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

// RequestEntrySize calculates what the size (in bytes) of the message will be after calling ToRequestEntry on it and
// marshalling it to json
func (m *Message) RequestEntrySize() (int, error) {
	buf, err := json.Marshal(m.ToRequestEntry())
	if err != nil {
		return 0, nil
	}
	return len(buf), nil
}

// ToRequestEntry creates a kinesis.PutRecordsRequestEntry to be used in the kinesis.PutRecords API call.
func (m *Message) ToRequestEntry() *kinesis.PutRecordsRequestEntry {
	return &kinesis.PutRecordsRequestEntry{
		Data:            m.Data,
		ExplicitHashKey: m.ExplicitHashKey,
		PartitionKey:    m.PartitionKey,
	}
}

// ToFirehoseRecord creates a firehose.Record to be used in the firehose.PutRecordBatch API call.
func (m *Message) ToFirehoseRecord() *firehose.Record {
	return &firehose.Record{
		Data: m.Data,
	}
}
