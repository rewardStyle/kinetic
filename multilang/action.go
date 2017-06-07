package multilang

import (
	"encoding/base64"
	"time"
	"strconv"

	"github.com/rewardStyle/kinetic/message"
)

// ActionType is used as an enum for KCL Multilang protocol action message types
type ActionType string

// These are the enumerated KCL Multilang protocol action message types
const (
	INITIALIZE ActionType = "initialize"
	PROCESSRECORDS ActionType = "processRecords"
	RECORD ActionType = "record"
	CHECKPOINT ActionType = "checkpoint"
	SHUTDOWN ActionType = "shutdown"
	STATUS ActionType = "status"
)

// ActionMessage is a struct used to marshal / unmarshal KCL Multilang protocol action messages
type ActionMessage struct {
	Action            ActionType `json:"action"`
	ShardID           string     `json:"shardId,omitempty"`
	SequenceNumber    string     `json:"sequenceNumber,omitempty"`
	Records           []Record   `json:"records,omitempty"`
	Checkpoint        string     `json:"checkpoint,omitempty"`
	Error             string     `json:"error,omitempty"`
	Reason            string     `json:"reason,omitempty"`
	ResponseFor       ActionType `json:"responseFor,omitempty"`
}

// Record is a struct used to marshal / unmarshal kinesis records from KCL Multilang protocol
type Record struct {
	Action		   ActionType `json:"action"`
	ApproximateArrival Timestamp  `json:"approximateArrivalTimestamp"`
	Data               string     `json:"data,omitempty"`
	PartitionKey       string     `json:"partitionKey,omitempty"`
	SequenceNumber     string     `json:"sequenceNumber,omitempty"`
	SubSequenceNumber  int        `json:"subSequenceNumber,omitempty"`
}

// Timestamp is a time.Time type
type Timestamp struct {
	time.Time
}

// UnmarshalJSON is used as a custom unmarshaller unmarshal unix time stamps
func (t *Timestamp) UnmarshalJSON(b []byte) error {
	ts, err := strconv.Atoi(string(b))
	if err != nil {
		return err
	}

	milliseconds := ts % 1000
	seconds := (ts - milliseconds) / 1000
	t.Time = time.Unix(int64(seconds), int64(milliseconds * 1000))

	return nil
}

// ToMessage is used to transform a multilang.Record struct into a message.Message struct
func (r *Record) ToMessage() *message.Message {
	b, err := base64.StdEncoding.DecodeString(r.Data)
	if err != nil {
		panic("There was a problem decoding kcl data")
	}

	return &message.Message{
		ApproximateArrivalTimestamp: &r.ApproximateArrival.Time,
		Data: b,
		PartitionKey: &r.PartitionKey,
		SequenceNumber: &r.SequenceNumber,
	}
}

// NewCheckpointMessage is used to create a new checkpoint message
func NewCheckpointMessage(seqNum string) *ActionMessage {
	return &ActionMessage{
		Action: CHECKPOINT,
		Checkpoint: seqNum,
	}
}

// NewStatusMessage is used to create a new status message
func NewStatusMessage(actionType ActionType) *ActionMessage {
	return &ActionMessage{
		Action: STATUS,
		ResponseFor: actionType,
	}
}
