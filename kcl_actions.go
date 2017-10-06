package kinetic

import (
	"encoding/base64"
	"strconv"
	"time"
)

// ActionType is used as an enum for KCL Multilang protocol action message types
type kclActionType string

// These are the enumerated KCL Multilang protocol action message types
const (
	kclActionTypeInitialize     kclActionType = "initialize"
	kclActionTypeProcessRecords kclActionType = "processRecords"
	kclActionTypeCheckpoint     kclActionType = "checkpoint"
	kcActionTypeShutdown        kclActionType = "shutdown"
	KclActionTypeStatus         kclActionType = "status"
)

// actionMessage is a struct used to marshal / unmarshal KCL Multilang protocol action messages
type actionMessage struct {
	Action         kclActionType `json:"action"`
	ShardID        string        `json:"shardId,omitempty"`
	SequenceNumber string        `json:"sequenceNumber,omitempty"`
	Records        []record      `json:"records,omitempty"`
	Checkpoint     string        `json:"checkpoint,omitempty"`
	Error          string        `json:"error,omitempty"`
	Reason         string        `json:"reason,omitempty"`
	ResponseFor    kclActionType `json:"responseFor,omitempty"`
}

// record is a struct used to marshal / unmarshal kinesis records from KCL Multilang protocol
type record struct {
	Action             kclActionType `json:"action"`
	ApproximateArrival timestamp     `json:"approximateArrivalTimestamp"`
	Data               string        `json:"data,omitempty"`
	PartitionKey       string        `json:"partitionKey,omitempty"`
	SequenceNumber     string        `json:"sequenceNumber,omitempty"`
	SubSequenceNumber  int           `json:"subSequenceNumber,omitempty"`
}

// timestamp is a time.Time type
type timestamp struct {
	time.Time
}

// UnmarshalJSON is used as a custom unmarshaller unmarshal unix time stamps
func (t *timestamp) UnmarshalJSON(b []byte) error {
	ts, err := strconv.Atoi(string(b))
	if err != nil {
		return err
	}

	milliseconds := ts % 1000
	seconds := (ts - milliseconds) / 1000
	t.Time = time.Unix(int64(seconds), int64(milliseconds*1000))

	return nil
}

// ToMessage is used to transform a multilang.Record struct into a Message struct
func (r *record) ToMessage() *Message {
	b, err := base64.StdEncoding.DecodeString(r.Data)
	if err != nil {
		panic("There was a problem decoding kcl data")
	}

	return &Message{
		ApproximateArrivalTimestamp: &r.ApproximateArrival.Time,
		Data:           b,
		PartitionKey:   &r.PartitionKey,
		SequenceNumber: &r.SequenceNumber,
	}
}

// newCheckpointMessage is used to create a new checkpoint message
func newCheckpointMessage(seqNum string) *actionMessage {
	return &actionMessage{
		Action:     kclActionTypeCheckpoint,
		Checkpoint: seqNum,
	}
}

// newStatusMessage is used to create a new status message
func newStatusMessage(actionType kclActionType) *actionMessage {
	return &actionMessage{
		Action:      KclActionTypeStatus,
		ResponseFor: actionType,
	}
}
