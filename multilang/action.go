package multilang

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/rewardStyle/kinetic/message"
)

// ActionType is used as an enum for KCL Multilang protocol action message types
type ActionType string

// These are the enumerated KCL Multilang protocol action message types
const (
	Initialize ActionType = "initialize"
	ProcessRecords ActionType = "processRecords"
	Checkpoint ActionType = "checkpoint"
	Shutdown ActionType = "shutdown"
	Status ActionType = "status"
)

// ActionMessage is a struct used to marshal / unmarshal KCL Multilang protocol action messages
type ActionMessage struct {
	Action      ActionType `json:"action"`
	ShardID     string `json:"shardId,omitempty"`
	Records     []Record `json:"records,omitempty"`
	Checkpoint  string `json:"checkpoint,omitempty"`
	Error       string `json:"error,omitempty"`
	Reason      string `json:"reason,omitempty"`
	ResponseFor ActionType `json:"responseFor,omitempty"`
}

// Record is a struct used to marshal / unmarshal kinesis records from KCL Multilang protocol
type Record struct {
	ApproximateArrivalTimestamp time.Time `json:"approximateArrivalTimestamp,omitempty"`
	Data                        []byte `json:"data,omitempty"`
	PartitionKey                string `json:"partitionKey,omitempty"`
	SequenceNumber              string `json:"sequenceNumber,omitempty"`
}

// UnmarshalJSON is used as a custom unmarshaller to unmarshal the KCL Multilang ActionMessage
func (a *ActionMessage) UnmarshalJSON(data []byte) error {
	am := &ActionMessage{}
	if err := json.Unmarshal(data, am); err != nil {
		return err
	}

	a.Action = am.Action
	a.ShardID = am.ShardID
	a.Records = am.Records
	if len(am.Checkpoint) > 2 {
		a.Checkpoint = am.Checkpoint[1:len(am.Checkpoint)-1]
	}

	if len(am.Error) > 2 {
		a.Error = am.Error[1:len(am.Error)-1]
	}

	if len(am.Reason) > 2 {
		a.Reason = am.Reason[1:len(am.Reason)-1]
	}

	return nil
}

// UnmarshalJSON is used as a custom unmarshaller to base64 decode the data field of the KCL Multilang
// processRecord message
func (r *Record) UnmarshalJSON(data []byte) error {
	record := &Record{}
	if err := json.Unmarshal(data, record); err != nil {
		return err
	}

	r.ApproximateArrivalTimestamp = record.ApproximateArrivalTimestamp

	if len(record.Data) > 2 {
		encodedString := string(record.Data[1:len(record.Data)-1])
		decodedMsg, err := base64.StdEncoding.DecodeString(encodedString)
		if err != nil {
			return err
		}
		r.Data = decodedMsg
	}

	if len(record.PartitionKey) > 2 {
		r.PartitionKey = record.PartitionKey[1:len(record.PartitionKey)-1]
	}

	if len(record.SequenceNumber) > 2 {
		r.SequenceNumber = record.SequenceNumber[1:len(record.SequenceNumber)-1]
	}

	return nil
}

// ToMessage is used to transform a multilang.Record struct into a message.Message struct
func (r *Record) ToMessage() *message.Message {
	return &message.Message{
		ApproximateArrivalTimestamp: &r.ApproximateArrivalTimestamp,
		Data: r.Data,
		PartitionKey: &r.PartitionKey,
		SequenceNumber: &r.SequenceNumber,
	}
}

// NewCheckpointMessage is used to create a new checkpoint message
func NewCheckpointMessage(seqNum string) *ActionMessage {
	return &ActionMessage{
		Action: Checkpoint,
		Checkpoint: seqNum,
	}
}

// NewStatusMessage is used to create a new status message
func NewStatusMessage(actionType ActionType) *ActionMessage {
	return &ActionMessage{
		Action: Status,
		ResponseFor: actionType,
	}
}