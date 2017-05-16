package multilang

import "github.com/rewardStyle/kinetic/message"

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

// ActionMessage is a struct used to marshal / unmarshall KCL Multilang protocol action messages
type ActionMessage struct {
	Action      ActionType `json:"action"`
	ShardID     string `json:"shardId,omitempty"`
	Records     []message.Message `json:"records,omitempty"`
	Checkpoint  int `json:"checkpoint,omitempty"`
	Error       string `json:"error,omitempty"`
	Reason      string `json:"reason,omitempty"`
	ResponseFor ActionType `json:"responseFor"`
}

// NewCheckpointMessage is used to create a new checkpoint message
func NewCheckpointMessage(seqNum int) *ActionMessage {
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