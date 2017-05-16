package multilang

import "github.com/rewardStyle/kinetic/message"

type ActionType string

const (
	Initialize ActionType = "initialize"
	ProcessRecords ActionType = "processRecords"
	Checkpoint ActionType = "checkpoint"
	Shutdown ActionType = "shutdown"
	Status ActionType = "status"
)

type ActionMessage struct {
	Action      ActionType `json:"action"`
	ShardId     string `json:"shardId,omitempty"`
	Records     []message.Message `json:"records,omitempty"`
	Checkpoint  int `json:"checkpoint,omitempty"`
	Error       string `json:"error,omitempty"`
	Reason      string `json:"reason,omitempty"`
	ResponseFor ActionType `json:"responseFor"`
}

func NewCheckpointMessage(seqNum int) *ActionMessage {
	return &ActionMessage{
		Action: Checkpoint,
		Checkpoint: seqNum,
	}
}

func NewStatusMessage(actionType ActionType) *ActionMessage {
	return &ActionMessage{
		Action: Status,
		ResponseFor: actionType,
	}
}