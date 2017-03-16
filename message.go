package kinetic

import (
	gokinesis "github.com/rewardStyle/go-kinesis"
)

// Message represents an item on the Kinesis stream
type Message struct {
	gokinesis.GetRecordsRecords
}

// Init initializes a Message
func (k *Message) Init(msg []byte, key string) *Message {
	return &Message{
		gokinesis.GetRecordsRecords{
			Data:         msg,
			PartitionKey: key,
		},
	}
}

// SetValue sets the underlying value of the gokinesis record
func (k *Message) SetValue(value []byte) {
	k.GetRecordsRecords.Data = value
}

// Value gets the underlying value of the gokinesis record
func (k *Message) Value() []byte {
	return k.GetData()
}

// Key gets the partion key of the message
func (k *Message) Key() []byte {
	return []byte(k.PartitionKey)
}
